import re
import sys
import calendar
from pathlib import Path
from datetime import datetime

import pandas as pd
import extract_msg

try:
    from zoneinfo import ZoneInfo
except Exception:
    ZoneInfo = None


# ----------------------------
# Config
# ----------------------------
SCRIPT_DIR = Path(__file__).resolve().parent

def detect_emails_dir(start: Path) -> Path:
    if start.name.lower() == "emails":
        return start
    for p in start.parents:
        if p.name.lower() == "emails":
            return p
    return start / "Emails"

EMAILS_DIR = detect_emails_dir(SCRIPT_DIR)
OUT_XLSX = EMAILS_DIR.parent / "Poten_West_Positions_All_Emails.xlsx"

BROKER = "Poten"
LOCAL_TZ = "Asia/Singapore"

COLS = ["Vessel", "Size/Built", "ETA USG", "ETA Marcus Hook", "Owner", "Additional Comments"]


# ----------------------------
# MSG read (body + html fallback)
# ----------------------------
def normalize_text(s: str) -> str:
    s = (s or "").replace("\r\n", "\n").replace("\r", "\n")
    s = s.replace("\u00a0", " ")
    s = s.replace("–", "-").replace("—", "-")
    return s

def strip_html_tags(html: str) -> str:
    html = html or ""
    html = re.sub(r"(?is)<(script|style).*?>.*?</\1>", " ", html)
    html = re.sub(r"(?is)<br\s*/?>", "\n", html)
    html = re.sub(r"(?is)</p\s*>", "\n", html)
    html = re.sub(r"(?is)<.*?>", " ", html)
    html = html.replace("&nbsp;", " ").replace("&amp;", "&")
    html = re.sub(r"[ \t]+", " ", html)
    html = re.sub(r"\n{3,}", "\n\n", html)
    return html.strip()

def read_msg_body_and_date(msg_path: Path):
    msg = extract_msg.Message(str(msg_path))
    if hasattr(msg, "process") and callable(getattr(msg, "process")):
        msg.process()

    body = normalize_text(getattr(msg, "body", "") or "").strip()
    if not body:
        html = getattr(msg, "htmlBody", "") or getattr(msg, "htmlbody", "") or ""
        body = normalize_text(strip_html_tags(html))

    sent_dt = getattr(msg, "date", None)
    return body, sent_dt

def to_local_time_str(dt) -> str:
    if not dt:
        return ""
    try:
        if ZoneInfo and getattr(dt, "tzinfo", None) is not None:
            dt = dt.astimezone(ZoneInfo(LOCAL_TZ))
        return dt.strftime("%Y-%m-%d %H:%M:%S")
    except Exception:
        try:
            return str(dt)
        except Exception:
            return ""


# ----------------------------
# Poten table extraction
# ----------------------------
def smart_split(line: str) -> list[str]:
    """
    Poten emails often come as tab-separated, but sometimes spacing.
    Prefer tabs, else split on 2+ spaces.
    """
    line = line.strip()
    if "\t" in line:
        parts = [p.strip() for p in re.split(r"\t+", line) if p.strip()]
    else:
        parts = [p.strip() for p in re.split(r"\s{2,}", line) if p.strip()]
    return parts

def extract_poten_west_rows(body: str) -> list[dict]:
    t = normalize_text(body)

    # Find "West:" block (case-insensitive)
    m = re.search(r"(?im)^\s*West:\s*$", t)
    if not m:
        # weaker match
        m = re.search(r"(?i)\bWest:\b", t)
        if not m:
            return []

    window = t[m.end(): m.end() + 15000]
    lines = [ln.rstrip() for ln in window.split("\n")]

    def is_section_label(s: str) -> bool:
        return bool(re.match(r"^\s*[A-Z][A-Za-z ]{1,30}\s*:\s*$", s.strip()))

    # Find table header start in either:
    # 1) one-line header, or
    # 2) stacked six-line header ("Vessel", "Size/Built", ...)
    header_end_idx = None
    for i, ln in enumerate(lines):
        if (
            re.search(r"(?i)\bVessel\b", ln)
            and re.search(r"(?i)\bSize/Built\b", ln)
            and re.search(r"(?i)\bETA\s+USG\b", ln)
            and re.search(r"(?i)\bETA\s+Marcus\s+Hook\b", ln)
        ):
            header_end_idx = i
            break

        if ln.strip().lower() == "vessel":
            vals = []
            last_idx = i
            for j in range(i, len(lines)):
                s = lines[j].strip()
                if not s:
                    continue
                vals.append(s.lower())
                last_idx = j
                if len(vals) == 6:
                    break
            if vals == [c.lower() for c in COLS]:
                header_end_idx = last_idx
                break

    if header_end_idx is None:
        return []

    rows = []
    data_lines = lines[header_end_idx + 1:]

    # First pass: horizontal rows (tab/space-separated columns on one line)
    started = False
    for ln in data_lines:
        if not ln.strip():
            continue

        if is_section_label(ln):
            break

        parts = smart_split(ln)
        if len(parts) < 5:
            continue

        started = True

        if len(parts) == 5:
            parts = parts + [""]
        elif len(parts) > 6:
            parts = parts[:5] + [" ".join(parts[5:])]

        if len(parts) == 6:
            rows.append(dict(zip(COLS, parts)))

    if rows:
        return rows

    # Second pass: vertical rows (one field per line, 6 lines per record)
    tokens = []
    for ln in data_lines:
        s = ln.strip()
        if not s:
            continue
        if is_section_label(s) or re.match(r"(?i)^regards,?\s*$", s):
            break
        tokens.append(s)

    for i in range(0, len(tokens), 6):
        chunk = tokens[i:i + 6]
        if len(chunk) < 6:
            break
        rows.append(dict(zip(COLS, chunk)))

    return rows


# ----------------------------
# Parsing Size/Built and ETA fields
# ----------------------------
SIZE_BUILT_RE = re.compile(r"^\s*(\d{2,3})\s*/\s*(?:blt|btl)\s*(\d{2,4})\s*$", re.IGNORECASE)

def parse_size_built(s: str):
    """
    '84/blt16' -> size_kcbm=84, built_year=2016
    '83/btl18' -> size_kcbm=83, built_year=2018
    """
    if not s:
        return None, None
    m = SIZE_BUILT_RE.match(s.strip())
    if not m:
        return None, None

    size = int(m.group(1))
    y = m.group(2)
    if len(y) == 2:
        yy = int(y)
        # assume 2000-2029 for yy<=29 else 1900s (unlikely here but safe)
        built_year = 2000 + yy if yy <= 29 else 1900 + yy
    else:
        built_year = int(y)

    return size, built_year

MONTH_FULL = {m.lower(): i for i, m in enumerate(calendar.month_name) if m}
MONTH_ABBR = {m.lower(): i for i, m in enumerate(calendar.month_abbr) if m}

ETA_RANGE_RE = re.compile(r"^\s*(\d{1,2})\s*-\s*(\d{1,2})\s+([A-Za-z]+)\s*$", re.IGNORECASE)

def infer_year(ref_dt: datetime | None, month: int) -> int:
    if ref_dt is None:
        return datetime.now().year
    y = ref_dt.year
    # handle Dec -> Jan rollover
    if ref_dt.month == 12 and month == 1:
        return y + 1
    return y

def parse_eta_range(eta_str: str, ref_dt: datetime | None):
    """
    '16-17 March' / '2-3 April' -> datetime start/end/mid
    """
    if not eta_str or not isinstance(eta_str, str):
        return None, None, None

    s = normalize_text(eta_str).strip()
    m = ETA_RANGE_RE.match(s)
    if not m:
        return None, None, None

    d1 = int(m.group(1))
    d2 = int(m.group(2))
    mon_raw = m.group(3).lower()

    month = MONTH_FULL.get(mon_raw) or MONTH_ABBR.get(mon_raw)
    if not month:
        return None, None, None

    year = infer_year(ref_dt, month)

    try:
        start = datetime(year, month, d1)
        end = datetime(year, month, d2)
        mid = start + (end - start) / 2
        return start, end, mid
    except Exception:
        return None, None, None


# ----------------------------
# Main
# ----------------------------
def main():
    if not EMAILS_DIR.exists():
        raise RuntimeError(f"Emails folder not found: {EMAILS_DIR}")

    msg_files = sorted(EMAILS_DIR.rglob("*.msg"))
    if not msg_files:
        raise RuntimeError(f"No .msg files found under: {EMAILS_DIR}")

    out = []
    scanned = matched = 0

    for msg_path in msg_files:
        scanned += 1
        try:
            body, sent_dt = read_msg_body_and_date(msg_path)
            rows = extract_poten_west_rows(body)
            if not rows:
                continue

            matched += 1
            sent_str = to_local_time_str(sent_dt if isinstance(sent_dt, datetime) else None)
            ref_dt = sent_dt if isinstance(sent_dt, datetime) else None

            for r in rows:
                size_kcbm, built_year = parse_size_built(r.get("Size/Built", ""))

                usg_s, usg_e, usg_m = parse_eta_range(r.get("ETA USG", ""), ref_dt)
                mh_s, mh_e, mh_m = parse_eta_range(r.get("ETA Marcus Hook", ""), ref_dt)

                r["Broker"] = BROKER
                r["Email Sent Date"] = sent_str
                r["Email File"] = str(msg_path)

                r["Size_kcbm"] = size_kcbm
                r["Built_year"] = built_year

                r["ETA USG Start"] = usg_s
                r["ETA USG End"] = usg_e
                r["ETA USG Midpoint"] = usg_m

                r["ETA MH Start"] = mh_s
                r["ETA MH End"] = mh_e
                r["ETA MH Midpoint"] = mh_m

                out.append(r)

        except Exception as e:
            print(f"[WARN] Failed on {msg_path.name}: {e}")

    if not out:
        print(f"No Poten West tables found. Scanned {scanned} emails.")
        return

    df = pd.DataFrame(out)

    ordered = (
        COLS
        + ["Size_kcbm", "Built_year"]
        + ["ETA USG Start", "ETA USG End", "ETA USG Midpoint"]
        + ["ETA MH Start", "ETA MH End", "ETA MH Midpoint"]
        + ["Broker", "Email Sent Date", "Email File"]
    )
    ordered = [c for c in ordered if c in df.columns]
    rest = [c for c in df.columns if c not in ordered]
    df = df[ordered + rest]

    df.to_excel(OUT_XLSX, index=False)

    print(f"[OK] Scanned {scanned} emails; matched {matched} West tables.")
    print(f"[OK] Wrote {len(df)} rows -> {OUT_XLSX}")


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print(f"[ERROR] {e}")
        sys.exit(1)
