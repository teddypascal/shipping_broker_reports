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
EMAILS_DIR = SCRIPT_DIR / "Emails"
OUT_XLSX = SCRIPT_DIR / "Fearnleys_USG_Positions_All_Emails.xlsx"

BROKER = "Fearnleys"
LOCAL_TZ = "Asia/Singapore"

# "Early/Mid/Late" month buckets (feel free to tweak)
RELATIVE_BUCKETS = {
    "early": (1, 10),
    "mid":   (11, 20),
    "late":  (21, 28),  # capped later to month end
    "end":   (25, 31),  # capped later to month end
}

MONTH_MAP = {m.lower(): i for i, m in enumerate(calendar.month_abbr) if m}
# jan->1, feb->2 ...


# ----------------------------
# MSG reading (body + html fallback)
# ----------------------------
def normalize_text(s: str) -> str:
    s = (s or "").replace("\r\n", "\n").replace("\r", "\n")
    s = s.replace("\u00a0", " ")
    # normalize various dashes to '-'
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
# Section extraction: USG Positions
# ----------------------------
def extract_usg_positions_lines(text: str) -> list[str]:
    t = normalize_text(text)

    # Find "USG Positions" header (allow extra spaces)
    m = re.search(r"(?im)^\s*USG\s+Positions\s*$", t)
    if not m:
        # weaker match if formatting is odd
        m = re.search(r"(?i)\bUSG\s+Positions\b", t)
        if not m:
            return []

    after = t[m.end():]
    lines = [ln.strip() for ln in after.split("\n")]

    out = []
    started = False
    blank_streak = 0

    for ln in lines:
        if not ln:
            if started:
                blank_streak += 1
                if blank_streak >= 3:
                    break
            continue

        # stop if we hit a new obvious section header (all caps-ish)
        if started and re.match(r"^[A-Z][A-Z\s/]{6,}$", ln) and "USG" not in ln.upper():
            break

        # we start when we see a line that contains ":" and "/"
        if ":" in ln and "/" in ln:
            started = True
            blank_streak = 0
            out.append(ln)
        else:
            # sometimes lines wrap; if already started, append as continuation
            if started and out:
                out[-1] = out[-1] + " " + ln

    return out


# ----------------------------
# Line parsing
# ----------------------------
LINE_RE = re.compile(
    r"^\s*(?P<eta>[^:]+?)\s*:\s*(?P<owner>[^/]+?)\s*/\s*(?P<vessel>.+?)\s*$",
    flags=re.IGNORECASE
)

PAREN_RE = re.compile(r"\((.*?)\)")

def parse_line(line: str) -> dict | None:
    line = normalize_text(line).strip()
    m = LINE_RE.match(line)
    if not m:
        return None

    eta_raw = m.group("eta").strip()
    owner = m.group("owner").strip()
    rest = m.group("vessel").strip()

    # Extract notes in parentheses (may be multiple)
    notes = "; ".join([x.strip() for x in PAREN_RE.findall(rest) if x.strip()])
    # Remove parentheses content from vessel string
    vessel = re.sub(r"\(.*?\)", "", rest).strip()

    # Clean double spaces
    vessel = re.sub(r"\s{2,}", " ", vessel).strip()

    # Any trailing hyphen fragments, keep in notes
    vessel = vessel.rstrip(" -").strip()

    return {
        "ETA_raw": eta_raw,
        "Owner": owner,
        "Vessel": vessel,
        "Notes": notes,
        "Raw Line": line,
    }


# ----------------------------
# ETA parsing (Start/End/Mid)
# ----------------------------
def month_end_day(year: int, month: int) -> int:
    return calendar.monthrange(year, month)[1]

def infer_year(ref_dt: datetime | None, month: int) -> int:
    """
    Use email sent date year by default. Handle Dec -> Jan rollover.
    """
    if ref_dt is None:
        return datetime.now().year
    y = ref_dt.year
    if ref_dt.month == 12 and month == 1:
        return y + 1
    return y

def parse_eta_dates(eta_raw: str, ref_dt: datetime | None):
    """
    Supports:
      - '14-17 Mar'
      - '17-18 Mar'
      - 'End Mar'
      - 'Early Apr'
      - 'Mid Apr'
      - 'Late Apr'
    Returns (start_dt, end_dt, midpoint_dt)
    """
    if not eta_raw:
        return None, None, None

    s = normalize_text(eta_raw).strip()

    # Explicit range: 14-17 Mar
    m = re.match(r"^(\d{1,2})\s*-\s*(\d{1,2})\s*([A-Za-z]{3})$", s, flags=re.IGNORECASE)
    if m:
        d1, d2, mon = int(m.group(1)), int(m.group(2)), m.group(3).lower()
        if mon not in MONTH_MAP:
            return None, None, None
        month = MONTH_MAP[mon]
        year = infer_year(ref_dt, month)
        try:
            start = datetime(year, month, d1)
            end = datetime(year, month, d2)
            mid = start + (end - start) / 2
            return start, end, mid
        except Exception:
            return None, None, None

    # Single day like "17 Mar" (rare but possible)
    m = re.match(r"^(\d{1,2})\s*([A-Za-z]{3})$", s, flags=re.IGNORECASE)
    if m:
        d, mon = int(m.group(1)), m.group(2).lower()
        if mon not in MONTH_MAP:
            return None, None, None
        month = MONTH_MAP[mon]
        year = infer_year(ref_dt, month)
        try:
            start = datetime(year, month, d)
            return start, start, start
        except Exception:
            return None, None, None

    # Relative buckets: End Mar, Early Apr, Mid Apr, Late Apr
    m = re.match(r"^(Early|Mid|Late|End)\s+([A-Za-z]{3})$", s, flags=re.IGNORECASE)
    if m:
        bucket = m.group(1).lower()
        mon = m.group(2).lower()
        if mon not in MONTH_MAP or bucket not in RELATIVE_BUCKETS:
            return None, None, None
        month = MONTH_MAP[mon]
        year = infer_year(ref_dt, month)

        d1, d2 = RELATIVE_BUCKETS[bucket]
        d2 = min(d2, month_end_day(year, month))

        try:
            start = datetime(year, month, d1)
            end = datetime(year, month, d2)
            mid = start + (end - start) / 2
            return start, end, mid
        except Exception:
            return None, None, None

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

    rows_out = []
    scanned = 0
    matched = 0

    for msg_path in msg_files:
        scanned += 1
        try:
            body, sent_dt = read_msg_body_and_date(msg_path)
            lines = extract_usg_positions_lines(body)
            if not lines:
                continue

            parsed_any = False
            for ln in lines:
                rec = parse_line(ln)
                if not rec:
                    continue

                parsed_any = True

                s_dt, e_dt, m_dt = parse_eta_dates(rec["ETA_raw"], sent_dt if isinstance(sent_dt, datetime) else None)

                rec["ETA Start"] = s_dt
                rec["ETA End"] = e_dt
                rec["ETA Midpoint"] = m_dt

                rec["Broker"] = BROKER
                rec["Email Sent Date"] = to_local_time_str(sent_dt if isinstance(sent_dt, datetime) else None)
                rec["Email File"] = str(msg_path)

                rows_out.append(rec)

            if parsed_any:
                matched += 1

        except Exception as e:
            print(f"[WARN] Failed on {msg_path.name}: {e}")

    if not rows_out:
        print(f"No USG Positions found. Scanned {scanned} emails.")
        return

    df = pd.DataFrame(rows_out)

    # Order columns nicely
    ordered = [
        "Broker",
        "Owner",
        "Vessel",
        "ETA_raw",
        "ETA Start",
        "ETA End",
        "ETA Midpoint",
        "Notes",
        "Email Sent Date",
        "Email File",
        "Raw Line",
    ]
    ordered = [c for c in ordered if c in df.columns]
    rest = [c for c in df.columns if c not in ordered]
    df = df[ordered + rest]

    df.to_excel(OUT_XLSX, index=False)

    print(f"[OK] Scanned {scanned} emails; extracted USG Positions from {matched}.")
    print(f"[OK] Wrote {len(df)} rows -> {OUT_XLSX}")


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print(f"[ERROR] {e}")
        sys.exit(1)