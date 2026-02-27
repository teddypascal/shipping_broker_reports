import re
import sys
import calendar
from pathlib import Path
from datetime import datetime, timedelta

import pandas as pd
import extract_msg

try:
    from zoneinfo import ZoneInfo  # py3.9+
except Exception:
    ZoneInfo = None


# ----------------------------
# Config
# ----------------------------
SCRIPT_DIR = Path(__file__).resolve().parent
EMAILS_DIR = SCRIPT_DIR / "Emails"
OUT_XLSX = SCRIPT_DIR / "Gibson_WEST_POSITION_LIST_All_Emails.xlsx"

BROKER_NAME = "Gibson"
LOCAL_TZ = "Asia/Singapore"

# Expected columns in Gibson table
COLS = [
    "Vessel",
    "BLT",
    "CBM",
    "OWNER",
    "SCHEDULE/ITINERARY",
    "ETA USG",
    "ETA MARCUS HOOK",
    "ETA WAF",
    "COMMENTS",
]


# ----------------------------
# Helpers
# ----------------------------
def normalize_text(s: str) -> str:
    s = (s or "").replace("\r\n", "\n").replace("\r", "\n")
    s = s.replace("\u00a0", " ")
    return s

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


def smart_split(line: str) -> list[str]:
    """
    Split a table row: prefer tabs; else split on 2+ spaces.
    """
    line = line.strip()
    if "\t" in line:
        parts = [p.strip() for p in re.split(r"\t+", line) if p.strip() != ""]
    else:
        parts = [p.strip() for p in re.split(r"\s{2,}", line) if p.strip() != ""]
    return parts

def parse_report_date_from_header_line(line: str) -> datetime | None:
    """
    Example: '26/2/2026    BLT  CBM  OWNER ...'
    """
    m = re.match(r"^\s*(\d{1,2})/(\d{1,2})/(\d{4})\b", line.strip())
    if not m:
        return None
    d, mo, y = map(int, m.groups())
    try:
        return datetime(y, mo, d)
    except Exception:
        return None

def cbm_to_number(cbm_str: str) -> float | None:
    if cbm_str is None:
        return None
    s = str(cbm_str).strip().replace(",", "")
    if s in ("", "-", "*"):
        return None
    try:
        x = float(s)
        # Gibson often uses 83 meaning 83k
        if x < 1000:
            x = x * 1000
        return x
    except Exception:
        return None

# ETA USG like "14-15 MAR", "03-05 APR"
ETA_USG_RE = re.compile(r"^\s*(\d{1,2})\s*-\s*(\d{1,2})\s*([A-Za-z]{3})\s*$", re.IGNORECASE)

def parse_eta_usg_range(eta_usg: str, ref_year: int) -> tuple[datetime | None, datetime | None]:
    if not eta_usg or not isinstance(eta_usg, str):
        return None, None
    m = ETA_USG_RE.match(eta_usg.strip())
    if not m:
        return None, None

    d1 = int(m.group(1))
    d2 = int(m.group(2))
    mon_abbr = m.group(3).title()

    month = list(calendar.month_abbr).index(mon_abbr) if mon_abbr in calendar.month_abbr else None
    if not month:
        return None, None

    try:
        start_dt = datetime(ref_year, month, d1)
        end_dt = datetime(ref_year, month, d2)
        return start_dt, end_dt
    except Exception:
        return None, None


# ----------------------------
# Table extraction
# ----------------------------
def strip_html_tags(html: str) -> str:
    # lightweight HTML -> text (no BeautifulSoup)
    html = html or ""
    html = re.sub(r"(?is)<(script|style).*?>.*?</\1>", " ", html)
    html = re.sub(r"(?is)<br\s*/?>", "\n", html)
    html = re.sub(r"(?is)</p\s*>", "\n", html)
    html = re.sub(r"(?is)<.*?>", " ", html)
    html = re.sub(r"&nbsp;", " ", html)
    html = re.sub(r"&amp;", "&", html)
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


# -------- Gibson WEST POSITION LIST parser (anchored on ETA columns) --------
ETA_USG_PAT = r"(?P<eta_usg>\??\s*\d{1,2}\s*-\s*\d{1,2}\s*[A-Za-z]{3})"
ETA_MH_PAT  = r"(?P<eta_mh>(?:\??\s*\d{1,2}\s*-\s*\d{1,2}\s*[A-Za-z]{3}|-))"
ETA_WAF_PAT = r"(?P<eta_waf>(?:\??\s*\d{1,2}\s*[A-Za-z]{3}\s*-\s*\d{1,2}\s*[A-Za-z]{3}|-))"

ROW_RE = re.compile(
    rf"^\s*(?P<vessel>.+?)\s+"
    rf"(?P<blt>\d{{4}}|\*)\s+"
    rf"(?P<cbm>\d{{1,3}}(?:,\d{{3}})*|\d+|\*)\s+"
    rf"(?P<owner>.+?)\s+"
    rf"(?P<schedule>.+?)\s+"
    rf"{ETA_USG_PAT}\s+{ETA_MH_PAT}\s+{ETA_WAF_PAT}\s+"
    rf"(?P<comments>.*)\s*$",
    flags=re.IGNORECASE
)

def extract_west_position_table(body: str):
    """
    Returns:
        (report_date, list_of_row_dicts)
    """
    t = normalize_text(body)

    # Find 'WEST POSITION LIST' even if not a standalone line
    m = re.search(r"(?i)WEST\s+POSITION\s+LIST", t)
    if not m:
        return None, []

    # Only work in a window after the section title (keeps parser focused)
    window = t[m.end(): m.end() + 12000]
    lines = [ln.strip() for ln in window.split("\n")]

    # Find report date (e.g. 26/2/2026) anywhere on a line
    report_date = None
    header_idx = None
    for idx, ln in enumerate(lines):
        mm = re.search(r"(\d{1,2})/(\d{1,2})/(\d{4})", ln)
        if mm:
            d, mo, y = map(int, mm.groups())
            try:
                report_date = datetime(y, mo, d)
                header_idx = idx
                break
            except Exception:
                pass

    if header_idx is None:
        return None, []

    # Start parsing rows AFTER the header line
    rows = []
    started = False
    miss_streak = 0

    # Build cleaned list of non-empty lines first; Gibson emails commonly
    # render table cells one-per-line in plain text.
    tail = [ln.strip() for ln in lines[header_idx + 1:] if ln.strip()]

    # Skip optional column headers if present as single lines.
    i = 0
    for col in COLS:
        if i < len(tail) and tail[i].upper() == col.upper():
            i += 1
        else:
            break
    # Some Gibson emails omit the "Vessel" label and start with BLT.
    if i == 0 and len(tail) >= 8:
        maybe = [x.upper() for x in tail[:8]]
        expected = [x.upper() for x in COLS[1:]]
        if maybe == expected:
            i = 8

    # Primary parser: 9-line vertical rows (one line per column value).
    j = i
    while j + 8 < len(tail):
        chunk = tail[j : j + 9]
        vessel, blt, cbm, owner, sched, eta_usg, eta_mh, eta_waf, comments = chunk

        # Stop when the next section begins.
        if re.search(r"(?i)\b(BLPG\d|Forward Assessment LPG|Route|Period|Year)\b", vessel):
            break

        if not re.fullmatch(r"\d{4}|\*", blt):
            break
        if not re.fullmatch(r"\d{1,3}(?:,\d{3})*|\d+|\*", cbm):
            break

        rows.append(
            {
                "Vessel": vessel.strip(),
                "BLT": blt.strip(),
                "CBM": cbm.strip(),
                "OWNER": owner.strip(),
                "SCHEDULE/ITINERARY": sched.strip(),
                "ETA USG": eta_usg.upper().replace("?", "").strip(),
                "ETA MARCUS HOOK": eta_mh.upper().replace("?", "").strip(),
                "ETA WAF": eta_waf.upper().replace("?", "").strip(),
                "COMMENTS": comments.strip(),
            }
        )
        j += 9

    if rows:
        return report_date, rows

    # Fallback parser: one-line rows.
    for ln in lines[header_idx + 1:]:
        if not ln:
            if started and rows:
                break
            continue

        rm = ROW_RE.match(ln)
        if not rm:
            if started:
                miss_streak += 1
                if miss_streak >= 6:
                    break
            continue

        started = True
        miss_streak = 0

        row = {
            "Vessel": rm.group("vessel").strip(),
            "BLT": rm.group("blt").strip(),
            "CBM": rm.group("cbm").strip(),
            "OWNER": rm.group("owner").strip(),
            "SCHEDULE/ITINERARY": rm.group("schedule").strip(),
            "ETA USG": rm.group("eta_usg").upper().replace("?", "").replace("  ", " ").strip(),
            "ETA MARCUS HOOK": rm.group("eta_mh").upper().replace("?", "").replace("  ", " ").strip(),
            "ETA WAF": rm.group("eta_waf").upper().replace("?", "").replace("  ", " ").strip(),
            "COMMENTS": (rm.group("comments") or "").strip(),
        }
        rows.append(row)

    return report_date, rows
# ----------------------------
# Main
# ----------------------------
def main():
    if not EMAILS_DIR.exists():
        raise RuntimeError(f"Emails folder not found: {EMAILS_DIR}")

    msg_files = sorted(EMAILS_DIR.rglob("*.msg"))
    if not msg_files:
        raise RuntimeError(f"No .msg files found under: {EMAILS_DIR}")

    all_rows = []
    scanned = 0
    matched = 0

    for msg_path in msg_files:
        scanned += 1
        try:
            body, sent_dt = read_msg_body_and_date(msg_path)
            report_date, rows = extract_west_position_table(body)
            if not rows:
                continue

            matched += 1
            sent_str = to_local_time_str(sent_dt)
            ref_year = (report_date.year if report_date else None) or (
                datetime.strptime(sent_str[:10], "%Y-%m-%d").year if sent_str else datetime.now().year
            )

            for r in rows:
                r["Broker"] = BROKER_NAME
                r["Email Sent Date"] = sent_str
                r["Email File"] = str(msg_path)
                r["Report Date"] = report_date.strftime("%Y-%m-%d") if report_date else ""

                # CBM numeric
                r["CBM_num"] = cbm_to_number(r.get("CBM"))

                # ETA USG split
                s_dt, e_dt = parse_eta_usg_range(r.get("ETA USG", ""), ref_year)
                r["ETA USG Start"] = s_dt
                r["ETA USG End"] = e_dt
                r["ETA USG Midpoint"] = (s_dt + (e_dt - s_dt) / 2) if (s_dt and e_dt) else None

                all_rows.append(r)

        except Exception as e:
            print(f"[WARN] Failed on {msg_path.name}: {e}")

    if not all_rows:
        print(f"No WEST POSITION LIST tables found. Scanned {scanned} emails.")
        return

    df = pd.DataFrame(all_rows)

    # Column order
    ordered = (
        COLS
        + ["CBM_num", "ETA USG Start", "ETA USG End", "ETA USG Midpoint"]
        + ["Report Date", "Broker", "Email Sent Date", "Email File"]
    )
    ordered = [c for c in ordered if c in df.columns]
    rest = [c for c in df.columns if c not in ordered]
    df = df[ordered + rest]

    df.to_excel(OUT_XLSX, index=False)

    print(f"[OK] Scanned {scanned} emails; found tables in {matched}.")
    print(f"[OK] Wrote {len(df)} rows -> {OUT_XLSX}")


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print(f"[ERROR] {e}")
        sys.exit(1)
