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
OUT_XLSX = SCRIPT_DIR / "BRS_FIXTURES_All_Emails.xlsx"

BROKER = "BRS"
LOCAL_TZ = "Asia/Singapore"

MONTH_ABBR = {m.lower(): i for i, m in enumerate(calendar.month_abbr) if m}
MONTH_MAP = {m.lower(): i for i, m in enumerate(calendar.month_abbr) if m}
MONTH_MAP.update({m.lower(): i for i, m in enumerate(calendar.month_name) if m})


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
    """
    Robust .msg reader:
    - tries default extract_msg
    - if decoding fails, retries with overrideEncoding
    - falls back to htmlBody if body empty
    """
    encodings_to_try = [None, "utf-8", "cp1252", "latin1"]

    last_err = None
    for enc in encodings_to_try:
        try:
            if enc is None:
                msg = extract_msg.Message(str(msg_path))
            else:
                # Some extract_msg versions support overrideEncoding
                msg = extract_msg.Message(str(msg_path), overrideEncoding=enc)

            if hasattr(msg, "process") and callable(getattr(msg, "process")):
                msg.process()

            body = normalize_text(getattr(msg, "body", "") or "").strip()
            if not body:
                html = getattr(msg, "htmlBody", "") or getattr(msg, "htmlbody", "") or ""
                body = normalize_text(strip_html_tags(html)).strip()

            sent_dt = getattr(msg, "date", None)
            subject = (getattr(msg, "subject", "") or "").strip()
            return body, sent_dt, subject

        except UnicodeDecodeError as e:
            last_err = e
            continue
        except TypeError as e:
            # overrideEncoding not supported by this extract_msg version
            # fall back to default attempt only
            last_err = e
            break
        except Exception as e:
            last_err = e
            continue

    raise RuntimeError(f"Failed to read MSG due to decoding/parse error: {last_err}")

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

def to_local_date_str(dt: datetime | None) -> str:
    if not dt:
        return ""
    try:
        if ZoneInfo and getattr(dt, "tzinfo", None) is not None:
            dt = dt.astimezone(ZoneInfo(LOCAL_TZ))
        return dt.strftime("%Y-%m-%d")
    except Exception:
        return ""

def parse_subject_report_date(subject: str, fallback_dt: datetime | None = None) -> datetime | None:
    s = normalize_text(subject or "")
    if not s:
        return None

    m = re.search(r"(?<!\d)(\d{1,2})[./-](\d{1,2})[./-](\d{2,4})(?!\d)", s)
    if m:
        d = int(m.group(1))
        mo = int(m.group(2))
        y_raw = m.group(3)
        y = int(y_raw)
        if len(y_raw) == 2:
            y = 2000 + y if y <= 79 else 1900 + y
        try:
            return datetime(y, mo, d)
        except Exception:
            pass

    m = re.search(r"(?<!\d)(\d{1,2})\s+([A-Za-z]{3,9})\.?(?:\s+(\d{4}))?(?!\d)", s)
    if m:
        d = int(m.group(1))
        mon = m.group(2).lower()
        y = int(m.group(3)) if m.group(3) else (fallback_dt.year if fallback_dt else datetime.now().year)
        month = MONTH_MAP.get(mon)
        if month:
            try:
                return datetime(y, month, d)
            except Exception:
                pass

    return None


# ----------------------------
# FIXTURES extraction
# ----------------------------
BULLET_RE = re.compile(r"^\s*[\*\u2022•\-]\s*(.+?)\s*$")

def extract_fixtures_lines(body: str) -> list[str]:
    t = normalize_text(body)

    # Find "FIXTURES" header
    m = re.search(r"(?im)^\s*FIXTURES\s*$", t)
    if not m:
        m = re.search(r"(?i)\bFIXTURES\b", t)
        if not m:
            return []

    window = t[m.end(): m.end() + 8000]
    lines = [ln.rstrip() for ln in window.split("\n")]

    out = []
    started = False
    blank_streak = 0

    for ln in lines:
        if not ln.strip():
            if started:
                blank_streak += 1
                if blank_streak >= 3:
                    break
            continue

        b = BULLET_RE.match(ln)
        if b:
            started = True
            blank_streak = 0
            out.append(b.group(1).strip())
        else:
            # if bullets wrap to next line, append as continuation
            if started and out:
                out[-1] = out[-1] + " " + ln.strip()

    return out


# ----------------------------
# Line parsing
# ----------------------------
# Example:
# 05-06 Mar AG @ $96 : Total / Gas Camelot (Mercuria)
# 23-24 Mar USG @ $154 & $80 HF: Exxon / BW Messina (BP)

LINE_RE = re.compile(
    r"^(?P<laycan>\d{1,2}\s*-\s*\d{1,2}\s*[A-Za-z]{3})\s+"
    r"(?P<route>[A-Za-z]{2,5})\s*@\s*"
    r"(?P<price1>\$\d+(?:\.\d+)?)"
    r"(?:\s*&\s*(?P<price2>\$\d+(?:\.\d+)?)\s*(?P<price2_type>[A-Za-z]{1,6})?)?"
    r"\s*:\s*"
    r"(?P<charterer>[^/]+?)\s*/\s*(?P<vessel>.+?)"
    r"\s*\((?P<owner>[^)]+)\)\s*$",
    flags=re.IGNORECASE
)

def infer_year(sent_dt: datetime | None, month: int) -> int:
    if sent_dt is None:
        return datetime.now().year
    y = sent_dt.year
    if sent_dt.month == 12 and month == 1:
        return y + 1
    return y

def parse_laycan_dates(laycan_str: str, sent_dt: datetime | None):
    m = re.match(r"^\s*(\d{1,2})\s*-\s*(\d{1,2})\s*([A-Za-z]{3})\s*$", laycan_str.strip(), flags=re.IGNORECASE)
    if not m:
        return None, None, None

    d1 = int(m.group(1))
    d2 = int(m.group(2))
    mon = m.group(3).lower()
    if mon not in MONTH_ABBR:
        return None, None, None

    month = MONTH_ABBR[mon]
    year = infer_year(sent_dt, month)

    try:
        start = datetime(year, month, d1)
        end = datetime(year, month, d2)
        mid = start + (end - start) / 2
        return start, end, mid
    except Exception:
        return None, None, None

def parse_fixture_line(line: str, sent_dt: datetime | None):
    line = normalize_text(line).strip()
    m = LINE_RE.match(line)
    if not m:
        return None

    laycan_raw = m.group("laycan").replace("  ", " ").strip()
    route = m.group("route").upper().strip()

    price1 = m.group("price1")
    price2 = m.group("price2")
    price2_type = (m.group("price2_type") or "").upper().strip()

    charterer = m.group("charterer").strip()
    vessel = m.group("vessel").strip()
    owner = m.group("owner").strip()

    lay_s, lay_e, lay_m = parse_laycan_dates(laycan_raw, sent_dt)

    def money_to_float(x):
        if not x:
            return None
        return float(x.replace("$", ""))

    return {
        "Laycan_raw": laycan_raw,
        "Laycan Start": lay_s,
        "Laycan End": lay_e,
        "Laycan Midpoint": lay_m,
        "Route": route,
        "Price1_raw": price1,
        "Price1": money_to_float(price1),
        "Price2_raw": price2,
        "Price2": money_to_float(price2) if price2 else None,
        "Price2 Type": price2_type if price2 else "",
        "Charterer": charterer,
        "Vessel": vessel,
        "Owner": owner,
        "Raw Line": line,
    }


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
            body, sent_dt, subject = read_msg_body_and_date(msg_path)
            lines = extract_fixtures_lines(body)
            if not lines:
                continue

            sent_dt_obj = sent_dt if isinstance(sent_dt, datetime) else None
            report_dt = parse_subject_report_date(subject, sent_dt_obj) or sent_dt_obj
            sent_str = to_local_date_str(report_dt)

            any_parsed = False
            for ln in lines:
                rec = parse_fixture_line(ln, report_dt)
                if not rec:
                    continue
                any_parsed = True

                rec["Broker"] = BROKER
                rec["Email Sent Date"] = sent_str
                rec["Email File"] = str(msg_path)

                out.append(rec)

            if any_parsed:
                matched += 1

        except Exception as e:
            print(f"[WARN] Failed on {msg_path.name}: {e}")

    if not out:
        print(f"No FIXTURES found. Scanned {scanned} emails.")
        return

    df = pd.DataFrame(out)

    ordered = [
        "Broker",
        "Laycan_raw",
        "Laycan Start",
        "Laycan End",
        "Laycan Midpoint",
        "Route",
        "Price1",
        "Price2",
        "Price2 Type",
        "Charterer",
        "Vessel",
        "Owner",
        "Email Sent Date",
        "Email File",
        "Raw Line",
    ]
    ordered = [c for c in ordered if c in df.columns]
    rest = [c for c in df.columns if c not in ordered]
    df = df[ordered + rest]

    df.to_excel(OUT_XLSX, index=False)

    print(f"[OK] Scanned {scanned} emails; parsed FIXTURES from {matched}.")
    print(f"[OK] Wrote {len(df)} rows -> {OUT_XLSX}")


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print(f"[ERROR] {e}")
        sys.exit(1)
