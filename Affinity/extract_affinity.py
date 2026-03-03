import re
import sys
import calendar

from pathlib import Path
from datetime import datetime

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
OUT_XLSX = SCRIPT_DIR / "Affinity_USG_All_Emails.xlsx"

LOCAL_TZ = "Asia/Singapore"  # for Email Sent Date display

HEADERS = ["Vessel", "Built", "CBM", "Control", "Position", "ETA USG", "Notes"]


# ----------------------------
# MSG reading
# ----------------------------
def read_msg_body_and_date(msg_path: Path) -> tuple[str, datetime | None]:
    msg = extract_msg.Message(str(msg_path))

    # ✅ compatibility across extract_msg versions
    if hasattr(msg, "process") and callable(getattr(msg, "process")):
        msg.process()

    body = getattr(msg, "body", "") or ""
    sent_dt = getattr(msg, "date", None)  # often datetime, sometimes string/None
    return body, sent_dt

def to_local_time_str(dt: datetime | None) -> str:
    if dt is None:
        return ""
    try:
        if ZoneInfo is not None and dt.tzinfo is not None:
            dt_local = dt.astimezone(ZoneInfo(LOCAL_TZ))
            return dt_local.strftime("%Y-%m-%d %H:%M:%S")
        # fallback (naive or no zoneinfo)
        return dt.strftime("%Y-%m-%d %H:%M:%S")
    except Exception:
        return ""


# ----------------------------
# USG section -> cells
# ----------------------------
def parse_eta_range(eta_str, email_sent_str):
    """
    Parses strings like:
      '14-20 Mar'
      '01-02 Apr'
      '5-6 Mar'

    Returns:
      (start_dt, end_dt)
    """

    if not eta_str or not isinstance(eta_str, str):
        return None, None

    eta_str = eta_str.strip()

    m = re.match(r"(\d{1,2})-(\d{1,2})\s+([A-Za-z]{3})", eta_str)
    if not m:
        return None, None

    day_start = int(m.group(1))
    day_end = int(m.group(2))
    month_abbr = m.group(3)

    # Use email year as reference
    year = None
    if email_sent_str:
        try:
            year = datetime.strptime(email_sent_str[:10], "%Y-%m-%d").year
        except Exception:
            pass

    if year is None:
        year = datetime.now().year

    month = list(calendar.month_abbr).index(month_abbr)

    try:
        start_dt = datetime(year, month, day_start)
        end_dt = datetime(year, month, day_end)
    except Exception:
        return None, None

    return start_dt, end_dt

def normalize_text(s: str) -> str:
    s = (s or "").replace("\r\n", "\n").replace("\r", "\n")
    s = s.replace("\u00a0", " ")
    return s


def extract_usg_cells(body: str) -> list[str] | None:
    """
    In Affinity .msg bodies, each cell is on its own line with blank lines between.
    We:
      - find the 'USG' header line
      - find 'Notes' header line after it
      - take everything after Notes
      - split into non-empty lines ("cells")
    """
    t = normalize_text(body)

    usg_m = re.search(r"(?im)^\s*USG\s*$", t)
    if not usg_m:
        return None

    after = t[usg_m.end():]

    # find the header end at Notes
    notes_m = re.search(r"(?im)^\s*Notes\s*$", after)
    if not notes_m:
        return None

    data = after[notes_m.end():]
    cells = [ln.strip() for ln in data.split("\n") if ln.strip() != ""]
    return cells


# ----------------------------
# Cell-stream parsing -> rows
# ----------------------------
ETA_RE = re.compile(r"^\d{1,2}(?:-\d{1,2})?\s+[A-Za-z]{3}$")  # e.g. 14-20 Mar, 01-02 Apr
BUILT_RE = re.compile(r"^(?:\*|\d{4})$")
CBM_RE = re.compile(r"^(?:\*|\d{1,3}(?:,\d{3})+|\d+)$")


def parse_usg_rows_from_cells(cells: list[str]) -> list[dict]:
    """
    Robust parser for the vertical "cell list" format.

    Assumes:
      Vessel, Built, CBM, Control are the next 4 cells.
      Position may span multiple cells, until an ETA cell is encountered.
      Notes is the cell immediately after ETA (may be missing -> empty).

    Stops naturally when it can’t find valid row starts anymore.
    """
    rows = []
    i = 0
    failures = 0

    while i < len(cells):
        if i + 4 > len(cells):
            break

        vessel = cells[i].strip()
        built = cells[i + 1].strip() if i + 1 < len(cells) else ""
        cbm = cells[i + 2].strip() if i + 2 < len(cells) else ""
        control = cells[i + 3].strip() if i + 3 < len(cells) else ""

        # validate row start
        if not BUILT_RE.match(built) or not CBM_RE.match(cbm):
            i += 1
            failures += 1
            if failures > 80:
                break
            continue

        failures = 0

        # position until ETA
        j = i + 4
        pos_parts = []
        eta = None

        while j < len(cells):
            token = cells[j].strip()
            if ETA_RE.match(token):
                eta = token
                j += 1
                break
            pos_parts.append(token)
            j += 1
            # guard: if position grows too long, this probably isn't the table anymore
            if len(pos_parts) > 12:
                break

        if eta is None:
            i += 1
            continue

        position = " / ".join([p for p in pos_parts if p])

        notes = cells[j].strip() if j < len(cells) else ""
        j += 1

        rows.append({
            "Vessel": vessel,
            "Built": built,
            "CBM": cbm,
            "Control": control,
            "Position": position,
            "ETA USG": eta,
            "Notes": notes,
        })

        i = j

    return rows


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
            cells = extract_usg_cells(body)
            if not cells:
                continue

            rows = parse_usg_rows_from_cells(cells)
            if not rows:
                continue

            matched += 1
            sent_str = to_local_time_str(sent_dt)

            for r in rows:
                r["Broker"] = "Affinity"
                r["Email Sent Date"] = sent_str
                r["Email File"] = str(msg_path)  # provenance/debug
                all_rows.append(r)

        except Exception as e:
            print(f"[WARN] Failed on {msg_path}: {e}")

    if not all_rows:
        print(f"No USG tables found. Scanned {scanned} emails.")
        return

    df = pd.DataFrame(all_rows)

    # clean numeric CBM helper
    df["CBM_clean"] = df["CBM"].astype(str).str.replace(",", "", regex=False).str.strip()
    df["CBM_num"] = pd.to_numeric(df["CBM_clean"], errors="coerce")

    eta_starts = []
    eta_ends = []
    eta_mids = []

    for _, row in df.iterrows():
        start_dt, end_dt = parse_eta_range(row["ETA USG"], row.get("Email Sent Date", ""))

        eta_starts.append(start_dt)
        eta_ends.append(end_dt)

        if start_dt and end_dt:
            mid_dt = start_dt + (end_dt - start_dt) / 2
        else:
            mid_dt = None

        eta_mids.append(mid_dt)

    df["ETA Start"] = eta_starts
    df["ETA End"] = eta_ends
    df["ETA Midpoint"] = eta_mids

        # order columns
    base_cols = HEADERS + ["Email Sent Date", "Email File", "CBM_num"]
    base_cols = [c for c in base_cols if c in df.columns]
    rest = [c for c in df.columns if c not in base_cols]
    df = df[base_cols + rest]

    df.to_excel(OUT_XLSX, index=False)

    print(f"[OK] Scanned {scanned} emails; found USG tables in {matched}.")
    print(f"[OK] Wrote {len(df)} rows -> {OUT_XLSX}")


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print(f"[ERROR] {e}")
        sys.exit(1)