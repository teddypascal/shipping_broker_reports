import os
import re
import sys
import sqlite3
from pathlib import Path
from datetime import datetime

import win32com.client  # pip install pywin32


# ----------------------------
# CONFIG
# ----------------------------
MAILBOX_OR_TOPLEVEL = "Thaddeaus.Low@axpo.com"
BASE_OUTLOOK_PATH = ["Inbox", "Ship Reports"]   # we'll iterate all subfolders under this
SAVE_MSG = True
SAVE_ATTACHMENTS = True
MAX_ITEMS_PER_BROKER = None  # e.g. 200 for testing; None = all


# ----------------------------
# Helpers
# ----------------------------
INVALID_FS_CHARS = r'<>:"/\|?*'

def safe_filename(s: str, max_len: int = 150) -> str:
    s = (s or "").strip()
    s = re.sub(rf"[{re.escape(INVALID_FS_CHARS)}]", "_", s)
    s = re.sub(r"\s+", " ", s).strip()
    if len(s) > max_len:
        s = s[:max_len].rstrip()
    return s or "no_subject"

def ensure_dir(p: Path) -> None:
    p.mkdir(parents=True, exist_ok=True)

def outlook_dt_str(dt) -> str:
    try:
        if isinstance(dt, datetime):
            return dt.strftime("%Y-%m-%d_%H%M%S")
        return str(dt)
    except Exception:
        return "unknown_time"

def init_db(db_path: Path) -> sqlite3.Connection:
    ensure_dir(db_path.parent)
    conn = sqlite3.connect(str(db_path))
    conn.execute("""
        CREATE TABLE IF NOT EXISTS downloaded_emails (
            store_name TEXT NOT NULL,
            entry_id TEXT NOT NULL,
            internet_message_id TEXT,
            received_time TEXT,
            subject TEXT,
            broker_folder TEXT,
            saved_path TEXT,
            created_at TEXT,
            PRIMARY KEY (store_name, entry_id)
        )
    """)
    conn.commit()
    return conn

def already_downloaded(conn: sqlite3.Connection, store_name: str, entry_id: str) -> bool:
    row = conn.execute(
        "SELECT 1 FROM downloaded_emails WHERE store_name = ? AND entry_id = ?",
        (store_name, entry_id)
    ).fetchone()
    return row is not None

def mark_downloaded(conn: sqlite3.Connection, store_name: str, entry_id: str,
                    internet_message_id: str | None, received_time: str,
                    subject: str, broker_folder: str, saved_path: str) -> None:
    conn.execute("""
        INSERT OR REPLACE INTO downloaded_emails
        (store_name, entry_id, internet_message_id, received_time, subject, broker_folder, saved_path, created_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
    """, (
        store_name, entry_id, internet_message_id, received_time, subject, broker_folder, saved_path,
        datetime.now().isoformat(timespec="seconds")
    ))
    conn.commit()

def get_internet_message_id(mail_item):
    try:
        return getattr(mail_item, "InternetMessageID", None)
    except Exception:
        return None


# ----------------------------
# Outlook navigation (robust)
# ----------------------------
def get_namespace():
    outlook = win32com.client.Dispatch("Outlook.Application")
    return outlook.GetNamespace("MAPI")

def get_store_root(ns, store_name: str):
    want = store_name.strip().lower()
    for i in range(1, ns.Folders.Count + 1):
        f = ns.Folders.Item(i)
        if f.Name.strip().lower() == want:
            return f
    available = [ns.Folders.Item(i).Name for i in range(1, ns.Folders.Count + 1)]
    raise RuntimeError(f"Store '{store_name}' not found. Available stores: {available}")

def get_subfolder_ci(parent, child_name: str):
    want = child_name.strip().lower()
    for i in range(1, parent.Folders.Count + 1):
        f = parent.Folders.Item(i)
        if f.Name.strip().lower() == want:
            return f
    available = [parent.Folders.Item(i).Name for i in range(1, parent.Folders.Count + 1)]
    raise RuntimeError(
        f"Folder '{child_name}' not found under '{parent.Name}'. Available: {available[:50]}"
    )

def get_folder_chain(root, chain):
    f = root
    for name in chain:
        f = get_subfolder_ci(f, name)
    return f


# ----------------------------
# Download logic
# ----------------------------
def download_folder_mails(conn, store_name: str, broker_folder, broker_name: str, out_dir: Path):
    """
    broker_folder: Outlook MAPIFolder (e.g. Affinity)
    out_dir: local path like <script_dir>/Affinity/Emails
    """
    ensure_dir(out_dir)

    items = broker_folder.Items
    try:
        items.Sort("[ReceivedTime]", True)  # newest first
    except Exception:
        pass

    seen = new = skipped = 0

    for item in items:
        # Only MailItem (Class 43)
        try:
            if getattr(item, "Class", None) != 43:
                continue
            entry_id = item.EntryID
        except Exception:
            continue

        seen += 1
        if MAX_ITEMS_PER_BROKER and seen > MAX_ITEMS_PER_BROKER:
            break

        if already_downloaded(conn, store_name, entry_id):
            skipped += 1
            continue

        subject_raw = getattr(item, "Subject", "")
        subject = safe_filename(subject_raw)
        received_str = outlook_dt_str(getattr(item, "ReceivedTime", None))

        # One folder per email (keeps attachments tidy)
        email_dir = out_dir / f"{received_str}__{subject}"
        ensure_dir(email_dir)

        # Save .msg
        if SAVE_MSG:
            msg_path = email_dir / f"{received_str}__{subject}.msg"
            try:
                item.SaveAs(str(msg_path), 3)  # 3 = olMSG
            except Exception as e:
                print(f"[WARN] MSG save failed ({broker_name}): {subject_raw} -> {e}")

        # Save attachments
        if SAVE_ATTACHMENTS:
            try:
                atts = item.Attachments
                for i in range(1, atts.Count + 1):
                    att = atts.Item(i)
                    att_name = safe_filename(att.FileName, max_len=180)
                    out_path = email_dir / att_name

                    # handle duplicate names
                    if out_path.exists():
                        stem, suf = out_path.stem, out_path.suffix
                        k = 2
                        while True:
                            cand = email_dir / f"{stem}__{k}{suf}"
                            if not cand.exists():
                                out_path = cand
                                break
                            k += 1

                    att.SaveAsFile(str(out_path))
            except Exception as e:
                print(f"[WARN] Attachment save failed ({broker_name}): {subject_raw} -> {e}")

        mark_downloaded(
            conn=conn,
            store_name=store_name,
            entry_id=entry_id,
            internet_message_id=get_internet_message_id(item),
            received_time=received_str,
            subject=subject_raw,
            broker_folder=broker_name,
            saved_path=str(email_dir),
        )

        new += 1
        print(f"[OK] {broker_name}: {received_str} | {subject_raw}")

    return seen, new, skipped


def main():
    # Local base is the folder the script lives in
    script_dir = Path(__file__).resolve().parent

    # One shared index for all brokers
    db_path = script_dir / "_download_index.sqlite"
    conn = init_db(db_path)

    ns = get_namespace()
    store_root = get_store_root(ns, MAILBOX_OR_TOPLEVEL)

    ship_reports = get_folder_chain(store_root, BASE_OUTLOOK_PATH)

    # Iterate broker folders under Ship Reports
    broker_count = ship_reports.Folders.Count
    if broker_count == 0:
        raise RuntimeError(f"No subfolders found under: {ship_reports.FolderPath}")

    totals = {"seen": 0, "new": 0, "skipped": 0, "brokers": 0}

    for i in range(1, broker_count + 1):
        broker_folder = ship_reports.Folders.Item(i)
        broker_name = broker_folder.Name.strip()

        # Local: <script_dir>/<Broker>/Emails
        broker_local_dir = script_dir / safe_filename(broker_name, max_len=80) / "Emails"

        print(f"\n=== Processing broker folder: {broker_name} ===")
        seen, new, skipped = download_folder_mails(
            conn=conn,
            store_name=MAILBOX_OR_TOPLEVEL,
            broker_folder=broker_folder,
            broker_name=broker_name,
            out_dir=broker_local_dir
        )

        totals["seen"] += seen
        totals["new"] += new
        totals["skipped"] += skipped
        totals["brokers"] += 1

    conn.close()

    print("\n--- ALL DONE ---")
    print(f"Brokers processed: {totals['brokers']}")
    print(f"Seen:    {totals['seen']}")
    print(f"New:     {totals['new']}")
    print(f"Skipped: {totals['skipped']}")
    print(f"Local base: {Path(__file__).resolve().parent}")


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print(f"[ERROR] {e}")
        sys.exit(1)