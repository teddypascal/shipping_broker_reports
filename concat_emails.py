from pathlib import Path
import re
import pandas as pd

def safe_sheet_name(name: str) -> str:
    """
    Excel sheet rules:
      - max 31 chars
      - cannot contain: : \ / ? * [ ]
      - cannot be blank
    """
    name = re.sub(r"[:\\/?*\[\]]", "_", name).strip()
    if not name:
        name = "Sheet"
    return name[:31]

def combine_all_emails_excels(
    root_dir: str = ".",
    pattern: str = "*_All_Emails.xlsx",
    output_file: str = "Combined_All_Emails.xlsx"
):
    root = Path(root_dir).resolve()
    output_path = root / output_file

    files = sorted(root.rglob(pattern))
    if not files:
        raise FileNotFoundError(f"No files matching '{pattern}' found under: {root}")

    used_sheet_names = set()

    with pd.ExcelWriter(output_path, engine="openpyxl") as writer:
        for fp in files:
            # Read first sheet of each workbook (common case)
            df = pd.read_excel(fp)

            # Sheet name based on file name (without .xlsx)
            base = safe_sheet_name(fp.stem)

            # Ensure unique sheet names
            sheet = base
            i = 2
            while sheet in used_sheet_names:
                suffix = f"_{i}"
                sheet = safe_sheet_name(base[:31 - len(suffix)] + suffix)
                i += 1
            used_sheet_names.add(sheet)

            df.to_excel(writer, sheet_name=sheet, index=False)

    print(f"✅ Wrote {len(files)} files into: {output_path}")
    print("Sheets:", ", ".join(sorted(used_sheet_names)))

if __name__ == "__main__":
    combine_all_emails_excels(
        root_dir=".",                   # run from the parent folder containing Affinity/BRS/...
        pattern="*_All_Emails.xlsx",
        output_file="All_Brokers_All_Emails_Combined.xlsx"
    )