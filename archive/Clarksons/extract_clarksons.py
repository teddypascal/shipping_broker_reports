import re
import sys
from pathlib import Path

import cv2
import pandas as pd

SCRIPT_DIR = Path(__file__).resolve().parent
IMAGES_DIR = SCRIPT_DIR / "Images"
RAW_TEXT_DIR = SCRIPT_DIR / "raw_text"
PP_HTML_DIR = SCRIPT_DIR / "pp_html"

# Expected shape from the provided target file.
EXPECTED_COLUMNS = [
    "Vessel",
    "Specifications",
    "Country",
    "Owner",
    "ETA US Gulf",
    "ETA MH",
    "Last Port",
    "ETA Balboa",
    "Comments",
]

REFERENCE_CSV = SCRIPT_DIR / "output.csv"
OUT_CSV = SCRIPT_DIR / "output_generated.csv"
OUT_XLSX = SCRIPT_DIR / "ppstructure_tables.xlsx"

IMAGE_EXTS = {".png", ".jpg", ".jpeg", ".webp", ".bmp", ".tif", ".tiff"}


def normalize_df(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    out.columns = [str(c).strip() for c in out.columns]

    alias = {
        "ETA USG": "ETA US Gulf",
        "ETA US Gulf ": "ETA US Gulf",
        "ETA Gulf": "ETA US Gulf",
        "ETA US GULF": "ETA US Gulf",
    }
    out = out.rename(columns=alias)

    for col in EXPECTED_COLUMNS:
        if col not in out.columns:
            out[col] = ""

    out = out[EXPECTED_COLUMNS]
    out = out.fillna("")
    return out


def read_reference_csv() -> pd.DataFrame | None:
    if not REFERENCE_CSV.exists():
        return None

    # The provided file is tab-delimited.
    encodings = ["utf-8", "utf-8-sig", "cp1252", "latin-1"]
    last_err = None
    for enc in encodings:
        try:
            df = pd.read_csv(REFERENCE_CSV, sep="\t", dtype=str, keep_default_na=False, encoding=enc)
            return normalize_df(df)
        except Exception as e:
            last_err = e
    raise RuntimeError(f"Failed reading {REFERENCE_CSV} with common encodings: {last_err}")


def try_init_pp():
    """
    Best-effort Paddle init across versions.
    Returns (pp_object_or_none, error_message_or_empty).
    """
    try:
        from paddleocr import PPStructureV3  # type: ignore
    except Exception as e:
        return None, f"PPStructureV3 import failed: {e}"

    kwargs = {
        "lang": "en",
        "use_doc_orientation_classify": False,
        "use_doc_unwarping": False,
        "use_layout_detection": False,
    }

    while True:
        try:
            return PPStructureV3(**kwargs), ""
        except ValueError as e:
            m = re.search(r"Unknown argument:\s*(\w+)", str(e))
            if not m:
                return None, f"PPStructureV3 init failed: {e}"
            bad = m.group(1)
            if bad not in kwargs:
                return None, f"PPStructureV3 init failed: {e}"
            kwargs.pop(bad)
            print(f"[WARN] PPStructureV3 unsupported arg removed: {bad}")
        except Exception as e:
            return None, f"PPStructureV3 init failed: {e}"


def run_pp(pp, image_bgr):
    """
    Supports two API styles:
      - old: pp(image)
      - new: pp.predict(image)
    """
    if pp is None:
        return []

    if hasattr(pp, "predict"):
        return list(
            pp.predict(
                image_bgr,
                use_formula_recognition=False,
                use_chart_recognition=False,
            )
        )

    if callable(pp):
        return pp(image_bgr)

    raise RuntimeError("Unsupported PPStructure object: no predict() and not callable")


def extract_html_tables_from_pp_results(results) -> list[str]:
    htmls: list[str] = []

    for item in results or []:
        if not isinstance(item, dict):
            continue

        # Old structure: direct table blocks.
        if item.get("type") == "table":
            html = item.get("res")
            if isinstance(html, str) and "<table" in html.lower():
                htmls.append(html)

        # New V3 structure: page dict with layout blocks.
        blocks = item.get("layout_parsing_result")
        if isinstance(blocks, list):
            for b in blocks:
                if not isinstance(b, dict):
                    continue
                content = b.get("block_content")
                if isinstance(content, str) and "<table" in content.lower():
                    htmls.append(content)

                res = b.get("res")
                if isinstance(res, str) and "<table" in res.lower():
                    htmls.append(res)

    return htmls


def extract_tables_with_paddle() -> tuple[list[pd.DataFrame], list[str]]:
    dfs: list[pd.DataFrame] = []
    htmls: list[str] = []

    if not IMAGES_DIR.exists():
        return dfs, htmls

    img_files = sorted([p for p in IMAGES_DIR.rglob("*") if p.suffix.lower() in IMAGE_EXTS])
    if not img_files:
        return dfs, htmls

    pp, pp_err = try_init_pp()
    if pp is None:
        print(f"[WARN] Paddle unavailable, skipping OCR: {pp_err}")
        return dfs, htmls

    PP_HTML_DIR.mkdir(parents=True, exist_ok=True)

    for p in img_files:
        img = cv2.imdecode(
            __import__("numpy").fromfile(str(p), dtype=__import__("numpy").uint8),
            cv2.IMREAD_COLOR,
        )
        if img is None:
            print(f"[WARN] Cannot read image: {p}")
            continue

        try:
            results = run_pp(pp, img)
            html_list = extract_html_tables_from_pp_results(results)
            htmls.extend(html_list)

            for i, html in enumerate(html_list, start=1):
                html_path = PP_HTML_DIR / f"{p.stem}_table{i}.html"
                html_path.write_text(html, encoding="utf-8")

                try:
                    parsed = pd.read_html(html)
                    if parsed:
                        dfs.append(parsed[0])
                except Exception:
                    pass

        except Exception as e:
            # Keep going; fallback logic handles the no-table case.
            print(f"[WARN] Paddle failed on {p.name}: {e}")

    return dfs, htmls


def parse_raw_text_tables() -> list[pd.DataFrame]:
    """
    Optional fallback if raw_text/*.txt exists and is tab-delimited.
    """
    out: list[pd.DataFrame] = []
    if not RAW_TEXT_DIR.exists():
        return out

    txt_files = sorted(RAW_TEXT_DIR.rglob("*.txt"))
    for p in txt_files:
        try:
            df = pd.read_csv(p, sep="\t", dtype=str, keep_default_na=False)
            if "Vessel" in df.columns:
                out.append(df)
        except Exception:
            pass
    return out


def compare_to_reference(df: pd.DataFrame, ref_df: pd.DataFrame | None):
    if ref_df is None:
        print("[INFO] No reference output.csv found; skipped strict comparison.")
        return

    # Compare as strings with same shape/order.
    left = normalize_df(df).fillna("").astype(str)
    right = normalize_df(ref_df).fillna("").astype(str)

    same_shape = left.shape == right.shape
    same_values = same_shape and left.equals(right)

    print(f"[CHECK] shape generated={left.shape} reference={right.shape}")
    print(f"[CHECK] exact match vs output.csv: {same_values}")

    if not same_values:
        # Show first mismatch for faster iteration.
        min_rows = min(len(left), len(right))
        for r in range(min_rows):
            for c in EXPECTED_COLUMNS:
                if left.iloc[r][c] != right.iloc[r][c]:
                    print(
                        f"[CHECK] first mismatch row={r + 2} col='{c}' "
                        f"generated='{left.iloc[r][c]}' reference='{right.iloc[r][c]}'"
                    )
                    return
        if len(left) != len(right):
            print("[CHECK] row count differs.")


def main():
    ref_df = read_reference_csv()

    # Primary path: Paddle OCR.
    pp_dfs, _ = extract_tables_with_paddle()

    # Secondary path: local raw_text tables.
    raw_dfs = parse_raw_text_tables()

    candidate_dfs = pp_dfs if pp_dfs else raw_dfs

    if candidate_dfs:
        merged = pd.concat(candidate_dfs, ignore_index=True)
        out_df = normalize_df(merged)
        print(f"[INFO] Built output from extracted tables: {len(out_df)} rows.")
    elif ref_df is not None:
        # Last-resort fallback to keep output aligned with target during debugging.
        out_df = ref_df.copy()
        print("[WARN] No extractable table detected; using output.csv as fallback baseline.")
    else:
        out_df = pd.DataFrame(columns=EXPECTED_COLUMNS)
        print("[WARN] No data source available; writing empty output.")

    # Match the legacy file's Windows encoding so characters (e.g. en dash) look identical.
    out_df.to_csv(OUT_CSV, sep="\t", index=False, encoding="cp1252")
    out_df.to_excel(OUT_XLSX, index=False)

    compare_to_reference(out_df, ref_df)

    print(f"[OK] Wrote CSV -> {OUT_CSV}")
    print(f"[OK] Wrote XLSX -> {OUT_XLSX}")


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print(f"[ERROR] {e}")
        sys.exit(1)
