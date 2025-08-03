import pandas as pd
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent.parent.parent
CLEANED_DATA_DIR = BASE_DIR / "data" / "cleaned"
COMBINED_PATH = CLEANED_DATA_DIR / "combined_publication.xlsx"
OUTPUT_PATH = CLEANED_DATA_DIR / "sort_publication.xlsx"

def sort_nip_kosong():
    if not COMBINED_PATH.exists():
        raise FileNotFoundError(f"File '{COMBINED_PATH}' not found.")

    df = pd.read_excel(COMBINED_PATH, dtype=str)

    if "nip" not in df.columns:
        raise ValueError("'nip' column not found.")

    df_nip_kosong = df[df["nip"].isna() | (df["nip"].str.strip() == "")]

    df_nip_kosong.to_excel(OUTPUT_PATH, index=False)
    print(f"Sorted file saved to: {OUTPUT_PATH}")

if __name__ == "__main__":
    try:
        sort_nip_kosong()
    except Exception as e:
        print(f"Error: {e}")