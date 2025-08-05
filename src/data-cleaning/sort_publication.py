import pandas as pd
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent.parent.parent
CLEANED_DATA_DIR = BASE_DIR / "data" / "cleaned"
COMBINED_PATH = CLEANED_DATA_DIR / "combined_publication.xlsx"
OUTPUT_NIP_KOSONG = CLEANED_DATA_DIR / "publication_nip_kosong.xlsx"
OUTPUT_NIP_ADA = CLEANED_DATA_DIR / "publication_nip.xlsx"

def sort_nip_data():
    if not COMBINED_PATH.exists():
        raise FileNotFoundError(f"File '{COMBINED_PATH}' not found.")

    df = pd.read_excel(COMBINED_PATH, dtype=str)

    if "nip" not in df.columns:
        raise ValueError("'nip' column not found.")

    df_nip_kosong = df[df["nip"].isna() | (df["nip"].str.strip() == "")]
    df_nip_ada = df[df["nip"].notna() & (df["nip"].str.strip() != "")]

    df_nip_kosong.to_excel(OUTPUT_NIP_KOSONG, index=False)
    df_nip_ada.to_excel(OUTPUT_NIP_ADA, index=False)

    print(f"Empty NIP saved to: {OUTPUT_NIP_KOSONG}")
    print(f"Non-empty NIP saved to: {OUTPUT_NIP_ADA}")

if __name__ == "__main__":
    try:
        sort_nip_data()
    except Exception as e:
        print(f"Error: {e}")