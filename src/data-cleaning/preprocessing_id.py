import pandas as pd
import re
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent.parent.parent
RAW_DATA_DIR = BASE_DIR / "data" / "raw"
CLEANED_DATA_DIR = BASE_DIR / "data" / "cleaned"
CLEANED_DATA_DIR.mkdir(parents=True, exist_ok=True)

INPUT_PATH = RAW_DATA_DIR / "nip_scopus_id.xlsx"
OUTPUT_PATH = CLEANED_DATA_DIR / "nip_scopus_id_cleaned.xlsx"

def normalize_name(name):
    name = str(name).strip().lower()
    name = re.sub(r"[^\w\s]", "", name)
    name = re.sub(r"\s+", " ", name)
    tokens = name.split()
    if len(tokens) == 2 and tokens[0] == tokens[1]:
        name = tokens[0]
    return name

def preprocess_nip_scopus_id():
    df = pd.read_excel(INPUT_PATH, dtype={"nip": str, "id_scopus": str})

    if not {"nip", "id_scopus", "nm"}.issubset(df.columns):
        raise ValueError("Kolom 'nip', 'id_scopus', dan 'nm' harus ada di file input")

    df["nip"] = df["nip"].apply(lambda x: str(x).strip() if pd.notna(x) else pd.NA)
    df["id_scopus"] = df["id_scopus"].apply(lambda x: str(x).strip() if pd.notna(x) else pd.NA)
    df["nm"] = df["nm"].apply(lambda x: str(x).strip() if pd.notna(x) else pd.NA)

    df["nm"] = df["nm"].apply(normalize_name)

    df = df.drop_duplicates(subset=["nip", "id_scopus", "nm"], keep="first")

    df.to_excel(OUTPUT_PATH, index=False)
    print(f"Cleaned data saved to: {OUTPUT_PATH}")

if __name__ == "__main__":
    preprocess_nip_scopus_id()