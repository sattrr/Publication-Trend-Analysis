import os
import pandas as pd
import re
from pathlib import Path
from rapidfuzz import process

BASE_DIR = Path(__file__).resolve().parent.parent.parent
RAW_DATA_DIR = BASE_DIR / "data" / "raw"
CLEANED_DATA_DIR = BASE_DIR / "data" / "cleaned"
DATA_PATH = RAW_DATA_DIR / "sister.csv"
MAPPING_PATH = CLEANED_DATA_DIR / "idscopus.csv"
OUTPUT_PATH = CLEANED_DATA_DIR / "sister_cleaned.csv"

CLEANED_DATA_DIR.mkdir(parents=True, exist_ok=True)

def clean_authors(author_str):
    if pd.isna(author_str):
        return ""
    authors = [a.strip(" ;") for a in author_str.split(",") if a.strip(" ;")]
    return ", ".join(sorted(set(authors))).lower().strip()

def clean_string_column(col):
    return col.apply(lambda x: str(x).strip().lower() if pd.notna(x) else "")

def fuzzy_match_name(name, candidate_list, threshold=85):
    match, score, _ = process.extractOne(name, candidate_list)
    return match if score >= threshold else None

def clean_id_scopus(value):
    if pd.isna(value):
        return ""
    return re.sub(r"[^0-9]", "", str(value))

def load_and_clean_data():
    if not DATA_PATH.exists():
        raise FileNotFoundError(f"File '{DATA_PATH}' not found.")
    if not MAPPING_PATH.exists():
        raise FileNotFoundError(f"Mapping file '{MAPPING_PATH}' not found.")

    df = pd.read_csv(DATA_PATH, encoding="utf-8")
    df.columns = df.columns.str.lower()

    required_columns = ["nip", "nama_sdm", "judul", "jenis_publikasi", "nama_jurnal", "tautan", "doi", "tahun", "sumber data"]
    df = df[[col for col in required_columns if col in df.columns]]

    df["judul"] = clean_string_column(df["judul"])
    df["jenis_publikasi"] = clean_string_column(df["jenis_publikasi"])
    df["nama_jurnal"] = clean_string_column(df["nama_jurnal"])
    df["nama_sdm"] = df["nama_sdm"].apply(clean_authors)
    df["nip"] = df["nip"].astype(str).str.strip()

    df = df.drop_duplicates(subset=["judul"], keep="first")

    df_map = pd.read_csv(MAPPING_PATH, dtype={"nip": str, "id_scopus": str})
    df_map["nama"] = df_map["nama"].astype(str).str.lower().str.strip()
    df_map["nip"] = df_map["nip"].astype(str).str.strip()

    df = df.merge(df_map[["nip", "id_scopus", "nama"]], how="left", on="nip", suffixes=('', '_map'))

    def resolve_id_scopus(row):
        if pd.notna(row["id_scopus"]):
            return row["id_scopus"]
        matched_name = fuzzy_match_name(row["nama_sdm"], df_map["nama"].tolist())
        if matched_name:
            matched_row = df_map[df_map["nama"] == matched_name]
            if not matched_row.empty:
                return matched_row.iloc[0]["id_scopus"]
        return ""

    df["id_scopus"] = df.apply(resolve_id_scopus, axis=1)
    df["id_scopus"] = df["id_scopus"].apply(clean_id_scopus)
    df = df.drop(columns=["nama"])

    return df

if __name__ == "__main__":
    try:
        df_cleaned = load_and_clean_data()
        df_cleaned.to_csv(OUTPUT_PATH, index=False)
        print(f"Cleaned data saved to: {OUTPUT_PATH}")
    except Exception as e:
        print(f"Error: {e}")