import os
import pandas as pd
import re
from pathlib import Path
from rapidfuzz import process

BASE_DIR = Path(__file__).resolve().parent.parent.parent
RAW_DATA_DIR = BASE_DIR / "data" / "raw"
DATA_PATH = RAW_DATA_DIR / "sister.csv"
CLEANED_DATA_DIR = BASE_DIR / "data" / "cleaned"
CLEANED_DATA_DIR.mkdir(parents=True, exist_ok=True)

MAPPING_PATH = CLEANED_DATA_DIR / "idscopus.csv"  # Pastikan file ini ada

def clean_authors(author_str):
    if pd.isna(author_str):
        return ""
    authors = [author.strip(" ;") for author in author_str.split(",") if author.strip(" ;")]
    cleaned = ", ".join(sorted(set(authors)))
    return cleaned.lower().strip()

def clean_string_column(col):
    return col.apply(lambda x: str(x).strip().lower() if pd.notna(x) else pd.NA)

def fuzzy_match_name(name, candidate_list, threshold=85):
    match, score, _ = process.extractOne(name, candidate_list)
    if score >= threshold:
        return match
    return None

def load_and_clean_data():
    if not DATA_PATH.exists():
        raise FileNotFoundError(f"File '{DATA_PATH}' not found.")
    
    df = pd.read_csv(DATA_PATH, encoding="utf-8")

    required_columns = ["nip", "nama_sdm", "judul", "jenis_publikasi", "nama_jurnal", "tautan", "doi", "tahun", "sumber data"]
    df = df[[col for col in required_columns if col in df.columns]]

    for col in ["judul", "jenis_publikasi", "nama_jurnal"]:
        if col in df.columns:
            df[col] = clean_string_column(df[col])

    if "nama_sdm" in df.columns:
        df["nama_sdm"] = df["nama_sdm"].apply(clean_authors)

    df = df.drop_duplicates(subset=["judul"], keep="first")

    if not MAPPING_PATH.exists():
        raise FileNotFoundError(f"Mapping file '{MAPPING_PATH}' not found.")
    df_map = pd.read_csv(MAPPING_PATH, dtype={"nip": str, "id_scopus": str})

    df["matched_name"] = df["nama_sdm"].apply(
        lambda x: fuzzy_match_name(x, df_map["nama"].astype(str).tolist())
    )

    df = df.merge(df_map[["nama", "id_scopus"]], how="left", left_on="matched_name", right_on="nama")

    df = df.drop(columns=["matched_name", "nama"])

    return df

if __name__ == "__main__":
    try:
        df_cleaned = load_and_clean_data()
        output_path = CLEANED_DATA_DIR / "sister_cleaned.csv"
        df_cleaned.to_csv(output_path, index=False)
        print(f"Cleaned data saved to: {output_path}")
    except Exception as e:
        print(f"Error: {e}")
