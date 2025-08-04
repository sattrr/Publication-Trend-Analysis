import os
import pandas as pd
import re
from pathlib import Path
from rapidfuzz import process

BASE_DIR = Path(__file__).resolve().parent.parent.parent
RAW_DATA_DIR = BASE_DIR / "data" / "raw"
CLEANED_DATA_DIR = BASE_DIR / "data" / "cleaned"
DATA_PATH = RAW_DATA_DIR / "sister.xlsx"
MAPPING_PATH = CLEANED_DATA_DIR / "nip_scopus_id_cleaned.xlsx"
OUTPUT_PATH = CLEANED_DATA_DIR / "sister_cleaned.xlsx"

CLEANED_DATA_DIR.mkdir(parents=True, exist_ok=True)

def clean_authors(author_str):
    if pd.isna(author_str):
        return ""
    authors = [a.strip(" ;").lower() for a in author_str.split(",") if a.strip(" ;")]
    return ", ".join(sorted(set(authors)))

def clean_string_column(col):
    return col.apply(lambda x: str(x).strip().lower() if pd.notna(x) else pd.NA)

def fuzzy_match_name(name, candidate_list, threshold=85):
    match, score, _ = process.extractOne(name, candidate_list)
    return match if score >= threshold else None

def clean_id_scopus(value):
    return re.sub(r"[^0-9]", "", str(value)) if pd.notna(value) else pd.NA

def extract_year_from_date_column(df, date_col="tanggal"):
    if date_col in df.columns:
        df[date_col] = pd.to_datetime(df[date_col], errors="coerce")
        df["tahun"] = df[date_col].dt.year
    return df

def move_column(df, column_name, after_column):
    cols = df.columns.tolist()
    if column_name in cols and after_column in cols:
        cols.remove(column_name)
        insert_at = cols.index(after_column) + 1
        cols.insert(insert_at, column_name)
        return df[cols]
    return df

def load_and_clean_data():
    if not DATA_PATH.exists():
        raise FileNotFoundError(f"File '{DATA_PATH}' not found.")
    if not MAPPING_PATH.exists():
        raise FileNotFoundError(f"Mapping file '{MAPPING_PATH}' not found.")

    df = pd.read_excel(DATA_PATH)
    df.columns = df.columns.str.lower()

    required_columns = ["nip", "nama_sdm", "judul", "jenis_publikasi", "nama_jurnal", "tautan", "doi", "tanggal", "sumber data"]
    df = df[[col for col in required_columns if col in df.columns]]

    df["judul"] = clean_string_column(df["judul"])
    df["jenis_publikasi"] = clean_string_column(df["jenis_publikasi"])
    df["nama_jurnal"] = clean_string_column(df["nama_jurnal"])
    df["nama_sdm"] = df["nama_sdm"].apply(clean_authors)
    df["nip"] = df["nip"].apply(lambda x: str(x).strip() if pd.notna(x) else pd.NA)

    df = extract_year_from_date_column(df, date_col="tanggal")
    df.drop(columns=["tanggal"], inplace=True)
    df = move_column(df, "tahun", after_column="doi")

    df = df.drop_duplicates(subset=["judul"], keep="first")

    df_map = pd.read_excel(MAPPING_PATH, dtype={"nip": str, "id_scopus": str})
    df_map["nm"] = df_map["nm"].astype(str).str.lower().str.strip()
    df_map["nip"] = df_map["nip"].astype(str).str.strip()

    df = df.merge(df_map[["nip", "id_scopus", "nm"]], how="left", on="nip", suffixes=('', '_map'))

    def resolve_id_scopus(row):
        if pd.notna(row["id_scopus"]):
            return row["id_scopus"]
        matched_name = fuzzy_match_name(row["nama_sdm"], df_map["nm"].tolist())
        if matched_name:
            matched_row = df_map[df_map["nm"] == matched_name]
            if not matched_row.empty:
                return matched_row.iloc[0]["id_scopus"]
        return pd.NA

    df["id_scopus"] = df.apply(resolve_id_scopus, axis=1)
    df["id_scopus"] = df["id_scopus"].apply(clean_id_scopus)
    df = df.drop(columns=["nm"])

    return df

if __name__ == "__main__":
    try:
        df_cleaned = load_and_clean_data()
        df_cleaned.to_excel(OUTPUT_PATH, index=False)
        print(f"Cleaned data saved to: {OUTPUT_PATH}")
    except Exception as e:
        print(f"Error: {e}")