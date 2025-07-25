import os
import re
import numpy as np
import pandas as pd
from pathlib import Path
from rapidfuzz import process

BASE_DIR = Path(__file__).resolve().parent.parent.parent
RAW_DATA_DIR = BASE_DIR / "data" / "raw"
DATA_PATH = RAW_DATA_DIR / "scopus.csv"
CLEANED_DATA_DIR = BASE_DIR / "data" / "cleaned"
CLEANED_DATA_DIR.mkdir(parents=True, exist_ok=True)

MAPPING_PATH = CLEANED_DATA_DIR / "idscopus.csv"

def split_and_clean_authors(authors_str):
    if pd.isna(authors_str):
        return []

    cleaned_authors = []
    for author in authors_str.split(";"):
        author = author.strip().lower()
        author = re.sub(r"\(.*?\)", "", author).strip()

        if "," in author:
            parts = [part.strip() for part in author.split(",")]
            if len(parts) == 2:
                author = f"{parts[1]} {parts[0]}"

        if author:
            cleaned_authors.append(author)
    return cleaned_authors

def split_and_clean_ids(ids_str):
    if pd.isna(ids_str):
        return []
    return [aid.strip() for aid in ids_str.split(";") if aid.strip()]

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

    df.columns = df.columns.str.lower()

    required_columns = [
        "author full names", "author(s) id", "title", "source title", 
        "conference name", "link", "doi", "year", "sumber data"
    ]
    df = df[[col for col in required_columns if col in df.columns]]

    for col in ["title", "source title", "conference name"]:
        df[col] = clean_string_column(df[col])

    df["author full names"] = df["author full names"].apply(split_and_clean_authors)
    df["author(s) id"] = df["author(s) id"].apply(split_and_clean_ids)

    max_len = df[["author full names", "author(s) id"]].applymap(len).max(axis=1)
    df["author full names"] = [a if isinstance(a, list) else [] for a in df["author full names"]]
    df["author(s) id"] = [i if isinstance(i, list) else [] for i in df["author(s) id"]]

    def pad_or_truncate(lst, length):
        return lst + [pd.NA] * (length - len(lst))

    df["author full names"] = [pad_or_truncate(a, l) for a, l in zip(df["author full names"], max_len)]
    df["author(s) id"] = [pad_or_truncate(i, l) for i, l in zip(df["author(s) id"], max_len)]

    df = df.explode(["author full names", "author(s) id"], ignore_index=True)

    df.rename(columns={
        "author full names": "author_name",
        "author(s) id": "author_id",
        "title": "judul",
        "source title": "jenis_publikasi",
        "conference name": "nama_jurnal",
        "link": "tautan",
        "doi": "doi",
        "year": "tahun",
        "sumber data": "sumber_data"
    }, inplace=True)

    df = df.drop_duplicates(subset=["judul", "author_name"], keep="first")

    df_map = pd.read_csv(MAPPING_PATH, dtype={"nip": str, "id_scopus": str})
    df["author_name"] = df["author_name"].astype(str)

    df = df.merge(df_map[["nip", "id_scopus", "nama"]], how="left", left_on="author_id", right_on="id_scopus")

    missing_nip = df["nip"].isna()

    df.loc[missing_nip, "matched_name"] = df.loc[missing_nip, "author_name"].apply(
        lambda x: fuzzy_match_name(x, df_map["nama"].astype(str).tolist())
    )

    df_fuzzy = df_map[["nama", "nip"]].rename(columns={"nama": "matched_name"})
    df = df.merge(df_fuzzy, how="left", on="matched_name", suffixes=("", "_fuzzy"))

    df["nip"] = df["nip"].combine_first(df["nip_fuzzy"])

    df.drop(columns=["matched_name", "nip_fuzzy", "nama", "id_scopus"], inplace=True)

    return df

if __name__ == "__main__":
    try:
        df_cleaned = load_and_clean_data()

        df_cleaned["nip"] = df_cleaned["nip"].apply(
            lambda x: np.format_float_positional(float(x), trim='-') if pd.notnull(x) and x != "" else ""
        )
        df_cleaned["author_id"] = df_cleaned["author_id"].apply(
            lambda x: np.format_float_positional(float(x), trim='-') if pd.notnull(x) and x != "" else ""
        )

        output_path = CLEANED_DATA_DIR / "scopus_cleaned.csv"

        df_cleaned.to_csv(output_path, index=False, na_rep="", float_format="%.0f")
        print(f"Cleaned data saved to: {output_path}")
    except Exception as e:
        print(f"Error: {e}")