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

    required_columns = [
        "Author full names", "Author(s) ID", "Title", "Source title", 
        "Conference name", "Link", "DOI", "Year", "Sumber data"
    ]
    df = df[[col for col in required_columns if col in df.columns]]

    for col in ["Title", "Source title", "Conference name"]:
        df[col] = clean_string_column(df[col])

    df["Author full names"] = df["Author full names"].apply(split_and_clean_authors)
    df["Author(s) ID"] = df["Author(s) ID"].apply(split_and_clean_ids)

    max_len = df[["Author full names", "Author(s) ID"]].applymap(len).max(axis=1)
    df["Author full names"] = [a if isinstance(a, list) else [] for a in df["Author full names"]]
    df["Author(s) ID"] = [i if isinstance(i, list) else [] for i in df["Author(s) ID"]]

    def pad_or_truncate(lst, length):
        return lst + [pd.NA] * (length - len(lst))

    df["Author full names"] = [pad_or_truncate(a, l) for a, l in zip(df["Author full names"], max_len)]
    df["Author(s) ID"] = [pad_or_truncate(i, l) for i, l in zip(df["Author(s) ID"], max_len)]

    df = df.explode(["Author full names", "Author(s) ID"], ignore_index=True)

    df.rename(columns={
        "Author full names": "author_name",
        "Author(s) ID": "author_id"
    }, inplace=True)

    df = df.drop_duplicates(subset=["Title", "author_name"], keep="first")

    df_map = pd.read_csv(MAPPING_PATH, dtype={"nip": str, "id_scopus": str})
    df["author_name"] = df["author_name"].astype(str)

    df["matched_name"] = df["author_name"].apply(
        lambda x: fuzzy_match_name(x, df_map["nama"].astype(str).tolist())
    )

    df = df.merge(df_map[["nama", "nip"]], how="left", left_on="matched_name", right_on="nama")

    df = df.drop(columns=["matched_name", "nama"])

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