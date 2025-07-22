import os
import pandas as pd
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent.parent.parent
RAW_DATA_DIR = BASE_DIR / "data" / "raw"
DATA_PATH = RAW_DATA_DIR / "scopus.csv"
CLEANED_DATA_DIR = BASE_DIR / "data" / "cleaned"
CLEANED_DATA_DIR.mkdir(parents=True, exist_ok=True)

def split_and_clean_authors(authors_str):
    if pd.isna(authors_str):
        return []
    authors = [author.strip().lower() for author in authors_str.split(";") if author.strip()]
    return authors

def split_and_clean_ids(ids_str):
    if pd.isna(ids_str):
        return []
    ids = [aid.strip() for aid in ids_str.split(";") if aid.strip()]
    return ids

def clean_string_column(col):
    return col.apply(lambda x: str(x).strip().lower() if pd.notna(x) else pd.NA)

def load_and_clean_data():
    if not DATA_PATH.exists():
        raise FileNotFoundError(f"File '{DATA_PATH}' not found.")
    
    df = pd.read_csv(DATA_PATH, encoding="utf-8")

    required_columns = [
        "Authors", "Author(s) ID", "Title", "Source title", 
        "Conference name", "Link", "DOI", "Year", "Sumber data"
    ]
    df = df[[col for col in required_columns if col in df.columns]]

    for col in ["Title", "Source title", "Conference name"]:
        if col in df.columns:
            df[col] = clean_string_column(df[col])

    df["Authors"] = df["Authors"].apply(split_and_clean_authors)
    df["Author(s) ID"] = df["Author(s) ID"].apply(split_and_clean_ids)

    max_len = df[["Authors", "Author(s) ID"]].applymap(len).max(axis=1)
    df["Authors"] = df["Authors"].apply(lambda lst: lst if isinstance(lst, list) else [])
    df["Author(s) ID"] = df["Author(s) ID"].apply(lambda lst: lst if isinstance(lst, list) else [])

    def pad_or_truncate(lst, length):
        return lst + [pd.NA] * (length - len(lst))

    df["Authors"] = [pad_or_truncate(a, l) for a, l in zip(df["Authors"], max_len)]
    df["Author(s) ID"] = [pad_or_truncate(i, l) for i, l in zip(df["Author(s) ID"], max_len)]

    df = df.explode(["Authors", "Author(s) ID"], ignore_index=True)

    df.rename(columns={
        "Authors": "author_name",
        "Author(s) ID": "author_id"
    }, inplace=True)

    df = df.drop_duplicates(subset=["Title", "author_name"], keep="first")

    return df

if __name__ == "__main__":
    try:
        df_cleaned = load_and_clean_data()
        output_path = CLEANED_DATA_DIR / "scopus_cleaned.csv"
        df_cleaned.to_csv(output_path, index=False)
        print(f"Cleaned data saved to: {output_path}")
    except Exception as e:
        print(f"Error: {e}")