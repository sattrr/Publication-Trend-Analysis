import os
import re
import pandas as pd
from pathlib import Path
from rapidfuzz import fuzz

BASE_DIR = Path(__file__).resolve().parent.parent.parent
RAW_DATA_DIR = BASE_DIR / "data" / "raw"
DATA_PATH = RAW_DATA_DIR / "scopus.xlsx"
CLEANED_DATA_DIR = BASE_DIR / "data" / "cleaned"
CLEANED_DATA_DIR.mkdir(parents=True, exist_ok=True)

MAPPING_PATH = CLEANED_DATA_DIR / "nip_scopus_id_cleaned.xlsx"

def normalize_name(name):
    name = str(name).lower().strip()
    name = re.sub(r"[^\w\s]", "", name)
    name = re.sub(r"\s+", " ", name)

    tokens = name.split()
    if len(tokens) == 2 and tokens[0] == tokens[1]:
        name = tokens[0]
    return name


def split_and_clean_authors(authors_str):
    if pd.isna(authors_str):
        return []
    authors_str = str(authors_str)
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
    ids_str = str(ids_str)
    return [aid.strip() for aid in ids_str.split(";") if aid.strip()]


def clean_string_column(col):
    return col.apply(lambda x: str(x).strip().lower() if pd.notna(x) else pd.NA)


def fuzzy_match_name(name, candidate_map):
    name_norm = normalize_name(name)
    best_match = None
    best_score = 0
    for candidate_name, nip in candidate_map:
        candidate_norm = normalize_name(candidate_name)
        score = fuzz.token_sort_ratio(name_norm, candidate_norm)
        if score > best_score:
            best_match = nip
            best_score = score
    return best_match if best_score >= 80 else pd.NA


def pad_or_truncate(lst, length):
    if not isinstance(lst, list):
        lst = []
    if len(lst) < length:
        return lst + [pd.NA] * (length - len(lst))
    return lst[:length]

def overwrite_author_name(df, df_map):
    """Timpa kolom author_name dengan nama standar (lowercase)."""
    nip_to_name = {}

    for _, row in df_map.dropna(subset=["nip", "nm"]).iterrows():
        nip_to_name[row["nip"]] = str(row["nm"]).strip().lower()

    for nip, group in df.groupby("nip"):
        if pd.isna(nip) or nip in nip_to_name:
            continue
        name_counts = group["author_name"].value_counts()
        best_name = max(name_counts.index, key=lambda x: (len(str(x)), name_counts[x]))
        nip_to_name[nip] = str(best_name).strip().lower()

    df["author_name"] = df["nip"].map(nip_to_name).fillna(df["author_name"].str.lower())
    return df

def load_and_clean_data():
    if not DATA_PATH.exists():
        raise FileNotFoundError(f"File '{DATA_PATH}' not found.")

    df = pd.read_excel(DATA_PATH)
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

    df_map = pd.read_excel(MAPPING_PATH, dtype={"nip": str, "id_scopus": str})
    df_map["nm_norm"] = df_map["nm"].astype(str).apply(normalize_name)

    id_scopus_to_nip = df_map.dropna(subset=["id_scopus", "nip"]).set_index("id_scopus")["nip"].to_dict()

    df["author_id"] = df["author_id"].astype(str).replace(["nan", "none", ""], pd.NA)
    df["nip"] = df["author_id"].map(id_scopus_to_nip)
    df["tahun"] = df["tahun"].astype(str)

    df["author_name_norm"] = df["author_name"].apply(normalize_name)
    candidate_map = list(df_map[["nm_norm", "nip"]].dropna().itertuples(index=False, name=None))

    missing_nip_mask = df["nip"].isna()
    df.loc[missing_nip_mask, "nip"] = df.loc[missing_nip_mask, "author_name_norm"].apply(
        lambda name: fuzzy_match_name(name, candidate_map)
    )

    known_nip_map = df.loc[df["nip"].notna(), ["author_name_norm", "nip"]].drop_duplicates().values.tolist()
    still_missing_mask = df["nip"].isna()
    df.loc[still_missing_mask, "nip"] = df.loc[still_missing_mask, "author_name_norm"].apply(
        lambda name: fuzzy_match_name(name, known_nip_map)
    )

    df.drop(columns=["author_name_norm"], inplace=True, errors="ignore")

    df = overwrite_author_name(df, df_map)

    return df

if __name__ == "__main__":
    try:
        df_cleaned = load_and_clean_data()

        df_cleaned["nip"] = df_cleaned["nip"].apply(lambda x: str(x) if pd.notna(x) else pd.NA)
        df_cleaned["author_id"] = df_cleaned["author_id"].apply(lambda x: str(x) if pd.notna(x) else pd.NA)

        output_path = CLEANED_DATA_DIR / "scopus_cleaned.xlsx"
        df_cleaned.to_excel(output_path, index=False)
        print(f"Cleaned data saved to: {output_path}")
    except Exception as e:
        print(f"Error: {e}")