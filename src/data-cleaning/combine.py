import pandas as pd
import re
from pathlib import Path
from rapidfuzz import fuzz

BASE_DIR = Path(__file__).resolve().parent.parent.parent
CLEANED_DATA_DIR = BASE_DIR / "data" / "cleaned"

sister_path = CLEANED_DATA_DIR / "sister_cleaned.csv"
scopus_path = CLEANED_DATA_DIR / "scopus_cleaned.csv"
mapping_path = CLEANED_DATA_DIR / "idscopus.csv"

df_sister = pd.read_csv(sister_path)
df_scopus = pd.read_csv(scopus_path)
df_map = pd.read_csv(mapping_path)

def normalize_name(nama):
    nama = nama.lower()
    nama = re.sub(r'[^a-z ]', '', nama)
    nama = re.sub(r'\s+', ' ', nama).strip()
    return nama

def extract_parts(nama):
    nama = normalize_name(nama)
    parts = nama.split()
    if len(parts) == 1:
        return ("", parts[0])
    return (" ".join(parts[:-1]), parts[-1])

def match_by_lastname(nama_input, df_mapping, threshold=85):
    depan_input, belakang_input = extract_parts(nama_input)
    best_match = None
    best_score = 0

    for _, row in df_mapping.iterrows():
        depan_ref, belakang_ref = extract_parts(row['nama'])
        if belakang_input == belakang_ref:
            score = fuzz.partial_ratio(depan_input, depan_ref)
            if score > best_score and score >= threshold:
                best_score = score
                best_match = row['nama']
    return best_match

df_sister["matched_nama"] = df_sister["nama_sdm"].apply(
    lambda x: match_by_lastname(x, df_map)
)
df_sister = df_sister.merge(
    df_map[["nip", "nama", "id_scopus"]],
    how="left",
    left_on="matched_nama",
    right_on="nama"
)

df_sister_formatted = pd.DataFrame({
    "nip": df_sister["nip_x"],
    "id_scopus": df_sister["id_scopus"],
    "nama": df_sister["nama_sdm"],
    "judul": df_sister["judul"],
    "jenis_publikasi": df_sister["jenis_publikasi"],
    "nama_jurnal": df_sister["nama_jurnal"],
    "tahun": df_sister["tahun"],
    "tautan": df_sister["tautan"],
    "doi": df_sister["doi"],
    "sumber_data": df_sister["sumber data"]
})

df_scopus = df_scopus.assign(author_name=df_scopus["author_name"].str.split(";")).explode("author_name")
df_scopus["author_name"] = df_scopus["author_name"].str.strip()

df_scopus["matched_nama"] = df_scopus["author_name"].apply(
    lambda x: match_by_lastname(x, df_map)
)

df_scopus = df_scopus.merge(
    df_map[["nip", "nama", "id_scopus"]],
    how="left",
    left_on="matched_nama",
    right_on="nama"
)

df_scopus["jenis_publikasi"] = df_scopus["Source title"].fillna("jurnal")

df_scopus_formatted = pd.DataFrame({
    "nip": df_scopus["nip"],
    "id_scopus": df_scopus["author_id"],
    "nama": df_scopus["author_name"],
    "judul": df_scopus["Title"],
    "jenis_publikasi": df_scopus["Source title"],
    "nama_jurnal": df_scopus["Conference name"],
    "tahun": df_scopus["Year"],
    "tautan": df_scopus["Link"],
    "doi": df_scopus["DOI"],
    "sumber_data": df_scopus["Sumber data"]
})

if __name__ == "__main__":
    try:
        df_final = pd.concat([df_sister_formatted, df_scopus_formatted], ignore_index=True)
        output_path = CLEANED_DATA_DIR / "gabungan_publikasi.csv"
        df_final.to_csv(output_path, index=False)
        print(f"Combined data saved at: {output_path}")
    except Exception as e:
        print(f"Error: {e}")