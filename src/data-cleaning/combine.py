import pandas as pd
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent.parent.parent
CLEANED_DATA_DIR = BASE_DIR / "data" / "cleaned"
SCOPUS_PATH = CLEANED_DATA_DIR / "scopus_cleaned.csv"
SISTER_PATH = CLEANED_DATA_DIR / "sister_cleaned.csv"
OUTPUT_PATH = CLEANED_DATA_DIR / "combined_publication.csv"

def load_and_prepare():
    df_scopus = pd.read_csv(SCOPUS_PATH, dtype={"nip": str, "author_id": str})
    df_sister = pd.read_csv(SISTER_PATH, dtype={"nip": str, "id_scopus": str})

    df_scopus = df_scopus.rename(columns={
        "author_name": "nama",
        "author_id": "id_scopus",
        "Title": "judul",
        "Source title": "jenis_publikasi",
        "Conference name": "nama_jurnal",
        "Link": "tautan",
        "DOI": "doi",
        "Year": "tahun",
        "sumber data": "sumber_data"
    })

    df_sister = df_sister.rename(columns={
        "nama_sdm": "nama",
        "sumber data": "sumber_data"
    })

    for df in [df_scopus, df_sister]:
        df["nip"] = df["nip"].astype(str).str.strip()
        df["id_scopus"] = df["id_scopus"].astype(str).str.strip()
        df["nama"] = df["nama"].str.strip().str.lower()

    df_scopus["nama_jurnal"] = df_scopus["nama_jurnal"].fillna(df_scopus["jenis_publikasi"])

    columns = ["nip", "id_scopus", "nama", "judul", "jenis_publikasi", "nama_jurnal", "tautan", "doi", "tahun", "sumber_data"]
    df_scopus = df_scopus[columns]
    df_sister = df_sister[columns]

    return df_sister, df_scopus

def combine_all(df_sister, df_scopus):
    df_combined = pd.concat([df_sister, df_scopus], ignore_index=True)
    df_combined = df_combined.replace({pd.NA: "", None: "", float("nan"): "", "nan": ""})
    df_combined = df_combined.fillna("")
    return df_combined

if __name__ == "__main__":
    df_sister, df_scopus = load_and_prepare()
    df_combined = combine_all(df_sister, df_scopus)
    df_combined.to_csv(OUTPUT_PATH, index=False, na_rep="")
    print(f"Combined publication file saved to: {OUTPUT_PATH}")