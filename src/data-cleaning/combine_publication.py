import pandas as pd
from pathlib import Path
from rapidfuzz import fuzz, process

BASE_DIR = Path(__file__).resolve().parent.parent.parent
CLEANED_DATA_DIR = BASE_DIR / "data" / "cleaned"
SCOPUS_PATH = CLEANED_DATA_DIR / "scopus_cleaned.xlsx"
SISTER_PATH = CLEANED_DATA_DIR / "sister_cleaned.xlsx"
OUTPUT_PATH = CLEANED_DATA_DIR / "combined_publication.xlsx"

def load_and_prepare():
    df_scopus = pd.read_excel(SCOPUS_PATH, dtype={"nip": str, "author_id": str})
    df_sister = pd.read_excel(SISTER_PATH, dtype={"nip": str, "id_scopus": str})

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
        df["nip"] = df["nip"].apply(lambda x: str(x).strip() if pd.notna(x) and str(x).strip().lower() not in ["nan", "none"] else pd.NA)
        df["id_scopus"] = df["id_scopus"].apply(lambda x: str(x).strip() if pd.notna(x) else pd.NA)
        df["nama"] = df["nama"].str.strip().str.lower()
        df["judul"] = df["judul"].str.strip().str.lower()

    df_scopus["nama_jurnal"] = df_scopus["nama_jurnal"].fillna(df_scopus["jenis_publikasi"])

    columns = ["nip", "id_scopus", "nama", "judul", "jenis_publikasi", "nama_jurnal", "tautan", "doi", "tahun", "sumber_data"]
    df_scopus = df_scopus[columns]
    df_sister = df_sister[columns]

    return df_sister, df_scopus

def combine_fuzzy(df_sister, df_scopus, threshold=90):
    combined = []
    matched_sister_idx = set()

    sister_titles = df_sister["judul"].tolist()
    sister_title_to_index = dict(zip(sister_titles, df_sister.index))

    for idx_s, row_s in df_scopus.iterrows():
        match = process.extractOne(row_s["judul"], sister_titles, scorer=fuzz.token_sort_ratio)

        if match and match[1] >= threshold:
            matched_title = match[0]
            match_idx = sister_title_to_index[matched_title]

            if match_idx in matched_sister_idx:
                combined.append(row_s.to_dict())
                continue

            row_t = df_sister.loc[match_idx]
            matched_sister_idx.add(match_idx)

            combined_row = row_s.copy()
            combined_row["sumber_data"] = "SISTER, SCOPUS"

            if pd.isna(combined_row["nip"]) or combined_row["nip"].strip().lower() in ["", "nan"]:
                combined_row["nip"] = row_t["nip"]

            combined_row["judul"] = row_s["judul"]

            combined.append(combined_row.to_dict())
        else:
            combined.append(row_s.to_dict())

        if idx_s % 100 == 0:
            print(f"Processed {idx_s}/{len(df_scopus)}")

    unmatched_sister = df_sister.loc[~df_sister.index.isin(matched_sister_idx)]
    combined += unmatched_sister.to_dict(orient="records")

    df_combined = pd.DataFrame(combined).fillna("")
    return df_combined

if __name__ == "__main__":
    df_sister, df_scopus = load_and_prepare()
    df_combined = combine_fuzzy(df_sister, df_scopus)
    df_combined.to_excel(OUTPUT_PATH, index=False)
    print(f"Combined publication saved to: {OUTPUT_PATH}")