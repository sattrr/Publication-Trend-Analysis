import pandas as pd
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent.parent.parent
CLEANED_DATA_DIR = BASE_DIR / "data" / "cleaned"
CLEANED_TOPIC_DIR = CLEANED_DATA_DIR / "output"

COMBINED_PATH = CLEANED_DATA_DIR / "combined_publication.xlsx"
TOPIC_PATH = CLEANED_TOPIC_DIR / "topic_assignments.xlsx"
OUTPUT_EMPTY_NIP = CLEANED_DATA_DIR / "empty_nip.xlsx"
OUTPUT_NIP = CLEANED_DATA_DIR / "final_publication.xlsx"
OUTPUT_JOURNALS = CLEANED_DATA_DIR / "journals_list.xlsx"
OUTPUT_TOPIC = CLEANED_DATA_DIR / "topics_list.xlsx"

def sort_nip_data():
    CLEANED_DATA_DIR.mkdir(parents=True, exist_ok=True)
    CLEANED_TOPIC_DIR.mkdir(parents=True, exist_ok=True)

    if not COMBINED_PATH.exists():
        raise FileNotFoundError(f"File '{COMBINED_PATH}' not found.")

    df = pd.read_excel(COMBINED_PATH, dtype=str)

    if "nip" not in df.columns:
        raise ValueError("'nip' column not found.")

    df_nip_kosong = df[df["nip"].isna() | (df["nip"].str.strip() == "")]
    df_nip_ada = df[df["nip"].notna() & (df["nip"].str.strip() != "")]

    df_nip_kosong.to_excel(OUTPUT_EMPTY_NIP, index=False)
    df_nip_ada.to_excel(OUTPUT_NIP, index=False)

    if "nama_jurnal" in df.columns:
        journals_unique = (
            df["nama_jurnal"]
            .dropna()
            .str.strip()
            .drop_duplicates()
            .sort_values()
            .reset_index(drop=True)
        )
        journals_unique.to_frame(name="nama_jurnal").to_excel(OUTPUT_JOURNALS, index=False)
    else:
        print("Kolom 'nama_jurnal' tidak ditemukan, lewati pembuatan daftar jurnal.")

    if TOPIC_PATH.exists():
        df_topic = pd.read_excel(TOPIC_PATH, dtype=str)
        if "topic_name" in df_topic.columns:
            topics_unique = (
                df_topic["topic_name"]
                .dropna()
                .str.strip()
                .drop_duplicates()
                .sort_values()
                .reset_index(drop=True)
            )
            topics_unique.to_frame(name="topic_name").to_excel(OUTPUT_TOPIC, index=False)
        else:
            print("Kolom 'topic_name' tidak ditemukan di topics_assignments.xlsx.")
    else:
        print(f"File '{TOPIC_PATH}' tidak ditemukan, lewati pembuatan daftar topik.")

    print(f"Empty NIP saved to: {OUTPUT_EMPTY_NIP}")
    print(f"Non-empty NIP saved to: {OUTPUT_NIP}")
    print(f"Journals list saved to: {OUTPUT_JOURNALS}")
    print(f"Topics list saved to: {OUTPUT_TOPIC}")

if __name__ == "__main__":
    try:
        sort_nip_data()
    except Exception as e:
        print(f"Error: {e}")