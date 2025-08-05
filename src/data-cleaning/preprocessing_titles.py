import pandas as pd
import re
import nltk
from pathlib import Path
from tqdm import tqdm
from nltk.corpus import stopwords as nltk_stopwords
from Sastrawi.StopWordRemover.StopWordRemoverFactory import StopWordRemoverFactory

BASE_DIR = Path(__file__).resolve().parent.parent.parent
RAW_FILE = BASE_DIR / "data" / "cleaned" / "combined_publication.xlsx"
OUTPUT_FILE = BASE_DIR / "data" / "cleaned" / "titles_cleaned.xlsx"

nltk.download("stopwords", quiet=True)

stopwords_eng = set(nltk_stopwords.words("english"))
stopwords_id = set(StopWordRemoverFactory().get_stop_words())
stopwords = stopwords_eng.union(stopwords_id)

non_alnum_re = re.compile(r"[^a-zA-Z0-9\s]")
multi_space_re = re.compile(r"\s+")

tqdm.pandas()

def clean_text(text):
    if pd.isna(text):
        return ""

    text = text.strip().lower()
    text = non_alnum_re.sub(" ", text)
    text = multi_space_re.sub(" ", text)
    tokens = text.split()

    filtered_tokens = [t for t in tokens if t not in stopwords and len(t) > 2]
    return " ".join(filtered_tokens)

def preprocess_titles():
    df = pd.read_excel(RAW_FILE, dtype=str)
    df.columns = df.columns.str.lower()

    if not {"judul", "tahun"}.issubset(df.columns):
        raise ValueError("Kolom 'judul' atau 'tahun' tidak ditemukan.")

    df = df[["judul", "tahun"]].dropna(subset=["judul"])
    df["judul"] = df["judul"].astype(str).progress_apply(clean_text)
    df["tahun"] = df["tahun"].astype(str).str.extract(r"(\d{4})")

    df = df.drop_duplicates(subset=["judul"])
    df.to_excel(OUTPUT_FILE, index=False)
    print(f"Cleaned titles saved to: {OUTPUT_FILE}")

if __name__ == "__main__":
    preprocess_titles()