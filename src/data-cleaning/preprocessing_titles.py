import pandas as pd
import re
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent.parent.parent
RAW_FILE = BASE_DIR / "data" / "cleaned" / "combined_publication.csv"
OUTPUT_FILE = BASE_DIR / "data" / "cleaned" / "titles_cleaned.csv"

def clean_text(text):
    if pd.isna(text):
        return ""
    text = text.strip().lower()
    text = re.sub(r"[^a-zA-Z0-9\s]", " ", text) 
    text = re.sub(r'\s+', ' ', text)
    text = re.sub(r'[^\w\s]', '', text)
    return text

def preprocess_titles():
    df = pd.read_csv(RAW_FILE, dtype=str)
    
    if not {"judul", "tahun"}.issubset(df.columns.str.lower()):
        raise ValueError("The 'judul' column is missing from the input file.")
    
    df.columns = df.columns.str.lower()
    df = df[["judul", "tahun"]].dropna()

    df['judul'] = df['judul'].apply(clean_text)
    df["tahun"] = df["tahun"].astype(str).str.extract(r"(\d{4})")
    
    df = df.drop_duplicates(subset=['judul'])
    
    df[["judul", "tahun"]].to_csv(OUTPUT_FILE, index=False)
    print(f"Cleaned titles saved to: {OUTPUT_FILE}")

if __name__ == "__main__":
    preprocess_titles()