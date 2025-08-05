import pandas as pd

def clean_and_match_data(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df = df.fillna('')
    df['nama'] = df['nama'].str.lower().str.strip()
    df['judul'] = df['judul'].str.strip()

    df = df[df['nip'] != '']

    return df