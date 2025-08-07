import pandas as pd
from fastapi import APIRouter, Depends
from sqlalchemy.orm import Session
from app.database import get_db
from app.utils.cleaner import clean_and_match_data
from app.utils.cleanup import clean_id_scopus
from app import models
from pathlib import Path

router = APIRouter()

@router.post("/upload/")
async def upload_exceo(db: Session = Depends(get_db)):
    BASE_DIR = Path(__file__).resolve().parents[2]
    file_path = BASE_DIR / "data" / "cleaned" / "final_publication.xlsx"

    try:
        df = pd.read_excel(file_path)
    except Exception as e:
        return {"error": f"failed to read file: {e}"}

    cleaned_df = clean_and_match_data(df)

    publikasi_list = []
    for _, row in cleaned_df.iterrows():
        try:
            publikasi = models.Publikasi(**row.to_dict())
            publikasi_list.append(publikasi)
        except Exception as e:
            print(f"Row parse error: {e}")
    
    try:
        db.bulk_save_objects(publikasi_list)
        db.commit()
    except Exception as e:
        db.rollback()
        return {"error": f"DB Error: {e}"}

    try:
        clean_id_scopus(db)
    except Exception as e:
        return {"message": f"{len(publikasi_list)} inserted, but clean failed", "error": str(e)}

    return {"message": f"{len(publikasi_list)} records inserted from local file and id_scopus cleaned"}