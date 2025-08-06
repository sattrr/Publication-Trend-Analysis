import pandas as pd
from fastapi import FastAPI, UploadFile, File, Depends
from sqlalchemy.orm import Session
from app.database import SessionLocal, engine
from app import models, crud
from app.utils.cleaner import clean_and_match_data
from app.routes import publication_collection, publication_analysis

models.Base.metadata.create_all(bind=engine)
app = FastAPI()

def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()

app.include_router(publication_collection.router, prefix="/collection", tags=["Publication Collection"])
app.include_router(publication_analysis.router, prefix="/analysis", tags=["Publication Analysis"])

@app.post("/upload/")
async def upload_excel(file: UploadFile = File(...), db: Session = Depends(get_db)):
    df = pd.read_excel(file.file)
    cleaned_df = clean_and_match_data(df)

    print(f"Starting insert {len(cleaned_df)} data into DB...")

    publikasi_list = []
    for _, row in cleaned_df.iterrows():
        try:
            publikasi = models.Publikasi(**row.to_dict())
            publikasi_list.append(publikasi)
        except Exception as e:
            print(f"Error parsing row: {e}")
    
    try:
        db.bulk_save_objects(publikasi_list, return_defaults=False)
        db.commit()
        print("Successful data insert into DB.")
    except Exception as e:
        db.rollback()
        print(f"DB Error: {e}")
        return {"error": str(e)}

    return {"message": f"{len(publikasi_list)} records inserted"}