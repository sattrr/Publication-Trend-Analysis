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

    for _, row in cleaned_df.iterrows():
        crud.insert_publikasi(db, row.to_dict())
    db.commit()
    return {"message": f"{len(cleaned_df)} records inserted"}