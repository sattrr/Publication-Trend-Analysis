import pandas as pd
import uvicorn
from fastapi import FastAPI, UploadFile, File, Depends
from sqlalchemy.orm import Session
from app.database import SessionLocal, engine
from app import models, crud
from app.utils.cleaner import clean_and_match_data
from app.routes import publication_collection, publication_analysis, upload

models.Base.metadata.create_all(bind=engine)
app = FastAPI()

app.include_router(publication_collection.router, prefix="/collection", tags=["Publication Collection"])
app.include_router(publication_analysis.router, prefix="/analysis", tags=["Publication Analysis"])
app.include_router(upload.router, prefix="/insertdb", tags=["Upload"])

if __name__ == "__main__":
    uvicorn.run("app.main:app", host="0.0.0.0", port=8000, reload=True)