from app import models

def insert_publikasi(db, data: dict):
    publikasi = models.Publikasi(**data)
    db.add(publikasi)