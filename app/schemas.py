from pydantic import BaseModel
from typing import Optional

class PublikasiBase(BaseModel):
    nip: Optional[str]
    id_scopus: Optional[str]
    nama: Optional[str]
    judul: Optional[str]
    jenis_publikasi: Optional[str]
    nama_jurnal: Optional[str]
    tahun: Optional[str]
    tautan: Optional[str]
    doi: Optional[str]

    class Config:
        orm_mode = True