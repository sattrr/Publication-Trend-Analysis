import uuid
from sqlalchemy import Column, String
from sqlalchemy.dialects.postgresql import UUID
from .database import Base

class Publikasi(Base):
    __tablename__ = "publikasi"
    __table_args__ = {'schema': 'penelitian'}

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    nip = Column(String(30))
    id_scopus = Column(String(20))
    nama = Column(String)
    judul = Column(String)
    jenis_publikasi = Column(String)
    nama_jurnal = Column(String)
    tahun = Column(String(10))
    tautan = Column(String)
    doi = Column(String)
    sumber_data = Column(String, nullable=True)