from sqlalchemy.orm import Session
from sqlalchemy import text

def clean_id_scopus(db: Session):
    query = text("""
        UPDATE penelitian.publikasi
        SET id_scopus = REGEXP_REPLACE(id_scopus, '\\.0$', '')
        WHERE id_scopus LIKE '%.0'
    """)
    db.execute(query)
    db.commit()
