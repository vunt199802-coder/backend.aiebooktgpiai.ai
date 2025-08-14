from typing import Generator
from fastapi import Depends
from database.connection import get_db
from sqlalchemy.orm import Session

def get_db_session() -> Generator[Session, None, None]:
    db = next(get_db())
    try:
        yield db
    finally:
        db.close()
