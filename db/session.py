from typing import Generator

from fastapi import HTTPException
from psycopg2 import OperationalError
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from starlette import status
from sqlalchemy.exc import OperationalError as SQLOperationalError
from sqlalchemy.exc import SQLAlchemyError, ArgumentError, DBAPIError, InvalidRequestError, DisconnectionError, \
    NoReferenceError

from core.config import settings

SQLALCHEMY_DATABASE_URL = settings.DATABASE_URL
print("Database URL is ", SQLALCHEMY_DATABASE_URL)

engine = create_engine(SQLALCHEMY_DATABASE_URL)

SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)


def get_db() -> Generator:  # new
    try:
        db = SessionLocal()
        yield db
    except SQLOperationalError as e:
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=f"DB 접속 에러 - {e}")
    finally:
        db.close()
