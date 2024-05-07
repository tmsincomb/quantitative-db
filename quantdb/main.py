<<<<<<< HEAD
from __future__ import annotations

from functools import lru_cache
from typing import Any, Generator

from fastapi import Depends, FastAPI
from sqlalchemy import text
from sqlalchemy.orm import Session  # type: ignore

from quantdb.config import Settings

from quantdb import mysql_app

app = FastAPI()


@lru_cache()
def get_settings():
    """
    Get settings from mongodb - alternative for test and production credientials

    Returns
    -------
    Settings
        pydantic BaseSettings model with MongoDB credentials
    """
    return Settings()


def get_mysql_db() -> Generator[Session, Any, None]:
    db = mysql_app.database.SessionLocal()
    try:
        yield db
    finally:
        db.close()


@app.get(
    "/example",
    status_code=200,
)
def get_example(
    mysql_db: Session = Depends(get_mysql_db),  # type: ignore
):
    query = text(
        """
        select * from <table_name> limit 10
    """
    )
    result = mysql_db.execute(query)
    data = result.mappings().all()
    return data
=======
from fastapi import FastAPI
from fastapi.middleware.wsgi import WSGIMiddleware
from quantdb.api_server import app as flask_app

app = FastAPI()

app.mount('/', WSGIMiddleware(flask_app))

if __name__ == "__main__":
    import uvicorn
    uvicorn.run("quantdb.main:app", host='127.0.0.1', port=8990, reload=True)
>>>>>>> master
