from __future__ import annotations

from .database import Database
from .database_postgres import PostgresDatabase


def create_database(*, database_url: str, database_path: str) -> Database | PostgresDatabase:
    if database_url.strip():
        return PostgresDatabase(database_url.strip())
    return Database(database_path)
