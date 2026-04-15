import time
import sqlite3

from .repository import ExcludedRepository


class SQLiteExcludedRepository(ExcludedRepository):

    def __init__(self, db_path: str):
        self._conn = sqlite3.connect(db_path, check_same_thread=False)
        self._conn.execute("PRAGMA journal_mode=WAL")
        self._create_tables()

    def _create_tables(self):
        self._conn.execute("""
            CREATE TABLE IF NOT EXISTS excluded_artists (
                artist_name TEXT PRIMARY KEY,
                first_seen  REAL NOT NULL
            )
        """)
        self._conn.commit()

    def add(self, artist_name: str) -> None:
        self._conn.execute("""
            INSERT OR IGNORE INTO excluded_artists (artist_name, first_seen)
            VALUES (?, ?)
        """, (artist_name, time.time()))
        self._conn.commit()

    def get_all(self) -> set:
        rows = self._conn.execute(
            "SELECT artist_name FROM excluded_artists"
        ).fetchall()
        return {r[0] for r in rows}

    def close(self) -> None:
        self._conn.close()
