import os
import sqlite3
from typing import Optional


class SqliteStore:
    def __init__(self, db_path: str) -> None:
        self._db_path = db_path
        self._conn: Optional[sqlite3.Connection] = None

    def open(self) -> None:
        os.makedirs(os.path.dirname(self._db_path), exist_ok=True)
        self._conn = sqlite3.connect(self._db_path)
        self._conn.execute("PRAGMA foreign_keys = ON")
        self._create_schema()

    def close(self) -> None:
        if self._conn is not None:
            self._conn.commit()
            self._conn.close()
            self._conn = None

    # --- schema ---
    def _create_schema(self) -> None:
        assert self._conn is not None
        cur = self._conn.cursor()
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS categories (
                id INTEGER PRIMARY KEY,
                name TEXT
            )
            """
        )
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS posts (
                id INTEGER PRIMARY KEY,
                value TEXT,
                likes INTEGER,
                source TEXT,
                user_id INTEGER
            )
            """
        )
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS connections (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                parent_id INTEGER,
                child_id INTEGER
            )
            """
        )
        cur.execute(
            """
            CREATE UNIQUE INDEX IF NOT EXISTS idx_connections_parent_child
            ON connections(parent_id, child_id)
            """
        )
        self._conn.commit()

    # --- operations ---
    def upsert_category(self, category_id: int, name: Optional[str]) -> None:
        assert self._conn is not None
        self._conn.execute(
            """
            INSERT INTO categories(id, name)
            VALUES (?, ?)
            ON CONFLICT(id) DO UPDATE SET name=excluded.name
            """,
            (category_id, name),
        )

    def link(self, parent_id: Optional[int], child_id: int) -> None:
        assert self._conn is not None
        if parent_id is None:
            return
        self._conn.execute(
            "INSERT OR IGNORE INTO connections(parent_id, child_id) VALUES (?, ?)",
            (parent_id, child_id),
        )

    def upsert_post(
        self,
        post_id: int,
        value: Optional[str],
        likes: Optional[int],
        source: Optional[str],
        user_id: Optional[int],
    ) -> None:
        assert self._conn is not None
        self._conn.execute(
            """
            INSERT INTO posts(id, value, likes, source, user_id)
            VALUES (?, ?, ?, ?, ?)
            ON CONFLICT(id) DO UPDATE SET
                value=excluded.value,
                likes=COALESCE(excluded.likes, posts.likes),
                source=COALESCE(excluded.source, posts.source),
                user_id=COALESCE(excluded.user_id, posts.user_id)
            """,
            (post_id, value, likes, source, user_id),
        )

    def commit(self) -> None:
        assert self._conn is not None
        self._conn.commit()


