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
                name TEXT,
                subtitle TEXT,
                url TEXT,
                raw_json TEXT
            )
            """
        )
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS posts (
                id INTEGER PRIMARY KEY,
                thread_id INTEGER NOT NULL,
                user_id INTEGER,
                created_at REAL,
                thanks_count INTEGER,
                nothanks_count INTEGER,
                raw_html TEXT,
                processed_html TEXT,
                is_first_post BOOLEAN,
                source TEXT
            )
            """
        )
        # --- lightweight migration for existing databases ---
        # Ensure new columns exist and attempt to backfill processed_html from legacy value column if present
        # Posts table migrations
        cur.execute("PRAGMA table_info(posts)")
        columns_info = cur.fetchall()
        existing_columns = {row[1] for row in columns_info}
        if "thread_id" not in existing_columns:
            cur.execute("ALTER TABLE posts ADD COLUMN thread_id INTEGER NOT NULL DEFAULT 0")
        if "user_id" not in existing_columns:
            cur.execute("ALTER TABLE posts ADD COLUMN user_id INTEGER")
        if "created_at" not in existing_columns:
            cur.execute("ALTER TABLE posts ADD COLUMN created_at REAL")
        if "thanks_count" not in existing_columns:
            cur.execute("ALTER TABLE posts ADD COLUMN thanks_count INTEGER")
        if "nothanks_count" not in existing_columns:
            cur.execute("ALTER TABLE posts ADD COLUMN nothanks_count INTEGER")
        if "raw_html" not in existing_columns:
            cur.execute("ALTER TABLE posts ADD COLUMN raw_html TEXT")
        if "processed_html" not in existing_columns:
            cur.execute("ALTER TABLE posts ADD COLUMN processed_html TEXT")
        if "is_first_post" not in existing_columns:
            cur.execute("ALTER TABLE posts ADD COLUMN is_first_post BOOLEAN")
        if "source" not in existing_columns:
            cur.execute("ALTER TABLE posts ADD COLUMN source TEXT")
        # Remove legacy backfill from unknown 'value' column if present; do not auto-backfill
        # Categories table migrations
        cur.execute("PRAGMA table_info(categories)")
        columns_info = cur.fetchall()
        existing_cat_columns = {row[1] for row in columns_info}
        if "subtitle" not in existing_cat_columns:
            cur.execute("ALTER TABLE categories ADD COLUMN subtitle TEXT")
        if "url" not in existing_cat_columns:
            cur.execute("ALTER TABLE categories ADD COLUMN url TEXT")
        if "raw_json" not in existing_cat_columns:
            cur.execute("ALTER TABLE categories ADD COLUMN raw_json TEXT")
        # Tags: single table mapping thread -> tag text
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS post_tags (
                thread_id INTEGER NOT NULL,
                tag TEXT NOT NULL,
                PRIMARY KEY (thread_id, tag)
            )
            """
        )
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS connections (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                parent_id INTEGER,
                child_id INTEGER,
                type_of_child TEXT
            )
            """
        )
        # Ensure unique constraint includes type to support many-to-many across mixed child types
        try:
            cur.execute("PRAGMA index_list(connections)")
            idx_rows = cur.fetchall()
            existing_indexes = {row[1] for row in idx_rows}
            if "idx_connections_parent_child" in existing_indexes:
                try:
                    cur.execute("DROP INDEX idx_connections_parent_child")
                except Exception:
                    pass
        except Exception:
            pass
        cur.execute(
            """
            CREATE UNIQUE INDEX IF NOT EXISTS idx_connections_parent_child_type
            ON connections(parent_id, child_id, type_of_child)
            """
        )
        self._conn.commit()

    # --- operations ---
    def upsert_category(self, category_id: int, name: Optional[str], subtitle: Optional[str] = None, url: Optional[str] = None, raw_json: Optional[str] = None) -> None:
        assert self._conn is not None
        self._conn.execute(
            """
            INSERT INTO categories(id, name, subtitle, url, raw_json)
            VALUES (?, ?, ?, ?, ?)
            ON CONFLICT(id) DO UPDATE SET
                name=excluded.name,
                subtitle=COALESCE(excluded.subtitle, categories.subtitle),
                url=COALESCE(excluded.url, categories.url),
                raw_json=COALESCE(excluded.raw_json, categories.raw_json)
            """,
            (category_id, name, subtitle, url, raw_json),
        )

    def link(self, parent_id: Optional[int], child_id: int, type_of_child: Optional[str] = None) -> None:
        assert self._conn is not None
        if parent_id is None:
            return
        self._conn.execute(
            "INSERT OR IGNORE INTO connections(parent_id, child_id, type_of_child) VALUES (?, ?, ?)",
            (parent_id, child_id, type_of_child),
        )

    def insert_post_message(
        self,
        thread_id: int,
        user_id: Optional[int],
        created_at: Optional[float],
        thanks_count: Optional[int],
        nothanks_count: Optional[int],
        raw_html: Optional[str],
        processed_html: Optional[str],
        is_first_post: Optional[bool],
        source: Optional[str],
    ) -> None:
        assert self._conn is not None
        self._conn.execute(
            """
            INSERT INTO posts(thread_id, user_id, created_at, thanks_count, nothanks_count, raw_html, processed_html, is_first_post, source)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (thread_id, user_id, created_at, thanks_count, nothanks_count, raw_html, processed_html, is_first_post, source),
        )

    def add_tag(self, thread_id: int, tag: str) -> None:
        assert self._conn is not None
        self._conn.execute(
            "INSERT OR IGNORE INTO post_tags(thread_id, tag) VALUES (?, ?)",
            (thread_id, tag),
        )

    def commit(self) -> None:
        assert self._conn is not None
        self._conn.commit()


