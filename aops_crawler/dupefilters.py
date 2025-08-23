import os
from typing import Optional
import logging

from scrapy.dupefilters import RFPDupeFilter
from twisted.internet.threads import deferToThread
from aops_crawler.db.sqlite_store import SqliteStore


logger = logging.getLogger(__name__)


class LinkingDupeFilter(RFPDupeFilter):
    """
    Custom dupefilter that preserves Scrapy's duplicate rejection while
    executing side-effects when a duplicate is detected. Specifically,
    it records parent/child connections to SQLite using `SqliteStore`.

    Expects requests to carry `meta["id"]` (child id) and
    `meta["parent_id"]` (parent id), as used by the project's spiders.
    """

    def __init__(self, path: Optional[str] = None, debug: bool = False, sqlite_path: Optional[str] = None, fingerprinter=None, **kwargs) -> None:
        # Pass through Scrapy's expected args (including fingerprinter)
        super().__init__(path=path, debug=debug, fingerprinter=fingerprinter)
        # Optional DB usage in background thread
        self._sqlite_path: Optional[str] = sqlite_path
        self._store = None
        # Where to write dupe logs
        self._log_path = os.path.join("test", "dupefilter.log")

    @classmethod
    def from_settings(cls, settings):
        # Mirror RFPDupeFilter's settings handling while adding sqlite path
        debug = settings.getbool("DUPEFILTER_DEBUG")
        jobdir = settings.get("JOBDIR")
        # Store fingerprints alongside scheduler state if jobdir is set
        fp_store_path = os.path.join(jobdir, "requests.seen") if jobdir else None

        sqlite_path = settings.get("AOPS_SQLITE_PATH")
        return cls(path=fp_store_path, debug=debug, sqlite_path=sqlite_path)

    def open(self):
        # Initialize parent (fingerprint persistence if any)
        try:
            super().open()
        except Exception:
            pass

        # Ensure log directory exists
        try:
            os.makedirs(os.path.dirname(self._log_path), exist_ok=True)
        except Exception:
            pass
        # Open SQLite store if configured (for background thread operations)
        if self._sqlite_path:
            try:
                # Use a dedicated connection; SQLite connection is thread-bound by default
                self._store = SqliteStore(self._sqlite_path)
                self._store.open()
                logger.info("[DupeFilter] Opened SqliteStore for duplicate link writes")
            except Exception as e:
                logger.warning(f"[DupeFilter] Failed to open SqliteStore: {e}")

    def close(self, reason):
        try:
            if self._store is not None:
                self._store.commit()
                self._store.close()
        except Exception:
            pass
        finally:
            self._store = None

        # Close parent resources
        try:
            return super().close(reason)
        except Exception:
            return None

    def request_seen(self, request):
        seen = super().request_seen(request)
        if seen:
            # Duplicate detected: append to log file in test/dupefilter.log
            try:
                parent_id = request.meta.get("parent_id")
                child_id = request.meta.get("id")
                driver = request.meta.get("driver")
                url = request.url
                with open(self._log_path, "a", encoding="utf-8") as f:
                    f.write(f"parent_id={parent_id} child_id={child_id} driver={driver} url={url}\n")
                # Non-blocking DB write of connection (if DB is configured)
                if self._store is not None and child_id is not None:
                    def _write_link():
                        try:
                            self._store.link(parent_id=parent_id, child_id=int(child_id), type_of_child=str(driver) if driver else None)
                            self._store.commit()
                        except Exception:
                            pass
                    deferToThread(_write_link)
            except Exception as e:
                logger.warning(f"[DupeFilter] Failed to write duplicate log: {e}")
        return seen


