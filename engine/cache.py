"""
engine/cache.py — two-layer cache

  Layer 1: SQLite (cross-session, persistent — survives across days)
  Layer 2: @st.cache_data (in-memory, current session speed)

Design:
  - Schema key is (sid, data_type) with NO date in PK.
  - last_updated stores the date when data was last fetched from API.
  - content is pickle-serialised DataFrame (BLOB) — ~80% smaller than JSON.
  - _smart_fetch checks last_updated == today to skip re-fetching same day.
  - Supports incremental fetch: caller learns last_updated and fetches only the delta.

Storage path:
  - Zeabur: /data/finmind_cache.db  (persistent volume, survives redeploy)
  - Local:  finmind_cache.db        (working directory)
"""

import os
import pickle
import sqlite3
import datetime
import pandas as pd

_DATA_DIR = "/data" if os.path.isdir("/data") else "."
DEFAULT_DB_PATH = os.path.join(_DATA_DIR, "finmind_cache.db")

# Scan-result cache table name (same DB, different table)
_SCAN_TABLE = "tw_scan_cache"


class DataCacheManager:
    def __init__(self, db_path: str = DEFAULT_DB_PATH):
        self.db_path = db_path
        self._init_db()

    # ── schema ──────────────────────────────────────────────
    def _init_db(self):
        with sqlite3.connect(self.db_path) as conn:
            conn.execute("PRAGMA journal_mode=WAL")

            # Migrate old schema (had fetch_date in PK, TEXT content)
            cols = {r[1] for r in conn.execute("PRAGMA table_info(api_cache)").fetchall()}
            if "fetch_date" in cols:
                conn.execute("DROP TABLE api_cache")

            conn.execute("""
                CREATE TABLE IF NOT EXISTS api_cache (
                    sid          TEXT,
                    data_type    TEXT,
                    last_updated TEXT,
                    content      BLOB,
                    PRIMARY KEY (sid, data_type)
                )
            """)

            # Scan-result cache (pre-computed daily scans)
            conn.execute(f"""
                CREATE TABLE IF NOT EXISTS {_SCAN_TABLE} (
                    scan_type    TEXT,
                    computed_date TEXT,
                    data_json    TEXT,
                    PRIMARY KEY (scan_type, computed_date)
                )
            """)

            # Schema version table — tracks one-time data fixes
            conn.execute("""
                CREATE TABLE IF NOT EXISTS schema_version (
                    key   TEXT PRIMARY KEY,
                    value TEXT
                )
            """)

            # v2: clear corrupted institutional data (stored with single row per date
            #     due to the old dedup_keys=("date",) bug in _smart_fetch).
            #     Also clear scan cache so stale empty results don't block fresh scans.
            ver = conn.execute(
                "SELECT value FROM schema_version WHERE key='institutional_dedup_v2'"
            ).fetchone()
            if not ver:
                conn.execute("DELETE FROM api_cache WHERE data_type='institutional'")
                conn.execute(f"DELETE FROM {_SCAN_TABLE}")
                conn.execute(
                    "INSERT OR REPLACE INTO schema_version VALUES ('institutional_dedup_v2', '1')"
                )

    # ── api_cache CRUD ───────────────────────────────────────

    def get(self, sid: str, data_type: str) -> tuple[pd.DataFrame | None, str | None]:
        """Return (DataFrame, last_updated_date) or (None, None) if not cached."""
        with sqlite3.connect(self.db_path) as conn:
            row = conn.execute(
                "SELECT content, last_updated FROM api_cache WHERE sid=? AND data_type=?",
                (sid, data_type),
            ).fetchone()
        if not row:
            return None, None
        try:
            df = pickle.loads(row[0])
            if "date" in df.columns:
                df["date"] = pd.to_datetime(df["date"])
            return df, row[1]
        except Exception:
            return None, row[1]  # sentinel row: blob empty/corrupt, but preserve last_updated

    def set(self, sid: str, data_type: str, df: pd.DataFrame):
        """Write DataFrame with today as last_updated."""
        if df is None or df.empty:
            return
        today = datetime.date.today().isoformat()
        blob = pickle.dumps(df, protocol=pickle.HIGHEST_PROTOCOL)
        with sqlite3.connect(self.db_path) as conn:
            conn.execute(
                "INSERT OR REPLACE INTO api_cache VALUES (?, ?, ?, ?)",
                (sid, data_type, today, blob),
            )

    def touch(self, sid: str, data_type: str):
        """Mark entry as checked today. Creates a sentinel row if none exists."""
        today = datetime.date.today().isoformat()
        with sqlite3.connect(self.db_path) as conn:
            conn.execute(
                "INSERT OR IGNORE INTO api_cache (sid, data_type, last_updated, content) VALUES (?, ?, ?, x'')",
                (sid, data_type, today),
            )
            conn.execute(
                "UPDATE api_cache SET last_updated=? WHERE sid=? AND data_type=?",
                (today, sid, data_type),
            )

    def clear_old(self, keep_days: int = 30):
        """Remove entries not updated in keep_days (stale / delisted stocks)."""
        cutoff = (datetime.date.today() - datetime.timedelta(days=keep_days)).isoformat()
        with sqlite3.connect(self.db_path) as conn:
            conn.execute("DELETE FROM api_cache WHERE last_updated < ?", (cutoff,))

    # ── scan cache CRUD ──────────────────────────────────────

    def get_scan(self, scan_type: str) -> list | None:
        """Return today's pre-computed scan results, or None if missing/empty."""
        import json
        today = datetime.date.today().isoformat()
        with sqlite3.connect(self.db_path) as conn:
            row = conn.execute(
                f"SELECT data_json FROM {_SCAN_TABLE} WHERE scan_type=? AND computed_date=?",
                (scan_type, today),
            ).fetchone()
        if row:
            try:
                result = json.loads(row[0])
                return result if result else None  # treat [] as cache-miss → force re-scan
            except Exception:
                return None
        return None

    def set_scan(self, scan_type: str, results: list):
        """Store pre-computed scan results for today. Skips empty results."""
        if not results:
            return
        import json
        today = datetime.date.today().isoformat()
        with sqlite3.connect(self.db_path) as conn:
            conn.execute(
                f"INSERT OR REPLACE INTO {_SCAN_TABLE} VALUES (?, ?, ?)",
                (scan_type, today, json.dumps(results, ensure_ascii=False)),
            )

    def set_tw_last_updated(self):
        """Record current timestamp as Taiwan prefetch completion time."""
        import json
        now = datetime.datetime.now().strftime("%Y-%m-%d %H:%M")
        today = datetime.date.today().isoformat()
        with sqlite3.connect(self.db_path) as conn:
            conn.execute(
                f"INSERT OR REPLACE INTO {_SCAN_TABLE} VALUES (?, ?, ?)",
                ("_meta", today, json.dumps({"time": now, "status": "success"}, ensure_ascii=False)),
            )

    def get_tw_last_updated(self) -> dict:
        """Return the Taiwan prefetch completion timestamp, or {} if never run."""
        import json
        with sqlite3.connect(self.db_path) as conn:
            row = conn.execute(
                f"SELECT data_json FROM {_SCAN_TABLE} WHERE scan_type='_meta' ORDER BY computed_date DESC LIMIT 1"
            ).fetchone()
        if row:
            try:
                return json.loads(row[0])
            except Exception:
                pass
        return {}
