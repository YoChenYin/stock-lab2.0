"""
engine/cache.py — two-layer cache
  Layer 1: SQLite (cross-session, daily, prevents repeat API calls)
  Layer 2: @st.cache_data (in-memory, current session speed)

Rule: every FinMind call must go through _smart_fetch, never call the API directly.

Storage path:
  - Zeabur: /data/finmind_cache.db  (persistent volume, survives redeploy)
  - Local:  finmind_cache.db        (working directory)
"""

import os
import sqlite3
import datetime
import pandas as pd
from io import StringIO

# On Zeabur, mount a Persistent Volume at /data
# Locally falls back to working directory
_DATA_DIR = "/data" if os.path.isdir("/data") else "."
DEFAULT_DB_PATH = os.path.join(_DATA_DIR, "finmind_cache.db")


class DataCacheManager:
    def __init__(self, db_path: str = DEFAULT_DB_PATH):
        self.db_path = db_path
        self._init_db()

    def _init_db(self):
        with sqlite3.connect(self.db_path) as conn:
            conn.execute("""
                CREATE TABLE IF NOT EXISTS api_cache (
                    sid TEXT,
                    data_type TEXT,
                    fetch_date TEXT,
                    content TEXT,
                    PRIMARY KEY (sid, data_type, fetch_date)
                )
            """)

    def get(self, sid: str, data_type: str) -> pd.DataFrame | None:
        """Return today's cached DataFrame, or None if not found."""
        today = datetime.date.today().strftime("%Y-%m-%d")
        with sqlite3.connect(self.db_path) as conn:
            row = conn.execute(
                "SELECT content FROM api_cache WHERE sid=? AND data_type=? AND fetch_date=?",
                (sid, data_type, today)
            ).fetchone()
        if row:
            df = pd.read_json(StringIO(row[0]))
            if "date" in df.columns:
                df["date"] = pd.to_datetime(df["date"])
            return df
        return None

    def set(self, sid: str, data_type: str, df: pd.DataFrame):
        """Write DataFrame to today's cache."""
        if df is None or df.empty:
            return
        today = datetime.date.today().strftime("%Y-%m-%d")
        with sqlite3.connect(self.db_path) as conn:
            conn.execute(
                "INSERT OR REPLACE INTO api_cache VALUES (?, ?, ?, ?)",
                (sid, data_type, today, df.to_json())
            )

    def clear_old(self, keep_days: int = 3):
        """Housekeeping: remove entries older than keep_days."""
        cutoff = (datetime.date.today() - datetime.timedelta(days=keep_days)).strftime("%Y-%m-%d")
        with sqlite3.connect(self.db_path) as conn:
            conn.execute("DELETE FROM api_cache WHERE fetch_date < ?", (cutoff,))
