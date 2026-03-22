"""
engine/scheduler.py — daily data freshness management

Strategy:
  - Trading day closes ~13:30, data published ~14:30 Taipei time
  - After 14:30 each weekday, all today's SQLite cache is considered stale
  - We do NOT run a background thread (Streamlit doesn't support it reliably)
  - Instead: check on every page load whether the last fetch is stale,
    and force a refresh via cache_clear() if so.

Two modes:
  1. In-app check (refresh_if_stale): run on app startup in main.py
  2. Standalone script (run_daily_refresh): call from cron / shell for pre-market pull

Cron example (refresh at 14:35 Taipei every weekday):
  35 14 * * 1-5 TZ=Asia/Taipei /usr/bin/python /path/to/stock_lab/engine/scheduler.py

DuckDB long-term storage:
  OHLCV older than 30 days is migrated from SQLite → DuckDB parquet shard
  so the cache DB stays small and fast.
"""

import datetime
import sqlite3
import time
import pytz
from pathlib import Path

# ─────────────────────────────────────────────────────────
# Constants
# ─────────────────────────────────────────────────────────
TAIPEI_TZ       = pytz.timezone("Asia/Taipei")
MARKET_CLOSE_H  = 14
MARKET_CLOSE_M  = 30
CACHE_DB_PATH   = Path("finmind_cache.db")
LAST_REFRESH_KEY = "last_scheduled_refresh"


# ─────────────────────────────────────────────────────────
# 1. Staleness check
# ─────────────────────────────────────────────────────────

def market_is_closed_today() -> bool:
    """Return True if it is past 14:30 on a Taipei weekday."""
    now = datetime.datetime.now(TAIPEI_TZ)
    if now.weekday() >= 5:   # Saturday=5, Sunday=6
        return False         # no close today, don't force refresh
    close_time = now.replace(hour=MARKET_CLOSE_H, minute=MARKET_CLOSE_M, second=0)
    return now >= close_time


def data_is_stale() -> bool:
    """
    Return True if:
      - Today's market has closed (>14:30 Taipei weekday)
      - AND no refresh has been recorded for today yet
    """
    if not market_is_closed_today():
        return False

    today = datetime.date.today().strftime("%Y-%m-%d")
    if not CACHE_DB_PATH.exists():
        return True

    try:
        with sqlite3.connect(str(CACHE_DB_PATH)) as conn:
            conn.execute("""
                CREATE TABLE IF NOT EXISTS refresh_log (
                    key TEXT PRIMARY KEY, value TEXT
                )
            """)
            row = conn.execute(
                "SELECT value FROM refresh_log WHERE key=?", (LAST_REFRESH_KEY,)
            ).fetchone()
        return row is None or row[0] != today
    except Exception:
        return True


def mark_refreshed():
    """Record today as successfully refreshed."""
    today = datetime.date.today().strftime("%Y-%m-%d")
    try:
        with sqlite3.connect(str(CACHE_DB_PATH)) as conn:
            conn.execute("""
                CREATE TABLE IF NOT EXISTS refresh_log (
                    key TEXT PRIMARY KEY, value TEXT
                )
            """)
            conn.execute(
                "INSERT OR REPLACE INTO refresh_log VALUES (?, ?)",
                (LAST_REFRESH_KEY, today)
            )
    except Exception as e:
        print(f"[scheduler] mark_refreshed error: {e}")


# ─────────────────────────────────────────────────────────
# 2. In-app staleness banner (call from main.py)
# ─────────────────────────────────────────────────────────

def show_staleness_banner():
    """
    Call once at the top of main.py.
    Shows a dismissible banner + refresh button when data is stale.
    Does NOT auto-clear cache — user confirms to avoid mid-session disruption.
    """
    import streamlit as st

    if not data_is_stale():
        return

    now_taipei = datetime.datetime.now(TAIPEI_TZ).strftime("%H:%M")
    col1, col2 = st.columns([6, 1])
    with col1:
        st.warning(
            f"📅 台北時間 {now_taipei}，今日盤後數據已更新。"
            f"點擊右側按鈕重新載入最新數據。"
        )
    with col2:
        if st.button("🔄 更新", use_container_width=True):
            _clear_today_cache()
            mark_refreshed()
            st.cache_data.clear()
            st.rerun()


def _clear_today_cache():
    """Delete today's SQLite cache rows so next fetch re-pulls from FinMind."""
    today = datetime.date.today().strftime("%Y-%m-%d")
    try:
        with sqlite3.connect(str(CACHE_DB_PATH)) as conn:
            deleted = conn.execute(
                "DELETE FROM api_cache WHERE fetch_date=?", (today,)
            ).rowcount
        print(f"[scheduler] Cleared {deleted} cache rows for {today}")
    except Exception as e:
        print(f"[scheduler] Cache clear error: {e}")


# ─────────────────────────────────────────────────────────
# 3. Standalone refresh script (for cron / pre-market pull)
# ─────────────────────────────────────────────────────────

def run_daily_refresh(stock_ids: list, fm_token: str):
    """
    Pre-warm cache for a list of stock IDs.
    Run this as a cron job at 14:35 Taipei time.

    Usage from shell:
        python -c "
        from engine.scheduler import run_daily_refresh
        from sector_data import STOCK_POOL
        run_daily_refresh(list(STOCK_POOL.keys()), 'your_fm_token')
        "
    """
    from FinMind.data import DataLoader
    from engine.cache import DataCacheManager

    dl = DataLoader()
    dl.login_by_token(api_token=fm_token)
    cache = DataCacheManager()
    start = (datetime.date.today() - datetime.timedelta(days=730)).strftime("%Y-%m-%d")

    print(f"[scheduler] Starting daily refresh for {len(stock_ids)} stocks...")
    success, failed = 0, 0

    for i, sid in enumerate(stock_ids):
        try:
            # OHLCV
            df = dl.taiwan_stock_daily(stock_id=sid, start_date=start)
            if not df.empty:
                cache.set(sid, "daily", df)

            # Institutional
            inst = dl.taiwan_stock_institutional_investors(stock_id=sid, start_date=start)
            if not inst.empty:
                cache.set(sid, "institutional", inst)

            # Revenue
            rev = dl.taiwan_stock_month_revenue(stock_id=sid, start_date=start)
            if not rev.empty:
                cache.set(sid, "revenue", rev)

            success += 1
            time.sleep(0.3)  # rate limit

            if (i + 1) % 10 == 0:
                print(f"  {i+1}/{len(stock_ids)} done...")

        except Exception as e:
            print(f"  [FAIL] {sid}: {e}")
            failed += 1
            time.sleep(1)

    mark_refreshed()
    print(f"[scheduler] Done. Success={success}, Failed={failed}")


# ─────────────────────────────────────────────────────────
# 4. DuckDB long-term OHLCV archive
# ─────────────────────────────────────────────────────────

def archive_old_cache_to_duckdb(days_threshold: int = 30):
    """
    Move OHLCV data older than `days_threshold` from SQLite → DuckDB.
    Keeps SQLite small and fast. Run weekly.

    Requires: pip install duckdb
    """
    try:
        import duckdb
        import pandas as pd
        from io import StringIO

        cutoff = (datetime.date.today() - datetime.timedelta(days=days_threshold)).strftime("%Y-%m-%d")
        duckdb_path = "stock_history.duckdb"

        with sqlite3.connect(str(CACHE_DB_PATH)) as sqlite_conn:
            old_rows = pd.read_sql(
                "SELECT sid, content FROM api_cache WHERE data_type='daily' AND fetch_date < ?",
                sqlite_conn, params=(cutoff,)
            )

        if old_rows.empty:
            print("[archive] Nothing to archive.")
            return

        duck_conn = duckdb.connect(duckdb_path)
        duck_conn.execute("""
            CREATE TABLE IF NOT EXISTS ohlcv_archive (
                sid TEXT, fetch_date TEXT, data JSON
            )
        """)

        for _, row in old_rows.iterrows():
            duck_conn.execute(
                "INSERT OR IGNORE INTO ohlcv_archive VALUES (?, ?, ?)",
                (row["sid"], cutoff, row["content"])
            )

        duck_conn.close()

        # Remove archived rows from SQLite
        with sqlite3.connect(str(CACHE_DB_PATH)) as sqlite_conn:
            deleted = sqlite_conn.execute(
                "DELETE FROM api_cache WHERE data_type='daily' AND fetch_date < ?",
                (cutoff,)
            ).rowcount

        print(f"[archive] Archived {len(old_rows)} stocks to DuckDB, deleted {deleted} SQLite rows.")
    except ImportError:
        print("[archive] DuckDB not installed. Run: pip install duckdb")
    except Exception as e:
        print(f"[archive] Error: {e}")


# ─────────────────────────────────────────────────────────
# Entrypoint for cron
# ─────────────────────────────────────────────────────────

if __name__ == "__main__":
    import os, sys
    sys.path.insert(0, str(Path(__file__).parent.parent))

    from sector_data import STOCK_POOL
    token = os.environ.get("FINMIND_TOKEN", "")
    if not token:
        print("Set FINMIND_TOKEN environment variable first.")
        sys.exit(1)

    run_daily_refresh(list(STOCK_POOL.keys()), token)
    archive_old_cache_to_duckdb(days_threshold=30)
