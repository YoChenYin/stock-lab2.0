"""chip_radar/_db.py — shared DB helpers for all chip_radar tabs"""

import sqlite3
import pandas as pd
import streamlit as st
from pathlib import Path
CHIP_DB        = Path(__file__).resolve().parents[2] / "chip_module" / "chip.db"
UNIVERSE_JSON  = Path(__file__).resolve().parents[2] / "chip_module" / "us_universe.json"

SCORE_COLS = [
    "insider_score", "short_score", "volume_score",
    "options_flow_score", "options_mkt_score", "institutional_score",
    "composite_short", "composite_swing", "composite_mid",
    "whale_alert", "entry_timing", "signal_flags",
]

COMPOSITE_KEY = {
    "短線 (1–5天)": "composite_short",
    "波段 (1–4週)": "composite_swing",
    "中線 (1–3月)": "composite_mid",
}


def _init_db():
    """建立所有 tables（冪等）。"""
    with sqlite3.connect(CHIP_DB) as conn:
        conn.executescript("""
        CREATE TABLE IF NOT EXISTS daily_prices (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            ticker TEXT NOT NULL, date TEXT NOT NULL,
            open REAL, high REAL, low REAL, close REAL, volume INTEGER,
            obv REAL, obv_signal REAL, cmf_20 REAL, mfi_14 REAL,
            avg_vol_20 REAL, vol_ratio REAL,
            UNIQUE(ticker, date)
        );
        CREATE TABLE IF NOT EXISTS insider_trades (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            ticker TEXT NOT NULL, report_date TEXT NOT NULL, trade_date TEXT,
            insider_name TEXT, insider_title TEXT, transaction_type TEXT,
            shares REAL, price_per_share REAL, total_value REAL,
            shares_owned_after REAL, accession_number TEXT UNIQUE,
            fetched_at TEXT DEFAULT (datetime('now'))
        );
        CREATE TABLE IF NOT EXISTS short_interest (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            ticker TEXT NOT NULL, settlement_date TEXT NOT NULL,
            short_volume INTEGER, avg_daily_vol REAL, short_float_pct REAL,
            days_to_cover REAL, prev_short_vol INTEGER, chg_pct REAL,
            UNIQUE(ticker, settlement_date)
        );
        CREATE TABLE IF NOT EXISTS options_sentiment (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            date TEXT NOT NULL, scope TEXT NOT NULL,
            pc_ratio REAL, pc_ma10 REAL, pc_ma20 REAL, pc_zscore_20 REAL,
            UNIQUE(date, scope)
        );
        CREATE TABLE IF NOT EXISTS institutional_holders (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            ticker TEXT NOT NULL, report_date TEXT NOT NULL,
            institution TEXT, shares_held REAL, pct_out REAL,
            value_usd REAL, prev_shares REAL, chg_pct REAL,
            UNIQUE(ticker, report_date, institution)
        );
        CREATE TABLE IF NOT EXISTS options_flow (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            ticker TEXT NOT NULL, date TEXT NOT NULL,
            underlying_price REAL,
            call_volume INTEGER, call_oi INTEGER,
            put_volume INTEGER, put_oi INTEGER,
            otm_call_volume INTEGER, otm_call_oi INTEGER,
            unusual_call_strikes INTEGER, unusual_put_strikes INTEGER,
            max_call_vol_oi_ratio REAL, max_put_vol_oi_ratio REAL,
            avg_call_iv REAL, avg_put_iv REAL,
            UNIQUE(ticker, date)
        );
        CREATE TABLE IF NOT EXISTS large_holders (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            ticker TEXT NOT NULL, filed_date TEXT NOT NULL,
            form_type TEXT, filer_name TEXT, accession_number TEXT UNIQUE,
            UNIQUE(ticker, filed_date, filer_name)
        );
        CREATE TABLE IF NOT EXISTS chip_scores (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            ticker TEXT NOT NULL, date TEXT NOT NULL,
            insider_score REAL, short_score REAL, volume_score REAL,
            options_flow_score REAL, options_mkt_score REAL, institutional_score REAL,
            composite_short REAL, composite_swing REAL, composite_mid REAL,
            whale_alert INTEGER DEFAULT 0, entry_timing INTEGER DEFAULT 0,
            signal_flags TEXT, calc_version TEXT DEFAULT '2.0',
            updated_at TEXT DEFAULT (datetime('now')),
            UNIQUE(ticker, date)
        );
        CREATE INDEX IF NOT EXISTS idx_prices_ticker_date  ON daily_prices(ticker, date DESC);
        CREATE INDEX IF NOT EXISTS idx_scores_ticker_date  ON chip_scores(ticker, date DESC);
        CREATE INDEX IF NOT EXISTS idx_scores_composite    ON chip_scores(date DESC, composite_swing DESC);
        CREATE INDEX IF NOT EXISTS idx_scores_whale        ON chip_scores(date DESC, whale_alert DESC);
        """)


def get_conn() -> sqlite3.Connection:
    _init_db()
    conn = sqlite3.connect(CHIP_DB)
    conn.row_factory = sqlite3.Row
    return conn


@st.cache_data(ttl=3600)
def load_latest_scores(as_of: str = None) -> pd.DataFrame:
    """每支股票最新一筆 chip_scores"""
    conn = get_conn()
    q = """
        SELECT s.*
        FROM chip_scores s
        INNER JOIN (
            SELECT ticker, MAX(date) AS max_date
            FROM chip_scores GROUP BY ticker
        ) t ON s.ticker=t.ticker AND s.date=t.max_date
    """
    if as_of:
        q = """
            SELECT s.*
            FROM chip_scores s
            INNER JOIN (
                SELECT ticker, MAX(date) AS max_date
                FROM chip_scores WHERE date<=? GROUP BY ticker
            ) t ON s.ticker=t.ticker AND s.date=t.max_date
        """
        df = pd.read_sql(q, conn, params=(as_of,))
    else:
        df = pd.read_sql(q, conn)
    conn.close()
    return df


@st.cache_data(ttl=3600)
def load_score_history(ticker: str, days: int = 60) -> pd.DataFrame:
    conn = get_conn()
    df = pd.read_sql("""
        SELECT * FROM chip_scores
        WHERE ticker=?
        ORDER BY date DESC LIMIT ?
    """, conn, params=(ticker, days))
    conn.close()
    return df.sort_values("date")


@st.cache_data(ttl=3600)
def load_insider_trades(ticker: str) -> pd.DataFrame:
    conn = get_conn()
    df = pd.read_sql("""
        SELECT trade_date, insider_name, insider_title,
               transaction_type, shares, price_per_share, total_value
        FROM insider_trades
        WHERE ticker=?
          AND transaction_type IN ('P','S')
        ORDER BY trade_date DESC
        LIMIT 50
    """, conn, params=(ticker,))
    conn.close()
    return df


@st.cache_data(ttl=3600)
def load_options_flow(ticker: str, days: int = 30) -> pd.DataFrame:
    conn = get_conn()
    df = pd.read_sql("""
        SELECT date, underlying_price,
               call_volume, put_volume, call_oi, put_oi,
               otm_call_volume, unusual_call_strikes, unusual_put_strikes,
               max_call_vol_oi_ratio, avg_call_iv, avg_put_iv
        FROM options_flow
        WHERE ticker=?
        ORDER BY date DESC LIMIT ?
    """, conn, params=(ticker, days))
    conn.close()
    return df.sort_values("date")


@st.cache_data(ttl=3600)
def load_large_holders(ticker: str) -> pd.DataFrame:
    conn = get_conn()
    df = pd.read_sql("""
        SELECT filed_date, form_type, filer_name
        FROM large_holders WHERE ticker=?
        ORDER BY filed_date DESC LIMIT 20
    """, conn, params=(ticker,))
    conn.close()
    return df


@st.cache_data(ttl=3600)
def load_market_pulse() -> dict:
    conn = get_conn()
    row = conn.execute("""
        SELECT date, pc_ratio, pc_ma20, pc_zscore_20
        FROM options_sentiment WHERE scope='equity'
        ORDER BY date DESC LIMIT 1
    """).fetchone()
    conn.close()
    return dict(row) if row else {}


@st.cache_data(ttl=86400)
def load_universe() -> dict:
    """
    Load ticker universe from chip_module/us_universe.json.
    Returns {ticker: {"name": str, "sector": str, "index": str}}.
    Returns {} if file missing or malformed — UI falls back to DB tickers.
    """
    import json
    try:
        with open(UNIVERSE_JSON, "r") as f:
            return json.load(f)
    except Exception:
        return {}
