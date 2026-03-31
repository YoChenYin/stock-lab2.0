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


def get_conn() -> sqlite3.Connection:
    from chip_module.db.schema import init_db
    init_db(CHIP_DB)
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
