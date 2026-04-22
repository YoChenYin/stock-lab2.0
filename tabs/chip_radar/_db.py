"""chip_radar/_db.py — shared DB helpers for all chip_radar tabs"""

import sqlite3
import pandas as pd
from pathlib import Path

import functools
import time

try:
    import streamlit as st
    _HAS_ST = True
except ImportError:
    _HAS_ST = False


def _is_streamlit() -> bool:
    """True 只在 Streamlit script run context 內（非 FastAPI）"""
    if not _HAS_ST:
        return False
    try:
        from streamlit.runtime.scriptrunner import get_script_run_ctx
        return get_script_run_ctx() is not None
    except Exception:
        return False


def _make_ttl_cache(fn, ttl: int = 3600):
    """Process-level TTL dict cache — works in FastAPI and Streamlit bare mode"""
    _store: dict = {}

    @functools.wraps(fn)
    def wrapper(*args, **kwargs):
        key = (args, tuple(sorted(kwargs.items())))
        now = time.monotonic()
        if key in _store and (now - _store[key][1]) < ttl:
            return _store[key][0]
        result = fn(*args, **kwargs)
        _store[key] = (result, now)
        return result

    wrapper._cache_store = _store   # allow manual invalidation if needed
    return wrapper


def _cache(fn):
    """
    Streamlit context  → st.cache_data(ttl=3600)
    FastAPI / bare     → process-level TTL dict cache (no ScriptRunContext warning)
    """
    if _HAS_ST and _is_streamlit():
        return st.cache_data(ttl=3600)(fn)
    return _make_ttl_cache(fn, ttl=3600)
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
        CREATE TABLE IF NOT EXISTS market_environment (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            date TEXT NOT NULL UNIQUE,
            vix REAL, vix_ma5 REAL, vix_level TEXT,
            tnx_10y REAL, tnx_slope_20d REAL,
            mag7_avg_deviation REAL, mag7_deviations TEXT, mag7_risk TEXT,
            xlk_chg_1d REAL, xlf_chg_1d REAL, xlp_chg_1d REAL,
            xlk_chg_5d REAL, xlf_chg_5d REAL, xlp_chg_5d REAL,
            updated_at TEXT DEFAULT (datetime('now'))
        );
        CREATE TABLE IF NOT EXISTS tech_signals (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            ticker TEXT NOT NULL, date TEXT NOT NULL,
            ma_aligned INTEGER DEFAULT 0, rsi_reversal INTEGER DEFAULT 0,
            macd_golden INTEGER DEFAULT 0, bb_breakout INTEGER DEFAULT 0,
            double_bottom INTEGER DEFAULT 0, vcp_pattern INTEGER DEFAULT 0,
            hard_stop INTEGER DEFAULT 0, below_20ma_3d INTEGER DEFAULT 0,
            rsi_divergence INTEGER DEFAULT 0, atr_trailing_stop INTEGER DEFAULT 0,
            rsi_14 REAL, macd_hist REAL, bb_upper REAL, bb_lower REAL,
            ma_20 REAL, ma_50 REAL, ma_200 REAL, atr_14 REAL, vol_ratio REAL,
            entry_score REAL, exit_risk REAL, signal_detail TEXT,
            updated_at TEXT DEFAULT (datetime('now')),
            UNIQUE(ticker, date)
        );
        CREATE INDEX IF NOT EXISTS idx_tech_ticker_date  ON tech_signals(ticker, date DESC);
        CREATE INDEX IF NOT EXISTS idx_market_env_date   ON market_environment(date DESC);
        CREATE TABLE IF NOT EXISTS factor_max_history (
            id              INTEGER PRIMARY KEY AUTOINCREMENT,
            date            TEXT    NOT NULL,
            market          TEXT    NOT NULL,
            factor_name     TEXT    NOT NULL,
            factor_label    TEXT,
            max_ret         REAL,
            max_date        TEXT,
            days_ago        INTEGER,
            avg_ret_20d     REAL,
            momentum_score  REAL,
            stock_count     INTEGER,
            top_stocks      TEXT,
            lottery_stocks  TEXT,
            updated_at      TEXT DEFAULT (datetime('now')),
            UNIQUE(date, market, factor_name)
        );
        CREATE INDEX IF NOT EXISTS idx_factor_max_date ON factor_max_history(date DESC, market, momentum_score DESC);
        """)


def get_conn() -> sqlite3.Connection:
    _init_db()
    conn = sqlite3.connect(CHIP_DB)
    conn.row_factory = sqlite3.Row
    return conn


@_cache
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


@_cache
def load_score_history(ticker: str, days: int = 60) -> pd.DataFrame:
    conn = get_conn()
    df = pd.read_sql("""
        SELECT * FROM chip_scores
        WHERE ticker=?
        ORDER BY date DESC LIMIT ?
    """, conn, params=(ticker, days))
    conn.close()
    return df.sort_values("date")


@_cache
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


@_cache
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


@_cache
def load_large_holders(ticker: str) -> pd.DataFrame:
    conn = get_conn()
    df = pd.read_sql("""
        SELECT filed_date, form_type, filer_name
        FROM large_holders WHERE ticker=?
        ORDER BY filed_date DESC LIMIT 20
    """, conn, params=(ticker,))
    conn.close()
    return df


@_cache
def load_market_pulse() -> dict:
    conn = get_conn()
    row = conn.execute("""
        SELECT date, pc_ratio, pc_ma20, pc_zscore_20
        FROM options_sentiment WHERE scope='equity'
        ORDER BY date DESC LIMIT 1
    """).fetchone()
    conn.close()
    return dict(row) if row else {}


def load_institutional_holders(ticker: str) -> list:
    """
    查詢前 10 大機構持股（來自 yfinance 13F 資料）。
    回傳 [{"institution": str, "shares_M": float, "value_B": float}]
    用於大型股（13D/13G 觸發門檻 5% 幾乎不可能達到時的替代顯示）。
    """
    conn = get_conn()
    rows = conn.execute("""
        SELECT institution, shares_held, value_usd
        FROM institutional_holders
        WHERE ticker = ?
        GROUP BY institution
        HAVING report_date = MAX(report_date)
        ORDER BY shares_held DESC
        LIMIT 10
    """, (ticker,)).fetchall()
    conn.close()

    result = []
    for institution, shares_held, value_usd in rows:
        result.append({
            "institution": institution or "Unknown",
            "shares_M":    round((shares_held or 0) / 1e6, 1),
            "value_B":     round((value_usd  or 0) / 1e9, 2),
        })
    return result


def load_latest_prices() -> dict:
    """
    每支股票最新兩天收盤價，計算漲跌幅。
    回傳 {ticker: {"price": float, "price_chg": float}}
    """
    conn = get_conn()
    rows = conn.execute("""
        SELECT ticker, date, close
        FROM (
            SELECT ticker, date, close,
                   ROW_NUMBER() OVER (PARTITION BY ticker ORDER BY date DESC) AS rn
            FROM daily_prices
        )
        WHERE rn <= 2
        ORDER BY ticker, date DESC
    """).fetchall()
    conn.close()

    from collections import defaultdict
    by_ticker = defaultdict(list)
    for ticker, _, close in rows:
        by_ticker[ticker].append(close)

    result = {}
    for ticker, closes in by_ticker.items():
        price = round(closes[0], 2)
        price_chg = round((closes[0] / closes[1] - 1) * 100, 2) if len(closes) > 1 else 0.0
        result[ticker] = {"price": price, "price_chg": price_chg}
    return result


@_cache
def load_market_env() -> dict:
    """最新一筆市場環境指標（VIX / 10Y / Mag7 / 板塊 ETF）"""
    conn = get_conn()
    row = conn.execute("""
        SELECT * FROM market_environment ORDER BY date DESC LIMIT 1
    """).fetchone()
    conn.close()
    if not row:
        return {}
    import json
    d = dict(row)
    if d.get("mag7_deviations"):
        try:
            d["mag7_deviations"] = json.loads(d["mag7_deviations"])
        except Exception:
            d["mag7_deviations"] = {}
    return d


@_cache
def load_tech_signal(ticker: str) -> dict:
    """單支股票最新一筆技術面信號"""
    conn = get_conn()
    row = conn.execute("""
        SELECT * FROM tech_signals
        WHERE ticker=?
        ORDER BY date DESC LIMIT 1
    """, (ticker,)).fetchone()
    conn.close()
    return dict(row) if row else {}


@_cache
def load_tech_signals(as_of: str = None) -> pd.DataFrame:
    """每支股票最新一筆技術面信號（entry_score / exit_risk + 各旗標）"""
    conn = get_conn()
    if as_of:
        q = """
            SELECT t.*
            FROM tech_signals t
            INNER JOIN (
                SELECT ticker, MAX(date) AS max_date
                FROM tech_signals WHERE date<=? GROUP BY ticker
            ) m ON t.ticker=m.ticker AND t.date=m.max_date
        """
        df = pd.read_sql(q, conn, params=(as_of,))
    else:
        q = """
            SELECT t.*
            FROM tech_signals t
            INNER JOIN (
                SELECT ticker, MAX(date) AS max_date
                FROM tech_signals GROUP BY ticker
            ) m ON t.ticker=m.ticker AND t.date=m.max_date
        """
        df = pd.read_sql(q, conn)
    conn.close()
    return df


@_cache
def load_factor_max(market: str = "US") -> list[dict]:
    """
    今日最新一批 Factor MAX 結果（依 momentum_score 降序）。
    回傳 list[dict]，每個 dict 代表一個因子組合。
    """
    import json as _json
    conn = get_conn()
    rows = conn.execute("""
        SELECT factor_name, factor_label, max_ret, max_date, days_ago,
               avg_ret_20d, momentum_score, stock_count, top_stocks, lottery_stocks
        FROM factor_max_history
        WHERE market = ?
          AND date = (SELECT MAX(date) FROM factor_max_history WHERE market = ?)
        ORDER BY momentum_score DESC
    """, (market, market)).fetchall()
    conn.close()
    result = []
    for row in rows:
        d = dict(row)
        d["top_stocks"]    = _json.loads(d["top_stocks"]    or "[]")
        d["lottery_stocks"] = _json.loads(d["lottery_stocks"] or "[]")
        result.append(d)
    return result


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
