"""
chip_module/signals/factor_max.py
Factor MAX 引擎

計算每個因子組合在過去 N 個交易日的「等權投資組合單日最大報酬」。

學術依據（Key Insight）：
  - 個股極端漲幅（MAX）→ 反轉（樂透效應，高估）
  - 因子組合極端漲幅（Factor MAX）→ 持續（系統性利多，市場注意力不足時尤強）

輸出：factor_max_history 表，每日一筆 per factor
"""

import json
import logging
import os
import sqlite3
from datetime import date, timedelta
from pathlib import Path
import pandas as pd

log = logging.getLogger(__name__)

_PROJ_ROOT = Path(__file__).resolve().parents[2]  # stock_lab2.0/
if os.path.isdir("/data"):
    CHIP_DB    = Path("/data/chip.db")
    FINMIND_DB = Path("/data/finmind_cache.db")
else:
    CHIP_DB    = _PROJ_ROOT / "chip_module" / "chip.db"
    FINMIND_DB = _PROJ_ROOT / "finmind_cache.db"

# ── US Factor Group 定義 ──────────────────────────────────────────────
# score-based: (label, score_col, threshold)
US_SCORE_GROUPS: dict[str, tuple] = {
    "institutional_buying": ("機構增持",   "institutional_score", 65),
    "insider_bullish":      ("內部人買進",  "insider_score",       65),
    "volume_surge":         ("量能爆發",   "volume_score",        65),
    "options_bullish":      ("選擇權看多", "options_flow_score",  65),
    "short_squeeze":        ("空頭壓縮",   "short_score",         65),
}

# sector-based: (label, [tickers])
US_SECTOR_GROUPS: dict[str, tuple] = {
    "ai_compute":    ("AI算力",   ["NVDA","AMD","INTC","QCOM","MRVL","AVGO","ARM","SMCI"]),
    "semieq":        ("半導體設備", ["AMAT","LRCX","KLAC","ASML","TER","ONTO","ENTG"]),
    "cloud_ai":      ("雲端AI",   ["MSFT","GOOGL","AMZN","META","ORCL","IBM"]),
    "cybersecurity": ("網路安全",  ["CRWD","PANW","FTNT","OKTA","ZS","S","CYBR"]),
    "ev":            ("電動車",    ["TSLA","NIO","XPEV","LI","RIVN"]),
    "saas":          ("SaaS",     ["CRM","NOW","SNOW","DDOG","MDB","TEAM","INTU"]),
    "fintech":       ("金融科技",  ["V","MA","PYPL","SQ","COIN"]),
}


# ── 美股 Factor MAX ────────────────────────────────────────────────────

def _get_score_tickers(score_col: str, threshold: float) -> list[str]:
    with sqlite3.connect(CHIP_DB) as conn:
        rows = conn.execute(f"""
            SELECT s.ticker FROM chip_scores s
            INNER JOIN (
                SELECT ticker, MAX(date) AS max_date FROM chip_scores GROUP BY ticker
            ) m ON s.ticker = m.ticker AND s.date = m.max_date
            WHERE s.{score_col} >= ?
        """, (threshold,)).fetchall()
    return [r[0] for r in rows]


def _portfolio_returns_us(tickers: list[str], days: int = 20) -> pd.Series:
    """等權投資組合日報酬 (%)，最近 days 個交易日"""
    if not tickers:
        return pd.Series(dtype=float)
    since = (date.today() - timedelta(days=days * 2 + 5)).isoformat()
    ph    = ",".join("?" * len(tickers))
    with sqlite3.connect(CHIP_DB) as conn:
        df = pd.read_sql(
            f"SELECT ticker, date, close FROM daily_prices WHERE ticker IN ({ph}) AND date >= ? ORDER BY ticker, date",
            conn, params=tickers + [since]
        )
    if df.empty:
        return pd.Series(dtype=float)
    pivot    = df.pivot(index="date", columns="ticker", values="close").sort_index()
    returns  = pivot.pct_change(fill_method=None).dropna(how="all").tail(days)
    if returns.empty:
        return pd.Series(dtype=float)
    return returns.mean(axis=1) * 100  # %


def _top_stocks_us(tickers: list[str], on_date: str, n: int = 3) -> list[str]:
    if not tickers:
        return []
    ph = ",".join("?" * len(tickers))
    with sqlite3.connect(CHIP_DB) as conn:
        rows = conn.execute(f"""
            SELECT p1.ticker,
                   (p1.close - p2.close) / p2.close * 100 AS ret
            FROM daily_prices p1
            JOIN daily_prices p2 ON p1.ticker = p2.ticker
            WHERE p1.date = ?
              AND p2.date = (
                  SELECT MAX(d) FROM (
                      SELECT date AS d FROM daily_prices
                      WHERE ticker = p1.ticker AND date < ?
                  )
              )
              AND p1.ticker IN ({ph})
            ORDER BY ret DESC LIMIT ?
        """, [on_date, on_date] + tickers + [n]).fetchall()
    return [r[0] for r in rows]


def _build_factor_record(fname: str, label: str, portfolio: pd.Series,
                          market: str, tickers: list[str]) -> dict | None:
    if portfolio.empty or len(portfolio) < 3:
        return None
    today_str = date.today().isoformat()
    max_ret   = float(portfolio.max())
    max_date  = str(portfolio.idxmax())
    avg_ret   = float(portfolio.mean())
    days_ago  = (date.today() - date.fromisoformat(max_date[:10])).days

    # 20 trading days ≈ 28 calendar days; use calendar-day denominator
    recency        = max(0.0, 1.0 - days_ago / 28)
    momentum_score = round(min(100.0, max(0.0, max_ret * 8)) * recency, 1)

    # individual stock lottery detection: stock-level MAX ret in last 5 days
    if market == "US":
        since5 = (date.today() - timedelta(days=10)).isoformat()
        ph = ",".join("?" * len(tickers))
        with sqlite3.connect(CHIP_DB) as conn:
            rows = conn.execute(f"""
                SELECT p1.ticker,
                       MAX((p1.close - p2.close) / p2.close * 100) AS max_ret
                FROM daily_prices p1
                JOIN daily_prices p2 ON p1.ticker = p2.ticker
                WHERE p1.date >= ?
                  AND p2.date = (
                      SELECT MAX(d) FROM (
                          SELECT date AS d FROM daily_prices
                          WHERE ticker = p1.ticker AND date < p1.date
                      )
                  )
                  AND p1.ticker IN ({ph})
                GROUP BY p1.ticker
                HAVING max_ret > 5
                ORDER BY max_ret DESC LIMIT 3
            """, [since5] + tickers).fetchall()
        lottery_stocks = [r[0] for r in rows]
        top_stocks     = _top_stocks_us(tickers, max_date[:10])
    else:
        lottery_stocks = []
        top_stocks     = tickers[:3]

    return {
        "date":           today_str,
        "market":         market,
        "factor_name":    fname,
        "factor_label":   label,
        "max_ret":        round(max_ret, 2),
        "max_date":       max_date[:10],
        "days_ago":       days_ago,
        "avg_ret_20d":    round(avg_ret, 2),
        "momentum_score": momentum_score,
        "stock_count":    len(tickers),
        "top_stocks":     json.dumps(top_stocks),
        "lottery_stocks": json.dumps(lottery_stocks),
    }


def compute_us_factor_max(days: int = 20) -> list[dict]:
    results = []

    # Score-based groups
    for fname, (label, col, thresh) in US_SCORE_GROUPS.items():
        try:
            tickers   = _get_score_tickers(col, thresh)
            if len(tickers) < 3:
                continue
            portfolio = _portfolio_returns_us(tickers, days=days)
            rec       = _build_factor_record(fname, label, portfolio, "US", tickers)
            if rec:
                results.append(rec)
        except Exception as e:
            log.warning(f"[factor_max] US {fname}: {e}")

    # Sector groups
    for fname, (label, tickers) in US_SECTOR_GROUPS.items():
        try:
            portfolio = _portfolio_returns_us(tickers, days=days)
            rec       = _build_factor_record(fname, label, portfolio, "US", tickers)
            if rec:
                results.append(rec)
        except Exception as e:
            log.warning(f"[factor_max] US sector {fname}: {e}")

    return results


# ── 台股 Factor MAX ───────────────────────────────────────────────────

def _load_tw_prices_from_cache() -> pd.DataFrame:
    """
    從 finmind_cache.db 讀取所有 daily 快照，重建價格時間序列。
    content 格式為 pandas orient='dict'：{column: {index: value}}
    """
    if not FINMIND_DB.exists():
        return pd.DataFrame()
    frames = []
    with sqlite3.connect(FINMIND_DB) as conn:
        rows = conn.execute(
            "SELECT sid, content FROM api_cache WHERE data_type='daily'"
        ).fetchall()
    for sid, content in rows:
        try:
            data = json.loads(content)
            if not isinstance(data, dict) or "close" not in data or "date" not in data:
                continue
            # pandas orient='dict' → reconstruct DataFrame
            df = pd.DataFrame(data)[["date", "close"]].copy()
            df["sid"]   = sid
            df["close"] = pd.to_numeric(df["close"], errors="coerce")
            frames.append(df[["sid", "date", "close"]])
        except Exception:
            continue
    if not frames:
        return pd.DataFrame()
    df = pd.concat(frames, ignore_index=True)
    df = df[df["close"] > 0].copy()
    df["date"] = pd.to_datetime(df["date"], errors="coerce")
    return df.dropna(subset=["date"])


def _load_tw_institutional_from_cache() -> pd.DataFrame:
    """
    讀取機構買賣超資料，重建每股外資/投信近期淨買超。
    institutional content 格式：pandas orient='dict'（{col: {idx: val}}）
    """
    if not FINMIND_DB.exists():
        return pd.DataFrame()
    frames = []
    with sqlite3.connect(FINMIND_DB) as conn:
        rows = conn.execute(
            "SELECT sid, content FROM api_cache WHERE data_type='institutional'"
        ).fetchall()
    for sid, content in rows:
        try:
            data = json.loads(content)
            if isinstance(data, dict) and "date" in data:
                df = pd.DataFrame(data)
            elif isinstance(data, list):
                df = pd.DataFrame(data)
            else:
                continue
            if df.empty:
                continue
            df["sid"] = sid
            df["date"] = pd.to_datetime(df["date"], errors="coerce")
            df["buy"]  = pd.to_numeric(df.get("buy",  0), errors="coerce").fillna(0)
            df["sell"] = pd.to_numeric(df.get("sell", 0), errors="coerce").fillna(0)
            df["net"]  = df["buy"] - df["sell"]
            frames.append(df[["sid", "date", "name", "net"]])
        except Exception:
            continue
    if not frames:
        return pd.DataFrame()
    return pd.concat(frames, ignore_index=True)


def _portfolio_returns_tw(prices: pd.DataFrame, sids: list[str],
                           days: int = 20) -> pd.Series:
    sub = prices[prices["sid"].isin(sids)].copy()
    if sub.empty:
        return pd.Series(dtype=float)
    pivot   = sub.pivot_table(index="date", columns="sid", values="close", aggfunc="last").sort_index()
    returns = pivot.pct_change().dropna(how="all").tail(days)
    if returns.empty:
        return pd.Series(dtype=float)
    return returns.mean(axis=1) * 100


def compute_tw_factor_max(days: int = 20) -> list[dict]:
    prices = _load_tw_prices_from_cache()
    if prices.empty:
        return []

    inst = _load_tw_institutional_from_cache()
    results = []

    if not inst.empty:
        cutoff  = pd.Timestamp.now() - pd.Timedelta(days=10)
        recent  = inst[inst["date"] >= cutoff]

        def _net_buyers(inst_name: str) -> list[str]:
            grp = recent[recent["name"].str.contains(inst_name, na=False)]
            net_by_sid = grp.groupby("sid")["net"].sum()
            return net_by_sid[net_by_sid > 0].index.tolist()

        foreign_sids = _net_buyers("Foreign_Investor")
        fund_sids    = _net_buyers("Investment_Trust")
        double_sids  = list(set(foreign_sids) & set(fund_sids))

        tw_groups = [
            ("tw_foreign_buying", "外資買超",  foreign_sids),
            ("tw_fund_buying",    "投信買超",   fund_sids),
            ("tw_double_strong",  "雙強",       double_sids),
        ]
        for fname, label, sids in tw_groups:
            if len(sids) < 3:
                continue
            try:
                portfolio = _portfolio_returns_tw(prices, sids, days=days)
                rec = _build_factor_record(fname, label, portfolio, "TW", sids)
                if rec:
                    results.append(rec)
            except Exception as e:
                log.warning(f"[factor_max] TW {fname}: {e}")

    # Market-wide (all sids in cache)
    all_sids = prices["sid"].unique().tolist()
    if len(all_sids) >= 5:
        try:
            portfolio = _portfolio_returns_tw(prices, all_sids, days=days)
            rec = _build_factor_record("tw_market", "大盤等權", portfolio, "TW", all_sids)
            if rec:
                results.append(rec)
        except Exception as e:
            log.warning(f"[factor_max] TW market: {e}")

    return results


# ── DB write ──────────────────────────────────────────────────────────

def write_factor_max(records: list[dict]) -> None:
    if not records:
        return
    with sqlite3.connect(CHIP_DB) as conn:
        conn.executemany("""
            INSERT OR REPLACE INTO factor_max_history
            (date, market, factor_name, factor_label,
             max_ret, max_date, days_ago, avg_ret_20d,
             momentum_score, stock_count, top_stocks, lottery_stocks)
            VALUES
            (:date, :market, :factor_name, :factor_label,
             :max_ret, :max_date, :days_ago, :avg_ret_20d,
             :momentum_score, :stock_count, :top_stocks, :lottery_stocks)
        """, records)
    log.info(f"[factor_max] wrote {len(records)} records")


def run(days: int = 20) -> None:
    log.info("[factor_max] US...")
    us = compute_us_factor_max(days=days)
    log.info(f"[factor_max] TW...")
    tw = compute_tw_factor_max(days=days)
    write_factor_max(us + tw)
    log.info(f"[factor_max] done — {len(us)} US + {len(tw)} TW")
