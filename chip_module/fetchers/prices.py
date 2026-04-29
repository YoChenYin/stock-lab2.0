"""
chip_module/fetchers/prices.py
抓取每日 OHLCV 並計算量能指標存入 daily_prices。
同時更新機構持倉 institutional_holders。
"""

import time
import sqlite3
import pandas as pd
import numpy as np
import yfinance as yf
from datetime import date, timedelta
from typing import List
from tqdm import tqdm

from ..db.schema import get_conn

BATCH_SIZE  = 50   # 每批下載的 ticker 數
BATCH_SLEEP = 3    # 批次之間等待秒數
RETRY_WAITS = [10, 30, 60]  # rate limit 時依序等待的秒數


# ── 技術指標計算 ──────────────────────────────────────────────────

def calc_obv(close: pd.Series, volume: pd.Series) -> pd.Series:
    direction = np.sign(close.diff()).fillna(0)
    return (direction * volume).cumsum()


def calc_cmf(high, low, close, volume, period=20) -> pd.Series:
    mfm = ((close - low) - (high - close)) / (high - low + 1e-9)
    mfv = mfm * volume
    return mfv.rolling(period).sum() / volume.rolling(period).sum()


def calc_mfi(high, low, close, volume, period=14) -> pd.Series:
    tp = (high + low + close) / 3
    raw_mf = tp * volume
    pos = raw_mf.where(tp > tp.shift(1), 0)
    neg = raw_mf.where(tp < tp.shift(1), 0)
    mfr = pos.rolling(period).sum() / (neg.rolling(period).sum() + 1e-9)
    return 100 - (100 / (1 + mfr))


# ── 批次下載 ──────────────────────────────────────────────────────

def _download_batch(batch: List[str], start: str) -> pd.DataFrame:
    """批次下載，rate limit 時自動 retry。"""
    for attempt, wait in enumerate([0] + RETRY_WAITS):
        if wait:
            print(f"[prices] rate limited，{wait}s 後重試 (attempt {attempt})...")
            time.sleep(wait)
        try:
            raw = yf.download(batch, start=start, progress=False, auto_adjust=True)
            return raw
        except Exception as e:
            if "RateLimit" in type(e).__name__ and attempt < len(RETRY_WAITS):
                continue
            print(f"[prices] batch 下載失敗: {e}")
            return pd.DataFrame()
    return pd.DataFrame()


def _extract_ticker(raw: pd.DataFrame, ticker: str, batch: List[str]) -> pd.DataFrame:
    """從批次結果中取出單一 ticker 的 DataFrame。"""
    if raw.empty:
        return pd.DataFrame()
    try:
        if len(batch) == 1:
            df = raw.copy()
            if isinstance(df.columns, pd.MultiIndex):
                df.columns = [c[0].lower() for c in df.columns]
            else:
                df.columns = [c.lower() for c in df.columns]
        else:
            if ticker not in raw.columns.get_level_values(1):
                return pd.DataFrame()
            df = raw.xs(ticker, axis=1, level=1).copy()
            df.columns = [c.lower() for c in df.columns]

        df.index = pd.to_datetime(df.index)
        df = df.sort_index().dropna(subset=["close"])
        return df
    except Exception:
        return pd.DataFrame()


# ── 主要 fetcher ──────────────────────────────────────────────────

def fetch_prices(tickers: List[str], lookback_days: int = 60, db_path=None):
    """
    批次下載最近 N 天的 OHLCV，計算技術指標後 upsert 進 daily_prices。
    每批 BATCH_SIZE 個 ticker，批次間休息 BATCH_SLEEP 秒。
    """
    conn = get_conn(db_path) if db_path else get_conn()
    start = (date.today() - timedelta(days=lookback_days)).isoformat()
    batches = [tickers[i:i + BATCH_SIZE] for i in range(0, len(tickers), BATCH_SIZE)]

    for batch in tqdm(batches, desc="[prices] downloading"):
        raw = _download_batch(batch, start)

        for ticker in batch:
            try:
                df = _extract_ticker(raw, ticker, batch)
                if df.empty:
                    print(f"[prices] {ticker}: 無資料，跳過")
                    continue

                obv             = calc_obv(df["close"], df["volume"])
                df["obv"]       = obv
                df["obv_signal"]= obv.ewm(span=20).mean()
                df["cmf_20"]    = calc_cmf(df["high"], df["low"], df["close"], df["volume"])
                df["mfi_14"]    = calc_mfi(df["high"], df["low"], df["close"], df["volume"])
                df["avg_vol_20"]= df["volume"].rolling(20).mean()
                df["vol_ratio"] = df["volume"] / (df["avg_vol_20"] + 1e-9)

                rows = [
                    (
                        ticker, dt.strftime("%Y-%m-%d"),
                        _f(row, "open"), _f(row, "high"), _f(row, "low"),
                        _f(row, "close"), _i(row, "volume"),
                        _f(row, "obv"), _f(row, "obv_signal"),
                        _f(row, "cmf_20"), _f(row, "mfi_14"),
                        _f(row, "avg_vol_20"), _f(row, "vol_ratio"),
                    )
                    for dt, row in df.iterrows()
                ]
                conn.executemany("""
                    INSERT INTO daily_prices
                        (ticker, date, open, high, low, close, volume,
                         obv, obv_signal, cmf_20, mfi_14, avg_vol_20, vol_ratio)
                    VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)
                    ON CONFLICT(ticker, date) DO UPDATE SET
                        close=excluded.close, volume=excluded.volume,
                        obv=excluded.obv, obv_signal=excluded.obv_signal,
                        cmf_20=excluded.cmf_20, mfi_14=excluded.mfi_14,
                        avg_vol_20=excluded.avg_vol_20, vol_ratio=excluded.vol_ratio
                """, rows)
                conn.commit()
                print(f"[prices] {ticker}: {len(rows)} 筆 upserted")

            except Exception as e:
                print(f"[prices] {ticker} 失敗: {e}")

        if batch is not batches[-1]:
            time.sleep(BATCH_SLEEP)

    conn.close()


def fetch_institutional(tickers: List[str], db_path=None):
    """
    從 yfinance 抓機構持倉，存入 institutional_holders。
    季度資料，建議每週跑一次即可。
    """
    conn = get_conn(db_path) if db_path else get_conn()
    today = date.today().isoformat()

    for ticker in tqdm(tickers, desc="[institutional]"):
        try:
            tk = yf.Ticker(ticker)
            holders = tk.institutional_holders
            if holders is None or holders.empty:
                continue

            rows = [
                (
                    ticker, today,
                    str(row.get("Holder", "")),
                    _safe(row.get("Shares")),
                    _safe(row.get("% Out")),
                    _safe(row.get("Value")),
                )
                for _, row in holders.iterrows()
            ]
            conn.executemany("""
                INSERT INTO institutional_holders
                    (ticker, report_date, institution, shares_held, pct_out, value_usd)
                VALUES (?,?,?,?,?,?)
                ON CONFLICT(ticker, report_date, institution) DO UPDATE SET
                    shares_held=excluded.shares_held,
                    pct_out=excluded.pct_out,
                    value_usd=excluded.value_usd
            """, rows)
            conn.commit()
            print(f"[institutional] {ticker}: {len(rows)} 筆 upserted")
            time.sleep(0.5)

        except Exception as e:
            print(f"[institutional] {ticker} 失敗: {e}")

    conn.close()


# ── helpers ───────────────────────────────────────────────────────

def _f(row, col):
    v = row.get(col)
    return float(v) if pd.notna(v) else None

def _i(row, col):
    v = row.get(col)
    return int(v) if pd.notna(v) else None

def _safe(v):
    try:
        return float(v) if pd.notna(v) else None
    except Exception:
        return None
