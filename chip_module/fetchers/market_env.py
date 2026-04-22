"""
chip_module/fetchers/market_env.py
每日抓取市場環境指標並寫入 market_environment table

指標：
  - VIX 恐慌指數（^VIX）
  - 10Y 美債殖利率（^TNX）+ 20 日趨勢斜率
  - Mag 7 相對 50MA 乖離率（AAPL/MSFT/NVDA/GOOGL/AMZN/META/TSLA）
  - 板塊 ETF 強弱（XLK/XLF/XLP 1日/5日漲跌幅）
"""

import json
import logging
import numpy as np
import yfinance as yf
from datetime import date, timedelta
from pathlib import Path

log = logging.getLogger(__name__)

MAG7 = ["AAPL", "MSFT", "NVDA", "GOOGL", "AMZN", "META", "TSLA"]


def _pct_change(series, periods: int) -> float | None:
    """計算 series 末尾 periods 期的漲跌幅（%）"""
    if len(series) < periods + 1:
        return None
    return round(float((series.iloc[-1] / series.iloc[-(periods + 1)] - 1) * 100), 2)


def fetch_market_env(as_of: str = None, db_path=None) -> dict:
    """
    抓取當日市場環境指標並寫入 DB。
    回傳 dict（方便 composite.py 使用 VIX 進行折扣調整）。
    """
    today = as_of or date.today().isoformat()

    row = {
        "date":               today,
        "vix":                None,
        "vix_ma5":            None,
        "vix_level":          None,   # 'low'|'medium'|'high'
        "tnx_10y":            None,
        "tnx_slope_20d":      None,
        "mag7_avg_deviation": None,
        "mag7_deviations":    None,   # JSON
        "mag7_risk":          None,   # 'ok'|'caution'
        "xlk_chg_1d":         None,
        "xlf_chg_1d":         None,
        "xlp_chg_1d":         None,
        "xlk_chg_5d":         None,
        "xlf_chg_5d":         None,
        "xlp_chg_5d":         None,
    }

    # ── VIX ──────────────────────────────────────────────────────────
    try:
        hist = yf.Ticker("^VIX").history(period="15d")
        if not hist.empty:
            vix = float(hist["Close"].iloc[-1])
            row["vix"] = round(vix, 2)
            row["vix_ma5"] = round(float(hist["Close"].tail(5).mean()), 2)
            row["vix_level"] = "high" if vix > 25 else ("medium" if vix >= 15 else "low")
            log.info(f"[market_env] VIX={vix:.2f} ({row['vix_level']})")
    except Exception as e:
        log.warning(f"[market_env] VIX 抓取失敗: {e}")

    # ── 10Y 美債殖利率 ────────────────────────────────────────────────
    try:
        hist = yf.Ticker("^TNX").history(period="35d")
        if not hist.empty:
            row["tnx_10y"] = round(float(hist["Close"].iloc[-1]), 3)
            if len(hist) >= 20:
                y = hist["Close"].tail(20).values.astype(float)
                x = np.arange(len(y))
                slope = float(np.polyfit(x, y, 1)[0])
                row["tnx_slope_20d"] = round(slope, 4)
            log.info(f"[market_env] 10Y={row['tnx_10y']:.3f}% slope={row['tnx_slope_20d']}")
    except Exception as e:
        log.warning(f"[market_env] 10Y 抓取失敗: {e}")

    # ── Mag 7 乖離率 ──────────────────────────────────────────────────
    devs = {}
    try:
        for ticker in MAG7:
            hist = yf.Ticker(ticker).history(period="80d")
            if len(hist) >= 50:
                price = float(hist["Close"].iloc[-1])
                ma50  = float(hist["Close"].tail(50).mean())
                devs[ticker] = round((price - ma50) / ma50 * 100, 2)
        if devs:
            avg_dev = round(float(np.mean(list(devs.values()))), 2)
            row["mag7_avg_deviation"] = avg_dev
            row["mag7_deviations"]    = json.dumps(devs)
            row["mag7_risk"]          = "caution" if avg_dev > 15 else "ok"
            log.info(f"[market_env] Mag7 avg deviation={avg_dev:.1f}% ({row['mag7_risk']})")
    except Exception as e:
        log.warning(f"[market_env] Mag7 抓取失敗: {e}")

    # ── 板塊 ETF ──────────────────────────────────────────────────────
    for sym in ["XLK", "XLF", "XLP"]:
        try:
            hist = yf.Ticker(sym).history(period="15d")
            key  = sym.lower()
            if len(hist) >= 2:
                row[f"{key}_chg_1d"] = _pct_change(hist["Close"], 1)
            if len(hist) >= 6:
                row[f"{key}_chg_5d"] = _pct_change(hist["Close"], 5)
        except Exception as e:
            log.warning(f"[market_env] {sym} 抓取失敗: {e}")

    log.info(f"[market_env] Sector 1d: XLK={row['xlk_chg_1d']} XLF={row['xlf_chg_1d']} XLP={row['xlp_chg_1d']}")

    # ── 寫入 DB ───────────────────────────────────────────────────────
    try:
        from ..db.schema import get_conn
        conn = get_conn(db_path) if db_path else get_conn()
        conn.execute("""
            INSERT INTO market_environment (
                date, vix, vix_ma5, vix_level,
                tnx_10y, tnx_slope_20d,
                mag7_avg_deviation, mag7_deviations, mag7_risk,
                xlk_chg_1d, xlf_chg_1d, xlp_chg_1d,
                xlk_chg_5d, xlf_chg_5d, xlp_chg_5d
            ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
            ON CONFLICT(date) DO UPDATE SET
                vix=excluded.vix, vix_ma5=excluded.vix_ma5,
                vix_level=excluded.vix_level,
                tnx_10y=excluded.tnx_10y,
                tnx_slope_20d=excluded.tnx_slope_20d,
                mag7_avg_deviation=excluded.mag7_avg_deviation,
                mag7_deviations=excluded.mag7_deviations,
                mag7_risk=excluded.mag7_risk,
                xlk_chg_1d=excluded.xlk_chg_1d,
                xlf_chg_1d=excluded.xlf_chg_1d,
                xlp_chg_1d=excluded.xlp_chg_1d,
                xlk_chg_5d=excluded.xlk_chg_5d,
                xlf_chg_5d=excluded.xlf_chg_5d,
                xlp_chg_5d=excluded.xlp_chg_5d,
                updated_at=datetime('now')
        """, (
            row["date"], row["vix"], row["vix_ma5"], row["vix_level"],
            row["tnx_10y"], row["tnx_slope_20d"],
            row["mag7_avg_deviation"], row["mag7_deviations"], row["mag7_risk"],
            row["xlk_chg_1d"], row["xlf_chg_1d"], row["xlp_chg_1d"],
            row["xlk_chg_5d"], row["xlf_chg_5d"], row["xlp_chg_5d"],
        ))
        conn.commit()
        conn.close()
        log.info(f"[market_env] 市場環境指標已寫入 DB，日期={today}")
    except Exception as e:
        log.error(f"[market_env] 寫入 DB 失敗: {e}")

    return row
