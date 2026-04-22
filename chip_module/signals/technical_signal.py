"""
chip_module/signals/technical_signal.py
技術面進出場信號引擎

generate_signals(df) — 輸入 OHLCV DataFrame，輸出進場信心分數與出場風險分數。

進場評分（Entry_Score 0–100）：
  ma_aligned   25pts  股價在 200MA 之上且 20MA > 50MA > 200MA（多頭排列）
  rsi_reversal 20pts  RSI(14) 近 5 日曾 < 35，目前回升至 ≥ 40
  macd_golden  20pts  MACD 柱狀體由負轉正
  bb_breakout  25pts  收盤突破布林上軌 + 量能 > 20日均量 1.5 倍
  pattern      10pts  偵測到雙重底或 VCP（加分項）

出場風險（Exit_Risk 0–100）：
  hard_stop       40pts  從近 20 日高點回落 > 7%（硬性止損代理指標）
  below_20ma_3d   30pts  連續 3 日收盤 < 20MA
  rsi_divergence  20pts  頂背離（價格新高但 RSI 未新高）或 RSI > 75 後掉頭
  atr_trailing    10pts  價格跌破 ATR(14) × 2 動態止盈線
"""

import json
import logging
import numpy as np
import pandas as pd
from datetime import date
from typing import Optional

log = logging.getLogger(__name__)


# ── 指標計算函式 ─────────────────────────────────────────────────────

def _rsi(close: pd.Series, period: int = 14) -> pd.Series:
    delta = close.diff()
    gain  = delta.clip(lower=0).ewm(com=period - 1, adjust=False).mean()
    loss  = (-delta.clip(upper=0)).ewm(com=period - 1, adjust=False).mean()
    rs    = gain / loss.replace(0, np.nan)
    return 100 - (100 / (1 + rs))


def _macd(close: pd.Series):
    ema12 = close.ewm(span=12, adjust=False).mean()
    ema26 = close.ewm(span=26, adjust=False).mean()
    line  = ema12 - ema26
    sig   = line.ewm(span=9, adjust=False).mean()
    return line, sig, line - sig


def _bollinger(close: pd.Series, period: int = 20, k: float = 2.0):
    ma  = close.rolling(period).mean()
    std = close.rolling(period).std(ddof=0)
    return ma + k * std, ma, ma - k * std


def _atr(high: pd.Series, low: pd.Series, close: pd.Series, period: int = 14) -> pd.Series:
    prev_close = close.shift(1)
    tr = pd.concat([
        high - low,
        (high - prev_close).abs(),
        (low  - prev_close).abs(),
    ], axis=1).max(axis=1)
    return tr.ewm(com=period - 1, adjust=False).mean()


def _double_bottom(close: pd.Series, window: int = 30, tol: float = 0.03) -> bool:
    """近 window 根 K 線內是否出現雙重底型態"""
    if len(close) < window + 2:
        return False
    seg = close.iloc[-window:].reset_index(drop=True)
    lows = [
        (i, float(seg.iloc[i]))
        for i in range(1, len(seg) - 1)
        if seg.iloc[i] < seg.iloc[i - 1] and seg.iloc[i] < seg.iloc[i + 1]
    ]
    if len(lows) < 2:
        return False
    for j in range(len(lows) - 1):
        p1, p2 = lows[j][1], lows[j + 1][1]
        if abs(p1 - p2) / max(p1, p2) <= tol:
            if float(seg.iloc[-1]) > max(p1, p2) * 1.02:
                return True
    return False


def _vcp(close: pd.Series, high: pd.Series, low: pd.Series, window: int = 20) -> bool:
    """波動收斂（VCP）：近 window 根 K 線振幅逐步縮小"""
    if len(close) < window + 2:
        return False
    seg_h = high.iloc[-window:].reset_index(drop=True)
    seg_l = low.iloc[-window:].reset_index(drop=True)
    q = max(window // 4, 3)
    ranges = [
        float(seg_h.iloc[i * q:(i + 1) * q].max() - seg_l.iloc[i * q:(i + 1) * q].min())
        for i in range(4)
    ]
    return all(ranges[i] > ranges[i + 1] for i in range(3)) and ranges[-1] > 0


# ── 主函式 ──────────────────────────────────────────────────────────

def generate_signals(df: pd.DataFrame) -> dict:
    """
    Parameters
    ----------
    df : DataFrame，需含欄位 date / open / high / low / close / volume

    Returns
    -------
    dict 含以下 keys：
        entry_score, exit_risk                   # 0–100 綜合分數
        ma_aligned, rsi_reversal, macd_golden,   # 進場指標旗標（0/1）
        bb_breakout, double_bottom, vcp_pattern
        hard_stop, below_20ma_3d, rsi_divergence, atr_trailing_stop
        rsi_14, macd_hist, bb_upper, bb_lower,   # 原始數值（透明化）
        ma_20, ma_50, ma_200, atr_14, vol_ratio
        error                                    # 非 None 時表示計算失敗
    """
    _null = {
        "entry_score": None, "exit_risk": None,
        "ma_aligned": 0, "rsi_reversal": 0, "macd_golden": 0,
        "bb_breakout": 0, "double_bottom": 0, "vcp_pattern": 0,
        "hard_stop": 0, "below_20ma_3d": 0, "rsi_divergence": 0,
        "atr_trailing_stop": 0,
        "rsi_14": None, "macd_hist": None,
        "bb_upper": None, "bb_lower": None,
        "ma_20": None, "ma_50": None, "ma_200": None,
        "atr_14": None, "vol_ratio": None,
        "error": None,
    }

    if df is None or df.empty:
        return {**_null, "error": "empty_dataframe"}

    required = {"close", "high", "low", "volume"}
    missing  = required - set(df.columns)
    if missing:
        return {**_null, "error": f"missing_columns:{','.join(missing)}"}

    if len(df) < 26:
        return {**_null, "error": "insufficient_rows"}

    try:
        df = df.copy().sort_values("date").reset_index(drop=True)

        # 補缺失值（前向填充，不影響訓練期數據）
        for col in ["open", "high", "low", "close", "volume"]:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors="coerce").ffill()

        close  = df["close"]
        high   = df["high"]
        low    = df["low"]
        volume = df["volume"]

        # 計算各指標 Series
        ma20  = close.rolling(20).mean()
        ma50  = close.rolling(50).mean()
        ma200 = close.rolling(200).mean()
        rsi_s = _rsi(close, 14)
        _, _, macd_hist_s = _macd(close)
        bb_up, _, bb_lo   = _bollinger(close, 20, 2.0)
        atr_s = _atr(high, low, close, 14)
        vol20 = volume.rolling(20).mean()

        # 最新一根的數值
        c     = float(close.iloc[-1])
        ma20v = float(ma20.iloc[-1])  if pd.notna(ma20.iloc[-1])  else None
        ma50v = float(ma50.iloc[-1])  if pd.notna(ma50.iloc[-1])  else None
        ma200v= float(ma200.iloc[-1]) if pd.notna(ma200.iloc[-1]) else None
        rsi_v = float(rsi_s.iloc[-1]) if pd.notna(rsi_s.iloc[-1]) else None
        hist_v= float(macd_hist_s.iloc[-1]) if pd.notna(macd_hist_s.iloc[-1]) else None
        bbup_v= float(bb_up.iloc[-1]) if pd.notna(bb_up.iloc[-1]) else None
        bblo_v= float(bb_lo.iloc[-1]) if pd.notna(bb_lo.iloc[-1]) else None
        atr_v = float(atr_s.iloc[-1]) if pd.notna(atr_s.iloc[-1]) else None
        vol_v = float(volume.iloc[-1])
        avg_v = float(vol20.iloc[-1])  if pd.notna(vol20.iloc[-1]) else None
        vol_ratio = round(vol_v / avg_v, 2) if avg_v and avg_v > 0 else None

        # ── 進場指標 ────────────────────────────────────────────────

        # 1. 多頭排列（需要足夠歷史）
        ma_aligned = bool(
            ma200v and ma50v and ma20v
            and c > ma200v
            and ma20v > ma50v > ma200v
        )

        # 2. RSI 超賣回升：近 5 根曾 ≤ 35 且現在 ≥ 40
        rsi_reversal = False
        if rsi_v is not None and len(rsi_s) >= 5:
            rsi_recent = rsi_s.iloc[-5:-1].dropna()
            rsi_reversal = bool(rsi_v >= 40 and (rsi_recent <= 35).any())

        # 3. MACD 柱由負轉正
        macd_golden = False
        if hist_v is not None and len(macd_hist_s) >= 2:
            prev = macd_hist_s.iloc[-2]
            macd_golden = bool(pd.notna(prev) and prev < 0 and hist_v > 0)

        # 4. 布林上軌突破 + 量能放大
        bb_breakout = bool(
            bbup_v is not None
            and vol_ratio is not None
            and c > bbup_v
            and vol_ratio >= 1.5
        )

        # 5. 型態加分
        double_bottom = False
        vcp_pattern   = False
        try:
            w = min(30, len(close) - 2)
            if w >= 10:
                double_bottom = _double_bottom(close, window=w)
        except Exception:
            pass
        try:
            w = min(20, len(close) - 2)
            if w >= 8:
                vcp_pattern = _vcp(close, high, low, window=w)
        except Exception:
            pass

        # ── 出場指標 ────────────────────────────────────────────────

        # 6. 硬性止損代理：從近 20 日高點下跌 > 7%
        n20   = min(20, len(high))
        peak  = float(high.iloc[-n20:].max())
        hard_stop = bool(c < peak * 0.93)

        # 7. 連續 3 日收盤 < 20MA
        below_20ma_3d = False
        if ma20v is not None and len(close) >= 3:
            pairs = list(zip(close.iloc[-3:], ma20.iloc[-3:]))
            below_20ma_3d = all(
                pd.notna(ma) and float(pr) < float(ma)
                for pr, ma in pairs
            )

        # 8. 頂背離 / RSI 超買後回落
        rsi_divergence = False
        if rsi_v is not None and len(close) >= 10:
            # 方法A：RSI > 75 且連續 2 根下降
            if rsi_v >= 75 and len(rsi_s) >= 3:
                rsi_divergence = bool(
                    pd.notna(rsi_s.iloc[-2])
                    and rsi_s.iloc[-1] < rsi_s.iloc[-2] < rsi_s.iloc[-3]
                )
            # 方法B：近 10 根價格新高但 RSI 未新高（頂背離）
            if not rsi_divergence:
                c_max10   = float(close.iloc[-10:].max())
                rsi_max10 = float(rsi_s.iloc[-10:].dropna().max()) if len(rsi_s.iloc[-10:].dropna()) > 0 else 0
                price_new_high = c >= c_max10 * 0.999
                rsi_no_new_high = rsi_v is not None and rsi_v < rsi_max10 * 0.97
                rsi_divergence = bool(price_new_high and rsi_no_new_high)

        # 9. ATR 動態止盈：近 20 日最高收盤 - 2×ATR
        atr_trailing_stop = False
        if atr_v is not None and len(close) >= 5:
            trailing_ref = float(high.iloc[-20:].max()) if len(high) >= 20 else float(high.max())
            atr_trailing_stop = bool(c < trailing_ref - 2 * atr_v)

        # ── 計算分數 ────────────────────────────────────────────────

        entry_score = (
            (25 if ma_aligned   else 0)
            + (20 if rsi_reversal else 0)
            + (20 if macd_golden  else 0)
            + (25 if bb_breakout  else 0)
            + (10 if (double_bottom or vcp_pattern) else 0)
        )

        exit_risk = (
            (40 if hard_stop         else 0)
            + (30 if below_20ma_3d   else 0)
            + (20 if rsi_divergence  else 0)
            + (10 if atr_trailing_stop else 0)
        )

        return {
            "entry_score":       min(100, entry_score),
            "exit_risk":         min(100, exit_risk),
            # 進場旗標
            "ma_aligned":        int(ma_aligned),
            "rsi_reversal":      int(rsi_reversal),
            "macd_golden":       int(macd_golden),
            "bb_breakout":       int(bb_breakout),
            "double_bottom":     int(double_bottom),
            "vcp_pattern":       int(vcp_pattern),
            # 出場旗標
            "hard_stop":         int(hard_stop),
            "below_20ma_3d":     int(below_20ma_3d),
            "rsi_divergence":    int(rsi_divergence),
            "atr_trailing_stop": int(atr_trailing_stop),
            # 原始指標數值（透明化）
            "rsi_14":            round(rsi_v, 1)   if rsi_v  is not None else None,
            "macd_hist":         round(hist_v, 3)  if hist_v is not None else None,
            "bb_upper":          round(bbup_v, 2)  if bbup_v is not None else None,
            "bb_lower":          round(bblo_v, 2)  if bblo_v is not None else None,
            "ma_20":             round(ma20v,  2)  if ma20v  is not None else None,
            "ma_50":             round(ma50v,  2)  if ma50v  is not None else None,
            "ma_200":            round(ma200v, 2)  if ma200v is not None else None,
            "atr_14":            round(atr_v,  2)  if atr_v  is not None else None,
            "vol_ratio":         vol_ratio,
            "error":             None,
        }

    except Exception as e:
        log.error(f"[technical_signal] generate_signals 失敗: {e}", exc_info=True)
        return {**_null, "error": str(e)}


# ── 批次計算並寫入 DB ────────────────────────────────────────────────

def run(tickers: list, as_of: str = None, db_path=None):
    """
    從 daily_prices 讀取 OHLCV → 計算技術信號 → 寫入 tech_signals table。
    """
    from ..db.schema import get_conn

    today = as_of or date.today().isoformat()
    conn  = get_conn(db_path) if db_path else get_conn()

    # 批次讀取所有所需 OHLCV（減少 DB round-trip）
    rows_df = pd.read_sql("""
        SELECT ticker, date, open, high, low, close, volume
        FROM daily_prices
        ORDER BY ticker, date ASC
    """, conn)

    for ticker in tickers:
        try:
            df = rows_df[rows_df["ticker"] == ticker].copy()
            sig = generate_signals(df)

            if sig.get("error"):
                log.warning(f"[tech_signal] {ticker}: {sig['error']}")
                continue

            detail = {
                k: sig[k] for k in [
                    "ma_aligned", "rsi_reversal", "macd_golden",
                    "bb_breakout", "double_bottom", "vcp_pattern",
                    "hard_stop", "below_20ma_3d", "rsi_divergence", "atr_trailing_stop",
                    "rsi_14", "macd_hist", "bb_upper", "bb_lower",
                    "ma_20", "ma_50", "ma_200", "atr_14", "vol_ratio",
                ]
            }

            conn.execute("""
                INSERT INTO tech_signals (
                    ticker, date,
                    ma_aligned, rsi_reversal, macd_golden,
                    bb_breakout, double_bottom, vcp_pattern,
                    hard_stop, below_20ma_3d, rsi_divergence, atr_trailing_stop,
                    rsi_14, macd_hist, bb_upper, bb_lower,
                    ma_20, ma_50, ma_200, atr_14, vol_ratio,
                    entry_score, exit_risk, signal_detail
                ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                ON CONFLICT(ticker, date) DO UPDATE SET
                    ma_aligned=excluded.ma_aligned,
                    rsi_reversal=excluded.rsi_reversal,
                    macd_golden=excluded.macd_golden,
                    bb_breakout=excluded.bb_breakout,
                    double_bottom=excluded.double_bottom,
                    vcp_pattern=excluded.vcp_pattern,
                    hard_stop=excluded.hard_stop,
                    below_20ma_3d=excluded.below_20ma_3d,
                    rsi_divergence=excluded.rsi_divergence,
                    atr_trailing_stop=excluded.atr_trailing_stop,
                    rsi_14=excluded.rsi_14,
                    macd_hist=excluded.macd_hist,
                    bb_upper=excluded.bb_upper,
                    bb_lower=excluded.bb_lower,
                    ma_20=excluded.ma_20,
                    ma_50=excluded.ma_50,
                    ma_200=excluded.ma_200,
                    atr_14=excluded.atr_14,
                    vol_ratio=excluded.vol_ratio,
                    entry_score=excluded.entry_score,
                    exit_risk=excluded.exit_risk,
                    signal_detail=excluded.signal_detail,
                    updated_at=datetime('now')
            """, (
                ticker, today,
                sig["ma_aligned"], sig["rsi_reversal"], sig["macd_golden"],
                sig["bb_breakout"], sig["double_bottom"], sig["vcp_pattern"],
                sig["hard_stop"], sig["below_20ma_3d"], sig["rsi_divergence"],
                sig["atr_trailing_stop"],
                sig["rsi_14"], sig["macd_hist"],
                sig["bb_upper"], sig["bb_lower"],
                sig["ma_20"], sig["ma_50"], sig["ma_200"],
                sig["atr_14"], sig["vol_ratio"],
                sig["entry_score"], sig["exit_risk"],
                json.dumps(detail),
            ))

            entry_icon = "🟢" if sig["entry_score"] >= 50 else ("🟡" if sig["entry_score"] >= 25 else "⚪")
            exit_icon  = "🔴" if sig["exit_risk"]   >= 50 else ("🟡" if sig["exit_risk"]   >= 25 else "⚪")
            log.info(
                f"[tech_signal] {ticker:6s} entry={sig['entry_score']:3.0f}{entry_icon} "
                f"exit={sig['exit_risk']:3.0f}{exit_icon}  "
                f"RSI={sig['rsi_14'] or '—'}  MA={sig['ma_aligned']} "
                f"MACD={sig['macd_golden']} BB={sig['bb_breakout']}"
            )

        except Exception as e:
            log.error(f"[tech_signal] {ticker} 失敗: {e}", exc_info=True)

    conn.commit()
    conn.close()
