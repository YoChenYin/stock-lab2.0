"""
api/tw.py — 台股 routes

GET  /tw/          → 台股主頁面（族群熱圖 + 選股）
POST /tw/scan      → HTMX: 執行選股掃描，回傳結果 partial
GET  /tw/heatmap   → HTMX: 族群熱圖資料（Plotly JSON + 明細卡片）
GET  /tw/stock/{sid}/detail → HTMX: 個股展開（法說 + 策略回測）
"""

import os
import json
import datetime
import numpy as np
import pandas as pd
from functools import lru_cache
from concurrent.futures import ThreadPoolExecutor, as_completed
from fastapi import APIRouter, Request
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates

import sys
sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

from engine.wall_street_engine import WallStreetEngine
from engine.smart_money import calc_smart_money_score, calc_revenue_accel_score
from engine.rocket_detector import detect_coiling
from sector_data import STOCK_POOL, SECTOR_GROUPS

router = APIRouter()
templates = Jinja2Templates(directory="templates")


def _get_engine() -> WallStreetEngine:
    return WallStreetEngine()


def _prefetch_stocks(sids: list, max_workers: int = 10) -> dict:
    """
    並行預取多支股票資料，回傳 {sid: (df, rev)}。
    每個 worker 建立獨立 engine，避免執行緒競爭。
    max_workers=10 在 FinMind 免費版不會觸發 rate-limit。
    """
    results: dict = {}

    def _fetch(sid: str):
        try:
            return sid, _get_engine().fetch_data(sid)
        except Exception as e:
            print(f"[prefetch] {sid} error: {e}")
            return sid, (pd.DataFrame(), pd.DataFrame())

    with ThreadPoolExecutor(max_workers=max_workers) as pool:
        futures = {pool.submit(_fetch, sid): sid for sid in sids}
        for fut in as_completed(futures):
            sid, data = fut.result()
            results[sid] = data

    return results


# ── Screener helpers ──────────────────────────────────────────────────

def _streak(series) -> int:
    vals = series.dropna().values
    if len(vals) == 0:
        return 0
    sign = 1 if vals[-1] > 0 else (-1 if vals[-1] < 0 else 0)
    if sign == 0:
        return 0
    count = 0
    for v in reversed(vals):
        if (v > 0) == (sign > 0):
            count += 1
        else:
            break
    return sign * count


def _has_flipped_to_buy(series, max_days: int = 3) -> bool:
    vals = series.dropna().values
    if len(vals) < max_days + 2:
        return False
    s = _streak(series)
    if not (1 <= s <= max_days):
        return False
    return vals[-(s + 1)] < 0


def _calc_cost_windows(df, col) -> dict:
    """計算法人加權平均持倉成本，分 20/60/120 日窗口。
    邏輯：近 N 日內，僅統計淨買超那幾天的成交量加權均價。
    """
    result = {}
    for days in (20, 60, 120):
        sub = df.tail(days)
        buy = sub[sub[col] > 0]
        if buy.empty:
            result[days] = 0.0
        else:
            result[days] = round(
                float((buy["close"] * buy[col]).sum() / (buy[col].sum() + 1e-9)), 1
            )
    return result


def _chip_battle(f_streak: int, it_streak: int,
                 f5: float, it5: float) -> dict:
    """
    判斷外資與投信誰勝出。
    同向：直接說明方向。
    對立：比較 5 日淨買超金額大小決定主導方。
    """
    if f_streak > 0 and it_streak > 0:
        return {"label": "外資+投信同買", "color": "green", "dominant": None}
    if f_streak < 0 and it_streak < 0:
        return {"label": "外資+投信同賣", "color": "red",   "dominant": None}
    if f_streak > 0 and it_streak < 0:
        dominant = "外資主導" if abs(f5) >= abs(it5) else "投信反壓"
        return {"label": dominant, "color": "blue" if abs(f5) >= abs(it5) else "yellow", "dominant": "f"}
    if f_streak < 0 and it_streak > 0:
        dominant = "投信主導" if abs(it5) >= abs(f5) else "外資反壓"
        return {"label": dominant, "color": "blue" if abs(it5) >= abs(f5) else "yellow", "dominant": "it"}
    return {"label": "觀望", "color": "gray", "dominant": None}


def _run_unified_scan(stock_map_items):
    """
    掃描所有股票，輸出統一格式的結果列表。
    整合原先 4 張表：營收加速、籌碼加速、翻轉訊號、雙強標的。
    """
    results = []
    prefetched = _prefetch_stocks([sid for sid, _ in stock_map_items])

    for sid, name in stock_map_items:
        try:
            df, rev = prefetched.get(sid, (pd.DataFrame(), pd.DataFrame()))
            if df.empty:
                continue

            df = df.copy()
            df["ma5"]  = df["close"].rolling(5).mean()
            df["ma10"] = df["close"].rolling(10).mean()
            c_p   = float(df["close"].iloc[-1])
            c_prev = float(df["close"].iloc[-2]) if len(df) >= 2 else c_p
            m20   = float(df["ma20"].iloc[-1])
            m5    = float(df["ma5"].iloc[-1])
            m10   = float(df["ma10"].iloc[-1])
            is_aligned = bool(m5 > m10 > m20 and c_p > m5)

            # 股價漲跌
            price_chg = round((c_p - c_prev) / (c_prev + 1e-9) * 100, 2)

            sms    = calc_smart_money_score(df) if "f_net" in df.columns else {"score": 0}
            raccel = calc_revenue_accel_score(rev)
            coil   = detect_coiling(df)

            # ── 壓力位 / 支撐位 ──────────────────────────────
            hc = "high" if "high" in df.columns else "max"
            lc = "low"  if "low"  in df.columns else "min"
            if hc in df.columns and lc in df.columns:
                resistance = round(float(df[hc].tail(20).max()), 1)
                support    = round(float(max(df[lc].tail(20).min(), m20)), 1)
            else:
                resistance = support = 0.0

            # ── 外資 / 投信成本（20/60/120 日）──────────────────
            _empty_costs = {20: 0.0, 60: 0.0, 120: 0.0}
            f_costs  = _calc_cost_windows(df, "f_net")  if "f_net"  in df.columns else _empty_costs
            it_costs = _calc_cost_windows(df, "it_net") if "it_net" in df.columns else _empty_costs

            # ── 集中度趨勢（5D vs 20D，並計算加速幅度）────────
            c5 = c10 = c20 = 0.0
            conc_accel = False
            if len(df) >= 20:
                vol5  = df["trading_volume"].tail(5).sum()  + 1e-9
                vol10 = df["trading_volume"].tail(10).sum() + 1e-9
                vol20 = df["trading_volume"].tail(20).sum() + 1e-9
                c5  = (df["f_net"].tail(5).sum()  + df["it_net"].tail(5).sum())  / vol5  * 100
                c10 = (df["f_net"].tail(10).sum() + df["it_net"].tail(10).sum()) / vol10 * 100
                c20 = (df["f_net"].tail(20).sum() + df["it_net"].tail(20).sum()) / vol20 * 100
                conc_accel = bool(c5 > c10 > c20)   # 全面加速才算真加速

            conc_delta = round(float(c5 - c20), 2)   # 正 = 近期比長期更積極
            conc_arrow = "↑" if conc_delta > 0.5 else ("↓" if conc_delta < -0.5 else "→")

            # ── 外資 / 投信連買連賣 ─────────────────────────
            f_streak  = _streak(df["f_net"])  if "f_net"  in df.columns else 0
            it_streak = _streak(df["it_net"]) if "it_net" in df.columns else 0
            f5_net    = float(df["f_net"].tail(5).sum())  if "f_net"  in df.columns else 0.0
            it5_net   = float(df["it_net"].tail(5).sum()) if "it_net" in df.columns else 0.0
            battle    = _chip_battle(f_streak, it_streak, f5_net, it5_net)

            # ── 翻轉偵測 ─────────────────────────────────────
            f_flip  = _has_flipped_to_buy(df["f_net"])  if "f_net"  in df.columns else False
            it_flip = _has_flipped_to_buy(df["it_net"]) if "it_net" in df.columns else False

            # ── 近三個月 YoY ─────────────────────────────────
            yoy_3m = raccel.get("yoy_trend", [0, 0, 0])   # [2月前, 1月前, 最新]

            # ── 營收連三月遞增 ────────────────────────────────
            rev_consec = False
            if not rev.empty and len(rev) >= 3:
                v = rev.sort_values("date")["revenue"].tail(3).values
                rev_consec = bool(len(v) == 3 and v[2] > v[1] > v[0])

            # ── 進場硬性排除（空頭特徵直接跳過）────────────────
            # 空頭排列：MA5 < MA10 < MA20，趨勢明確向下
            if m5 < m10 < m20:
                continue
            # 籌碼加速流出：近期法人賣速明顯大於長期
            if conc_delta < -1.0:
                continue
            # 雙月營收衰退（同時觸發出場警示，不應出現在進場）
            if len(yoy_3m) >= 2 and yoy_3m[-1] < 0 and yoy_3m[-2] < 0:
                continue

            # ── 標籤 ─────────────────────────────────────────
            tags = []
            if rev_consec and conc_accel:
                tags.append("雙強")
            if f_flip or it_flip:
                tags.append("翻轉↑")
            if coil["is_coiling"]:
                tags.append("蓄力")
            if raccel["is_accelerating"]:
                tags.append("營收↑")
            if conc_accel and not (rev_consec and conc_accel):
                tags.append("籌碼↑")
            # 單月營收衰退只顯示警告標籤，不作為進場依據
            if yoy_3m and yoy_3m[-1] < 0:
                tags.append("⚠️營收衰退")

            # 過濾：至少一個正向標籤（排除純警告標籤），或 SMS >= 50
            positive_tags = [t for t in tags if t != "⚠️營收衰退"]
            if not positive_tags and sms["score"] < 50:
                continue

            results.append({
                "sid":        sid,
                "name":       name,
                "price":      round(c_p, 2),
                "price_chg":  price_chg,
                # 近三個月 YoY [2月前, 1月前, 最新]
                "yoy_3m":     [round(float(y), 1) for y in yoy_3m],
                # 集中度趨勢
                "conc_5d":    round(float(c5), 2),
                "conc_20d":   round(float(c20), 2),
                "conc_delta": conc_delta,
                "conc_arrow": conc_arrow,
                # 籌碼對決
                "f_streak":   int(f_streak),
                "it_streak":  int(it_streak),
                "battle":     battle,
                # 成本與技術位（20/60/120 日）
                "f_cost":     f_costs[20],
                "f_cost_60":  f_costs[60],
                "f_cost_120": f_costs[120],
                "it_cost":    it_costs[20],
                "it_cost_60":  it_costs[60],
                "it_cost_120": it_costs[120],
                "resistance": resistance,
                "support":    support,
                "ma20":       round(m20, 1),
                # 均線多頭排列
                "is_aligned": is_aligned,
                "tags":       tags,
            })
        except Exception as e:
            print(f"[scan] {sid} error: {e}")
            continue

    # 排序：有「雙強」先，其次按集中度加速幅度
    return sorted(results,
                  key=lambda x: (("雙強" in x["tags"]), x["conc_delta"]),
                  reverse=True)


def _has_flipped_to_sell(series, max_days: int = 3) -> bool:
    """近 max_days 天內由買超翻轉為賣超"""
    vals = series.dropna().values
    if len(vals) < max_days + 2:
        return False
    s = _streak(series)
    if not (-max_days <= s <= -1):
        return False
    return vals[-(abs(s) + 1)] > 0


def _run_exit_scan(stock_map_items):
    """
    出場警示掃描。
    觸發條件（符合任一即列入）：
      - 外資或投信由買轉賣（3日內翻轉）
      - 外資+投信同步賣超 3 天以上
      - 股價跌破五日線（close < MA5）
      - 空頭排列：MA5 < MA10 < MA20
      - 籌碼集中度 5D 低於 20D 且擴大（加速賣出）
      - 營收連三月衰退
    """
    results = []
    prefetched = _prefetch_stocks([sid for sid, _ in stock_map_items])

    for sid, name in stock_map_items:
        try:
            df, rev = prefetched.get(sid, (pd.DataFrame(), pd.DataFrame()))
            if df.empty or len(df) < 20:
                continue

            df = df.copy()
            df["ma5"]  = df["close"].rolling(5).mean()
            df["ma10"] = df["close"].rolling(10).mean()
            c_p   = float(df["close"].iloc[-1])
            c_prev = float(df["close"].iloc[-2]) if len(df) >= 2 else c_p
            price_chg = round((c_p - c_prev) / (c_prev + 1e-9) * 100, 2)
            m5  = float(df["ma5"].iloc[-1])
            m10 = float(df["ma10"].iloc[-1])
            m20 = float(df["ma20"].iloc[-1])

            f_streak  = _streak(df["f_net"])  if "f_net"  in df.columns else 0
            it_streak = _streak(df["it_net"]) if "it_net" in df.columns else 0

            # 集中度
            vol5  = df["trading_volume"].tail(5).sum()  + 1e-9
            vol20 = df["trading_volume"].tail(20).sum() + 1e-9
            c5  = (df["f_net"].tail(5).sum()  + df["it_net"].tail(5).sum())  / vol5  * 100
            c20 = (df["f_net"].tail(20).sum() + df["it_net"].tail(20).sum()) / vol20 * 100
            conc_delta = round(float(c5 - c20), 2)

            # 技術位
            hc = "high" if "high" in df.columns else "max"
            lc = "low"  if "low"  in df.columns else "min"
            resistance = round(float(df[hc].tail(20).max()), 1) if hc in df.columns else 0.0
            support    = round(float(max(df[lc].tail(20).min(), m20)), 1) if lc in df.columns else 0.0

            # 法人成本（20/60/120 日）
            _ec = {20: 0.0, 60: 0.0, 120: 0.0}
            f_costs  = _calc_cost_windows(df, "f_net")  if "f_net"  in df.columns else _ec
            it_costs = _calc_cost_windows(df, "it_net") if "it_net" in df.columns else _ec

            # 營收
            raccel = calc_revenue_accel_score(rev)
            yoy_3m = raccel.get("yoy_trend", [0, 0, 0])

            # ── 警示條件偵測 ──────────────────────────────────
            warnings = []

            f_flip_sell  = _has_flipped_to_sell(df["f_net"])  if "f_net"  in df.columns else False
            it_flip_sell = _has_flipped_to_sell(df["it_net"]) if "it_net" in df.columns else False
            if f_flip_sell:
                warnings.append("外資轉賣")
            if it_flip_sell:
                warnings.append("投信轉賣")

            if f_streak <= -3 and it_streak <= -3:
                warnings.append("法人同步賣超")
            elif f_streak <= -3:
                warnings.append(f"外資連賣{abs(f_streak)}天")
            elif it_streak <= -3:
                warnings.append(f"投信連賣{abs(it_streak)}天")

            if c_p < m5:
                warnings.append("跌破五日線")

            if m5 < m10 < m20:
                warnings.append("空頭排列")

            if conc_delta < -1.0:
                warnings.append("籌碼加速流出")

            rev_decline = bool(yoy_3m and yoy_3m[-1] < 0 and yoy_3m[-2] < 0)
            if rev_decline:
                warnings.append("營收連月衰退")

            if not warnings:
                continue

            # 嚴重度：≥3 個警示為高風險
            severity = "high" if len(warnings) >= 3 else "mid" if len(warnings) == 2 else "low"

            results.append({
                "sid":        sid,
                "name":       name,
                "price":      round(c_p, 2),
                "price_chg":  price_chg,
                "warnings":   warnings,
                "severity":   severity,
                "yoy_3m":     [round(float(y), 1) for y in yoy_3m],
                "conc_5d":    round(float(c5), 2),
                "conc_delta": conc_delta,
                "f_streak":   int(f_streak),
                "it_streak":  int(it_streak),
                "f_cost":     f_costs[20],
                "f_cost_60":  f_costs[60],
                "f_cost_120": f_costs[120],
                "it_cost":    it_costs[20],
                "it_cost_60":  it_costs[60],
                "it_cost_120": it_costs[120],
                "resistance": resistance,
                "support":    support,
                "ma20":       round(m20, 1),
                "below_ma5":  c_p < m5,
                "death_cross": m5 < m10 < m20,
            })
        except Exception as e:
            print(f"[exit_scan] {sid} error: {e}")
            continue

    # 高風險優先，其次按警示數量
    return sorted(results,
                  key=lambda x: (x["severity"] == "high", len(x["warnings"])),
                  reverse=True)


def _run_all_scan(stock_map_items):
    """
    全部股票狀態一覽（不過濾）。
    回傳每支股票的綜合狀態，供「全部」tab 使用。
    狀態三分法：entry（進場機會）/ exit（出場警示）/ neutral（觀望）
    """
    results = []
    prefetched = _prefetch_stocks([sid for sid, _ in stock_map_items])

    for sid, name in stock_map_items:
        try:
            df, rev = prefetched.get(sid, (pd.DataFrame(), pd.DataFrame()))
            if df.empty or len(df) < 5:
                continue

            df = df.copy()
            df["ma5"]  = df["close"].rolling(5).mean()
            df["ma10"] = df["close"].rolling(10).mean()

            c_p       = float(df["close"].iloc[-1])
            c_prev    = float(df["close"].iloc[-2]) if len(df) >= 2 else c_p
            price_chg = round((c_p - c_prev) / (c_prev + 1e-9) * 100, 2)
            m5  = float(df["ma5"].iloc[-1])
            m10 = float(df["ma10"].iloc[-1])
            m20 = float(df["ma20"].iloc[-1])
            is_aligned = bool(m5 > m10 > m20 and c_p > m5)
            is_bearish = bool(m5 < m10 < m20)

            sms = calc_smart_money_score(df) if "f_net" in df.columns else {"score": 0}

            f_streak  = _streak(df["f_net"])  if "f_net"  in df.columns else 0
            it_streak = _streak(df["it_net"]) if "it_net" in df.columns else 0
            f5_net    = float(df["f_net"].tail(5).sum())  if "f_net"  in df.columns else 0.0
            it5_net   = float(df["it_net"].tail(5).sum()) if "it_net" in df.columns else 0.0
            battle    = _chip_battle(f_streak, it_streak, f5_net, it5_net)

            conc_delta = 0.0
            if len(df) >= 20 and "f_net" in df.columns and "it_net" in df.columns:
                vol5  = df["trading_volume"].tail(5).sum()  + 1e-9
                vol20 = df["trading_volume"].tail(20).sum() + 1e-9
                c5_   = (df["f_net"].tail(5).sum()  + df["it_net"].tail(5).sum())  / vol5  * 100
                c20_  = (df["f_net"].tail(20).sum() + df["it_net"].tail(20).sum()) / vol20 * 100
                conc_delta = round(float(c5_ - c20_), 2)

            # 進場訊號
            raccel = calc_revenue_accel_score(rev)
            entry_tags = []
            if raccel["is_accelerating"]:
                entry_tags.append("營收↑")
            if _has_flipped_to_buy(df["f_net"])  if "f_net"  in df.columns else False:
                entry_tags.append("外資翻轉↑")
            if _has_flipped_to_buy(df["it_net"]) if "it_net" in df.columns else False:
                entry_tags.append("投信翻轉↑")
            coil = detect_coiling(df)
            if coil["is_coiling"]:
                entry_tags.append("蓄力")

            # 出場警示
            exit_warnings = []
            if _has_flipped_to_sell(df["f_net"])  if "f_net"  in df.columns else False:
                exit_warnings.append("外資轉賣")
            if _has_flipped_to_sell(df["it_net"]) if "it_net" in df.columns else False:
                exit_warnings.append("投信轉賣")
            if f_streak <= -3 and it_streak <= -3:
                exit_warnings.append("法人同步賣超")
            if c_p < m5:
                exit_warnings.append("跌破五日線")
            if is_bearish:
                exit_warnings.append("空頭排列")
            if conc_delta < -1.0:
                exit_warnings.append("籌碼加速流出")

            # 綜合狀態
            if len(exit_warnings) >= 2:
                status = "exit"
            elif entry_tags or sms["score"] >= 55:
                status = "entry"
            else:
                status = "neutral"

            results.append({
                "sid":           sid,
                "name":          name,
                "price":         round(c_p, 2),
                "price_chg":     price_chg,
                "sms":           sms["score"],
                "f_streak":      int(f_streak),
                "it_streak":     int(it_streak),
                "battle":        battle,
                "is_aligned":    is_aligned,
                "is_bearish":    is_bearish,
                "conc_delta":    conc_delta,
                "entry_tags":    entry_tags,
                "exit_warnings": exit_warnings,
                "status":        status,
                "ma20":          round(m20, 1),
                "f_cost":        _calc_cost_windows(df, "f_net")[20]  if "f_net"  in df.columns else 0.0,
                "it_cost":       _calc_cost_windows(df, "it_net")[20] if "it_net" in df.columns else 0.0,
            })
        except Exception as e:
            print(f"[all_scan] {sid} error: {e}")
            continue

    # 進場優先 → 觀望 → 出場警示；同組內按 SMS 由高到低
    _order = {"entry": 0, "neutral": 1, "exit": 2}
    return sorted(results, key=lambda x: (_order.get(x["status"], 9), -x["sms"]))


def _get_heatmap_data():
    """族群熱圖資料：每族群平均籌碼強度 + 個股明細卡片"""
    all_sids = list({sid for stocks in SECTOR_GROUPS.values() for sid in stocks})
    prefetched = _prefetch_stocks(all_sids)

    heatmap_rows = []
    sector_details = {}

    for sector, stocks in SECTOR_GROUPS.items():
        strengths, stock_list = [], []
        for sid in stocks:
            try:
                df, _ = prefetched.get(sid, (pd.DataFrame(), pd.DataFrame()))
                if df.empty or len(df) < 5:
                    continue
                f_sum  = float(df["f_net"].tail(5).sum())
                it_sum = float(df["it_net"].tail(5).sum())
                v_sum  = float(df["trading_volume"].tail(5).sum()) + 1e-9
                strength = (f_sum + it_sum) / v_sum * 100
                strengths.append(strength)
                change = ((df["close"].iloc[-1] - df["close"].iloc[-5]) / df["close"].iloc[-5]) * 100
                stock_list.append({
                    "sid":    sid,
                    "name":   STOCK_POOL.get(sid, sid),
                    "price":  round(float(df["close"].iloc[-1]), 2),
                    "change": round(float(change), 2),
                    "f_net":  int(f_sum / 1000),
                    "it_net": int(it_sum / 1000),
                })
            except Exception:
                continue

        if strengths:
            heatmap_rows.append({
                "sector":   sector,
                "strength": round(float(np.mean(strengths)), 3),
            })
            sector_details[sector] = stock_list

    heatmap_rows.sort(key=lambda x: x["strength"], reverse=True)
    return heatmap_rows, sector_details


# ── Routes ────────────────────────────────────────────────────────────

@router.get("/", response_class=HTMLResponse)
async def tw_page(request: Request):
    """台股主頁面"""
    fetch_time = datetime.datetime.now().strftime("%Y-%m-%d %H:%M")
    return templates.TemplateResponse("tw.html", {
        "request":    request,
        "fetch_time": fetch_time,
    })


@router.get("/factor-max", response_class=HTMLResponse)
async def tw_factor_max(request: Request):
    """HTMX: TW Factor MAX 熱度榜"""
    import sys, os
    sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))
    from tabs.chip_radar._db import load_factor_max
    factor_max_list = load_factor_max("TW")
    return templates.TemplateResponse("partials/factor_heatmap.html", {
        "request":         request,
        "factor_max_list": factor_max_list,
        "market":          "TW",
    })


@router.post("/all-scan", response_class=HTMLResponse)
async def tw_all_scan(request: Request):
    """HTMX: 全部股票狀態一覽（無過濾）"""
    stocks = _run_all_scan(list(STOCK_POOL.items()))
    fetch_time = datetime.datetime.now().strftime("%Y-%m-%d %H:%M")
    return templates.TemplateResponse("partials/tw_all_results.html", {
        "request":    request,
        "stocks":     stocks,
        "fetch_time": fetch_time,
    })


@router.post("/exit-scan", response_class=HTMLResponse)
async def tw_exit_scan(request: Request):
    """HTMX: 執行出場警示掃描"""
    stocks = _run_exit_scan(list(STOCK_POOL.items()))
    fetch_time = datetime.datetime.now().strftime("%Y-%m-%d %H:%M")
    return templates.TemplateResponse("partials/tw_exit_results.html", {
        "request":    request,
        "stocks":     stocks,
        "fetch_time": fetch_time,
    })


@router.post("/scan", response_class=HTMLResponse)
async def tw_scan(request: Request):
    """HTMX: 執行掃描，回傳選股結果 partial"""
    stocks = _run_unified_scan(list(STOCK_POOL.items()))
    fetch_time = datetime.datetime.now().strftime("%Y-%m-%d %H:%M")
    return templates.TemplateResponse("partials/tw_scan_results.html", {
        "request":    request,
        "stocks":     stocks,
        "fetch_time": fetch_time,
    })


@router.get("/heatmap", response_class=HTMLResponse)
async def tw_heatmap(request: Request):
    """HTMX: 族群熱圖 partial"""
    heatmap_rows, sector_details = _get_heatmap_data()
    return templates.TemplateResponse("partials/tw_heatmap.html", {
        "request":        request,
        "heatmap_rows":   heatmap_rows,
        "sector_details": sector_details,
        "sectors":        [r["sector"] for r in heatmap_rows],
    })


@router.get("/stock/{sid}/detail", response_class=HTMLResponse)
async def tw_stock_detail(request: Request, sid: str):
    """HTMX: 個股展開（法說摘要 + 策略資訊）"""
    engine = _get_engine()
    name = STOCK_POOL.get(sid, sid)

    # 法說資料
    mops = engine.fetch_latest_mops_pdf_info(sid)

    # 基本資料
    df, rev = engine.fetch_data(sid)
    strategy_data = {}
    if not df.empty:
        curr  = float(df["close"].iloc[-1])
        low52 = float(df["close"].tail(252).min())
        hi52  = float(df["close"].tail(252).max())
        sms   = calc_smart_money_score(df) if "f_net" in df.columns else {"score": 0}
        # 外資均成本（簡易估算）
        buy_mask = df["f_net"] > 0
        fc_sum = (df["close"] * df["f_net"] * buy_mask).tail(60).sum()
        fv_sum = (df["f_net"] * buy_mask).tail(60).sum()
        f_cost = round(fc_sum / (fv_sum + 1e-9), 1)

        strategy_data = {
            "curr":   curr,
            "low52":  round(low52, 1),
            "hi52":   round(hi52, 1),
            "f_cost": f_cost,
            "sms":    sms["score"],
        }

    return templates.TemplateResponse("partials/stock_detail.html", {
        "request":       request,
        "sid":           sid,
        "name":          name,
        "mops":          mops,
        "strategy_data": strategy_data,
    })
