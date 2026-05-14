"""
engine/tw_scanner.py — Taiwan stock scan logic

Provides three scan functions shared by:
  - api/tw.py       (live route handlers)
  - tw_prefetch.py  (pre-computation after daily prefetch)

Scan types:
  "unified"  → entry candidates (_run_unified_scan)
  "exit"     → exit warnings   (_run_exit_scan)
  "all"      → all-stock view  (_run_all_scan)
"""

import numpy as np
import pandas as pd
from concurrent.futures import ThreadPoolExecutor, as_completed

from engine.wall_street_engine import WallStreetEngine
from engine.smart_money import calc_smart_money_score, calc_revenue_accel_score
from engine.rocket_detector import detect_coiling
from sector_data import STOCK_POOL, SECTOR_GROUPS


# ── helpers ──────────────────────────────────────────────────────────────────

def _get_engine() -> WallStreetEngine:
    return WallStreetEngine()


def prefetch_stocks(sids: list, max_workers: int = 10) -> dict:
    """Parallel-fetch multiple stocks; returns {sid: (df, rev)}."""
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


def _has_flipped_to_sell(series, max_days: int = 3) -> bool:
    vals = series.dropna().values
    if len(vals) < max_days + 2:
        return False
    s = _streak(series)
    if not (-max_days <= s <= -1):
        return False
    return vals[-(abs(s) + 1)] > 0


def _calc_cost_windows(df, col) -> dict:
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


def _chip_battle(f_streak: int, it_streak: int, f5: float, it5: float) -> dict:
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


def _conc_stats(df):
    """Return (c5, c10, c20, conc_delta, conc_arrow, conc_accel)."""
    c5 = c10 = c20 = 0.0
    if len(df) >= 20:
        vol5  = df["trading_volume"].tail(5).sum()  + 1e-9
        vol10 = df["trading_volume"].tail(10).sum() + 1e-9
        vol20 = df["trading_volume"].tail(20).sum() + 1e-9
        c5  = (df["f_net"].tail(5).sum()  + df["it_net"].tail(5).sum())  / vol5  * 100
        c10 = (df["f_net"].tail(10).sum() + df["it_net"].tail(10).sum()) / vol10 * 100
        c20 = (df["f_net"].tail(20).sum() + df["it_net"].tail(20).sum()) / vol20 * 100
    conc_accel = bool(c5 > c10 > c20)
    conc_delta = round(float(c5 - c20), 2)
    conc_arrow = "↑" if conc_delta > 0.5 else ("↓" if conc_delta < -0.5 else "→")
    return c5, c10, c20, conc_delta, conc_arrow, conc_accel


# ── scan functions ────────────────────────────────────────────────────────────

def run_unified_scan(stock_map_items, prefetched: dict | None = None) -> list:
    """Entry-candidate scan. Returns sorted list of dicts."""
    if prefetched is None:
        prefetched = prefetch_stocks([sid for sid, _ in stock_map_items])

    results = []
    _empty_costs = {20: 0.0, 60: 0.0, 120: 0.0}

    for sid, name in stock_map_items:
        try:
            df, rev = prefetched.get(sid, (pd.DataFrame(), pd.DataFrame()))
            if df.empty:
                continue

            df = df.copy()
            df["ma5"]  = df["close"].rolling(5).mean()
            df["ma10"] = df["close"].rolling(10).mean()
            c_p    = float(df["close"].iloc[-1])
            c_prev = float(df["close"].iloc[-2]) if len(df) >= 2 else c_p
            m20  = float(df["ma20"].iloc[-1])
            m5   = float(df["ma5"].iloc[-1])
            m10  = float(df["ma10"].iloc[-1])
            is_aligned = bool(m5 > m10 > m20 and c_p > m5)
            price_chg  = round((c_p - c_prev) / (c_prev + 1e-9) * 100, 2)

            sms    = calc_smart_money_score(df) if "f_net" in df.columns else {"score": 0}
            raccel = calc_revenue_accel_score(rev)
            coil   = detect_coiling(df)

            hc = "high" if "high" in df.columns else "max"
            lc = "low"  if "low"  in df.columns else "min"
            resistance = round(float(df[hc].tail(20).max()), 1) if hc in df.columns else 0.0
            support    = round(float(max(df[lc].tail(20).min(), m20)), 1) if lc in df.columns else 0.0

            f_costs  = _calc_cost_windows(df, "f_net")  if "f_net"  in df.columns else _empty_costs
            it_costs = _calc_cost_windows(df, "it_net") if "it_net" in df.columns else _empty_costs

            c5, c10, c20, conc_delta, conc_arrow, conc_accel = _conc_stats(df)

            f_streak  = _streak(df["f_net"])  if "f_net"  in df.columns else 0
            it_streak = _streak(df["it_net"]) if "it_net" in df.columns else 0
            f5_net    = float(df["f_net"].tail(5).sum())  if "f_net"  in df.columns else 0.0
            it5_net   = float(df["it_net"].tail(5).sum()) if "it_net" in df.columns else 0.0
            battle    = _chip_battle(f_streak, it_streak, f5_net, it5_net)

            f_flip  = _has_flipped_to_buy(df["f_net"])  if "f_net"  in df.columns else False
            it_flip = _has_flipped_to_buy(df["it_net"]) if "it_net" in df.columns else False

            yoy_3m = raccel.get("yoy_trend", [0, 0, 0])

            rev_consec = False
            if not rev.empty and len(rev) >= 3:
                v = rev.sort_values("date")["revenue"].tail(3).values
                rev_consec = bool(len(v) == 3 and v[2] > v[1] > v[0])

            # Hard exclusions
            if m5 < m10 < m20:
                continue
            if conc_delta < -1.0:
                continue
            if len(yoy_3m) >= 2 and yoy_3m[-1] < 0 and yoy_3m[-2] < 0:
                continue
            if f_streak < 0 and it_streak < 0:
                continue

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
            if yoy_3m and yoy_3m[-1] < 0:
                tags.append("⚠️營收衰退")

            positive_tags = [t for t in tags if t != "⚠️營收衰退"]
            if not positive_tags and sms["score"] < 50:
                continue

            results.append({
                "sid":        sid,
                "name":       name,
                "price":      round(c_p, 2),
                "price_chg":  price_chg,
                "yoy_3m":     [round(float(y), 1) for y in yoy_3m],
                "conc_5d":    round(float(c5), 2),
                "conc_20d":   round(float(c20), 2),
                "conc_delta": conc_delta,
                "conc_arrow": conc_arrow,
                "f_streak":   int(f_streak),
                "it_streak":  int(it_streak),
                "battle":     battle,
                "f_cost":     f_costs[20],
                "f_cost_60":  f_costs[60],
                "f_cost_120": f_costs[120],
                "it_cost":    it_costs[20],
                "it_cost_60":  it_costs[60],
                "it_cost_120": it_costs[120],
                "resistance": resistance,
                "support":    support,
                "ma20":       round(m20, 1),
                "is_aligned": is_aligned,
                "tags":       tags,
            })
        except Exception as e:
            print(f"[unified_scan] {sid} error: {e}")

    return sorted(results,
                  key=lambda x: (("雙強" in x["tags"]), x["conc_delta"]),
                  reverse=True)


def run_exit_scan(stock_map_items, prefetched: dict | None = None) -> list:
    """Exit-warning scan. Returns sorted list of dicts."""
    if prefetched is None:
        prefetched = prefetch_stocks([sid for sid, _ in stock_map_items])

    results = []
    _ec = {20: 0.0, 60: 0.0, 120: 0.0}

    for sid, name in stock_map_items:
        try:
            df, rev = prefetched.get(sid, (pd.DataFrame(), pd.DataFrame()))
            if df.empty or len(df) < 20:
                continue

            df = df.copy()
            df["ma5"]  = df["close"].rolling(5).mean()
            df["ma10"] = df["close"].rolling(10).mean()
            c_p    = float(df["close"].iloc[-1])
            c_prev = float(df["close"].iloc[-2]) if len(df) >= 2 else c_p
            price_chg = round((c_p - c_prev) / (c_prev + 1e-9) * 100, 2)
            m5  = float(df["ma5"].iloc[-1])
            m10 = float(df["ma10"].iloc[-1])
            m20 = float(df["ma20"].iloc[-1])

            f_streak  = _streak(df["f_net"])  if "f_net"  in df.columns else 0
            it_streak = _streak(df["it_net"]) if "it_net" in df.columns else 0

            c5, _, c20, conc_delta, _, _ = _conc_stats(df)

            hc = "high" if "high" in df.columns else "max"
            lc = "low"  if "low"  in df.columns else "min"
            resistance = round(float(df[hc].tail(20).max()), 1) if hc in df.columns else 0.0
            support    = round(float(max(df[lc].tail(20).min(), m20)), 1) if lc in df.columns else 0.0

            f_costs  = _calc_cost_windows(df, "f_net")  if "f_net"  in df.columns else _ec
            it_costs = _calc_cost_windows(df, "it_net") if "it_net" in df.columns else _ec

            raccel = calc_revenue_accel_score(rev)
            yoy_3m = raccel.get("yoy_trend", [0, 0, 0])

            warnings = []
            if _has_flipped_to_sell(df["f_net"])  if "f_net"  in df.columns else False:
                warnings.append("外資轉賣")
            if _has_flipped_to_sell(df["it_net"]) if "it_net" in df.columns else False:
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
            if yoy_3m and yoy_3m[-1] < 0 and yoy_3m[-2] < 0:
                warnings.append("營收連月衰退")

            if not warnings:
                continue

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

    return sorted(results,
                  key=lambda x: (x["severity"] == "high", len(x["warnings"])),
                  reverse=True)


def run_all_scan(stock_map_items, prefetched: dict | None = None) -> list:
    """All-stock status scan. Returns sorted list of dicts."""
    if prefetched is None:
        prefetched = prefetch_stocks([sid for sid, _ in stock_map_items])

    results = []

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

            _, _, _, conc_delta, _, _ = _conc_stats(df) if len(df) >= 20 and "f_net" in df.columns else (0,0,0,0,"→",False)

            raccel = calc_revenue_accel_score(rev)
            entry_tags = []
            if raccel["is_accelerating"]:
                entry_tags.append("營收↑")
            if _has_flipped_to_buy(df["f_net"])  if "f_net"  in df.columns else False:
                entry_tags.append("外資翻轉↑")
            if _has_flipped_to_buy(df["it_net"]) if "it_net" in df.columns else False:
                entry_tags.append("投信翻轉↑")
            if detect_coiling(df)["is_coiling"]:
                entry_tags.append("蓄力")

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

    _order = {"entry": 0, "neutral": 1, "exit": 2}
    return sorted(results, key=lambda x: (_order.get(x["status"], 9), -x["sms"]))


def run_heatmap() -> tuple[list, dict]:
    """Sector heatmap data. Returns (heatmap_rows, sector_details)."""
    all_sids = list({sid for stocks in SECTOR_GROUPS.values() for sid in stocks})
    prefetched = prefetch_stocks(all_sids)

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
