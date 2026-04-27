"""
api/us.py — 美股 routes

GET /us/           → 美股主頁面
GET /us/radar      → HTMX: 籌碼分數排行 partial
GET /us/exit-scan  → HTMX: 出場警示掃描 partial
GET /us/stock/{ticker}/detail → HTMX: 個股五大分數展開
"""

import os
import sys
import time as _time_mod
import datetime
from fastapi import APIRouter, Request
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates

sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

from tabs.chip_radar._db import (
    load_latest_scores, load_market_pulse, load_universe,
    load_institutional_holders,
    COMPOSITE_KEY, load_insider_trades, load_options_flow, load_large_holders,
    load_market_env, load_tech_signals, load_tech_signal, load_factor_max,
)

router = APIRouter()
templates = Jinja2Templates(directory="templates")

# ── 概念股分類 ────────────────────────────────────────────────────────
# 優先於 us_universe.json 的產業欄位
CONCEPT_MAP: dict[str, str] = {
    # AI 算力
    "NVDA": "AI算力", "AMD": "AI算力", "INTC": "AI算力",
    "QCOM": "AI算力", "MRVL": "AI算力", "AVGO": "AI算力",
    "ARM":  "AI算力", "SMCI": "AI算力",
    # 半導體設備
    "AMAT": "半導體設備", "LRCX": "半導體設備", "KLAC": "半導體設備",
    "ASML": "半導體設備", "TER":  "半導體設備", "ONTO": "半導體設備",
    "ENTG": "半導體設備",
    # 雲端/基礎設施
    "MSFT": "雲端AI", "GOOGL": "雲端AI", "GOOG": "雲端AI",
    "AMZN": "雲端AI", "META": "雲端AI", "ORCL": "雲端AI",
    "IBM":  "雲端AI",
    # SaaS / 企業軟體
    "CRM":  "SaaS", "NOW":  "SaaS", "SNOW": "SaaS", "DDOG": "SaaS",
    "MDB":  "SaaS", "TEAM": "SaaS", "WDAY": "SaaS", "ZM":   "SaaS",
    "HUBS": "SaaS", "VEEV": "SaaS", "ADSK": "SaaS", "INTU": "SaaS",
    "PCTY": "SaaS",
    # 電力 / 資料中心電力
    "VST":  "電力基建", "CEG":  "電力基建", "NRG":  "電力基建",
    "ETR":  "電力基建", "EXC":  "電力基建", "PEG":  "電力基建",
    "NEE":  "電力基建", "SO":   "電力基建", "AEP":  "電力基建",
    "D":    "電力基建", "DUK":  "電力基建", "PCG":  "電力基建",
    "SRE":  "電力基建", "AWK":  "電力基建",
    # 資料中心 REIT
    "EQIX": "資料中心", "DLR":  "資料中心", "AMT":  "資料中心",
    "CCI":  "資料中心", "SBAC": "資料中心", "IRM":  "資料中心",
    # 網路安全
    "CRWD": "網路安全", "PANW": "網路安全", "FTNT": "網路安全",
    "OKTA": "網路安全", "ZS":   "網路安全", "S":    "網路安全",
    "CYBR": "網路安全",
    # 電動車 / 自駕
    "TSLA": "電動車", "NIO":  "電動車", "XPEV": "電動車",
    "LI":   "電動車", "RIVN": "電動車", "LCID": "電動車",
    # 金融科技
    "V":    "金融科技", "MA":   "金融科技", "PYPL": "金融科技",
    "SQ":   "金融科技", "COIN": "金融科技", "AFRM": "金融科技",
    "SOFI": "金融科技",
    # 消費科技 / 電商
    "AAPL": "消費科技", "NFLX": "消費科技", "SPOT": "消費科技",
    "UBER": "消費科技", "LYFT": "消費科技", "SHOP": "電商",
    "MELI": "電商",    "ETSY": "電商",
    # 生技 / 醫療
    "LLY":  "生技醫療", "MRNA": "生技醫療", "REGN": "生技醫療",
    "VRTX": "生技醫療", "AMGN": "生技醫療", "GILD": "生技醫療",
    "BIIB": "生技醫療", "BMY":  "生技醫療",
    # 新能源
    "ENPH": "新能源", "SEDG": "新能源", "FSLR": "新能源",
    "RUN":  "新能源", "PLUG": "新能源", "ARRY": "新能源",
    # 傳統金融
    "JPM":  "金融", "BAC":  "金融", "GS":   "金融",
    "MS":   "金融", "WFC":  "金融", "C":    "金融",
    "BLK":  "金融", "SCHW": "金融",
}

# 概念 → 標籤顏色
CONCEPT_COLOR: dict[str, str] = {
    "AI算力":    "tag-blue",
    "半導體設備": "tag-blue",
    "雲端AI":    "tag-blue",
    "SaaS":      "tag-purple",
    "電力基建":   "tag-yellow",
    "資料中心":   "tag-yellow",
    "網路安全":   "tag-green",
    "電動車":     "tag-green",
    "金融科技":   "tag-purple",
    "消費科技":   "tag-purple",
    "電商":       "tag-purple",
    "生技醫療":   "tag-green",
    "新能源":     "tag-green",
    "金融":       "tag-yellow",
}

# S&P 產業 → 短名（fallback）
SECTOR_ABBR: dict[str, str] = {
    "Information Technology":  "IT",
    "Health Care":             "Healthcare",
    "Consumer Discretionary":  "Discretionary",
    "Consumer Staples":        "Staples",
    "Financials":              "Financials",
    "Industrials":             "Industrials",
    "Energy":                  "Energy",
    "Materials":               "Materials",
    "Real Estate":             "Real Estate",
    "Utilities":               "Utilities",
    "Communication Services":  "Comm. Services",
}

BADGE_MAP = {
    "insider_cluster":     "insider↑",
    "insider_selling":     "insider↓",
    "unusual_options":     "options異常",
    "volume_accumulation": "量能↑",
    "volume_distribution": "量能↓",
    "high_short_interest": "空頭高",
}

_VIX_DISCOUNT = {"low": 1.0, "medium": 0.95, "high": 0.80}

# ── 即時股價快取（30 分鐘 TTL）────────────────────────────────────────
_price_cache:    dict  = {}
_price_cache_ts: float = 0.0
_PRICE_TTL = 1800  # 30 min


def _get_realtime_prices(tickers: list[str]) -> dict:
    """
    批次從 yfinance 取最新兩天收盤，計算漲跌幅。
    快取 30 分鐘，首次呼叫約 10-15 秒（全宇宙），之後即時。
    回傳 {ticker: {"price": float, "price_chg": float, "price_date": str}}
    """
    global _price_cache, _price_cache_ts
    now = _time_mod.time()
    if _price_cache and (now - _price_cache_ts) < _PRICE_TTL:
        return _price_cache

    try:
        import yfinance as yf
        import pandas as pd
        raw = yf.download(tickers, period="2d", progress=False, auto_adjust=True)
        if raw.empty:
            return _price_cache  # return stale on error

        close = raw["Close"] if "Close" in raw.columns else raw[("Close",)]
        if isinstance(close.columns, pd.MultiIndex):
            close.columns = [c[0] if isinstance(c, tuple) else c for c in close.columns]

        result: dict = {}
        price_date = raw.index[-1].strftime("%Y-%m-%d") if len(raw) > 0 else ""
        for t in tickers:
            if t not in close.columns:
                continue
            series = close[t].dropna()
            if len(series) < 1:
                continue
            c0 = float(series.iloc[-1])
            c1 = float(series.iloc[-2]) if len(series) >= 2 else c0
            result[t] = {
                "price":      round(c0, 2),
                "price_chg":  round((c0 / c1 - 1) * 100, 2) if c1 else 0.0,
                "price_date": price_date,
            }
        _price_cache = result
        _price_cache_ts = now
        return result

    except Exception as e:
        print(f"[us] realtime price error: {e}")
        return _price_cache  # stale fallback


def _sector_for(ticker: str, universe: dict) -> tuple[str, str]:
    """回傳 (概念名稱, CSS class)"""
    if ticker in CONCEPT_MAP:
        concept = CONCEPT_MAP[ticker]
        return concept, CONCEPT_COLOR.get(concept, "tag-yellow")
    # fallback: S&P 產業
    uni  = universe.get(ticker, {})
    sect = SECTOR_ABBR.get(uni.get("sector", ""), uni.get("sector", ""))
    return sect, "tag-yellow"


# ── Radar loader ─────────────────────────────────────────────────────

def _load_radar(timeframe: str = "波段 (1–4週)"):
    import json
    import datetime as dt

    df = load_latest_scores()
    if df.empty:
        return [], None, {}

    comp_col = COMPOSITE_KEY.get(timeframe, "composite_swing")

    yesterday = (dt.date.today() - dt.timedelta(days=1)).isoformat()
    df_prev   = load_latest_scores(as_of=yesterday)
    prev_map  = df_prev.set_index("ticker")[comp_col].to_dict() if not df_prev.empty else {}

    universe    = load_universe()
    all_tickers = df["ticker"].tolist()
    prices_map  = _get_realtime_prices(all_tickers)

    # 市場環境 + VIX 折扣
    env          = load_market_env()
    vix_level    = env.get("vix_level", "medium") if env else "medium"
    vix_discount = _VIX_DISCOUNT.get(vix_level, 0.95)

    # 技術面信號
    tech_df  = load_tech_signals()
    tech_map = {}
    if not tech_df.empty:
        for _, tr in tech_df.iterrows():
            tech_map[tr["ticker"]] = tr.to_dict()

    # Factor MAX：建立 sector → factor_name 反查表
    # 近期爆發（days_ago <= 5, max_ret > 2%）的因子加 badge
    _CONCEPT_TO_FACTOR = {
        "AI算力": "ai_compute", "半導體設備": "semieq", "雲端AI": "cloud_ai",
        "網路安全": "cybersecurity", "電動車": "ev", "SaaS": "saas", "金融科技": "fintech",
    }
    factor_max_list = load_factor_max("US")
    hot_factors: set[str] = {
        r["factor_name"] for r in factor_max_list
        if (r.get("days_ago") or 99) <= 5 and (r.get("max_ret") or 0) >= 2.0
    }
    # stocks in lottery_stocks of any factor → individual reversal risk
    lottery_set: set[str] = set()
    for r in factor_max_list:
        lottery_set.update(r.get("lottery_stocks") or [])

    stocks = []
    for _, row in df.sort_values(comp_col, ascending=False).iterrows():
        ticker = row["ticker"]

        flags = []
        try:
            flags = json.loads(row["signal_flags"]) if row.get("signal_flags") else []
        except Exception:
            pass

        badges = []
        if row.get("whale_alert"):
            badges.append("🐋巨鯨")
        if row.get("entry_timing"):
            badges.append("⚡進場")
        for f in flags:
            label = BADGE_MAP.get(f)
            if label:
                badges.append(label)

        prev  = prev_map.get(ticker)
        delta = None
        if prev is not None:
            d = (row.get(comp_col) or 0) - prev
            delta = f"+{d:.0f}" if d > 0.5 else (f"{d:.0f}" if d < -0.5 else None)

        sector, sector_cls = _sector_for(ticker, universe)

        # Factor MAX badges（需在 sector 確定後）
        factor_key = _CONCEPT_TO_FACTOR.get(sector)
        if factor_key and factor_key in hot_factors:
            badges.append("🔥因子爆發")
        if ticker in lottery_set:
            badges.append("⚠個股過熱")
        uni = universe.get(ticker, {})
        pi  = prices_map.get(ticker, {})
        t   = tech_map.get(ticker, {})

        entry_raw    = t.get("entry_score")
        entry_score  = round(entry_raw * vix_discount, 1) if entry_raw is not None else None

        stocks.append({
            "ticker":      ticker,
            "name":        uni.get("name", ticker),
            "sector":      sector,
            "sector_cls":  sector_cls,
            "date":        row.get("date", ""),
            "price":       pi.get("price"),
            "price_chg":   pi.get("price_chg"),
            "price_date":  pi.get("price_date", ""),
            "insider":     row.get("insider_score"),
            "short":       row.get("short_score"),
            "volume":      row.get("volume_score"),
            "options":     row.get("options_flow_score"),
            "institution": row.get("institutional_score"),
            "badges":      badges,
            "delta":       delta,
            "is_whale":       bool(row.get("whale_alert")),
            "is_entry":       bool(row.get("entry_timing")),
            "has_insider_buy": "insider_cluster" in flags,
            "composite":   row.get(comp_col),
            # 技術面
            "entry_score":       entry_score,
            "exit_risk":         t.get("exit_risk"),
            "ma_aligned":        t.get("ma_aligned", 0),
            "rsi_reversal":      t.get("rsi_reversal", 0),
            "macd_golden":       t.get("macd_golden", 0),
            "bb_breakout":       t.get("bb_breakout", 0),
            "double_bottom":     t.get("double_bottom", 0),
            "vcp_pattern":       t.get("vcp_pattern", 0),
            "hard_stop":         t.get("hard_stop", 0),
            "below_20ma_3d":     t.get("below_20ma_3d", 0),
            "rsi_divergence":    t.get("rsi_divergence", 0),
            "atr_trailing_stop": t.get("atr_trailing_stop", 0),
            "rsi_14":            t.get("rsi_14"),
            "macd_hist":         t.get("macd_hist"),
            "vol_ratio":         t.get("vol_ratio"),
            "ma_20":             t.get("ma_20"),
            "ma_50":             t.get("ma_50"),
            "ma_200":            t.get("ma_200"),
            "atr_14":            t.get("atr_14"),
        })

    pulse      = load_market_pulse()
    env_summary = {
        "vix":       env.get("vix"),
        "vix_level": vix_level,
        "discount":  vix_discount,
    }
    return stocks, pulse, env_summary, factor_max_list


def _run_us_exit_scan(timeframe: str = "波段 (1–4週)") -> list:
    """
    美股出場警示：
      - 綜合分 < 35
      - 內部人大量賣出（insider_score < 30）
      - 空頭率高（short_score < 30）
      - 量能派發（volume_score < 30）
      - 機構減倉（institutional_score < 30）
      - 技術面止損觸發（hard_stop / below_20ma_3d / rsi_divergence / atr_trailing_stop）
    """
    import json

    df = load_latest_scores()
    if df.empty:
        return []

    comp_col   = COMPOSITE_KEY.get(timeframe, "composite_swing")
    universe   = load_universe()
    all_tickers = df["ticker"].tolist()
    prices_map  = _get_realtime_prices(all_tickers)

    # Build tech_signals lookup {ticker: row_dict}
    try:
        tech_df  = load_tech_signals()
        tech_map = {r["ticker"]: r for _, r in tech_df.iterrows()} if not tech_df.empty else {}
    except Exception:
        tech_map = {}

    # Factor MAX lottery set（個股層面過熱）
    try:
        lottery_exit: set[str] = set()
        for r in load_factor_max("US"):
            lottery_exit.update(r.get("lottery_stocks") or [])
    except Exception:
        lottery_exit = set()

    results = []
    for _, row in df.iterrows():
        ticker = row["ticker"]
        comp   = row.get(comp_col) or 0

        flags = []
        try:
            flags = json.loads(row["signal_flags"]) if row.get("signal_flags") else []
        except Exception:
            pass

        tech = tech_map.get(ticker, {})

        warnings = []

        # ── chip-based warnings ────────────────────────────────────────
        if comp < 35:
            warnings.append(f"綜合分偏低 ({int(comp)})")
        # insider_selling 不列為出場條件（不代表需要離場，僅做資訊揭露）
        if (row.get("short_score") or 100) < 30:
            warnings.append("空頭率高")
        if (row.get("volume_score") or 100) < 30 or "volume_distribution" in flags:
            warnings.append("量能派發")
        if (row.get("institutional_score") or 100) < 30:
            warnings.append("機構減倉")
        if "high_short_interest" in flags:
            warnings.append("空頭興趣異常")

        # ── tech-based exit warnings ───────────────────────────────────
        if tech.get("hard_stop"):
            warnings.append("硬性止損 -7%")
        if tech.get("below_20ma_3d"):
            warnings.append("跌破20MA×3日")
        if tech.get("rsi_divergence"):
            warnings.append("RSI頂背離")
        if tech.get("atr_trailing_stop"):
            warnings.append("ATR止盈線破")
        # ── 個股層面過熱（Factor MAX 樂透效應）────────────────────────
        if ticker in lottery_exit:
            warnings.append("個股過熱↩反轉")

        if not warnings:
            continue

        exit_risk = tech.get("exit_risk")

        # High: exit_risk >= 60 OR chip warnings >= 3; mid: >= 2
        tech_severity = "high" if (exit_risk or 0) >= 60 else "mid" if (exit_risk or 0) >= 30 else "low"
        chip_severity = "high" if len(warnings) >= 3 else "mid" if len(warnings) == 2 else "low"
        _sev_rank = {"high": 2, "mid": 1, "low": 0}
        severity = max([tech_severity, chip_severity], key=lambda s: _sev_rank[s])

        sector, sector_cls = _sector_for(ticker, universe)
        uni = universe.get(ticker, {})
        pi  = prices_map.get(ticker, {})

        results.append({
            "ticker":      ticker,
            "name":        uni.get("name", ticker),
            "sector":      sector,
            "sector_cls":  sector_cls,
            "price":       pi.get("price"),
            "price_chg":   pi.get("price_chg"),
            "warnings":    warnings,
            "severity":    severity,
            "insider":     row.get("insider_score"),
            "short":       row.get("short_score"),
            "volume":      row.get("volume_score"),
            "options":     row.get("options_flow_score"),
            "institution": row.get("institutional_score"),
            "composite":   comp,
            "exit_risk":   exit_risk,
        })

    _sev_rank2 = {"high": 2, "mid": 1, "low": 0}
    return sorted(results,
                  key=lambda x: (_sev_rank2[x["severity"]], len(x["warnings"])),
                  reverse=True)


def _load_us_all(timeframe: str = "波段 (1–4週)") -> list:
    """全宇宙掃描：為每支股票計算 status，有內部人買進者排最前。"""
    import json

    df = load_latest_scores()
    if df.empty:
        return []

    comp_col     = COMPOSITE_KEY.get(timeframe, "composite_swing")
    universe     = load_universe()
    all_tickers  = df["ticker"].tolist()
    prices_map   = _get_realtime_prices(all_tickers)

    env          = load_market_env()
    vix_level    = env.get("vix_level", "medium") if env else "medium"
    vix_discount = _VIX_DISCOUNT.get(vix_level, 0.95)

    tech_df  = load_tech_signals()
    tech_map = {}
    if not tech_df.empty:
        for _, tr in tech_df.iterrows():
            tech_map[tr["ticker"]] = tr.to_dict()

    try:
        lottery_set: set[str] = set()
        for r in load_factor_max("US"):
            lottery_set.update(r.get("lottery_stocks") or [])
    except Exception:
        lottery_set = set()

    stocks = []
    for _, row in df.iterrows():
        ticker = row["ticker"]
        comp   = float(row.get(comp_col) or 0)

        flags = []
        try:
            flags = json.loads(row["signal_flags"]) if row.get("signal_flags") else []
        except Exception:
            pass

        tech             = tech_map.get(ticker, {})
        has_insider_buy  = "insider_cluster" in flags

        # ── 決定 status ───────────────────────────────────────────────
        tech_exit = any([
            tech.get("hard_stop"), tech.get("below_20ma_3d"),
            tech.get("rsi_divergence"), tech.get("atr_trailing_stop"),
        ])
        chip_exit = (
            comp < 35
            or (row.get("short_score") or 100) < 30
            or (row.get("volume_score") or 100) < 30
        )

        entry_timing = bool(row.get("entry_timing"))
        whale        = bool(row.get("whale_alert"))

        if (tech_exit and comp < 40) or (chip_exit and comp < 35):
            status = "exit"
        elif entry_timing or comp >= 65 or (whale and comp >= 50):
            status = "entry"
        else:
            status = "neutral"

        badges = []
        if whale:
            badges.append("🐋巨鯨")
        if entry_timing:
            badges.append("⚡進場")
        for f in flags:
            label = BADGE_MAP.get(f)
            if label:
                badges.append(label)
        if ticker in lottery_set:
            badges.append("⚠個股過熱")

        sector, sector_cls = _sector_for(ticker, universe)
        uni        = universe.get(ticker, {})
        pi         = prices_map.get(ticker, {})
        entry_raw  = tech.get("entry_score")
        entry_score = round(entry_raw * vix_discount, 1) if entry_raw is not None else None

        stocks.append({
            "ticker":         ticker,
            "name":           uni.get("name", ticker),
            "sector":         sector,
            "sector_cls":     sector_cls,
            "price":          pi.get("price"),
            "price_chg":      pi.get("price_chg"),
            "composite":      comp,
            "insider":        row.get("insider_score"),
            "short":          row.get("short_score"),
            "volume":         row.get("volume_score"),
            "options":        row.get("options_flow_score"),
            "institution":    row.get("institutional_score"),
            "status":         status,
            "badges":         badges,
            "has_insider_buy": has_insider_buy,
            "entry_score":    entry_score,
            "exit_risk":      tech.get("exit_risk"),
        })

    _status_rank = {"entry": 2, "neutral": 1, "exit": 0}
    stocks.sort(key=lambda x: (
        1 if x["has_insider_buy"] else 0,
        _status_rank.get(x["status"], 1),
        x["composite"],
    ), reverse=True)
    return stocks


# ── Routes ────────────────────────────────────────────────────────────

@router.get("/", response_class=HTMLResponse)
async def us_page(request: Request):
    fetch_time = datetime.datetime.now().strftime("%Y-%m-%d %H:%M")
    env = load_market_env()
    return templates.TemplateResponse("us.html", {
        "request":    request,
        "fetch_time": fetch_time,
        "timeframes": list(COMPOSITE_KEY.keys()),
        "env":        env,
    })


@router.get("/radar", response_class=HTMLResponse)
async def us_radar(request: Request, timeframe: str = "波段 (1–4週)"):
    stocks, pulse, env_summary, factor_max_list = _load_radar(timeframe)
    price_date = stocks[0]["price_date"] if stocks else ""
    return templates.TemplateResponse("partials/us_radar.html", {
        "request":        request,
        "stocks":         stocks,
        "pulse":          pulse,
        "env_summary":    env_summary,
        "factor_max_list": factor_max_list,
        "timeframe":      timeframe,
        "timeframes":     list(COMPOSITE_KEY.keys()),
        "price_date":     price_date,
    })


@router.get("/factor-max", response_class=HTMLResponse)
async def us_factor_max(request: Request):
    factor_max_list = load_factor_max("US")
    return templates.TemplateResponse("partials/factor_heatmap.html", {
        "request":        request,
        "factor_max_list": factor_max_list,
        "market":         "US",
    })


@router.post("/all-scan", response_class=HTMLResponse)
async def us_all_scan(request: Request):
    stocks     = _load_us_all()
    fetch_time = datetime.datetime.now().strftime("%Y-%m-%d %H:%M")
    return templates.TemplateResponse("partials/us_all_results.html", {
        "request":    request,
        "stocks":     stocks,
        "fetch_time": fetch_time,
    })


@router.get("/exit-scan", response_class=HTMLResponse)
async def us_exit_scan(request: Request, timeframe: str = "波段 (1–4週)"):
    stocks = _run_us_exit_scan(timeframe)
    return templates.TemplateResponse("partials/us_exit_results.html", {
        "request":   request,
        "stocks":    stocks,
        "timeframe": timeframe,
    })


@router.get("/stock/{ticker}/detail", response_class=HTMLResponse)
async def us_stock_detail(request: Request, ticker: str):
    import pandas as pd

    insider_df = load_insider_trades(ticker)
    options_df = load_options_flow(ticker)
    holders_df = load_large_holders(ticker)

    # Format insider trades
    insider_rows = []
    if not insider_df.empty:
        for _, r in insider_df.iterrows():
            insider_rows.append({
                "date":   r.get("trade_date", ""),
                "name":   r.get("insider_name", ""),
                "title":  r.get("insider_title", ""),
                "type":   "買入" if r.get("transaction_type") == "P" else "賣出",
                "is_buy": r.get("transaction_type") == "P",
                "shares": f"{r['shares']:,.0f}" if pd.notna(r.get("shares")) else "—",
                "price":  f"${r['price_per_share']:.2f}" if pd.notna(r.get("price_per_share")) else "—",
                "value":  f"${r['total_value']:,.0f}" if pd.notna(r.get("total_value")) else "—",
            })

    # Format options
    options_latest = {}
    if not options_df.empty:
        latest = options_df.iloc[-1]
        call_vol = latest.get("call_volume") or 0
        put_vol  = latest.get("put_volume") or 0
        otm_call = latest.get("otm_call_volume") or 0
        options_latest = {
            "cp_ratio": f"{call_vol / (put_vol + 1e-9):.2f}",
            "otm_pct":  f"{otm_call / (call_vol + 1e-9) * 100:.1f}%" if call_vol else "—",
            "unusual":  f"{latest.get('unusual_call_strikes', 0):.0f}",
        }

    # 13D/13G 大戶
    holder_rows = []
    if not holders_df.empty:
        for _, r in holders_df.iterrows():
            form  = str(r.get("form_type", ""))
            label = ("🔴 主動持股" if "13D" in form and "/A" not in form
                     else "🟡 修正申報" if "13D/A" in form
                     else "🔵 被動持股" if "13G" in form else form)
            holder_rows.append({
                "date":  r.get("filed_date", ""),
                "form":  label,
                "filer": r.get("filer_name", ""),
            })

    # 機構持倉（13F，大型股 13D/13G 為空時顯示）
    inst_holders = load_institutional_holders(ticker)

    # 技術面信號
    t = load_tech_signal(ticker)
    tech_sig = {
        "entry_score":       t.get("entry_score"),
        "exit_risk":         t.get("exit_risk"),
        "ma_aligned":        t.get("ma_aligned", 0),
        "rsi_reversal":      t.get("rsi_reversal", 0),
        "macd_golden":       t.get("macd_golden", 0),
        "bb_breakout":       t.get("bb_breakout", 0),
        "double_bottom":     t.get("double_bottom", 0),
        "vcp_pattern":       t.get("vcp_pattern", 0),
        "hard_stop":         t.get("hard_stop", 0),
        "below_20ma_3d":     t.get("below_20ma_3d", 0),
        "rsi_divergence":    t.get("rsi_divergence", 0),
        "atr_trailing_stop": t.get("atr_trailing_stop", 0),
        "rsi_14":            t.get("rsi_14"),
        "macd_hist":         t.get("macd_hist"),
        "vol_ratio":         t.get("vol_ratio"),
        "ma_20":             t.get("ma_20"),
        "ma_50":             t.get("ma_50"),
        "ma_200":            t.get("ma_200"),
        "atr_14":            t.get("atr_14"),
    } if t else {}

    return templates.TemplateResponse("partials/us_stock_detail.html", {
        "request":        request,
        "ticker":         ticker,
        "insider_rows":   insider_rows,
        "options_latest": options_latest,
        "holder_rows":    holder_rows,
        "inst_holders":   inst_holders,
        "tech_sig":       tech_sig,
    })
