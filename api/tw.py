"""
api/tw.py — 台股 routes

GET  /tw/          → 台股主頁面（族群熱圖 + 選股）
POST /tw/scan      → HTMX: 執行選股掃描，回傳結果 partial
GET  /tw/heatmap   → HTMX: 族群熱圖資料（Plotly JSON + 明細卡片）
GET  /tw/stock/{sid}/detail → HTMX: 個股展開（法說 + 策略回測）

掃描邏輯全部移至 engine/tw_scanner.py（prefetch 也用同一份）。
Routes 優先讀 tw_scan_cache（每日 prefetch 後預計算），cache miss 才即時算。
"""

import datetime
from fastapi import APIRouter, Request
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates

import sys, os
sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

from engine.wall_street_engine import WallStreetEngine
from engine.smart_money import calc_smart_money_score
from engine.cache import DataCacheManager
from engine.tw_scanner import run_unified_scan, run_exit_scan, run_all_scan, run_heatmap
from sector_data import STOCK_POOL

router = APIRouter()
templates = Jinja2Templates(directory="templates")
_cache = DataCacheManager()


def _get_engine() -> WallStreetEngine:
    return WallStreetEngine()


def _stock_items() -> list:
    return list(STOCK_POOL.items())


# ── Routes ────────────────────────────────────────────────────────────

@router.get("/", response_class=HTMLResponse)
async def tw_page(request: Request):
    from tabs.chip_radar._db import load_last_updated
    fetch_time   = datetime.datetime.now().strftime("%Y-%m-%d %H:%M")
    last_updated = load_last_updated()
    return templates.TemplateResponse("tw.html", {
        "request":      request,
        "fetch_time":   fetch_time,
        "last_updated": last_updated,
    })


@router.get("/factor-max", response_class=HTMLResponse)
async def tw_factor_max(request: Request):
    from chip_module.signals.factor_ranker import FactorRanker
    heatmap = FactorRanker(market="TW").fit().get_heatmap()
    return templates.TemplateResponse("partials/factor_heatmap.html", {
        "request":         request,
        "factor_max_list": heatmap,
        "market":          "TW",
    })


@router.post("/scan", response_class=HTMLResponse)
async def tw_scan(request: Request):
    """優先讀 prefetch 預計算的 scan cache；cache miss 才即時掃描。"""
    stocks = _cache.get_scan("unified")
    if stocks is None:
        stocks = run_unified_scan(_stock_items())
    fetch_time = datetime.datetime.now().strftime("%Y-%m-%d %H:%M")
    return templates.TemplateResponse("partials/tw_scan_results.html", {
        "request":    request,
        "stocks":     stocks,
        "fetch_time": fetch_time,
    })


@router.post("/exit-scan", response_class=HTMLResponse)
async def tw_exit_scan(request: Request):
    stocks = _cache.get_scan("exit")
    if stocks is None:
        stocks = run_exit_scan(_stock_items())
    fetch_time = datetime.datetime.now().strftime("%Y-%m-%d %H:%M")
    return templates.TemplateResponse("partials/tw_exit_results.html", {
        "request":    request,
        "stocks":     stocks,
        "fetch_time": fetch_time,
    })


@router.post("/all-scan", response_class=HTMLResponse)
async def tw_all_scan(request: Request):
    stocks = _cache.get_scan("all")
    if stocks is None:
        stocks = run_all_scan(_stock_items())
    fetch_time = datetime.datetime.now().strftime("%Y-%m-%d %H:%M")
    return templates.TemplateResponse("partials/tw_all_results.html", {
        "request":    request,
        "stocks":     stocks,
        "fetch_time": fetch_time,
    })


@router.get("/heatmap", response_class=HTMLResponse)
async def tw_heatmap(request: Request):
    heatmap_rows, sector_details = run_heatmap()
    return templates.TemplateResponse("partials/tw_heatmap.html", {
        "request":        request,
        "heatmap_rows":   heatmap_rows,
        "sector_details": sector_details,
        "sectors":        [r["sector"] for r in heatmap_rows],
    })


@router.get("/stock/{sid}/detail", response_class=HTMLResponse)
async def tw_stock_detail(request: Request, sid: str):
    engine = _get_engine()
    name = STOCK_POOL.get(sid, sid)

    mops = engine.fetch_latest_mops_pdf_info(sid)

    df, _ = engine.fetch_data(sid)
    strategy_data = {}
    if not df.empty:
        curr  = float(df["close"].iloc[-1])
        low52 = float(df["close"].tail(252).min())
        hi52  = float(df["close"].tail(252).max())
        sms   = calc_smart_money_score(df) if "f_net" in df.columns else {"score": 0}
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
