"""
chip_module/fetchers/tw_prefetch.py
預先抓取所有台股資料，暖機 finmind_cache.db，並預計算三種 scan 結果。

執行順序：
  1. 逐一 prefetch 174 支股票（增量抓取，只補今日差量）
  2. 一次性用已暖好的 cache 計算 unified / exit / all 三種 scan
  3. 把結果寫入 tw_scan_cache 表，API routes 優先讀取此表

每日台股收盤後（TST 14:30 = UTC 06:30）執行。
"""

import sys
import time
from pathlib import Path
from tqdm import tqdm

_ROOT = Path(__file__).resolve().parents[3]
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

from engine.wall_street_engine import WallStreetEngine
from engine.cache import DataCacheManager
from sector_data import STOCK_POOL

SLEEP_BETWEEN = 0.5   # FinMind 免費版每秒 ~2 requests，0.5s 間隔安全


def run(tickers: list[str] | None = None):
    """
    Step 1: 預抓所有台股資料進 finmind_cache.db（增量）。
    Step 2: 預計算三種 scan 並寫入 tw_scan_cache。
    tickers=None 時使用 STOCK_POOL 全部。
    """
    targets = tickers or list(STOCK_POOL.keys())
    print(f"[tw_prefetch] 開始預抓 {len(targets)} 支台股...")

    ok, skip, fail = 0, 0, 0
    for sid in tqdm(targets, desc="[tw_prefetch] fetch"):
        try:
            engine = WallStreetEngine()
            df, _ = engine.fetch_data(sid)
            if df.empty:
                skip += 1
            else:
                ok += 1
            time.sleep(SLEEP_BETWEEN)
        except Exception as e:
            print(f"[tw_prefetch] {sid} 失敗: {e}")
            fail += 1

    print(f"[tw_prefetch] 抓取完成：ok={ok}, skip={skip}, fail={fail}")

    # Step 2: 預計算 scan（cache 已暖，直接從 SQLite 讀，不打 API）
    _precompute_scans(targets)


def _precompute_scans(targets: list[str]):
    """用已暖的 cache 計算三種 scan，結果存入 tw_scan_cache。"""
    from engine.tw_scanner import run_unified_scan, run_exit_scan, run_all_scan, prefetch_stocks

    cache = DataCacheManager()
    items = [(sid, STOCK_POOL[sid]) for sid in targets if sid in STOCK_POOL]

    print("[tw_prefetch] 預計算 scan（從 cache 讀取，不打 API）...")

    # 一次性並行載入所有 cache，供三種 scan 共用
    prefetched = prefetch_stocks([sid for sid, _ in items], max_workers=20)

    print("[tw_prefetch] 計算 unified scan...")
    unified = run_unified_scan(items, prefetched=prefetched)
    cache.set_scan("unified", unified)

    print("[tw_prefetch] 計算 exit scan...")
    exit_results = run_exit_scan(items, prefetched=prefetched)
    cache.set_scan("exit", exit_results)

    print("[tw_prefetch] 計算 all scan...")
    all_results = run_all_scan(items, prefetched=prefetched)
    cache.set_scan("all", all_results)

    print(f"[tw_prefetch] scan 預計算完成：unified={len(unified)}, exit={len(exit_results)}, all={len(all_results)}")
