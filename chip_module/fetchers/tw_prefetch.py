"""
chip_module/fetchers/tw_prefetch.py
預先抓取所有台股資料，暖機 finmind_cache.db。

每日台股收盤後（TST 14:30 / UTC 06:30）執行，
讓用戶點「掃描」時直接從 SQLite cache 讀取，不需即時打 FinMind API。
"""

import sys
import os
import time
from pathlib import Path
from tqdm import tqdm

# 讓 chip_module 可以 import 專案根目錄的 engine / sector_data
_ROOT = Path(__file__).resolve().parents[3]
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

from engine.wall_street_engine import WallStreetEngine
from sector_data import STOCK_POOL

SLEEP_BETWEEN = 0.5   # FinMind 免費版每秒 ~2 requests，0.5s 間隔安全


def run(tickers: list[str] | None = None):
    """
    預抓所有台股資料進 finmind_cache.db。
    tickers=None 時使用 STOCK_POOL 全部。
    """
    targets = tickers or list(STOCK_POOL.keys())
    print(f"[tw_prefetch] 開始預抓 {len(targets)} 支台股...")

    ok, skip, fail = 0, 0, 0

    for sid in tqdm(targets, desc="[tw_prefetch]"):
        try:
            engine = WallStreetEngine()
            df, rev = engine.fetch_data(sid)
            if df.empty:
                skip += 1
            else:
                ok += 1
            time.sleep(SLEEP_BETWEEN)
        except Exception as e:
            print(f"[tw_prefetch] {sid} 失敗: {e}")
            fail += 1

    print(f"[tw_prefetch] 完成：ok={ok}, skip={skip}, fail={fail}")
