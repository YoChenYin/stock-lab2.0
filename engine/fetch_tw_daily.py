"""
engine/fetch_tw_daily.py
台股每日資料預熱：遍歷 STOCK_POOL，把 daily / institutional / revenue
全部抓進 finmind_cache.db，讓 app 直接從 DB 讀，不需要打 API。

執行方式（排程內呼叫）：
    from engine.fetch_tw_daily import run
    run()
"""

import os
import time
import logging
from datetime import date, timedelta

from FinMind.data import DataLoader

from engine.cache import DataCacheManager

log = logging.getLogger(__name__)


def run():
    from sector_data import STOCK_POOL

    fm_token = os.environ.get("FINMIND_TOKEN", "")
    if not fm_token:
        log.warning("[tw_daily] FINMIND_TOKEN 未設定，跳過台股更新")
        return

    dl = DataLoader()
    try:
        dl.login_by_token(api_token=fm_token)
    except Exception as e:
        log.error(f"[tw_daily] FinMind login 失敗: {e}")
        return

    cache = DataCacheManager()
    start = (date.today() - timedelta(days=730)).strftime("%Y-%m-%d")
    tickers = list(STOCK_POOL.keys())

    log.info(f"[tw_daily] 開始更新台股資料，共 {len(tickers)} 支")
    success, failed = 0, 0

    for i, sid in enumerate(tickers):
        try:
            # 強制清掉今天 cache，確保拿到最新資料
            cache.invalidate(sid, "daily")
            cache.invalidate(sid, "institutional")
            cache.invalidate(sid, "revenue")

            for data_type, fetch_fn in [
                ("daily",       lambda: dl.taiwan_stock_daily(stock_id=sid, start_date=start)),
                ("institutional", lambda: dl.taiwan_stock_institutional_investors(stock_id=sid, start_date=start)),
                ("revenue",     lambda: dl.taiwan_stock_month_revenue(stock_id=sid, start_date=start)),
            ]:
                df = fetch_fn()
                if not df.empty:
                    cache.set(sid, data_type, df)

            success += 1
            time.sleep(0.3)  # rate limit

            if (i + 1) % 20 == 0:
                log.info(f"[tw_daily]   {i+1}/{len(tickers)} 完成")

        except Exception as e:
            log.error(f"[tw_daily] {sid} 失敗: {e}")
            failed += 1
            time.sleep(1)

    log.info(f"[tw_daily] 完成，成功={success} 失敗={failed}")
