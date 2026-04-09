"""
engine/background.py
統一背景排程，容器啟動時呼叫 start_scheduler() 一次。

排程時間（UTC）：
  - 台股：06:30 UTC = 台灣 14:30（收盤後立即抓）
  - 美股：15:00 UTC = 台灣 23:00（美股收盤後 1 小時）
  - 週末跳過（schedule 預設對 every().day 不分週末，用 weekday guard 處理）
"""

import logging
import threading
import time
import datetime

import schedule

log = logging.getLogger(__name__)

_started = False
_lock = threading.Lock()


def _is_weekday() -> bool:
    return datetime.datetime.utcnow().weekday() < 5  # 0=Mon, 4=Fri


def _job_tw():
    if not _is_weekday():
        return
    log.info("[scheduler] 開始台股每日資料更新")
    try:
        from engine.fetch_tw_daily import run
        run()
        # 清除 st.cache_data 讓 app 下次讀時重新從 DB 載入
        import streamlit as st
        st.cache_data.clear()
        log.info("[scheduler] 台股更新完成，st.cache_data 已清除")
    except Exception as e:
        log.error(f"[scheduler] 台股更新失敗: {e}")


def _job_us():
    if not _is_weekday():
        return
    log.info("[scheduler] 開始美股每日資料更新")
    try:
        from chip_module.fetch_daily import load_watchlist_from_json, run
        tickers = load_watchlist_from_json()
        run(tickers=tickers, skip_institutional=False)
        log.info("[scheduler] 美股更新完成")
    except Exception as e:
        log.error(f"[scheduler] 美股更新失敗: {e}")


def _run_loop():
    while True:
        schedule.run_pending()
        time.sleep(30)


def start_scheduler():
    global _started
    with _lock:
        if _started:
            return
        _started = True

    schedule.every().day.at("06:30").do(_job_tw)   # UTC 06:30 = 台灣 14:30
    schedule.every().day.at("15:00").do(_job_us)   # UTC 15:00 = 台灣 23:00

    t = threading.Thread(target=_run_loop, daemon=True, name="bg-scheduler")
    t.start()
    log.info("[scheduler] 背景排程已啟動：台股 UTC 06:30，美股 UTC 15:00")
