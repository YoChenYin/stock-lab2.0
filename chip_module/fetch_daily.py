"""
chip_module/fetch_daily.py
每日排程的主入口，按順序執行所有 fetcher。

每個步驟獨立 try/catch — 單步失敗不中斷後續流程。
執行結果寫入 run_log 表並寄送 Gmail 報告。

用法：
    python -m chip_module.fetch_daily
    或由 Zeabur Cron Job 觸發（建議台灣時間 23:30，美股收盤後）
"""

import argparse
import json
import logging
import re
import sqlite3
from pathlib import Path
from datetime import datetime

_VALID_TICKER = re.compile(r'^[A-Z]{1,5}$')

from .db.schema import init_db, DB_PATH
from .fetchers.prices import fetch_prices, fetch_institutional
from .fetchers.insider import fetch_insider
from .fetchers.short_interest import fetch_short_interest
from .fetchers.options_sentiment import fetch_options_sentiment
from .fetchers.options_flow import fetch_options_flow
from .fetchers.large_holder import fetch_large_holders
from .signals.composite import run as calc_scores
from .signals.technical_signal import run as calc_tech_signals
from .fetchers.market_env import fetch_market_env
from .signals.factor_max import run as calc_factor_max
from .fetchers.tw_prefetch import run as prefetch_tw
from .notifier import send_run_report

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger(__name__)


def load_watchlist_from_json() -> list[str]:
    json_path = Path(__file__).parent / "us_universe.json"
    try:
        if not json_path.exists():
            log.warning(f"找不到檔案: {json_path}，使用空列表")
            return []
        with open(json_path, "r", encoding="utf-8") as f:
            universe_data = json.load(f)
        watchlist = [t for t in universe_data if _VALID_TICKER.match(t)]
        log.info(f"載入 {len(watchlist)} 個標的（已過濾 warrant/unit）")
        return watchlist
    except Exception as e:
        log.error(f"讀取 JSON 時發生錯誤: {e}")
        return []


def _write_run_log(run_date: str, started_at: str, finished_at: str,
                   status: str, steps_ok: int, steps_fail: int, detail: dict) -> None:
    try:
        with sqlite3.connect(DB_PATH) as conn:
            conn.execute("""
                INSERT OR REPLACE INTO run_log
                (run_date, started_at, finished_at, status, steps_ok, steps_fail, detail)
                VALUES (?, ?, ?, ?, ?, ?, ?)
            """, (run_date, started_at, finished_at, status,
                  steps_ok, steps_fail, json.dumps(detail, ensure_ascii=False)))
    except Exception as e:
        log.warning(f"[fetch_daily] run_log 寫入失敗: {e}")


def run(tickers: list, skip_institutional: bool = False) -> None:
    start_time = datetime.now()
    run_date   = start_time.strftime("%Y-%m-%d")
    results: dict = {}

    log.info(f"=== 每日籌碼更新開始，目標 {len(tickers)} 支股票 ===")

    def _step(name: str, fn, *args, **kwargs) -> None:
        t0 = datetime.now()
        try:
            fn(*args, **kwargs)
            elapsed = (datetime.now() - t0).total_seconds()
            results[name] = {"ok": True, "elapsed_s": elapsed}
            log.info(f"  ✓ {name} ({elapsed:.0f}s)")
        except Exception as e:
            elapsed = (datetime.now() - t0).total_seconds()
            results[name] = {"ok": False, "elapsed_s": elapsed, "error": str(e)[:400]}
            log.error(f"  ✗ {name} ({elapsed:.0f}s): {e}")

    # ── 執行各步驟 ────────────────────────────────────────────────
    _step("init_db",            init_db)
    _step("prices",             fetch_prices,          tickers, lookback_days=60)
    _step("insider",            fetch_insider,         tickers, days_back=30)
    _step("short_interest",     fetch_short_interest,  tickers)
    _step("options_sentiment",  fetch_options_sentiment)
    _step("options_flow",       fetch_options_flow,    tickers)
    _step("large_holders",      fetch_large_holders,   tickers, days_back=90)
    if not skip_institutional:
        _step("institutional",  fetch_institutional,   tickers)
    _step("market_env",         fetch_market_env)
    _step("tech_signals",       calc_tech_signals,     tickers)
    _step("scores",             calc_scores,           tickers)
    _step("factor_max",         calc_factor_max)
    _step("tw_prefetch",        prefetch_tw)

    # ── 統計結果 ──────────────────────────────────────────────────
    elapsed    = int((datetime.now() - start_time).total_seconds())
    steps_ok   = sum(1 for v in results.values() if v["ok"])
    steps_fail = sum(1 for v in results.values() if not v["ok"])
    status     = "success" if steps_fail == 0 else ("failed" if steps_ok == 0 else "partial")

    log.info(f"=== 更新完成，耗時 {elapsed}s | ✓ {steps_ok} / ✗ {steps_fail} ===")

    # ── 寫入 run_log ──────────────────────────────────────────────
    _write_run_log(
        run_date,
        start_time.isoformat(timespec="seconds"),
        datetime.now().isoformat(timespec="seconds"),
        status, steps_ok, steps_fail, results,
    )

    # ── 寄送 Gmail 報告 ───────────────────────────────────────────
    send_run_report(status, results, elapsed, run_date)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="每日籌碼資料更新")
    parser.add_argument(
        "--tickers", nargs="+",
        default=load_watchlist_from_json(),
        help="指定追蹤股票，例如：--tickers NVDA TSLA AAPL"
    )
    parser.add_argument(
        "--skip-institutional", action="store_true",
        help="跳過機構持倉抓取（節省時間）"
    )
    args = parser.parse_args()
    run(tickers=args.tickers, skip_institutional=args.skip_institutional)
