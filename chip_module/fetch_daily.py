"""
chip_module/fetch_daily.py
每日排程的主入口，按順序執行所有 fetcher。
用法：
    python -m chip_module.fetch_daily
    或由 Zeabur Cron Job 觸發（建議台灣時間 23:30，美股收盤後）
"""

import argparse
import logging
import json
import re
from pathlib import Path
from datetime import datetime

_VALID_TICKER = re.compile(r'^[A-Z]{1,5}$')

from .db.schema import init_db
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

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger(__name__)

# ── 你的追蹤名單（整合進 stock lab 時改成從 DB 讀取）────────────
def load_watchlist_from_json():
    # 取得當前檔案所在目錄，並指向 us_universe.json
    # 假設 json 檔跟此腳本在同一個資料夾，或在專案根目錄
    base_path = Path(__file__).parent
    json_path = base_path / "us_universe.json"
    
    try:
        if not json_path.exists():
            log.warning(f"找不到檔案: {json_path}，使用空列表")
            return []
            
        with open(json_path, "r", encoding="utf-8") as f:
            universe_data = json.load(f)
            # 取得所有 Key (Ticker)，並轉換成清單
            watchlist = [t for t in universe_data if _VALID_TICKER.match(t)]
            log.info(f"成功從 {json_path} 載入 {len(watchlist)} 個標的（已過濾 warrant/unit）")
            return watchlist
    except Exception as e:
        log.error(f"讀取 JSON 時發生錯誤: {e}")
        return []


def run(tickers: list, skip_institutional: bool = False):
    start_time = datetime.now()
    log.info(f"=== 每日籌碼更新開始，目標 {len(tickers)} 支股票 ===")

    # 1. 確保 schema 存在（冪等操作）
    log.info("Step 1/5: 初始化 DB schema")
    init_db()

    # 2. 股價 + 技術指標（每日必跑）
    log.info("Step 2/5: 股價 & 量能指標")
    fetch_prices(tickers, lookback_days=60)

    # 3. 內部人交易（每日，只抓最近 30 天）
    log.info("Step 3/5: 內部人交易 Form 4")
    fetch_insider(tickers, days_back=30)

    # 4. 空頭興趣（FINRA 每半月，重複執行安全）
    log.info("Step 4/5: 空頭興趣 (FINRA)")
    fetch_short_interest(tickers)

    # 5. 選擇權情緒（CBOE P/C Ratio，每日）
    log.info("Step 5/6: 選擇權 P/C Ratio (SPY)")
    fetch_options_sentiment()

    # 6. 個股選擇權流量快照（每日）
    log.info("Step 6/6: 個股選擇權流量")
    fetch_options_flow(tickers)

    # 7. 13D/13G 大戶持股申報（每日）
    log.info("Step +: 大戶持股申報 (EDGAR 13D/13G)")
    fetch_large_holders(tickers, days_back=90)

    # 8. 機構持倉（可選，建議週跑一次即可）
    if not skip_institutional:
        log.info("Step +: 機構持倉 (yfinance)")
        fetch_institutional(tickers)

    # 9. 市場環境指標（VIX / 10Y / Mag7 / 板塊 ETF）
    log.info("Step +: 市場環境指標")
    fetch_market_env()

    # 10. 技術面進出場信號（每支股票）
    log.info("Step +: 技術面信號 (Entry/Exit Score)")
    calc_tech_signals(tickers)

    # 11. 計算籌碼綜合分數（所有資料到位後才跑）
    log.info("Step final: 籌碼綜合分數")
    calc_scores(tickers)

    # 12. Factor MAX（因子組合動能）
    log.info("Step +: Factor MAX")
    calc_factor_max()

    # 13. 台股資料預抓（暖機 finmind_cache.db）
    log.info("Step +: 台股資料預抓")
    prefetch_tw()

    elapsed = (datetime.now() - start_time).seconds
    log.info(f"=== 更新完成，耗時 {elapsed}s ===")


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
