"""
chip_module/build_universe.py
建立美股追蹤宇宙（us_universe.json）

流程：
  1. 從 Nasdaq Trader 下載全納斯達克上市股票清單
  2. 用 yfinance 批次下載近 1 個月 OHLCV
  3. 剔除月交易量 < MIN_MONTHLY_VOLUME 股（流動性過低）
  4. 剔除 ETF、基金、warrants（ticker 含 ^ / . 或 長度 > 5）
  5. 加入 yfinance sector / shortName 資訊
  6. 輸出至 chip_module/us_universe.json

使用方式：
    python -m chip_module.build_universe
    python -m chip_module.build_universe --min-vol 50000 --out custom_universe.json
"""

import argparse
import json
import time
import requests
import pandas as pd
import yfinance as yf
from io import StringIO
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed

NASDAQ_LISTED_URL = (
    "https://www.nasdaqtrader.com/dynamic/SymDir/nasdaqlisted.txt"
)
OUT_PATH = Path(__file__).parent / "us_universe.json"
MIN_MONTHLY_VOLUME = 5_000_000   # 近一個月總交易量門檻（約 25 萬股/日）

# 已知概念分類（供 build_universe 覆寫 sector）
CONCEPT_MAP: dict[str, str] = {
    "NVDA": "AI算力",    "AMD": "AI算力",    "INTC": "AI算力",
    "QCOM": "AI算力",    "MRVL": "AI算力",   "AVGO": "AI算力",
    "ARM":  "AI算力",    "SMCI": "AI算力",
    "AMAT": "半導體設備", "LRCX": "半導體設備","KLAC": "半導體設備",
    "ASML": "半導體設備", "TER":  "半導體設備",
    "MSFT": "雲端AI",    "GOOGL": "雲端AI",  "GOOG": "雲端AI",
    "AMZN": "雲端AI",    "META": "雲端AI",   "ORCL": "雲端AI",
    "CRM":  "SaaS",     "NOW":  "SaaS",     "SNOW": "SaaS",
    "DDOG": "SaaS",     "MDB":  "SaaS",     "WDAY": "SaaS",
    "VST":  "電力基建",  "CEG":  "電力基建",  "NRG":  "電力基建",
    "NEE":  "電力基建",  "SO":   "電力基建",  "AEP":  "電力基建",
    "EQIX": "資料中心",  "DLR":  "資料中心",  "AMT":  "資料中心",
    "CRWD": "網路安全",  "PANW": "網路安全",  "FTNT": "網路安全",
    "TSLA": "電動車",    "NIO":  "電動車",    "RIVN": "電動車",
    "V":    "金融科技",  "MA":   "金融科技",  "PYPL": "金融科技",
    "COIN": "金融科技",
    "AAPL": "消費科技",  "NFLX": "消費科技",  "SPOT": "消費科技",
    "LLY":  "生技醫療",  "MRNA": "生技醫療",  "REGN": "生技醫療",
    "ENPH": "新能源",    "FSLR": "新能源",
    "JPM":  "金融",     "BAC":  "金融",     "GS":   "金融",
}


def _fetch_nasdaq_tickers() -> list[str]:
    """下載納斯達克上市股票，過濾 ETF / warrant / 非標準 ticker"""
    print("Downloading Nasdaq listed stocks...")
    r = requests.get(NASDAQ_LISTED_URL, timeout=30)
    r.raise_for_status()
    # 格式：Symbol|Security Name|Market Category|...|ETF|...|File Creation Time
    df = pd.read_csv(StringIO(r.text), sep="|")
    # 最後一行是 File Creation Time，不是真正的資料
    df = df[df["Symbol"].notna()]
    df = df[~df["Symbol"].str.startswith("File Creation")]

    # 過濾 ETF
    if "ETF" in df.columns:
        df = df[df["ETF"] != "Y"]

    # 過濾非標準 ticker（含特殊字元、長度 > 5）
    df = df[df["Symbol"].str.match(r"^[A-Z]{1,5}$", na=False)]

    tickers = sorted(df["Symbol"].unique().tolist())
    print(f"  → {len(tickers)} tickers after ETF/warrant filter")
    return tickers


def _batch_volume(tickers: list[str], period: str = "1mo") -> dict[str, int]:
    """批次下載近 1 個月成交量，回傳 {ticker: total_volume}"""
    print(f"Downloading {len(tickers)} tickers volume ({period})...")
    batch_size = 200
    vol_map: dict[str, int] = {}

    for i in range(0, len(tickers), batch_size):
        batch = tickers[i:i + batch_size]
        try:
            data = yf.download(batch, period=period, progress=False,
                               auto_adjust=True, threads=True)
            if data.empty:
                continue
            vol = data["Volume"] if "Volume" in data.columns else data[("Volume",)]
            if isinstance(vol.columns, pd.MultiIndex):
                vol.columns = [c[0] if isinstance(c, tuple) else c for c in vol.columns]
            for t in batch:
                if t in vol.columns:
                    total = int(vol[t].dropna().sum())
                    vol_map[t] = total
        except Exception as e:
            print(f"  batch {i}-{i+batch_size} error: {e}")
        time.sleep(0.5)
        if i % 1000 == 0 and i > 0:
            print(f"  processed {i}/{len(tickers)}")

    return vol_map


def _fetch_info_batch(tickers: list[str], max_workers: int = 20) -> dict[str, dict]:
    """平行查詢 yfinance Ticker.info 取得 sector / shortName"""
    print(f"Fetching sector info for {len(tickers)} tickers (parallel)...")
    results: dict[str, dict] = {}

    def _get(ticker: str) -> tuple[str, dict]:
        try:
            info = yf.Ticker(ticker).fast_info
            return ticker, {
                "name":   getattr(info, "name", ticker) or ticker,
                "sector": "",   # fast_info 沒有 sector，用 concept map 覆蓋
            }
        except Exception:
            return ticker, {"name": ticker, "sector": ""}

    with ThreadPoolExecutor(max_workers=max_workers) as exe:
        futs = {exe.submit(_get, t): t for t in tickers}
        done = 0
        for fut in as_completed(futs):
            t, info = fut.result()
            results[t] = info
            done += 1
            if done % 200 == 0:
                print(f"  info fetched: {done}/{len(tickers)}")

    return results


def build_universe(
    min_vol: int = MIN_MONTHLY_VOLUME,
    out_path: Path = OUT_PATH,
    skip_info: bool = False,
) -> dict:
    """
    主流程：下載 → 過濾流動性 → 補充資訊 → 存檔
    """
    # 1. 取得所有 Nasdaq 股票
    all_tickers = _fetch_nasdaq_tickers()

    # 2. 批次下載月成交量
    vol_map = _batch_volume(all_tickers)

    # 3. 過濾流動性
    liquid = [t for t, v in vol_map.items() if v >= min_vol]
    excluded = len(all_tickers) - len(liquid)
    print(f"Volume filter: {len(liquid)} pass / {excluded} excluded (< {min_vol:,} shares/month)")

    # 4. 補充名稱 & sector（可跳過，加快速度）
    if skip_info:
        info_map = {t: {"name": t, "sector": ""} for t in liquid}
    else:
        info_map = _fetch_info_batch(liquid)

    # 5. 套用 concept map，覆蓋 sector
    universe: dict = {}
    for t in liquid:
        info = info_map.get(t, {})
        sector = CONCEPT_MAP.get(t) or info.get("sector") or ""
        universe[t] = {
            "name":   info.get("name") or t,
            "sector": sector,
            "index":  "NASDAQ",
        }

    # 6. 存檔
    out_path.parent.mkdir(parents=True, exist_ok=True)
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(universe, f, ensure_ascii=False, indent=2)

    print(f"\nDone! {len(universe)} tickers → {out_path}")
    return universe


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Build US stock universe")
    parser.add_argument("--min-vol", type=int, default=MIN_MONTHLY_VOLUME,
                        help=f"Min monthly volume (default {MIN_MONTHLY_VOLUME:,})")
    parser.add_argument("--out", type=str, default=str(OUT_PATH),
                        help="Output JSON path")
    parser.add_argument("--skip-info", action="store_true",
                        help="Skip yfinance info fetch (faster, no name/sector)")
    args = parser.parse_args()

    build_universe(
        min_vol=args.min_vol,
        out_path=Path(args.out),
        skip_info=args.skip_info,
    )
