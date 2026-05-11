"""
engine/wall_street_engine.py — WallStreetEngine

Responsibilities:
  - All FinMind API calls (via _smart_fetch → SQLite cache)
  - Real chip data: compute foreign ownership % from actual holdings data
  - MOPS scraper

Design rules:
  - _self pattern on instance methods (legacy Streamlit convention, kept for clarity)
  - _smart_fetch is the ONLY entry point to FinMind API
"""

import pandas as pd
import numpy as np
import datetime
import time
import os
import requests
import urllib3
from FinMind.data import DataLoader
from bs4 import BeautifulSoup

from engine.cache import DataCacheManager

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)


# ─────────────────────────────────────────────────────────
# Engine
# ─────────────────────────────────────────────────────────
class WallStreetEngine:
    def __init__(self, fm_token: str = "", force: bool = False):
        self.dl       = DataLoader()
        self.cache    = DataCacheManager()
        self.fm_token = fm_token or os.environ.get("FINMIND_TOKEN", "")
        self.force    = force  # bypass today-guard for manual re-fetch

        if self.fm_token:
            try:
                self.dl.login_by_token(api_token=self.fm_token)
            except Exception:
                print("FinMind login failed — check FINMIND_TOKEN.")

    # ── internal fetch (cache-first, incremental) ─────────
    def _smart_fetch(self, sid: str, data_type: str, fetch_func,
                     dedup_keys: tuple = ("date",), **kwargs) -> pd.DataFrame:
        cached_df, last_updated = self.cache.get(sid, data_type)
        today = datetime.date.today().isoformat()

        if last_updated == today and not self.force:
            return cached_df  # Already fresh today, no API call needed

        # Incremental: if we have cached data, only fetch the delta since last update
        fetch_kwargs = dict(kwargs)
        if last_updated:
            next_day = (
                datetime.date.fromisoformat(last_updated) + datetime.timedelta(days=1)
            ).isoformat()
            fetch_kwargs["start_date"] = next_day

        time.sleep(0.2)
        try:
            new_data = fetch_func(stock_id=sid, **fetch_kwargs)
            if isinstance(new_data, dict):
                new_data = pd.DataFrame(new_data.get("data", []))

            if new_data is not None and not new_data.empty:
                if cached_df is not None and not cached_df.empty:
                    result = (
                        pd.concat([cached_df, new_data])
                        .drop_duplicates(subset=list(dedup_keys), keep="last")
                        .sort_values("date")
                        .reset_index(drop=True)
                    )
                else:
                    result = new_data
                self.cache.set(sid, data_type, result)
                return result

            # No new data (holiday / weekend) — mark as current so we skip tomorrow
            if cached_df is not None and not cached_df.empty:
                self.cache.touch(sid, data_type)
                return cached_df
            return pd.DataFrame()

        except Exception as e:
            print(f"[API Error] {sid} / {data_type}: {e}")
            return cached_df if cached_df is not None else pd.DataFrame()

    # ─────────────────────────────────────────────────────
    # DATA FETCHERS
    # ─────────────────────────────────────────────────────

    def fetch_data(_self, sid: str) -> tuple[pd.DataFrame, pd.DataFrame]:
        """Returns (df_daily_with_institutional_and_valuation, df_revenue).

        df columns include:
          price:   date, open, high, low, close, trading_volume
          chip:    f_net, it_net, ma20
          valuation: market_value (NT$千), log_mkt_cap, pbratio, peratio, psratio
        """
        start = (datetime.date.today() - datetime.timedelta(days=730)).strftime("%Y-%m-%d")

        p = _self._smart_fetch(sid, "daily", _self.dl.taiwan_stock_daily, start_date=start)
        if p.empty:
            return pd.DataFrame(), pd.DataFrame()

        p.columns = [c.lower() for c in p.columns]
        p["date"] = pd.to_datetime(p["date"])

        inst = _self._smart_fetch(sid, "institutional",
                                   _self.dl.taiwan_stock_institutional_investors,
                                   dedup_keys=("date", "name"), start_date=start)
        rev  = _self._smart_fetch(sid, "revenue",
                                   _self.dl.taiwan_stock_month_revenue, start_date=start)
        mv   = _self._smart_fetch(sid, "market_value",
                                   _self.dl.taiwan_stock_market_value, start_date=start)

        # taiwan_stock_per_pbr_ps may not exist in older FinMind releases
        _pbr_func = getattr(_self.dl, "taiwan_stock_per_pbr_ps", None)
        val = (
            _self._smart_fetch(sid, "valuation", _pbr_func, start_date=start)
            if _pbr_func is not None
            else pd.DataFrame()
        )

        # Pivot institutional into f_net / it_net columns
        net = pd.DataFrame(index=p["date"].unique()).sort_index()
        net["f_net"] = 0.0
        net["it_net"] = 0.0
        if not inst.empty:
            inst["date"] = pd.to_datetime(inst["date"])
            inst["net_diff"] = inst["buy"] - inst["sell"]
            pivot = inst.pivot_table(index="date", columns="name",
                                     values="net_diff", aggfunc="sum").fillna(0)
            if "Foreign_Investor" in pivot.columns:
                net["f_net"]  = pivot["Foreign_Investor"]
            if "Investment_Trust" in pivot.columns:
                net["it_net"] = pivot["Investment_Trust"]

        df = p.merge(net.reset_index().rename(columns={"index": "date"}),
                     on="date", how="left").fillna(0)
        df["ma20"] = df["close"].rolling(20).mean()

        # Merge market value (NT$千) → derive log_mkt_cap
        if not mv.empty:
            mv = mv.copy()
            mv.columns = [c.lower() for c in mv.columns]
            mv["date"] = pd.to_datetime(mv["date"])
            if "market_value" in mv.columns:
                df = df.merge(mv[["date", "market_value"]], on="date", how="left")
                df["log_mkt_cap"] = np.log(df["market_value"].clip(lower=1))

        # Merge P/E, P/B, P/S ratios
        if not val.empty:
            val = val.copy()
            val.columns = [c.lower() for c in val.columns]
            val["date"] = pd.to_datetime(val["date"])
            ratio_cols = [c for c in ["peratio", "pbratio", "psratio"] if c in val.columns]
            if ratio_cols:
                df = df.merge(val[["date"] + ratio_cols], on="date", how="left")

        return df, rev

    def fetch_latest_mops_pdf_info(self, sid: str) -> dict:
        url = "https://mopsov.twse.com.tw/mops/web/ajax_t100sb07_1"
        headers = {
            "User-Agent": "Mozilla/5.0",
            "Referer": "https://mopsov.twse.com.tw/mops/web/t100sb07_1",
            "Content-Type": "application/x-www-form-urlencoded",
        }
        payload = {
            "encodeURIComponent": "1", "step": "1", "firstin": "true",
            "off": "1", "queryName": "co_id", "inpuType": "co_id",
            "TYPEK": "all", "co_id": sid,
        }
        try:
            res = requests.post(url, data=payload, headers=headers, timeout=10, verify=False)
            res.encoding = "utf-8"
            if "查詢無資料" in res.text:
                return {"date": "無資料", "event": "近期未上傳簡報", "url": "#", "status": "empty"}

            soup = BeautifulSoup(res.text, "html.parser")
            date_lbl = soup.find("td", string=lambda x: x and "召開法人說明會日期" in x)
            date_val = "未知日期"
            if date_lbl:
                td = date_lbl.find_next_sibling("td")
                if td:
                    date_val = td.get_text(separator=" ", strip=True).split("時間")[0].strip()

            info_lbl = soup.find("td", string=lambda x: x and "法人說明會擇要訊息" in x)
            info_val = info_lbl.find_next_sibling("td").get_text(strip=True) if info_lbl else "無摘要"

            pdf_url = "#"
            cn_td = soup.find("td", string=lambda x: x and "中文檔案" in x)
            if cn_td:
                nxt = cn_td.find_next_sibling("td")
                if nxt:
                    a = nxt.find("a")
                    if a and "href" in a.attrs:
                        href = a['href']
                        pdf_url = href if href.startswith('http') else f"https://mopsov.twse.com.tw{href}"

            return {"date": date_val, "event": info_val, "url": pdf_url, "status": "success"}
        except Exception as e:
            return {"date": "讀取錯誤", "event": str(e), "url": "#", "status": "error"}
