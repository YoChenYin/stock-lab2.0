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
    def __init__(self, fm_token: str = ""):
        self.dl       = DataLoader()
        self.cache    = DataCacheManager()
        self.fm_token = fm_token or os.environ.get("FINMIND_TOKEN", "")

        if self.fm_token:
            try:
                self.dl.login_by_token(api_token=self.fm_token)
            except Exception:
                print("FinMind login failed — check FINMIND_TOKEN.")

    # ── internal fetch (cache-first) ──────────────────────
    def _smart_fetch(self, sid: str, data_type: str, fetch_func, **kwargs) -> pd.DataFrame:
        cached = self.cache.get(sid, data_type)
        if cached is not None:
            return cached
        time.sleep(0.2)
        try:
            data = fetch_func(stock_id=sid, **kwargs)
            if isinstance(data, dict):
                data = pd.DataFrame(data.get("data", []))
            if not data.empty:
                self.cache.set(sid, data_type, data)
            return data
        except Exception as e:
            print(f"[API Error] {sid} / {data_type}: {e}")
            return pd.DataFrame()

    # ─────────────────────────────────────────────────────
    # DATA FETCHERS
    # ─────────────────────────────────────────────────────

    def fetch_data(_self, sid: str) -> tuple[pd.DataFrame, pd.DataFrame]:
        """Returns (df_daily_with_institutional, df_revenue)."""
        start = (datetime.date.today() - datetime.timedelta(days=730)).strftime("%Y-%m-%d")

        p = _self._smart_fetch(sid, "daily", _self.dl.taiwan_stock_daily, start_date=start)
        if p.empty:
            return pd.DataFrame(), pd.DataFrame()

        p.columns = [c.lower() for c in p.columns]
        p["date"] = pd.to_datetime(p["date"])

        inst = _self._smart_fetch(sid, "institutional",
                                   _self.dl.taiwan_stock_institutional_investors, start_date=start)
        rev  = _self._smart_fetch(sid, "revenue",
                                   _self.dl.taiwan_stock_month_revenue, start_date=start)

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
        return df, rev

    def fetch_ml_ready_data(_self, sid: str) -> pd.DataFrame:
        """10-year OHLCV + institutional + margin + computed features for ML/backtest."""
        start = (datetime.date.today() - datetime.timedelta(days=3650)).strftime("%Y-%m-%d")
        try:
            p = _self._smart_fetch(sid, "daily", _self.dl.taiwan_stock_daily, start_date=start)
            if p.empty:
                return pd.DataFrame()
            p.columns = [c.lower() for c in p.columns]
            p["date"] = pd.to_datetime(p["date"])
            p = p.sort_values("date").drop_duplicates("date")

            inst = _self._smart_fetch(sid, "institutional",
                                       _self.dl.taiwan_stock_institutional_investors, start_date=start)
            if not inst.empty:
                inst["date"] = pd.to_datetime(inst["date"])
                inst_p = inst.pivot_table(index="date", columns="name",
                                           values=["buy", "sell"], aggfunc="sum").fillna(0)
                net = pd.DataFrame(index=inst_p.index)
                net["f_net"]  = inst_p["buy"].get("Foreign_Investor", 0) - inst_p["sell"].get("Foreign_Investor", 0)
                net["it_net"] = inst_p["buy"].get("Investment_Trust", 0) - inst_p["sell"].get("Investment_Trust", 0)
                net = net.reset_index()
            else:
                net = pd.DataFrame(columns=["date", "f_net", "it_net"])

            margin = _self._smart_fetch(sid, "margin",
                                         _self.dl.taiwan_stock_margin_purchase_short_sale, start_date=start)
            if not margin.empty:
                margin["date"] = pd.to_datetime(margin["date"])
                margin = margin.sort_values("date").drop_duplicates("date")
                margin["m_net"] = margin["MarginPurchaseTodayBalance"].diff().fillna(0)
                m_net = margin[["date", "m_net"]]
            else:
                m_net = pd.DataFrame(columns=["date", "m_net"])

            net["date"]   = pd.to_datetime(net["date"])
            m_net["date"] = pd.to_datetime(m_net["date"])
            df = p.merge(net, on="date", how="left").merge(m_net, on="date", how="left").fillna(0)
            df = df.sort_values("date").reset_index(drop=True)

            df["ma5"]  = df["close"].rolling(5).mean()
            df["ma20"] = df["close"].rolling(20).mean()

            buy_mask   = df["f_net"] > 0
            f_cost_sum = (df["close"] * df["f_net"] * buy_mask).rolling(20).sum()
            f_vol_sum  = (df["f_net"] * buy_mask).rolling(20).sum()
            df["f_cost"]      = (f_cost_sum / (f_vol_sum + 1e-9)).fillna(df["close"])
            df["bias_f_cost"] = (df["close"] - df["f_cost"]) / (df["f_cost"] + 1e-9)
            df["conc"]        = (df["f_net"].abs() + df["it_net"].abs()) / (df["trading_volume"] + 1e-9)
            df["f_streak"]    = (df["f_net"] > 0).astype(int).rolling(5).sum().fillna(0)
            hc = "high" if "high" in df.columns else "max"
            lc = "low"  if "low"  in df.columns else "min"
            df["volatility"]  = (df[hc] - df[lc]) / (df["close"] + 1e-9)
            df["target_max_ret"] = df["close"].shift(-20).rolling(20).max() / df["close"] - 1

            return df.dropna(subset=["ma20", "bias_f_cost"])
        except Exception as e:
            print(f"ML data error: {e}")
            return pd.DataFrame()

    def fetch_quarterly_financials(_self, sid: str) -> pd.DataFrame:
        start = (datetime.date.today() - datetime.timedelta(days=3650)).strftime("%Y-%m-%d")
        try:
            fin = _self._smart_fetch(sid, "financial_stat",
                                      _self.dl.taiwan_stock_financial_statement, start_date=start)
            if fin.empty:
                return pd.DataFrame()
            fin["date"] = pd.to_datetime(fin["date"])
            df_q = fin.pivot_table(index="date", columns="type", values="value").reset_index()

            rev_col = "OperatingRevenue" if "OperatingRevenue" in df_q.columns else "Revenue"
            gp_col  = "GrossProfit" if "GrossProfit" in df_q.columns else "GrossProfitFromOperations"

            if rev_col in df_q.columns:
                if gp_col in df_q.columns:
                    df_q["margin"] = df_q[gp_col] / df_q[rev_col] * 100
                else:
                    cost_col = "CostOfGoodsSold" if "CostOfGoodsSold" in df_q.columns else None
                    if cost_col:
                        df_q["margin"] = (df_q[rev_col] - df_q[cost_col]) / df_q[rev_col] * 100
                df_q = df_q.sort_values("date")
                df_q["rev_yoy"] = df_q[rev_col].pct_change(4) * 100
                if "EPS" in df_q.columns:
                    df_q["eps_yoy"] = df_q["EPS"].pct_change(4) * 100
                if "margin" in df_q.columns:
                    df_q["margin_delta"] = df_q["margin"].diff(4)
                df_q = df_q.rename(columns={rev_col: "Revenue"})

            return df_q.dropna(subset=["EPS"]).tail(12)
        except Exception as e:
            print(f"Financial parse error: {e}")
            return pd.DataFrame()

    def fetch_real_chip_data(_self, sid: str) -> dict:
        """
        Fetch REAL ownership percentages from FinMind.
        Replaces the Gemini-hallucinated chips.major_holder / chips.foreign_inst.

        Returns:
          foreign_pct    — foreign investor ownership %
          major_pct      — top-10 shareholder ownership %  (if available)
          source         — "finmind_real" or "estimated"
        """
        start = (datetime.date.today() - datetime.timedelta(days=90)).strftime("%Y-%m-%d")
        try:
            holding = _self._smart_fetch(
                sid, "holding_shares",
                _self.dl.taiwan_stock_holding_shares_per,
                start_date=start
            )
            if not holding.empty and "ForeignInvestmentRatio" in holding.columns:
                latest = holding.sort_values("date").iloc[-1]
                return {
                    "foreign_pct":  round(float(latest.get("ForeignInvestmentRatio", 0)), 1),
                    "major_pct":    round(float(latest.get("Top10HoldingRatio", 0)), 1),
                    "update_date":  str(latest.get("date", "")[:10]),
                    "source":       "finmind_real",
                }
        except Exception as e:
            print(f"[chip data] {sid}: {e}")

        # Fallback: estimate from cumulative institutional net-buy vs avg volume
        try:
            df, _ = _self.fetch_data(sid)
            if not df.empty and len(df) >= 60:
                avg_shares = df["trading_volume"].tail(60).mean()
                f_accum    = df["f_net"].tail(60).sum()
                est_pct    = round(np.clip(f_accum / (avg_shares * 60) * 100, 0, 80), 1)
                return {
                    "foreign_pct":  est_pct,
                    "major_pct":    None,
                    "update_date":  "estimated",
                    "source":       "estimated",
                }
        except Exception:
            pass
        return {"foreign_pct": None, "major_pct": None, "source": "unavailable"}

    def fetch_detailed_sentiment(_self, sid: str) -> pd.DataFrame:
        start = (datetime.date.today() - datetime.timedelta(days=120)).strftime("%Y-%m-%d")
        try:
            inst   = _self.dl.taiwan_stock_institutional_investors(stock_id=sid, start_date=start)
            margin = _self.dl.taiwan_stock_margin_purchase_short_sale(stock_id=sid, start_date=start)
            if inst.empty:
                return pd.DataFrame()
            inst["date"]    = pd.to_datetime(inst["date"])
            inst["net_buy"] = inst["buy"] - inst["sell"]
            pivot = inst.pivot_table(index="date", columns="name",
                                     values="net_buy", aggfunc="sum").fillna(0)

            def cs(df, col):
                return df[col].cumsum() if col in df.columns else pd.Series(0, index=df.index).cumsum()

            out = pd.DataFrame(index=pivot.index)
            out["f_cumsum"]  = cs(pivot, "Foreign_Investor")
            out["it_cumsum"] = cs(pivot, "Investment_Trust")
            out["d_cumsum"]  = cs(pivot, "Dealer")

            margin["date"] = pd.to_datetime(margin["date"])
            margin = margin[["date", "MarginPurchaseTodayBalance"]].rename(
                columns={"MarginPurchaseTodayBalance": "retail_margin"})
            return out.reset_index().merge(margin, on="date", how="inner")
        except Exception as e:
            print(f"Sentiment error: {e}")
            return pd.DataFrame()

    def fetch_broker_tracking(_self, sid: str) -> list:
        start = (datetime.date.today() - datetime.timedelta(days=120)).strftime("%Y-%m-%d")
        try:
            p = _self.dl.taiwan_stock_daily(stock_id=sid, start_date=start)
            p["date"]   = pd.to_datetime(p["date"])
            p["change"] = p["close"].pct_change()
            surges      = p[p["change"] > 0.04].tail(3)
            insights    = []
            for _, row in surges.iterrows():
                d   = row["date"]
                bdf = _self.dl.taiwan_stock_broker_make_daily(
                    stock_id=sid,
                    start_date=(d - datetime.timedelta(days=5)).strftime("%Y-%m-%d"),
                    end_date=(d - datetime.timedelta(days=1)).strftime("%Y-%m-%d"),
                )
                if not bdf.empty:
                    bdf["net"] = bdf["buy"] - bdf["sell"]
                    top = bdf.groupby("broker")["net"].sum().nlargest(5)
                    insights.append({"surge_date": d.strftime("%Y-%m-%d"),
                                     "top_buyers": top.to_dict()})
            return insights
        except Exception:
            return []

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
