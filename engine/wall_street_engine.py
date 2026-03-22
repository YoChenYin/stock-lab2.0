"""
engine/wall_street_engine.py — WallStreetEngine

Responsibilities:
  - All FinMind API calls (via _smart_fetch → SQLite cache → @st.cache_data)
  - Real chip data: compute foreign ownership % from actual holdings data
  - All Gemini AI calls (interpretation only, never data invention)
  - MOPS scraper

Design rules:
  - _self pattern for @st.cache_data on instance methods
  - _smart_fetch is the ONLY entry point to FinMind API
  - Gemini receives real numbers; it interprets, not invents
"""

import streamlit as st
import pandas as pd
import numpy as np
import datetime
import time
import json
import re
import requests
import urllib3
from FinMind.data import DataLoader
import google.generativeai as genai
from bs4 import BeautifulSoup

from engine.cache import DataCacheManager

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)


# ─────────────────────────────────────────────────────────
# Gemini helper — used by all AI methods below
# ─────────────────────────────────────────────────────────
def _call_gemini(model, prompt: str, fallback: dict | None = None,
                  generation_config: dict | None = None) -> dict:
    """
    Unified Gemini caller with JSON extraction fallback.
    Always returns a dict (never raises).

    generation_config overrides the default JSON-only config.
    Pass temperature=0.0, top_p=0.1, top_k=1 for deterministic output.
    """
    if fallback is None:
        fallback = {}
    base_config = {"response_mime_type": "application/json"}
    if generation_config:
        base_config.update(generation_config)
    try:
        res = model.generate_content(prompt, generation_config=base_config)
        raw = res.text.replace("```json", "").replace("```", "").strip()
        return json.loads(raw)
    except json.JSONDecodeError:
        m = re.search(r"\{.*\}", res.text, re.DOTALL)
        if m:
            try:
                return json.loads(m.group())
            except Exception:
                pass
        return fallback
    except Exception as e:
        print(f"[Gemini error] {e}")
        return fallback


# ─────────────────────────────────────────────────────────
# Engine
# ─────────────────────────────────────────────────────────
class WallStreetEngine:
    def __init__(self, fm_token: str = "", gemini_key: str = ""):
        self.dl        = DataLoader()
        self.cache     = DataCacheManager()
        self.fm_token  = fm_token  or st.secrets.get("FINMIND_TOKEN", "")
        self.api_key   = gemini_key or st.secrets.get("GEMINI_API_KEY", "")
        self.ai_model  = None

        if self.fm_token:
            try:
                self.dl.login_by_token(api_token=self.fm_token)
            except Exception:
                st.error("FinMind login failed — check FINMIND_TOKEN.")

        if self.api_key:
            try:
                genai.configure(api_key=self.api_key)
                self.ai_model = genai.GenerativeModel("gemini-2.5-flash-lite")
            except Exception as e:
                st.error(f"Gemini config error: {e}")

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

    @st.cache_data(ttl=3600)
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

    @st.cache_data(ttl=3600)
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
            st.error(f"ML data error: {e}")
            return pd.DataFrame()

    @st.cache_data(ttl=86400)
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
            st.error(f"Financial parse error: {e}")
            return pd.DataFrame()

    @st.cache_data(ttl=86400)
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

    @st.cache_data(ttl=3600)
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
            st.error(f"Sentiment error: {e}")
            return pd.DataFrame()

    @st.cache_data(ttl=3600)
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

    # ─────────────────────────────────────────────────────
    # AI CALLERS  (Gemini interprets real data, never invents)
    # ─────────────────────────────────────────────────────

    @st.cache_data(ttl=3600)
    def get_ai_dashboard_data(_self, sid: str, name: str,
                               df: pd.DataFrame, rev: pd.DataFrame,
                               real_chip: dict | None = None) -> dict | None:
        """
        AI overview analysis.
        real_chip is fetched separately and passed in — Gemini does NOT invent chip numbers.
        """
        if not _self.ai_model or df.empty:
            return None

        price_stats = {
            "current": round(df["close"].iloc[-1], 1),
            "low_52":  round(df["close"].tail(252).min(), 1),
            "high_52": round(df["close"].tail(252).max(), 1),
            "ma20":    round(df["ma20"].iloc[-1], 1),
        }
        # Use real chip data when available
        chip_context = ""
        if real_chip and real_chip.get("source") != "unavailable":
            chip_context = f"""
真實持股數據（來源：{real_chip['source']}）：
- 外資持股比例：{real_chip.get('foreign_pct', 'N/A')}%
- 前十大股東持股：{real_chip.get('major_pct', 'N/A')}%
請在 chips 欄位直接使用這些數字，不要另行估計。"""
        else:
            chip_context = "真實持股數據暫不可用，請在 chips 欄位標記 '資料獲取中'。"

        rev_data = rev.tail(6).to_dict(orient="records") if not rev.empty else []

        prompt = f"""
你是一位專業的證券分析師。現在是 {datetime.date.today()}。
請針對台股 {sid} {name} 進行深度分析。

價格資訊：{price_stats}
近期營收：{rev_data}
{chip_context}

請嚴格依照以下 JSON 格式，不要有任何解釋文字：
{{
  "header": {{"category": "產業類別"}},
  "overview": {{
    "business_model": "簡述如何賺錢、核心產品（50字內）",
    "moat_metrics": {{
      "mkt_share": "市佔率估計（請標明：AI估計）",
      "tech_pr": 技術門檻PR整數0到100,
      "rd_intensity": "高/中/低",
      "barrier_desc": "技術門檻一句話總結",
      "moat": "競爭優勢分析（60字內）"
    }},
    "competitor_diff": "與同業主要優勢（30字內）"
  }},
  "diagnosis": {{
    "margin_trend": "毛利率變動看法",
    "growth_status": "營收成長動能與發展性",
    "leader_score": 領頭羊指數整數0到100
  }},
  "chips": {{
    "foreign_inst": "見上方真實持股數據",
    "major_holder": "見上方真實持股數據",
    "comment": "籌碼面解讀（30字內）"
  }},
  "valuation": {{
    "level_pct": 根據當前價{price_stats['current']}在{price_stats['low_52']}到{price_stats['high_52']}之間計算0到100的整數,
    "conclusion": "估值評價與建議",
    "opportunities": ["利多1", "利多2"],
    "risks": ["利空1", "利空2"]
  }}
}}
"""
        return _call_gemini(_self.ai_model, prompt)

    @st.cache_data(ttl=86400)
    def get_real_world_outlook(_self, sid: str, name: str,
                                mops_data: dict, _content_hash: str = "") -> dict | None:
        """
        Analyse the latest investor relations meeting.

        _content_hash is derived from mops_data['event'] text — it ensures
        the cache key changes only when the actual MOPS content changes,
        not just because the date ticked over. Same document = same result.

        temperature=0 is set via top_k/top_p constraints to minimise
        Gemini output variability for identical input text.
        """
        if not _self.ai_model:
            return None

        event_text = mops_data.get("event", "").strip()
        pdf_url    = mops_data.get("url", "")
        mops_date  = mops_data.get("date", "未知日期")

        # Warn in prompt when event text is sparse (< 50 chars = basically empty)
        if len(event_text) < 50:
            honesty_note = """
【重要】法說摘要文字非常短，代表 MOPS 公開資訊有限。
請根據有限資訊如實回答，不足之處請標示「摘要不足，無法確認」，
絕對不要根據公司過去的印象或推測來補充數字。"""
        else:
            honesty_note = "請嚴格根據上方摘要文字回答，不要超出摘要範圍推論數字。"

        prompt = f"""你是資深賣方分析師。現在是 {datetime.date.today()}。
請針對台股 {sid} {name} 的法說會資料進行分析。

【法說日期】{mops_date}
【MOPS 官方摘要原文（這是唯一可信來源）】
{event_text}
【官方簡報連結】{pdf_url}

{honesty_note}

請嚴格回傳 JSON，不要有多餘解釋：
{{
  "data_quality": "sufficient（摘要足夠）或 limited（摘要不足）",
  "scorecard": {{
    "rev_status": "營收目標（摘要有提到才填，否則填：摘要未提及）",
    "margin_status": "毛利表現（摘要有提到才填，否則填：摘要未提及）",
    "guidance_tone": "展望基調（根據摘要措辭判斷）",
    "alpha_factor": "法說最值得關注的一點（限摘要內容）"
  }},
  "guidance_detail": {{
    "revenue": "具體營收指引（摘要無則：摘要未提及）",
    "margin": "具體毛利指引（摘要無則：摘要未提及）",
    "capex": "資本支出/擴產計畫（摘要無則：摘要未提及）"
  }},
  "growth_drivers": ["動能1（限摘要內容）", "動能2", "動能3"],
  "analyst_concerns": ["法人質疑點1（限摘要內容）", "法人質疑點2"],
  "valuation_anchor": "估值邏輯（僅限摘要有提到的業績指引，否則填：需閱讀完整簡報）",
  "radar": [訂單強度整數, 獲利能力整數, 產業地位整數, 技術優勢整數, 財務穩健整數]
}}
注意：radar 陣列僅包含 5 個 0-100 的整數。"""

        data = _call_gemini(
            _self.ai_model, prompt,
            generation_config={
                "response_mime_type": "application/json",
                "temperature": 0.0,       # deterministic output
                "top_p": 0.1,
                "top_k": 1,
            }
        )
        if data and "guidance_detail" not in data:
            data["guidance_detail"] = {
                "revenue": data.get("scorecard", {}).get("rev_status", "未提供"),
                "margin":  data.get("scorecard", {}).get("margin_status", "未提供"),
                "capex":   "詳見官方簡報",
            }
        return data

    @st.cache_data(ttl=3600)
    def get_strategy_card_ai(
        _self, sid: str, name: str,
        curr_price: float, low52: float, high52: float,
        f_cost: float, sms_score: float,
        mops_event: str, hit_rate: float,
    ) -> dict | None:
        """
        Generate a concrete strategy card.
        All numbers are real data passed in — Gemini anchors on them, not invents.
        """
        if not _self.ai_model:
            return None

        # Hard safety floor for stop loss
        min_stop = round(curr_price * 0.92, 1)

        prompt = f"""
你是一位風控嚴謹的操盤手。現在是 {datetime.date.today()}。
根據以下【真實數據】輸出一張具體可執行的策略卡。

股票：{sid} {name}
當前價格：{curr_price:.1f}
52週低/高：{low52:.1f} / {high52:.1f}
外資平均成本區：{f_cost:.1f}
Smart Money Score：{sms_score:.0f}/100
AI 回測歷史勝率：{hit_rate:.1%}
最新法說重點：{mops_event[:200] if mops_event else '無'}
停損不得低於：{min_stop}

請嚴格回傳 JSON，不可有多餘文字：
{{
  "strategy_type": "趨勢追蹤|反轉|區間震盪",
  "entry": {{
    "ideal_zone": "進場區間如{curr_price*0.97:.0f}-{curr_price:.0f}",
    "trigger": "進場觸發條件15字內",
    "timing": "進場時機說明"
  }},
  "risk": {{
    "stop_loss": 停損價數字（不得低於{min_stop}）,
    "stop_reason": "停損邏輯",
    "max_loss_pct": 最大虧損百分比數字
  }},
  "targets": [
    {{"price": 目標一數字, "reason": "理由10字內", "action": "減倉50%"}},
    {{"price": 目標二數字, "reason": "理由10字內", "action": "清倉"}}
  ],
  "position_size": {{
    "kelly_fraction": Kelly分數0到0.25的浮點數,
    "suggested_pct": "建議佔總資金%字串",
    "rationale": "部位邏輯"
  }},
  "validity": "策略有效期如下季財報前",
  "confidence": 信心度0到100整數
}}
"""
        return _call_gemini(_self.ai_model, prompt)

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
                        pdf_url = f"https://mopsov.twse.com.tw{a['href']}"

            return {"date": date_val, "event": info_val, "url": pdf_url, "status": "success"}
        except Exception as e:
            return {"date": "讀取錯誤", "event": str(e), "url": "#", "status": "error"}
