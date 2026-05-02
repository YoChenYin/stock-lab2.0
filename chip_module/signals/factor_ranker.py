"""
chip_module/signals/factor_ranker.py
多因子選股引擎 — FactorRanker

架構：
  Technical  (30%)  MA20/50/200 排列 + RSI(14) 橫截面 Z-Score
  Flow       (50%)  雙強因子（外資+投信同買連續天數）+ 籌碼加速 Z-Score
  Quality    (20%)  月營收 YoY 3 月均值 Z-Score

環境開關：
  VIX > 25  → entry_score *= 0.80
  VIX 15-25 → entry_score *= 0.95
  VIX < 15  → entry_score *= 1.00

所有連續型因子使用橫截面（cross-sectional）Z-Score 標準化，
再線性映射至 0-100 分（Z = -3 → 0 分，Z = 0 → 50 分，Z = +3 → 100 分）。
"""

from __future__ import annotations

import json
import logging
import os
import sqlite3
from dataclasses import dataclass, field
from datetime import date
from pathlib import Path
from typing import Optional

import numpy as np
import pandas as pd

log = logging.getLogger(__name__)

# ── DB 路徑 ───────────────────────────────────────────────────────────
_PROJ_ROOT = Path(__file__).resolve().parents[2]   # stock_lab2.0/
if os.path.isdir("/data"):
    _CHIP_DB    = Path("/data/chip.db")
    _FINMIND_DB = Path("/data/finmind_cache.db")
else:
    _CHIP_DB    = _PROJ_ROOT / "chip_module" / "chip.db"
    _FINMIND_DB = _PROJ_ROOT / "finmind_cache.db"

# ── 因子權重 & VIX 折扣 ───────────────────────────────────────────────
WEIGHTS: dict[str, float] = {
    "technical": 0.30,
    "flow":      0.50,
    "quality":   0.20,
}
VIX_DISCOUNT: dict[str, float] = {
    "high":   0.80,
    "medium": 0.95,
    "low":    1.00,
}


# ─────────────────────────────────────────────────────────────────────
# FactorScore — 單股因子快照
# ─────────────────────────────────────────────────────────────────────

@dataclass
class FactorScore:
    ticker:             str
    technical:          float = 50.0   # 0-100
    flow:               float = 50.0   # 0-100
    quality:            float = 50.0   # 0-100
    composite:          float = 50.0   # weighted, before VIX
    vix_adj:            float = 1.0    # VIX context multiplier

    # raw Z-scores（透明度用）
    z_ma_align:         float = 0.0
    z_rsi:              float = 0.0
    z_double_strong:    float = 0.0
    z_chip_accel:       float = 0.0
    z_revenue_yoy:      float = 0.0

    # raw values
    double_strong_days: int   = 0
    rsi_raw:            float = 50.0

    def entry_score(self) -> float:
        """VIX 折扣後的最終進場分。"""
        return round(self.composite * self.vix_adj, 1)

    def to_dict(self) -> dict:
        return {
            "ticker":             self.ticker,
            "technical":          self.technical,
            "flow":               self.flow,
            "quality":            self.quality,
            "composite":          self.composite,
            "entry_score":        self.entry_score(),
            "vix_adj":            self.vix_adj,
            "double_strong_days": self.double_strong_days,
            "rsi":                self.rsi_raw,
            "z_ma_align":         self.z_ma_align,
            "z_rsi":              self.z_rsi,
            "z_double_strong":    self.z_double_strong,
            "z_chip_accel":       self.z_chip_accel,
            "z_revenue_yoy":      self.z_revenue_yoy,
        }


# ─────────────────────────────────────────────────────────────────────
# FactorRanker — 主引擎
# ─────────────────────────────────────────────────────────────────────

class FactorRanker:
    """
    使用方式：
        ranker = FactorRanker(market="TW").fit()
        top = ranker.rank()[:10]
        heatmap = ranker.get_heatmap()
    """

    def __init__(self, market: str = "TW") -> None:
        if market not in ("TW", "US"):
            raise ValueError(f"market 必須是 'TW' 或 'US'，傳入 {market!r}")
        self.market   = market
        self._vix_adj = 1.0
        self._scores: dict[str, FactorScore] = {}
        self._feature_df: pd.DataFrame = pd.DataFrame()

    # ── 公開介面 ─────────────────────────────────────────────────────

    def fit(self) -> "FactorRanker":
        """載入資料、計算 Z-Score、完成排名。回傳 self 支援鏈式呼叫。"""
        self._vix_adj = self._load_vix_adj()
        log.info(f"[FactorRanker] market={self.market}, vix_adj={self._vix_adj}")

        if self.market == "TW":
            df = self._build_tw_features()
        else:
            df = self._build_us_features()

        if df.empty:
            log.warning(f"[FactorRanker] 無法取得特徵資料 market={self.market}")
            return self

        self._feature_df = df
        self._scores     = self._compute_scores(df)
        log.info(f"[FactorRanker] 完成，共 {len(self._scores)} 支股票")
        return self

    def rank(self) -> list[FactorScore]:
        """回傳所有股票，依 entry_score 降序排列。"""
        return sorted(self._scores.values(),
                      key=lambda s: s.entry_score(), reverse=True)

    def get_heatmap(self) -> list[dict]:
        """
        產出因子熱度榜。

        每個因子組回傳：
          factor_name, factor_label, group,
          max_score, avg_score, momentum_score (0-100),
          top_stocks (list), stock_count, vix_adj
        """
        if not self._scores:
            return []

        groups = self._define_factor_groups()
        result = []

        # groups 回傳 5-tuple：(fname, label, group_key, tickers, sort_attr)
        for fname, label, group_key, tickers, sort_attr in groups:
            members = [self._scores[t] for t in tickers if t in self._scores]
            if len(members) < 3:
                continue

            # 動能分：以「群組主因子」平均分 vs 全市場橫截面 Z → 映射 0-100
            primary_scores = [getattr(s, sort_attr) for s in members]
            all_primary    = [getattr(s, sort_attr) for s in self._scores.values()]
            p_mean = float(np.mean(all_primary))
            p_std  = float(np.std(all_primary)) or 1.0
            group_avg      = float(np.mean(primary_scores))
            z_group        = (group_avg - p_mean) / p_std
            momentum_score = round(float(np.clip(50 + z_group * 15, 0, 100)), 1)

            max_score = float(max(primary_scores))
            avg_score = group_avg

            # top stocks 用群組主因子排序（不用 composite）
            top = sorted(members, key=lambda s: getattr(s, sort_attr), reverse=True)[:5]

            result.append({
                "factor_name":    fname,
                "factor_label":   label,
                "group":          group_key,
                "sort_by":        sort_attr,
                "max_score":      round(max_score, 1),
                "avg_score":      round(avg_score, 1),
                "momentum_score": momentum_score,
                "stock_count":    len(members),
                "top_stocks":     [s.ticker for s in top],
                "vix_adj":        self._vix_adj,
                "date":           date.today().isoformat(),
                "market":         self.market,
            })

        return sorted(result, key=lambda x: x["momentum_score"], reverse=True)

    # ── VIX 環境開關 ─────────────────────────────────────────────────

    def _load_vix_adj(self) -> float:
        try:
            with sqlite3.connect(_CHIP_DB) as conn:
                row = conn.execute(
                    "SELECT vix_level FROM market_environment ORDER BY date DESC LIMIT 1"
                ).fetchone()
            if row and row[0]:
                return VIX_DISCOUNT.get(row[0], 1.0)
        except Exception as e:
            log.debug(f"[FactorRanker] VIX 載入失敗（使用預設 1.0）: {e}")
        return 1.0

    # ── TW 特徵工程 ──────────────────────────────────────────────────

    def _build_tw_features(self) -> pd.DataFrame:
        prices = self._load_tw_prices()
        inst   = self._load_tw_institutional()
        rev    = self._load_tw_revenue()

        if prices.empty:
            return pd.DataFrame()

        rows: list[dict] = []
        for sid, grp in prices.groupby("sid"):
            p = grp.sort_values("date").reset_index(drop=True)
            if len(p) < 20:
                continue

            close = p["close"].to_numpy(dtype=float)

            # ── Technical ────────────────────────────────────────────
            ma_align = _calc_ma_align(close)
            rsi      = _calc_rsi(close, period=14)

            # ── Flow — 雙強因子 ───────────────────────────────────────
            double_strong_days = 0
            chip_accel         = 0.0
            if not inst.empty:
                si = inst[inst["sid"] == sid].sort_values("date")
                if not si.empty:
                    cutoff = pd.Timestamp.now() - pd.Timedelta(days=40)
                    recent = si[si["date"] >= cutoff]
                    f  = recent[recent["name"] == "Foreign_Investor"][["date","net"]].rename(columns={"net":"f"})
                    it = recent[recent["name"] == "Investment_Trust"][["date","net"]].rename(columns={"net":"it"})
                    if not f.empty and not it.empty:
                        m = pd.merge(f, it, on="date").sort_values("date")
                        # 雙強連續天數
                        both = ((m["f"] > 0) & (m["it"] > 0)).to_numpy()
                        streak = 0
                        for v in reversed(both):
                            if v:
                                streak += 1
                            else:
                                break
                        double_strong_days = streak
                        # 籌碼加速：5D vs 20D 淨買超差值（以均價正規化）
                        net  = (m["f"] + m["it"]).to_numpy()
                        ref  = abs(net).mean() or 1.0
                        n5   = net[-5:].mean()  if len(net) >= 5  else 0.0
                        n20  = net[-20:].mean() if len(net) >= 20 else 0.0
                        chip_accel = float((n5 - n20) / ref)

            # ── Quality — 月營收 YoY ─────────────────────────────────
            revenue_yoy = 0.0
            if not rev.empty:
                sr = rev[rev["sid"] == sid].sort_values("date")
                yoys = sr["yoy"].dropna().tail(3).to_numpy()
                if len(yoys) > 0:
                    revenue_yoy = float(np.mean(yoys))

            rows.append({
                "ticker":             str(sid),
                "ma_align":           float(ma_align),
                "rsi":                float(rsi),
                "double_strong_days": double_strong_days,
                "chip_accel":         chip_accel,
                "revenue_yoy":        revenue_yoy,
            })

        return pd.DataFrame(rows) if rows else pd.DataFrame()

    # ── US 特徵工程 ──────────────────────────────────────────────────

    def _build_us_features(self) -> pd.DataFrame:
        """
        US：直接從 chip.db 讀已計算好的指標，
        ma_aligned / rsi_14 來自 tech_signals；
        volume_score / insider_score 來自 chip_scores 作為 flow / quality proxy。
        """
        try:
            with sqlite3.connect(_CHIP_DB) as conn:
                df = pd.read_sql("""
                    SELECT cs.ticker,
                           COALESCE(ts.ma_aligned, 0)  AS ma_align,
                           COALESCE(ts.rsi_14,     50) AS rsi,
                           COALESCE(cs.volume_score,      50) AS chip_accel,
                           COALESCE(cs.institutional_score,50) AS double_strong_days,
                           COALESCE(cs.insider_score,      50) AS revenue_yoy
                    FROM chip_scores cs
                    LEFT JOIN tech_signals ts
                           ON cs.ticker = ts.ticker AND cs.date = ts.date
                    WHERE cs.date = (SELECT MAX(date) FROM chip_scores)
                """, conn)
        except Exception as e:
            log.warning(f"[FactorRanker] US 特徵載入失敗: {e}")
            return pd.DataFrame()
        return df if not df.empty else pd.DataFrame()

    # ── Z-Score 標準化與分數計算 ─────────────────────────────────────

    def _compute_scores(self, df: pd.DataFrame) -> dict[str, FactorScore]:
        """
        橫截面 Z-Score 標準化所有連續因子，
        映射至 0-100，加權合成 composite。
        """
        df = df.copy().reset_index(drop=True)

        def _zscore(col: pd.Series, clip: float = 3.0) -> pd.Series:
            mu, sigma = col.mean(), col.std()
            if sigma < 1e-9:
                return pd.Series(np.zeros(len(col)), index=col.index)
            return ((col - mu) / sigma).clip(-clip, clip)

        def _to_score(z: pd.Series) -> pd.Series:
            """Z ∈ [-3, 3]  →  score ∈ [0, 100]"""
            return (z + 3.0) / 6.0 * 100.0

        # ── Z-Score 各因子 ───────────────────────────────────────────
        df["z_ma_align"]      = _zscore(df["ma_align"])
        df["z_rsi"]           = _zscore(df["rsi"])
        df["z_double_strong"] = _zscore(df["double_strong_days"].astype(float))
        df["z_chip_accel"]    = _zscore(df["chip_accel"])
        df["z_revenue_yoy"]   = _zscore(df["revenue_yoy"])

        # ── 組別分數（0-100）────────────────────────────────────────
        df["technical"] = _to_score((df["z_ma_align"] + df["z_rsi"]) / 2.0)
        df["flow"]      = _to_score((df["z_double_strong"] + df["z_chip_accel"]) / 2.0)
        df["quality"]   = _to_score(df["z_revenue_yoy"])

        # ── 加權綜合分 ───────────────────────────────────────────────
        df["composite"] = (
            df["technical"] * WEIGHTS["technical"] +
            df["flow"]      * WEIGHTS["flow"]      +
            df["quality"]   * WEIGHTS["quality"]
        )

        result: dict[str, FactorScore] = {}
        for _, row in df.iterrows():
            fs = FactorScore(
                ticker             = str(row["ticker"]),
                technical          = round(float(row["technical"]),  1),
                flow               = round(float(row["flow"]),       1),
                quality            = round(float(row["quality"]),    1),
                composite          = round(float(row["composite"]),  1),
                vix_adj            = self._vix_adj,
                z_ma_align         = round(float(row["z_ma_align"]),      3),
                z_rsi              = round(float(row["z_rsi"]),           3),
                z_double_strong    = round(float(row["z_double_strong"]), 3),
                z_chip_accel       = round(float(row["z_chip_accel"]),    3),
                z_revenue_yoy      = round(float(row["z_revenue_yoy"]),  3),
                double_strong_days = int(row.get("double_strong_days", 0)),
                rsi_raw            = round(float(row["rsi"]), 1),
            )
            result[fs.ticker] = fs

        return result

    # ── 因子組定義 ───────────────────────────────────────────────────

    def _define_factor_groups(self) -> list[tuple[str, str, str, list[str], str]]:
        """
        回傳 [(factor_name, factor_label, group_key, [tickers], sort_attr), ...]

        設計原則：各組互有差異，用篩選條件造成實質分群
          三因子共振  — 三維度均強，最高信心
          技術突破    — 技術強但籌碼尚未跟進（領先信號）
          籌碼強攻    — 外資/投信買超顯著，按 flow 排
          營收加速    — 基本面驅動，按 quality 排
          大盤等權    — 全市場基準，按 composite 排
        """
        s = self._scores

        # 三因子共振：三個維度全部 > 57
        triple = [t for t, v in s.items()
                  if v.technical > 57 and v.flow > 57 and v.quality > 57]

        # 技術突破：技術強（> 62）但籌碼尚弱（< 57）→ 領先信號
        tech_lead = [t for t, v in s.items()
                     if v.technical > 62 and v.flow < 57]

        # 籌碼強攻：flow > 60（不限技術）
        chip_strong = [t for t, v in s.items() if v.flow > 60]

        # 營收加速：quality > 60（不限其他維度）
        rev_up = [t for t, v in s.items() if v.quality > 60]

        # 大盤等權：全市場基準
        all_tickers = list(s.keys())

        groups: list[tuple[str, str, str, list[str], str]] = []

        if len(triple) >= 3:
            groups.append(("triple_sync",    "三因子共振", "composite", triple,      "composite"))
        if len(tech_lead) >= 3:
            groups.append(("tech_breakout",  "技術突破",   "technical", tech_lead,   "technical"))
        if len(chip_strong) >= 3:
            groups.append(("chip_offensive", "籌碼強攻",   "flow",      chip_strong, "flow"))
        if len(rev_up) >= 3:
            groups.append(("rev_accel",      "營收加速",   "quality",   rev_up,      "quality"))
        groups.append(    ("market_eq",      "大盤等權",   "market",    all_tickers, "composite"))

        return groups

    # ── 資料載入（TW）────────────────────────────────────────────────

    def _load_tw_prices(self) -> pd.DataFrame:
        if not _FINMIND_DB.exists():
            log.warning("[FactorRanker] finmind_cache.db 不存在")
            return pd.DataFrame()
        frames: list[pd.DataFrame] = []
        with sqlite3.connect(_FINMIND_DB) as conn:
            rows = conn.execute(
                "SELECT sid, content FROM api_cache WHERE data_type='daily'"
            ).fetchall()
        for sid, content in rows:
            try:
                data = json.loads(content)
                if not isinstance(data, dict) or "close" not in data or "date" not in data:
                    continue
                df = pd.DataFrame(data)[["date", "close"]].copy()
                df["sid"]   = sid
                df["close"] = pd.to_numeric(df["close"], errors="coerce")
                frames.append(df[["sid", "date", "close"]])
            except Exception:
                continue
        if not frames:
            return pd.DataFrame()
        out = pd.concat(frames, ignore_index=True)
        out["date"] = pd.to_datetime(out["date"], errors="coerce")
        return out.dropna(subset=["date", "close"])

    def _load_tw_institutional(self) -> pd.DataFrame:
        if not _FINMIND_DB.exists():
            return pd.DataFrame()
        frames: list[pd.DataFrame] = []
        with sqlite3.connect(_FINMIND_DB) as conn:
            rows = conn.execute(
                "SELECT sid, content FROM api_cache WHERE data_type='institutional'"
            ).fetchall()
        for sid, content in rows:
            try:
                data = json.loads(content)
                df = pd.DataFrame(data)
                if df.empty or "date" not in df.columns or "name" not in df.columns:
                    continue
                df["sid"]  = sid
                df["date"] = pd.to_datetime(df["date"], errors="coerce")
                df["buy"]  = pd.to_numeric(df.get("buy",  0), errors="coerce").fillna(0)
                df["sell"] = pd.to_numeric(df.get("sell", 0), errors="coerce").fillna(0)
                df["net"]  = df["buy"] - df["sell"]
                frames.append(df[["sid", "date", "name", "net"]])
            except Exception:
                continue
        if not frames:
            return pd.DataFrame()
        return pd.concat(frames, ignore_index=True)

    def _load_tw_revenue(self) -> pd.DataFrame:
        """載入月營收並計算 YoY（與前一年同月比較）。"""
        if not _FINMIND_DB.exists():
            return pd.DataFrame()
        frames: list[pd.DataFrame] = []
        with sqlite3.connect(_FINMIND_DB) as conn:
            rows = conn.execute(
                "SELECT sid, content FROM api_cache WHERE data_type='revenue'"
            ).fetchall()
        for sid, content in rows:
            try:
                data = json.loads(content)
                df   = pd.DataFrame(data)
                if df.empty or "revenue" not in df.columns:
                    continue
                df["sid"]     = sid
                df["date"]    = pd.to_datetime(df["date"], errors="coerce")
                df["revenue"] = pd.to_numeric(df["revenue"], errors="coerce")
                df = df.dropna(subset=["date", "revenue"]).sort_values("date").copy()
                # YoY：與 12 個月前同月比較
                if len(df) >= 13:
                    df["yoy"] = df["revenue"].pct_change(12) * 100
                else:
                    df["yoy"] = np.nan
                frames.append(df[["sid", "date", "yoy"]])
            except Exception:
                continue
        if not frames:
            return pd.DataFrame()
        return pd.concat(frames, ignore_index=True)


# ─────────────────────────────────────────────────────────────────────
# 純函式工具
# ─────────────────────────────────────────────────────────────────────

def _calc_rsi(close: np.ndarray, period: int = 14) -> float:
    """Wilder's RSI。資料不足時回傳中性值 50。"""
    if len(close) < period + 1:
        return 50.0
    delta = np.diff(close.astype(float))
    gain  = np.where(delta > 0, delta, 0.0)
    loss  = np.where(delta < 0, -delta, 0.0)
    avg_g = gain[:period].mean()
    avg_l = loss[:period].mean()
    for i in range(period, len(gain)):
        avg_g = (avg_g * (period - 1) + gain[i]) / period
        avg_l = (avg_l * (period - 1) + loss[i]) / period
    if avg_l < 1e-9:
        return 100.0
    return float(100.0 - 100.0 / (1.0 + avg_g / avg_l))


def _calc_ma_align(close: np.ndarray) -> int:
    """
    MA 多頭排列得分：
      收盤 > MA20        +1
      MA20  > MA50       +1
      MA50  > MA200      +1  (資料 < 200 筆時改用 MA50 vs MA100)
    滿分 3 分。
    """
    n = len(close)
    if n < 20:
        return 0
    s   = pd.Series(close.astype(float))
    cur  = float(close[-1])
    ma20 = float(s.rolling(20).mean().iloc[-1])
    ma50 = float(s.rolling(min(50, n)).mean().iloc[-1]) if n >= 50 else np.nan
    ma_long = float(s.rolling(min(200, n)).mean().iloc[-1]) if n >= 30 else np.nan

    score = 0
    if cur > ma20:                             score += 1
    if not np.isnan(ma50)  and ma20 > ma50:   score += 1
    if not np.isnan(ma_long) and not np.isnan(ma50) and ma50 > ma_long:
        score += 1
    return score


# ─────────────────────────────────────────────────────────────────────
# CLI 快速測試
# ─────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    import sys
    logging.basicConfig(level=logging.INFO,
                        format="%(asctime)s [%(levelname)s] %(message)s")

    market = sys.argv[1] if len(sys.argv) > 1 else "TW"
    ranker = FactorRanker(market=market).fit()

    print(f"\n{'='*60}")
    print(f"  因子熱度榜 — {market}  (VIX adj: {ranker._vix_adj})")
    print(f"{'='*60}")
    for row in ranker.get_heatmap():
        bar = "█" * int(row["momentum_score"] / 5)
        print(f"  {row['factor_label']:10s}  動能={row['momentum_score']:5.1f}  "
              f"avg={row['avg_score']:5.1f}  n={row['stock_count']:3d}  "
              f"{bar}")
        print(f"    Top: {', '.join(row['top_stocks'][:3])}")

    print(f"\n  Top 10 個股排名：")
    for i, fs in enumerate(ranker.rank()[:10], 1):
        print(f"  {i:2d}. {fs.ticker:6s}  entry={fs.entry_score():5.1f}  "
              f"T={fs.technical:5.1f}  F={fs.flow:5.1f}  Q={fs.quality:5.1f}  "
              f"DS={fs.double_strong_days}d")
