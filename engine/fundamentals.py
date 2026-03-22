"""
engine/fundamentals.py — real fundamental scores (no AI guessing)

Replaces three previously AI-hallucinated fields with calculations
grounded in actual financial statement data from FinMind:

  calc_tech_barrier_score(fin_df)
    → tech_pr: 0-100 based on R&D intensity, gross margin premium, margin stability

  calc_peer_market_share(engine, sid, sector_stocks)
    → market_share_rank: revenue rank within SECTOR_GROUPS peers (clearly labelled as peer estimate)

  calc_leader_score(engine, sid, sector_stocks, fin_df, df_chip)
    → leader_score: 0-100 from revenue rank + growth vs peers + margin vs peers + institutional trend
"""

import pandas as pd
import numpy as np
from typing import Optional


# ─────────────────────────────────────────────────────────
# 1. TECH BARRIER SCORE  (replaces AI tech_pr)
# ─────────────────────────────────────────────────────────

def calc_tech_barrier_score(fin_df: pd.DataFrame,
                             sector_avg_margin: Optional[float] = None) -> dict:
    """
    Compute a real tech barrier score (0-100) from financial statement data.

    Scoring (100 pts total):
      A. R&D intensity      (0-40 pts): ResearchAndDevelopmentExpense / OperatingRevenue
      B. Gross margin premium (0-30 pts): company margin vs sector_avg_margin
      C. Margin stability   (0-30 pts): low std dev of margin over 8 quarters = moat evidence

    Args:
        fin_df: Output of taiwan_stock_financial_statement pivoted to wide format
                (columns include 'Revenue', 'margin', 'ResearchAndDevelopmentExpense', etc.)
        sector_avg_margin: Average gross margin % of sector peers (optional, defaults to 30%)

    Returns:
        {tech_pr: int, breakdown: {...}, label: str, data_quality: str}
    """
    if fin_df.empty:
        return {"tech_pr": 0, "breakdown": {}, "label": "資料不足", "data_quality": "no_data"}

    scores = {}
    notes  = {}

    # ── A. R&D Intensity ──
    rd_cols = [c for c in fin_df.columns if "Research" in c or "RD" in c or "rd_expense" in c.lower()]
    rev_col = "Revenue" if "Revenue" in fin_df.columns else None

    if rd_cols and rev_col and not fin_df[rev_col].empty:
        rd_vals = fin_df[rd_cols[0]].dropna()
        rv_vals = fin_df[rev_col].dropna()
        if not rd_vals.empty and not rv_vals.empty:
            # Align indices
            aligned = pd.concat([rd_vals, rv_vals], axis=1).dropna()
            if not aligned.empty:
                rd_intensity = (aligned.iloc[:, 0] / (aligned.iloc[:, 1] + 1e-9) * 100).mean()
                # Scale: 0% = 0 pts, 10%+ = 40 pts
                scores["rd_intensity"] = round(np.clip(rd_intensity / 10 * 40, 0, 40), 1)
                notes["rd_intensity_pct"] = round(rd_intensity, 1)
            else:
                scores["rd_intensity"] = 0
                notes["rd_intensity_pct"] = None
        else:
            scores["rd_intensity"] = 10  # partial credit
            notes["rd_intensity_pct"] = None
    else:
        # No R&D data available — give partial credit based on margin alone
        scores["rd_intensity"] = 10
        notes["rd_intensity_pct"] = None
        notes["rd_note"] = "FinMind 無 R&D 欄位，以毛利替代"

    # ── B. Gross Margin Premium ──
    baseline = sector_avg_margin if sector_avg_margin is not None else 30.0
    if "margin" in fin_df.columns:
        latest_margin = fin_df["margin"].dropna().tail(4).mean()
        premium = latest_margin - baseline
        # +10% premium = 30 pts, -10% = 0 pts
        scores["margin_premium"] = round(np.clip((premium + 10) / 20 * 30, 0, 30), 1)
        notes["latest_margin"] = round(latest_margin, 1)
        notes["sector_baseline"] = round(baseline, 1)
    else:
        scores["margin_premium"] = 0
        notes["latest_margin"] = None

    # ── C. Margin Stability ──
    if "margin" in fin_df.columns:
        margin_series = fin_df["margin"].dropna().tail(8)
        if len(margin_series) >= 4:
            margin_std = margin_series.std()
            # std < 2% = 30 pts (very stable), std > 10% = 0 pts
            scores["margin_stability"] = round(np.clip((10 - margin_std) / 8 * 30, 0, 30), 1)
            notes["margin_std"] = round(margin_std, 1)
        else:
            scores["margin_stability"] = 15  # neutral
            notes["margin_std"] = None
    else:
        scores["margin_stability"] = 0

    total = round(sum(scores.values()), 0)

    if total >= 75:   label = "🔬 高技術門檻"
    elif total >= 50: label = "⚙️ 中等門檻"
    elif total >= 25: label = "📦 輕資產模式"
    else:             label = "🔄 同質競爭"

    data_quality = "real" if notes.get("rd_intensity_pct") is not None else "partial"

    return {
        "tech_pr":     int(total),
        "breakdown":   scores,
        "notes":       notes,
        "label":       label,
        "data_quality":data_quality,
    }


# ─────────────────────────────────────────────────────────
# 2. PEER MARKET SHARE  (replaces AI mkt_share)
# ─────────────────────────────────────────────────────────

def calc_peer_market_share(engine, sid: str, sector_stocks: list) -> dict:
    """
    Estimate market share as relative revenue rank within SECTOR_GROUPS peers.
    This is NOT global market share — clearly labelled as peer comparison.

    Returns:
        {rank: int, total: int, share_pct: float, label: str, note: str}
    """
    revenues = {}
    for peer_sid in sector_stocks:
        try:
            _, rev = engine.fetch_data(peer_sid)
            if not rev.empty:
                latest = rev.sort_values("date")["revenue"].iloc[-1]
                revenues[peer_sid] = float(latest)
        except Exception:
            continue

    if not revenues or sid not in revenues:
        return {"rank": 0, "total": len(sector_stocks), "share_pct": 0,
                "label": "資料不足", "note": "同類股估算"}

    sorted_sids = sorted(revenues, key=revenues.get, reverse=True)
    rank  = sorted_sids.index(sid) + 1
    total = len(sorted_sids)
    sid_rev   = revenues[sid]
    total_rev = sum(revenues.values())
    share_pct = round(sid_rev / total_rev * 100, 1) if total_rev > 0 else 0

    if rank == 1:   label = f"🥇 同類股營收第一"
    elif rank <= 3: label = f"🥈 同類股前三名（第 {rank} 名）"
    elif rank <= total // 2: label = f"📊 同類股中上游（第 {rank}/{total} 名）"
    else:           label = f"📉 同類股後段（第 {rank}/{total} 名）"

    return {
        "rank":      rank,
        "total":     total,
        "share_pct": share_pct,
        "label":     label,
        "note":      f"同類股 {total} 支估算，非全球市場數字",
    }


# ─────────────────────────────────────────────────────────
# 3. LEADER SCORE  (replaces AI leader_score)
# ─────────────────────────────────────────────────────────

def calc_leader_score(
    engine,
    sid: str,
    sector_stocks: list,
    fin_df: pd.DataFrame,
    df_chip: pd.DataFrame,
) -> dict:
    """
    Four-dimension real leader score (0-100):
      A. Revenue size rank in sector      (0-25 pts)
      B. Revenue growth vs sector median  (0-25 pts)
      C. Gross margin vs sector median    (0-25 pts)
      D. Institutional net accumulation   (0-25 pts)

    Args:
        engine:        WallStreetEngine instance
        sid:           Stock ID
        sector_stocks: List of peer stock IDs from SECTOR_GROUPS
        fin_df:        Quarterly financials for this stock
        df_chip:       Daily price+institutional data for this stock
    """
    scores = {}
    details = {}

    # ── A. Revenue size rank ──
    peer_share = calc_peer_market_share(engine, sid, sector_stocks)
    rank, total = peer_share["rank"], peer_share["total"]
    if total > 0:
        # Rank 1 = 25 pts, last = 0 pts
        scores["size_rank"] = round((1 - (rank - 1) / total) * 25, 1)
        details["size_rank"] = f"第 {rank}/{total} 名"
    else:
        scores["size_rank"] = 0

    # ── B. Revenue growth vs sector median ──
    peer_growths = []
    own_growth   = 0.0
    for peer_sid in sector_stocks:
        try:
            _, rev = engine.fetch_data(peer_sid)
            if not rev.empty and len(rev) >= 13:
                rv = rev.sort_values("date")
                curr = rv["revenue"].iloc[-1]
                prev = rv["revenue"].iloc[-13]
                g = (curr / (prev + 1e-9) - 1) * 100 if prev > 0 else 0
                peer_growths.append(g)
                if peer_sid == sid:
                    own_growth = g
        except Exception:
            continue

    if peer_growths:
        median_growth = float(np.median(peer_growths))
        premium_growth = own_growth - median_growth
        # +20% above median = 25 pts, -20% = 0 pts
        scores["growth_premium"] = round(np.clip((premium_growth + 20) / 40 * 25, 0, 25), 1)
        details["own_growth"]    = round(own_growth, 1)
        details["sector_median_growth"] = round(median_growth, 1)
    else:
        scores["growth_premium"] = 0

    # ── C. Gross margin vs sector median ──
    peer_margins = []
    own_margin   = 0.0
    for peer_sid in sector_stocks:
        try:
            peer_q = engine.fetch_quarterly_financials(peer_sid)
            if not peer_q.empty and "margin" in peer_q.columns:
                m = peer_q["margin"].dropna().tail(4).mean()
                peer_margins.append(m)
                if peer_sid == sid and not fin_df.empty and "margin" in fin_df.columns:
                    own_margin = fin_df["margin"].dropna().tail(4).mean()
        except Exception:
            continue

    if peer_margins and "margin" in fin_df.columns:
        median_margin = float(np.median(peer_margins))
        premium_margin = own_margin - median_margin
        # +10% above median = 25 pts, -10% = 0 pts
        scores["margin_leadership"] = round(np.clip((premium_margin + 10) / 20 * 25, 0, 25), 1)
        details["own_margin"]    = round(own_margin, 1)
        details["sector_median_margin"] = round(median_margin, 1)
    else:
        scores["margin_leadership"] = 12  # neutral when no peer data

    # ── D. Institutional net accumulation ──
    if not df_chip.empty and "f_net" in df_chip.columns:
        f20  = df_chip["f_net"].tail(20).sum()
        vol20 = df_chip["trading_volume"].tail(20).sum()
        conc = f20 / (vol20 + 1e-9) * 100 if vol20 > 0 else 0
        # conc > 1% = 25 pts, < -1% = 0 pts
        scores["inst_accumulation"] = round(np.clip((conc + 1) / 2 * 25, 0, 25), 1)
        details["f_net_20d_conc"] = round(conc, 2)
    else:
        scores["inst_accumulation"] = 0

    total = round(sum(scores.values()), 0)

    if total >= 75:   label = "🏆 產業領頭羊"
    elif total >= 55: label = "🥈 產業前段班"
    elif total >= 35: label = "📊 產業中游"
    else:             label = "⚡ 追趕者"

    return {
        "leader_score": int(total),
        "breakdown":    scores,
        "details":      details,
        "label":        label,
    }


# ─────────────────────────────────────────────────────────
# 4. SECTOR AVERAGE MARGIN  (helper for tech_pr)
# ─────────────────────────────────────────────────────────

def get_sector_avg_margin(engine, sector_stocks: list) -> float:
    """
    Compute the median gross margin % across all stocks in a sector.
    Used as the baseline for tech_pr margin premium calculation.
    """
    margins = []
    for peer_sid in sector_stocks[:10]:  # cap at 10 to limit API calls
        try:
            fin = engine.fetch_quarterly_financials(peer_sid)
            if not fin.empty and "margin" in fin.columns:
                m = fin["margin"].dropna().tail(4).mean()
                if not np.isnan(m):
                    margins.append(m)
        except Exception:
            continue
    return float(np.median(margins)) if margins else 30.0
