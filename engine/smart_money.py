"""
engine/smart_money.py — quantitative chip / revenue signals

Functions:
  calc_smart_money_score(df)       → 0-100 composite chip score
  calc_revenue_accel_score(rev)    → 0-100 revenue acceleration score
  build_broker_dna(engine, sids)   → cross-stock broker fingerprint
"""

import pandas as pd
import numpy as np


def calc_smart_money_score(df: pd.DataFrame) -> dict:
    """
    Input:  DataFrame with columns f_net, it_net, trading_volume, m_net, bias_f_cost
    Output: {score: 0-100, breakdown: {4 components}, signal: str}

    Scoring breakdown (25 pts each):
      1. foreign_streak     — foreign net-buy days in last 5
      2. concentration_accel— 5d concentration > 20d concentration
      3. cost_proximity     — close near foreign avg cost (safe cushion)
      4. retail_exit        — margin balance declining (smart-money washing out retail)
    """
    scores = {}

    # 1. Foreign buying streak
    f5 = df["f_net"].tail(5)
    scores["foreign_streak"] = round((f5 > 0).sum() / 5 * 25, 1)

    # 2. Concentration acceleration
    vol5  = df["trading_volume"].tail(5).sum()  + 1e-9
    vol20 = df["trading_volume"].tail(20).sum() + 1e-9
    c5  = (df["f_net"].tail(5).sum()  + df["it_net"].tail(5).sum())  / vol5  * 100
    c20 = (df["f_net"].tail(20).sum() + df["it_net"].tail(20).sum()) / vol20 * 100
    accel = np.clip((c5 - c20) / (abs(c20) + 1e-9), -1, 3)
    scores["concentration_accel"] = round(np.clip(accel / 3 * 25, 0, 25), 1)

    # 3. Foreign cost proximity (small positive bias = healthy, >30% = overextended)
    bias = df["bias_f_cost"].iloc[-1] if "bias_f_cost" in df.columns else 0
    scores["cost_proximity"] = round(np.clip((0.30 - bias) / 0.35 * 25, 0, 25), 1)

    # 4. Retail exit signal (margin declining = retail scared out = good)
    m5 = df["m_net"].tail(5) if "m_net" in df.columns else pd.Series([0] * 5)
    scores["retail_exit"] = round((m5 < 0).sum() / 5 * 25, 1)

    total = round(sum(scores.values()), 1)
    if total >= 75:   signal = "🟢 強力買入"
    elif total >= 50: signal = "🟡 法人關注"
    elif total >= 25: signal = "🟠 觀望中"
    else:             signal = "🔴 法人退場"

    return {"score": total, "breakdown": scores, "signal": signal}


def calc_revenue_accel_score(rev: pd.DataFrame) -> dict:
    """
    Input:  FinMind taiwan_stock_month_revenue DataFrame
    Output: {accel_score: 0-100, yoy_trend: [3 months], is_accelerating: bool, label: str}

    Key insight: 5%→15%→30% scores higher than 30%→15%→5%
    even though both have 3 consecutive positive months.
    """
    if rev.empty or len(rev) < 15:
        return {
            "accel_score": 0, "is_accelerating": False,
            "label": "資料不足", "yoy_trend": [0, 0, 0], "acceleration": 0
        }

    rev = rev.sort_values("date").copy()
    yoy_list = []
    for i in range(-1, -4, -1):
        curr = rev["revenue"].iloc[i]
        prev = rev["revenue"].iloc[i - 12]
        yoy_list.append((curr / (prev + 1e-9) - 1) * 100 if prev > 0 else 0)
    yoy_list.reverse()  # [2 months ago, 1 month ago, latest]

    acceleration  = yoy_list[2] - yoy_list[0]
    base_score    = 40 if all(y > 0 for y in yoy_list) else 0
    accel_bonus   = np.clip(acceleration / 30 * 40, -20, 40)
    abs_bonus     = np.clip(yoy_list[2] / 50 * 20, 0, 20)
    total         = round(np.clip(base_score + accel_bonus + abs_bonus, 0, 100), 1)

    return {
        "accel_score": total,
        "yoy_trend": [round(y, 1) for y in yoy_list],
        "acceleration": round(acceleration, 1),
        "is_accelerating": acceleration > 5 and all(y > 0 for y in yoy_list),
        "label": (
            "🔥 加速成長" if acceleration > 5 else
            "📈 穩定成長" if yoy_list[2] > 0 else
            "📉 衰退中"
        ),
    }


def build_broker_dna(engine, stock_ids: list, top_n: int = 10) -> pd.DataFrame:
    """
    Cross-stock broker fingerprint analysis.
    Find brokers that appear before rallies across multiple stocks → market-maker DNA.

    Returns DataFrame: [分點, 覆蓋股票數, 股票列表]
    """
    broker_record: dict[str, set] = {}

    for sid in stock_ids:
        insights = engine.fetch_broker_tracking(sid)
        for insight in insights:
            for broker in insight["top_buyers"].keys():
                broker_record.setdefault(broker, set()).add(sid)

    rows = [
        {"分點": b, "覆蓋股票數": len(sids), "股票列表": ", ".join(sorted(sids))}
        for b, sids in broker_record.items()
        if len(sids) >= 2
    ]
    if not rows:
        return pd.DataFrame()
    return pd.DataFrame(rows).sort_values("覆蓋股票數", ascending=False).head(top_n)
