"""
engine/alerts.py — real-time alert system

Scans STOCK_MAP for three signal types:
  1. Foreign buying streak  (≥ 5 consecutive days)
  2. Coiling pattern        (price-stable + volume-shrinking)
  3. Broker appearance      (≥ 3 key brokers buying before a surge)

Returns sorted alert list; high-priority shown as st.toast in UI.
"""

import streamlit as st
from engine.rocket_detector import detect_coiling
from config import FOREIGN_STREAK_DAYS, ALERT_MAX_STOCKS


def check_alerts(engine, stock_map: dict) -> list[dict]:
    alerts = []

    for sid, name in list(stock_map.items())[:ALERT_MAX_STOCKS]:
        df, _ = engine.fetch_data(sid)
        if df.empty or len(df) < 20:
            continue

        # 1. Foreign buying streak
        if "f_net" in df.columns and (df["f_net"].tail(FOREIGN_STREAK_DAYS) > 0).all():
            alerts.append({
                "type": "foreign", "sid": sid, "name": name, "priority": "medium",
                "msg": f"📈 {name} 外資連買 {FOREIGN_STREAK_DAYS} 日，籌碼持續集中",
            })

        # 2. Coiling pattern
        coil = detect_coiling(df)
        if coil["is_coiling"]:
            alerts.append({
                "type": "coiling", "sid": sid, "name": name, "priority": "high",
                "msg": f"🔥 {name} 蓄力完成！壓力位 {coil['key_resistance']}，等待突破",
            })

        # 3. Broker appearance (3+ key brokers)
        insights = engine.fetch_broker_tracking(sid)
        for ins in insights:
            if len(ins["top_buyers"]) >= 3:
                alerts.append({
                    "type": "broker", "sid": sid, "name": name, "priority": "high",
                    "msg": f"🕵️ {name} 偵測到 {len(ins['top_buyers'])} 個關鍵分點同步進場",
                })
                break

    return sorted(alerts, key=lambda x: {"high": 0, "medium": 1}.get(x["priority"], 2))


def show_alert_toasts(alerts: list[dict]):
    """Show top-3 high-priority alerts as st.toast."""
    for a in [x for x in alerts if x["priority"] == "high"][:3]:
        st.toast(a["msg"], icon="🚨")
