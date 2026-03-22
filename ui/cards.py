"""
ui/cards.py — reusable display components

All functions write directly to Streamlit via st.markdown / st.plotly_chart.
No business logic here — pure presentation.
"""

import streamlit as st
import plotly.graph_objects as go
from config import PLOTLY_BASE, plotly_layout


def metric_card(label: str, value: str, delta: float | None = None,
                color: str = "#1e293b", icon: str = ""):
    delta_html = ""
    if delta is not None:
        d_color = "#10b981" if delta >= 0 else "#ef4444"
        sign    = "+" if delta >= 0 else ""
        delta_html = f'<div style="color:{d_color};font-size:0.8em;font-weight:600;">{sign}{delta:.2f}%</div>'
    st.markdown(f"""
    <div style="background:white;border:1px solid #e2e8f0;border-top:3px solid {color};
                border-radius:10px;padding:14px;text-align:center;margin-bottom:8px;">
      <div style="color:#64748b;font-size:0.72em;font-weight:600;letter-spacing:0.5px;">
        {icon} {label.upper()}</div>
      <div style="font-size:1.6em;font-weight:800;color:#0f172a;margin:4px 0;">{value}</div>
      {delta_html}
    </div>
    """, unsafe_allow_html=True)


def sms_gauge(score: float, signal: str):
    """Compact SMS score gauge with coloured bar."""
    color = "#10b981" if score >= 75 else "#f59e0b" if score >= 50 else "#f43f5e"
    st.markdown(f"""
    <div style="background:white;border:1px solid #e2e8f0;border-radius:12px;
                padding:16px;text-align:center;">
      <div style="font-size:0.68em;font-weight:600;color:#64748b;
                  letter-spacing:1px;margin-bottom:6px;">SMART MONEY SCORE</div>
      <div style="font-size:3em;font-weight:900;color:{color};line-height:1;">{score:.0f}</div>
      <div style="color:#94a3b8;font-size:0.75em;margin-bottom:10px;">/ 100</div>
      <div style="background:#f1f5f9;border-radius:6px;height:6px;margin-bottom:10px;">
        <div style="width:{int(score)}%;background:{color};height:6px;border-radius:6px;"></div>
      </div>
      <div style="background:{color}18;color:{color};padding:5px 14px;border-radius:20px;
                  display:inline-block;font-weight:700;font-size:0.85em;">{signal}</div>
    </div>
    """, unsafe_allow_html=True)


def sms_breakdown_chart(breakdown: dict) -> go.Figure:
    """Horizontal bar chart of the 4 SMS sub-scores (0-25 each)."""
    labels = ["外資連買", "集中度加速", "成本安全墊", "散戶退場"]
    keys   = ["foreign_streak", "concentration_accel", "cost_proximity", "retail_exit"]
    values = [breakdown.get(k, 0) for k in keys]
    colors = ["#10b981" if v >= 17.5 else "#f59e0b" if v >= 10 else "#ef4444" for v in values]

    fig = go.Figure(go.Bar(
        x=values, y=labels, orientation="h",
        marker_color=colors,
        text=[f"{v:.0f}/25" for v in values],
        textposition="outside",
    ))
    fig.update_layout(**plotly_layout(
        height=180,
        xaxis=dict(range=[0, 30], showgrid=False),
        yaxis=dict(showgrid=False),
        showlegend=False,
        margin=dict(l=0, r=40, t=0, b=0),
    ))
    return fig


def render_strategy_card(card: dict, curr_price: float):
    """Full dark strategy card with entry / stop / targets / position size."""
    if not card:
        st.warning("策略卡生成失敗，請確認 Gemini API Key。")
        return

    entry   = card.get("entry", {})
    risk    = card.get("risk", {})
    targets = card.get("targets", [])
    pos     = card.get("position_size", {})
    conf    = int(card.get("confidence", 0))

    try:
        sl = float(risk.get("stop_loss", curr_price * 0.95))
        t1 = float(targets[0]["price"]) if targets else curr_price * 1.1
        rr = (t1 - curr_price) / (curr_price - sl + 1e-9) if curr_price > sl else 0
    except Exception:
        sl, t1, rr = curr_price * 0.95, curr_price * 1.1, 0

    conf_color = "#10b981" if conf >= 70 else "#f59e0b" if conf >= 40 else "#ef4444"

    # Build targets row — always 2 cells, second is empty placeholder if missing
    t1_price  = targets[0].get("price", "—") if len(targets) > 0 else "—"
    t1_reason = targets[0].get("reason", "—") if len(targets) > 0 else "—"
    t1_action = targets[0].get("action", "減倉50%") if len(targets) > 0 else "—"
    t2_price  = targets[1].get("price", "—") if len(targets) > 1 else "—"
    t2_reason = targets[1].get("reason", "—") if len(targets) > 1 else "—"
    t2_action = targets[1].get("action", "清倉") if len(targets) > 1 else "—"

    st.markdown(f"""
    <div style="border:2px solid #334155;border-radius:16px;padding:22px;
                background:#0f172a;color:white;margin:12px 0;">

      <!-- Header row -->
      <div style="display:flex;justify-content:space-between;align-items:center;margin-bottom:18px;">
        <span style="font-size:1.05em;font-weight:700;color:#f1f5f9;">
          ⚡ 策略卡 · {card.get('strategy_type','趨勢追蹤')}</span>
        <span style="background:{conf_color};padding:4px 14px;border-radius:20px;
                     font-size:0.78em;font-weight:700;">信心度 {conf}/100</span>
      </div>

      <!-- Row 1: Entry / Stop / Risk-Reward -->
      <div style="display:grid;grid-template-columns:1fr 1fr 1fr;gap:10px;margin-bottom:10px;">
        <div style="background:#1e293b;border-radius:10px;padding:13px;border-left:3px solid #3b82f6;">
          <div style="color:#94a3b8;font-size:0.68em;margin-bottom:3px;">進場區間</div>
          <div style="font-size:1.2em;font-weight:800;color:#60a5fa;">{entry.get('ideal_zone','—')}</div>
          <div style="color:#94a3b8;font-size:0.72em;margin-top:4px;">{entry.get('trigger','—')}</div>
        </div>
        <div style="background:#1e293b;border-radius:10px;padding:13px;border-left:3px solid #ef4444;">
          <div style="color:#94a3b8;font-size:0.68em;margin-bottom:3px;">停損位</div>
          <div style="font-size:1.2em;font-weight:800;color:#f87171;">{risk.get('stop_loss','—')}</div>
          <div style="color:#94a3b8;font-size:0.72em;margin-top:4px;">{str(risk.get('stop_reason',''))[:22]}</div>
        </div>
        <div style="background:#1e293b;border-radius:10px;padding:13px;border-left:3px solid #10b981;">
          <div style="color:#94a3b8;font-size:0.68em;margin-bottom:3px;">風報比</div>
          <div style="font-size:1.2em;font-weight:800;color:#34d399;">1 : {rr:.1f}</div>
          <div style="color:#94a3b8;font-size:0.72em;margin-top:4px;">最大損失 {risk.get('max_loss_pct','—')}%</div>
        </div>
      </div>

      <!-- Row 2: Target 1 / Target 2 side by side -->
      <div style="display:grid;grid-template-columns:1fr 1fr;gap:10px;margin-bottom:10px;">
        <div style="background:#1e293b;border-radius:10px;padding:13px;border-left:3px solid #f59e0b;">
          <div style="color:#94a3b8;font-size:0.68em;margin-bottom:3px;">目標一</div>
          <div style="font-size:1.2em;font-weight:800;color:#fcd34d;">{t1_price}</div>
          <div style="display:flex;justify-content:space-between;margin-top:5px;">
            <span style="color:#94a3b8;font-size:0.72em;">{t1_reason}</span>
            <span style="background:#78350f;color:#fcd34d;padding:1px 8px;border-radius:4px;
                         font-size:0.68em;font-weight:600;">{t1_action}</span>
          </div>
        </div>
        <div style="background:#1e293b;border-radius:10px;padding:13px;border-left:3px solid #a78bfa;">
          <div style="color:#94a3b8;font-size:0.68em;margin-bottom:3px;">目標二</div>
          <div style="font-size:1.2em;font-weight:800;color:#c4b5fd;">{t2_price}</div>
          <div style="display:flex;justify-content:space-between;margin-top:5px;">
            <span style="color:#94a3b8;font-size:0.72em;">{t2_reason}</span>
            <span style="background:#3b0764;color:#c4b5fd;padding:1px 8px;border-radius:4px;
                         font-size:0.68em;font-weight:600;">{t2_action}</span>
          </div>
        </div>
      </div>

      <!-- Row 3: Position size -->
      <div style="background:#1e293b;border-radius:10px;padding:13px;">
        <div style="color:#94a3b8;font-size:0.7em;margin-bottom:6px;">
          部位建議 · Kelly {pos.get('kelly_fraction', 0):.2f}</div>
        <div style="color:#f1f5f9;font-size:0.85em;">
          {pos.get('suggested_pct','—')} — {pos.get('rationale','—')}</div>
      </div>

      <div style="margin-top:10px;color:#475569;font-size:0.72em;">有效期：{card.get('validity','—')}</div>
    </div>
    """, unsafe_allow_html=True)


def stock_header(sid: str, name: str, curr: float, change: float,
                 change_pct: float, update_date: str, category: str,
                 coil_label: str, rev_label: str):
    """Top header card for the individual stock page."""
    c_arrow = "▲" if change >= 0 else "▼"
    c_color = "#ef4444" if change >= 0 else "#22c55e"
    st.markdown(f"""
    <div style="background:white;padding:22px 28px;border-radius:14px;
                border:1px solid #e2e8f0;margin-bottom:20px;
                box-shadow:0 2px 8px rgba(0,0,0,0.04);">
      <div style="display:flex;justify-content:space-between;align-items:center;flex-wrap:wrap;gap:12px;">
        <div>
          <span style="background:#1e293b;color:white;padding:3px 10px;border-radius:5px;
                       font-weight:700;font-size:0.78em;">{sid}</span>
          <span style="font-size:1.8em;font-weight:800;color:#1e293b;margin-left:10px;">{name}</span>
          <p style="color:#64748b;font-size:0.82em;margin-top:4px;margin-bottom:0;">
            更新：{update_date} | 產業：{category} | {coil_label} | {rev_label}
          </p>
        </div>
        <div style="text-align:right;">
          <div style="font-size:2.2em;font-weight:900;color:#1e293b;">{curr:.1f}</div>
          <div style="font-size:1em;font-weight:700;color:{c_color};">
            {c_arrow} {abs(change):.1f} ({change_pct:+.2f}%)
          </div>
        </div>
      </div>
    </div>
    """, unsafe_allow_html=True)
