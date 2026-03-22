"""tabs/stock/chips.py — Tab 4: 籌碼面

Key additions vs previous version:
  - Real ownership % from fetch_real_chip_data (not AI-generated)
  - 水位計 (watermark gauge): shows absolute institutional level, not just net flow direction
  - Rolling window of estimated holdings: cumsum normalised by avg daily volume
  - Clearer legend placement (below chart, not overlapping title)
"""

import streamlit as st
import pandas as pd
import numpy as np
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import plotly.express as px
from config import COLORS, plotly_layout


# ─────────────────────────────────────────────────────────
# Water level helpers
# ─────────────────────────────────────────────────────────

def _build_water_level(df: pd.DataFrame, lookback: int = 60) -> pd.DataFrame:
    """
    Build a rolling estimate of how much of average daily volume has been
    accumulated by institutions over the lookback window.

    Formula:
      running_net = f_net.cumsum() (within this window)
      water_level_pct = running_net / avg_daily_volume * 100

    This shows the DIRECTION and MAGNITUDE of institutional positioning,
    not just daily net flows.
    """
    d = df.tail(lookback).copy().reset_index(drop=True)
    avg_vol = d["trading_volume"].mean()
    if avg_vol == 0:
        d["f_water"] = 0.0
        d["it_water"] = 0.0
        return d
    d["f_water"]  = d["f_net"].cumsum()  / avg_vol * 100
    d["it_water"] = d["it_net"].cumsum() / avg_vol * 100
    return d


def _water_level_card(label: str, pct: float, color: str):
    """Render a compact watermark gauge card."""
    clamped  = max(-100.0, min(100.0, float(pct)))
    bar_w    = min(50.0, abs(clamped) / 2)   # 0–50% width from centre
    arrow    = "▲" if pct > 0 else "▼" if pct < 0 else "—"
    a_color  = "#10b981" if pct > 0 else "#ef4444" if pct < 0 else "#94a3b8"
    pct_str  = f"{pct:+.1f}%"

    # Build the fill div as a plain string — no f-string nesting
    if pct > 0:
        fill_div = (
            "<div style=\"position:absolute;left:50%;width:" + str(bar_w * 2) +
            "%;height:8px;background:" + color +
            ";border-radius:0 4px 4px 0;opacity:0.7;\"></div>"
        )
    elif pct < 0:
        fill_div = (
            "<div style=\"position:absolute;right:50%;width:" + str(bar_w * 2) +
            "%;height:8px;background:" + color +
            ";border-radius:4px 0 0 4px;opacity:0.7;\"></div>"
        )
    else:
        fill_div = ""

    st.markdown(f"""
<div style="background:white;border:1px solid #e2e8f0;border-radius:10px;
            padding:12px 14px;margin-bottom:8px;">
  <div style="font-size:0.7em;font-weight:600;color:#64748b;
              letter-spacing:0.5px;margin-bottom:8px;">{label.upper()}</div>
  <div style="display:flex;align-items:center;gap:10px;">
    <div style="font-size:1.3em;font-weight:800;color:{color};min-width:60px;
                flex-shrink:0;">{pct_str}</div>
    <div style="flex:1;position:relative;height:8px;background:#f1f5f9;
                border-radius:4px;overflow:hidden;">
      {fill_div}
      <div style="position:absolute;left:50%;top:0;width:2px;height:8px;
                  background:#cbd5e1;transform:translateX(-50%);z-index:2;"></div>
    </div>
    <span style="color:{a_color};font-size:1em;flex-shrink:0;">{arrow}</span>
  </div>
  <div style="color:#94a3b8;font-size:0.7em;margin-top:5px;">
    相對於近 60 日均量的累積比例
  </div>
</div>
    """, unsafe_allow_html=True)


# ─────────────────────────────────────────────────────────
# Main render
# ─────────────────────────────────────────────────────────

def render(engine, sid: str, name: str, df: pd.DataFrame, real_chip: dict):

    # ── Row 1: Real ownership metrics ──
    st.markdown("### 🏦 真實持股水位")
    col_metrics, col_trend = st.columns([1, 2])

    with col_metrics:
        # Real data from FinMind
        if real_chip and real_chip.get("source") != "unavailable":
            src  = real_chip["source"]
            fpct = real_chip.get("foreign_pct")
            mpct = real_chip.get("major_pct")

            if fpct is not None:
                st.metric("外資持股比例",
                          f"{fpct}%",
                          help="來源：FinMind taiwan_stock_holding_shares_per" if src == "finmind_real"
                               else "估算值（流量累積法）")
            else:
                st.metric("外資持股比例", "—")

            if mpct is not None:
                st.metric("前十大股東合計", f"{mpct}%")
            else:
                st.metric("前十大股東合計", "—（僅付費帳號可取得）")

            if src == "estimated":
                st.caption("⚠️ 真實持股 API 暫不可用，數值為流量估算。")
            else:
                date_str = real_chip.get("update_date", "")
                st.caption(f"✅ 官方數據 · 更新日 {date_str}")
        else:
            st.info("持股數據暫不可用。")

        st.divider()

        # Water level from net flows
        wl_df = _build_water_level(df)
        if not wl_df.empty:
            f_level  = round(wl_df["f_water"].iloc[-1], 1)
            it_level = round(wl_df["it_water"].iloc[-1], 1)
            _water_level_card("外資累積水位", f_level, COLORS["foreign"])
            _water_level_card("投信累積水位", it_level, COLORS["trust"])

    with col_trend:
        st.markdown("**法人持股變動趨勢（近 60 日）**")
        df_chips = df.tail(60).copy()
        fig = px.line(df_chips, x="date", y=["f_net", "it_net"],
                      template="plotly_white",
                      color_discrete_map={"f_net": COLORS["foreign"], "it_net": COLORS["trust"]})
        fig.update_layout(**plotly_layout(
            legend=dict(orientation="h", yanchor="top", y=-0.15, xanchor="center", x=0.5),
        ))
        st.plotly_chart(fig, use_container_width=True)

    # ── Row 2: Water level trend chart ──
    st.markdown("### 💧 法人持倉水位趨勢")
    st.caption("以近60日平均量為分母，累積淨買超為分子 — 顯示法人目前的絕對水位方向，而非單日流量")

    wl_df = _build_water_level(df, lookback=120)
    if not wl_df.empty and "date" in wl_df.columns:
        fig_wl = go.Figure()
        fig_wl.add_trace(go.Scatter(
            x=wl_df["date"], y=wl_df["f_water"],
            name="外資水位 (%)", fill="tozeroy",
            fillcolor="rgba(37,99,235,0.1)",
            line=dict(color=COLORS["foreign"], width=2),
        ))
        fig_wl.add_trace(go.Scatter(
            x=wl_df["date"], y=wl_df["it_water"],
            name="投信水位 (%)", fill="tozeroy",
            fillcolor="rgba(22,163,74,0.1)",
            line=dict(color=COLORS["trust"], width=1.5),
        ))
        fig_wl.add_hline(y=0, line_dash="dash", line_color="#94a3b8", opacity=0.5)
        fig_wl.update_layout(**plotly_layout(
            height=260,
            yaxis=dict(showgrid=True, gridcolor="#f1f5f9", title="水位 (%)"),
            legend=dict(orientation="h", yanchor="top", y=-0.18, xanchor="center", x=0.5),
            margin=dict(l=0, r=0, t=10, b=60),
        ))
        st.plotly_chart(fig_wl, use_container_width=True)

    # ── Row 3: Battle chart (institutional vs retail) ──
    st.markdown("### ⚔️ 三大法人 vs 散戶")
    df_sent = engine.fetch_detailed_sentiment(sid)

    if not df_sent.empty:
        st.markdown(f"**📈 {name} 籌碼結構分析**")
        fig_b = make_subplots(specs=[[{"secondary_y": True}]])
        fig_b.add_trace(go.Scatter(x=df_sent["date"], y=df_sent["f_cumsum"],
            name="外資累計", line=dict(color=COLORS["foreign"], width=2)), secondary_y=False)
        fig_b.add_trace(go.Scatter(x=df_sent["date"], y=df_sent["it_cumsum"],
            name="投信累計", line=dict(color=COLORS["trust"], width=2.5)), secondary_y=False)
        fig_b.add_trace(go.Scatter(x=df_sent["date"], y=df_sent["d_cumsum"],
            name="自營商", line=dict(color=COLORS["dealer"], width=1.5, dash="dot")), secondary_y=False)
        fig_b.add_trace(go.Scatter(x=df_sent["date"], y=df_sent["retail_margin"],
            name="散戶融資", line=dict(color=COLORS["retail"], width=2, dash="dash")), secondary_y=True)
        fig_b.update_layout(**plotly_layout(
            height=480,
            legend=dict(orientation="h", yanchor="top", y=-0.12, xanchor="center", x=0.5),
            margin=dict(l=0, r=0, t=10, b=60),
        ))
        st.plotly_chart(fig_b, use_container_width=True)

        # Auto-diagnosis
        st.markdown("#### 💡 實時籌碼診斷")
        curr_s = df_sent.iloc[-1]
        prev_s = df_sent.iloc[-5] if len(df_sent) >= 5 else df_sent.iloc[0]
        c1, c2, c3 = st.columns(3)

        f_flow = curr_s["f_cumsum"] - prev_s["f_cumsum"]
        (c1.success if f_flow >= 0 else c1.error)(
            f"{'📈 外資增持' if f_flow >= 0 else '📉 外資拋售'}\n5日：{int(f_flow/1000)}張")

        it_flow = curr_s["it_cumsum"] - prev_s["it_cumsum"]
        (c2.success if it_flow >= 0 else c2.warning)(
            f"{'🛡️ 投信護盤' if it_flow >= 0 else '⏳ 投信觀望'}\n5日：{int(it_flow/1000)}張")

        m_flow = curr_s["retail_margin"] - prev_s["retail_margin"]
        (c3.warning if m_flow > 0 else c3.success)(
            f"{'⚠️ 散戶進場' if m_flow > 0 else '🧹 散戶退場'}\n融資：{int(m_flow/1000)}張")

    else:
        st.info("法人詳細數據暫不可用（需要 FinMind 付費帳號）。")

    # ── Row 4: Broker tracking ──
    st.divider()
    st.subheader("📍 歷史起漲點：關鍵分點追蹤")
    st.caption("分點資料來源：FinMind taiwan_stock_broker_make_daily（需要付費帳號）")

    with st.status("🔍 交叉比對起漲分點...", expanded=True) as status:
        broker_insights = engine.fetch_broker_tracking(sid)
        status.update(label="✅ 分點比對完成", state="complete")

    if not broker_insights:
        st.info("近期無明顯起漲點（漲幅 > 4%），暫無分點數據。若持續無資料，請確認 FinMind token 有分點資料授權。")
    else:
        cols = st.columns(len(broker_insights))
        for i, ins in enumerate(broker_insights):
            with cols[i]:
                st.markdown(f"**📅 起漲日：{ins['surge_date']}**")
                for broker, net in ins["top_buyers"].items():
                    st.write(f"- {broker}: `+{int(net/1000)}` 張")
        st.info("💡 觀察是否有重複出現的分點——那就是值得追蹤的神秘大戶。")
