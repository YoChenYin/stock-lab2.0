"""tabs/stock/financials.py — Tab 2: 財報分析"""

import streamlit as st
import pandas as pd
import plotly.graph_objects as go
import plotly.express as px
from plotly.subplots import make_subplots
from config import PLOTLY_BASE, plotly_layout


def _fmt_quarter(dt) -> str:
    """2024-01-01 → '24Q1'  |  period string '2024Q1' → '24Q1'"""
    try:
        if hasattr(dt, "year"):
            q = (dt.month - 1) // 3 + 1
            return f"{str(dt.year)[2:]}Q{q}"
        s = str(dt)
        if "Q" in s:
            parts = s.split("Q")
            return f"{parts[0][2:]}Q{parts[1]}"
        return str(dt)[:7]
    except Exception:
        return str(dt)


def render(engine, sid: str):
    df_q = engine.fetch_quarterly_financials(sid)

    if df_q.empty:
        st.warning("查無季度財報數據。")
        return

    df_q["date"] = pd.to_datetime(df_q["date"])
    df_q = df_q.sort_values("date").reset_index(drop=True)
    df_q["q_label"] = df_q["date"].apply(_fmt_quarter)

    # ── Scatter: quality of earnings ──
    st.subheader("🔥 獲利含金量：成長質量散點圖")
    col_chart, col_info = st.columns([2, 1], gap="large")

    with col_chart:
        scatter_df = df_q.dropna(subset=["rev_yoy", "margin_delta", "EPS"])
        if not scatter_df.empty:
            fig = px.scatter(
                scatter_df, x="rev_yoy", y="margin_delta",
                text="q_label",
                size=scatter_df["EPS"].clip(lower=0.1),
                color="eps_yoy",
                color_continuous_scale="RdYlGn",
                labels={"rev_yoy": "營收 YoY (%)", "margin_delta": "毛利變動 (pts)"},
            )
            fig.add_vline(x=0, line_dash="dash", line_color="#94a3b8", opacity=0.5)
            fig.add_hline(y=0, line_dash="dash", line_color="#94a3b8", opacity=0.5)
            fig.update_traces(textposition="top center", textfont=dict(size=10, color="#334155"))
            fig.update_layout(**plotly_layout(height=400))
            st.plotly_chart(fig, use_container_width=True)
        else:
            st.info("散點圖數據不足（需要至少 5 季含毛利數據）。")

    with col_info:
        st.markdown("#### 💡 解讀")
        st.markdown("""
        <div style="font-size:0.83em;color:#64748b;line-height:1.7;
                    background:#f8fafc;padding:14px;border-radius:8px;">
          🚀 <b>右上 (雙強)</b>：黃金擴張期<br>
          💎 <b>左上 (優化)</b>：毛利提升，品質改善<br>
          📉 <b>右下 (犧牲)</b>：搶市佔，毛利受壓<br>
          ⚠️ <b>左下 (衰退)</b>：基本面轉弱風險<br><br>
          <span style="color:#94a3b8;font-size:0.9em;">
            泡泡大小 = EPS 絕對值<br>
            顏色深淺 = 獲利 YoY 成長率
          </span>
        </div>
        """, unsafe_allow_html=True)

    st.divider()

    # ── EPS trend: categorical Q labels on x-axis ──
    st.subheader("📊 季度獲利趨勢")
    st.caption("X 軸格式：24Q1 = 2024年第一季")

    fig_q = make_subplots(specs=[[{"secondary_y": True}]])
    fig_q.add_trace(go.Bar(
        x=df_q["q_label"], y=df_q["EPS"],
        name="季度 EPS (元)",
        marker_color=["#10b981" if v >= 0 else "#ef4444" for v in df_q["EPS"].fillna(0)],
        opacity=0.85,
        text=df_q["EPS"].round(2), textposition="outside", textfont=dict(size=9),
    ), secondary_y=False)

    if "eps_yoy" in df_q.columns:
        fig_q.add_trace(go.Scatter(
            x=df_q["q_label"], y=df_q["eps_yoy"],
            name="獲利 YoY%",
            line=dict(color="#e11d48", width=2),
            mode="lines+markers", marker=dict(size=5),
        ), secondary_y=True)
        fig_q.add_hline(y=0, line_dash="dot", line_color="#94a3b8", opacity=0.4, secondary_y=True)

    if "margin" in df_q.columns:
        fig_q.add_trace(go.Scatter(
            x=df_q["q_label"], y=df_q["margin"],
            name="毛利率%",
            line=dict(color="#6366f1", width=1.5, dash="dot"),
            mode="lines", visible="legendonly",
        ), secondary_y=True)

    fig_q.update_layout(**plotly_layout(
        height=400,
        xaxis=dict(showgrid=False, type="category"),
        yaxis=dict(showgrid=True, gridcolor="#f1f5f9", title="EPS (元)"),
        yaxis2=dict(showgrid=False, title="YoY (%) / 毛利率 (%)"),
        bargap=0.25,
    ))
    st.plotly_chart(fig_q, use_container_width=True)

    # ── Revenue trend ──
    if "Revenue" in df_q.columns:
        st.divider()
        st.subheader("📈 季度營收趨勢")
        fig_rev = make_subplots(specs=[[{"secondary_y": True}]])
        fig_rev.add_trace(go.Bar(
            x=df_q["q_label"], y=df_q["Revenue"],
            name="季度營收 (千元)", marker_color="#2563eb", opacity=0.7,
        ), secondary_y=False)
        if "rev_yoy" in df_q.columns:
            fig_rev.add_trace(go.Scatter(
                x=df_q["q_label"], y=df_q["rev_yoy"],
                name="營收 YoY%",
                line=dict(color="#f59e0b", width=2),
                mode="lines+markers", marker=dict(size=5),
            ), secondary_y=True)
            fig_rev.add_hline(y=0, line_dash="dot", line_color="#94a3b8", opacity=0.4, secondary_y=True)
        fig_rev.update_layout(**plotly_layout(
            height=320,
            xaxis=dict(showgrid=False, type="category"),
            yaxis=dict(showgrid=True, gridcolor="#f1f5f9", title="營收 (千元)"),
            yaxis2=dict(showgrid=False, title="YoY (%)"),
            bargap=0.25,
        ))
        st.plotly_chart(fig_rev, use_container_width=True)

    # ── Data table ──
    with st.expander("📋 完整財報數據 (近 12 季)"):
        show = [c for c in ["q_label", "Revenue", "EPS", "margin", "rev_yoy", "eps_yoy"]
                if c in df_q.columns]
        disp = df_q[show].copy().rename(columns={
            "q_label": "季度", "Revenue": "營收(千)", "EPS": "EPS",
            "margin": "毛利率%", "rev_yoy": "營收YoY%", "eps_yoy": "獲利YoY%",
        })
        grad = [c for c in ["獲利YoY%", "毛利率%"] if c in disp.columns]
        st.dataframe(
            disp.sort_values("季度", ascending=False)
                .style.background_gradient(subset=grad, cmap="RdYlGn")
                .format(precision=2),
            use_container_width=True,
        )
