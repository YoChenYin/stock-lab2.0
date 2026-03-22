"""tabs/stock/valuation.py — Tab 5: 估值位置"""

import streamlit as st
import plotly.graph_objects as go


def render(ai_data: dict, df, curr_close: float):
    val   = ai_data["valuation"]
    level = float(val.get("level_pct", 50))
    low52 = df["close"].tail(252).min()
    high52= df["close"].tail(252).max()

    col_gauge, col_info = st.columns([2, 1], gap="large")
    with col_gauge:
        fig = go.Figure(go.Indicator(
            mode="gauge+number+delta",
            value=level,
            title={"text": "52週位置百分位"},
            gauge={
                "axis": {"range": [0, 100]},
                "bar": {"color": "#1e293b"},
                "steps": [
                    {"range": [0, 33],  "color": "#dcfce7"},
                    {"range": [33, 67], "color": "#fef9c3"},
                    {"range": [67, 100],"color": "#fee2e2"},
                ],
                "threshold": {"value": level, "line": {"color": "#1e293b", "width": 3}},
            },
            delta={"reference": 50, "suffix": "%"},
        ))
        fig.update_layout(height=280, margin=dict(t=20, b=0, l=20, r=20),
                          paper_bgcolor="rgba(0,0,0,0)")
        st.plotly_chart(fig, use_container_width=True)

    with col_info:
        st.metric("52週低點", f"{low52:.1f}")
        st.metric("當前價格", f"{curr_close:.1f}")
        st.metric("52週高點", f"{high52:.1f}")

    st.markdown(f"""
    <div style="background:linear-gradient(90deg,#1e293b,#334155);color:#facc15;
                padding:14px 22px;border-radius:10px;margin:16px 0;">
      <span style="color:white;font-size:0.8em;opacity:0.8;">✨ 估值結論：</span>
      <b style="margin-left:8px;">{val['conclusion']}</b>
    </div>
    """, unsafe_allow_html=True)

    op_col, ri_col = st.columns(2)
    with op_col:
        st.markdown("**📗 潛在利多**")
        for o in val.get("opportunities", []):
            st.markdown(f"<div style='font-size:0.84em;margin-bottom:5px;color:#166534;"
                        f"padding:4px 10px;background:#f0fdf4;border-radius:6px;'>✓ {o}</div>",
                        unsafe_allow_html=True)
    with ri_col:
        st.markdown("**📕 潛在利空**")
        for r in val.get("risks", []):
            st.markdown(f"<div style='font-size:0.84em;margin-bottom:5px;color:#991b1b;"
                        f"padding:4px 10px;background:#fff1f2;border-radius:6px;'>✗ {r}</div>",
                        unsafe_allow_html=True)
