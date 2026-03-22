"""tabs/heatmap.py — Page 2: 族群資金熱圖"""

import streamlit as st
import numpy as np
import plotly.express as px

from sector_data import STOCK_POOL, SECTOR_GROUPS
from config import PLOTLY_BASE


@st.cache_data(ttl=3600)
def _get_heatmap_data(_engine, sector_map):
    heatmap_data, sector_details = [], {}
    for sector, stocks in sector_map.items():
        strengths, stock_list = [], []
        for sid in stocks:
            df, _ = _engine.fetch_data(sid)
            if df.empty:
                continue
            if len(df) < 5:      
                continue
            
            f_sum = df["f_net"].tail(5).sum()
            it_sum = df["it_net"].tail(5).sum()
            v_sum  = df["trading_volume"].tail(5).sum()
            strength = (f_sum + it_sum) / v_sum * 100
            strengths.append(strength)
            change = ((df["close"].iloc[-1] - df["close"].iloc[-5]) / df["close"].iloc[-5]) * 100
            stock_list.append({
                "代號": sid, "名稱": STOCK_POOL.get(sid, sid),
                "現價": round(df["close"].iloc[-1], 2),
                "漲跌幅": round(change, 2),
                "外資(張)": int(f_sum / 1000),
                "投信(張)": int(it_sum / 1000),
                "融資":     int(df["m_net"].tail(5).sum()) if "m_net" in df.columns else 0,
            })
        if strengths:
            heatmap_data.append({"族群": sector, "平均強度": np.mean(strengths)})
            sector_details[sector] = stock_list
    return heatmap_data, sector_details


def render(engine):
    st.header("🔥 科技族群資金流向熱圖")

    with st.spinner("計算族群強度..."):
        raw, sector_details = _get_heatmap_data(engine, SECTOR_GROUPS)

    if not raw:
        st.warning("無法取得族群數據。")
        return

    import pandas as pd
    heat_df = pd.DataFrame(raw)
    fig = px.bar(heat_df.sort_values("平均強度"), x="平均強度", y="族群",
                 orientation="h", color="平均強度",
                 color_continuous_scale="Teal", template="plotly_white")
    fig.update_layout(**{k: v for k, v in PLOTLY_BASE.items() if k != "template"}, height=420)
    st.plotly_chart(fig, use_container_width=True)

    selected = st.selectbox("🎯 選擇族群查看細節：", options=heat_df["族群"].tolist()[::-1])
    st.markdown(f"#### 🛰️ {selected} 族群")

    stocks = list({str(s["代號"]): s for s in sector_details.get(selected, [])
                   if str(s.get("代號", "")).strip()}.values())

    def chip_span(val):
        c = "#ef4444" if val > 0 else "#22c55e" if val < 0 else "#64748b"
        return f'<span style="color:{c};font-weight:700;">{"+" if val > 0 else ""}{val:,} 張</span>'

    cols = st.columns(3)
    for idx, s in enumerate(stocks):
        with cols[idx % 3]:
            ch = s["漲跌幅"]
            pc = "#ef4444" if ch > 0 else "#22c55e" if ch < 0 else "#64748b"
            st.markdown(f"""
            <div style="border:1px solid #e2e8f0;border-radius:12px;padding:16px;
                        margin-bottom:8px;background:white;box-shadow:0 2px 6px rgba(0,0,0,0.04);">
              <div style="display:flex;justify-content:space-between;align-items:center;margin-bottom:8px;">
                <b style="font-size:1.05em;color:#1e293b;">{s['名稱']}</b>
                <span style="color:#64748b;font-size:0.72em;background:#f1f5f9;
                             padding:2px 8px;border-radius:6px;">{s['代號']}</span>
              </div>
              <div style="display:flex;align-items:baseline;gap:8px;margin-bottom:10px;">
                <span style="font-size:1.5em;font-weight:800;color:#0f172a;">{s['現價']}</span>
                <span style="font-size:1em;font-weight:700;color:{pc};">{"+" if ch>0 else ""}{ch:.2f}%</span>
                <span style="font-size:0.72em;color:#94a3b8;">(5日)</span>
              </div>
              <div style="border-top:1px dotted #e2e8f0;padding-top:8px;font-size:0.8em;
                          color:#475569;display:grid;grid-template-columns:1fr 1fr;row-gap:4px;">
                <div>外資：{chip_span(s['外資(張)'])}</div>
                <div>投信：{chip_span(s['投信(張)'])}</div>
              </div>
            </div>
            """, unsafe_allow_html=True)
