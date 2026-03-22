"""tabs/stock/outlook.py — Tab 3: 法說展望"""

import hashlib
import streamlit as st
import plotly.graph_objects as go
from config import PLOTLY_BASE


def _mops_hash(event_text: str) -> str:
    """Stable 8-char hash of MOPS event text — used as Gemini cache key."""
    return hashlib.md5(event_text.encode("utf-8")).hexdigest()[:8]


def render(engine, sid: str, name: str):
    with st.spinner("🔍 存取公開資訊觀測站..."):
        mops = engine.fetch_latest_mops_pdf_info(sid)

    event_text = mops.get("event", "").strip()
    is_sparse  = len(event_text) < 50

    # Official MOPS card
    quality_badge = (
        '<span style="background:#fef9c3;color:#854d0e;padding:2px 8px;'
        'border-radius:4px;font-size:0.68em;font-weight:600;">⚠️ 摘要有限</span>'
        if is_sparse else
        '<span style="background:#f0fdf4;color:#15803d;padding:2px 8px;'
        'border-radius:4px;font-size:0.68em;font-weight:600;">✅ 摘要完整</span>'
    )

    st.markdown(f"""
    <div style="background:white;padding:20px;border-radius:12px;
                border:1px solid #edf2f7;border-left:6px solid #1e293b;
                box-shadow:0 2px 6px rgba(0,0,0,0.04);margin-bottom:22px;">
      <div style="display:flex;justify-content:space-between;align-items:flex-start;gap:16px;">
        <div style="flex:1;">
          <div style="display:flex;align-items:center;gap:10px;margin-bottom:8px;">
            <span style="background:#1e293b;color:white;padding:2px 10px;
                         border-radius:4px;font-size:0.7em;font-weight:700;">MOPS</span>
            <span style="color:#64748b;font-size:0.83em;">📅 {mops['date']}</span>
            {quality_badge}
          </div>
          <p style="color:#1a202c;font-size:0.88em;margin:0;line-height:1.6;">{event_text or "（無摘要文字）"}</p>
        </div>
        <a href="{mops['url']}" target="_blank"
           style="background:#f8fafc;color:#1e293b;padding:10px 16px;border-radius:8px;
                  text-decoration:none;font-size:0.8em;font-weight:700;
                  border:1px solid #e2e8f0;white-space:nowrap;">查看簡報 ↗</a>
      </div>
    </div>
    """, unsafe_allow_html=True)

    if is_sparse:
        st.info(
            "📋 MOPS 摘要文字較短，AI 分析只能根據有限資訊作答。"
            "建議點擊「查看簡報」閱讀完整 PDF 取得詳細數據。"
        )

    # Content hash ensures same MOPS text = same Gemini output
    content_hash = _mops_hash(event_text)

    with st.spinner("AI 解析法說內容..."):
        outlook = engine.get_real_world_outlook(sid, name, mops,
                                                 _content_hash=content_hash)

    if not outlook:
        st.info("AI 解析暫不可用。")
        return

    # Data quality badge from AI response
    dq = outlook.get("data_quality", "sufficient")
    if dq == "limited":
        st.warning("AI 回報：法說摘要資訊有限，以下分析部分欄位可能為「摘要未提及」。")

    alpha = outlook["scorecard"].get("alpha_factor", "—")
    st.markdown(f"""
    <div style="background:linear-gradient(90deg,#1e293b,#334155);color:#facc15;
                padding:12px 20px;border-radius:8px;margin-bottom:18px;">
      <span style="color:white;font-size:0.78em;opacity:0.8;">✨ 核心 Alpha：</span>
      <b style="font-size:0.93em;margin-left:8px;">{alpha}</b>
    </div>
    """, unsafe_allow_html=True)

    g = outlook.get("guidance_detail", {})
    c1, c2, c3 = st.columns(3)
    for col, (title, val, clr) in zip([c1, c2, c3], [
        ("📊 營收展望", g.get("revenue", "—"), "#3b82f6"),
        ("💎 毛利指引", g.get("margin", "—"),  "#10b981"),
        ("🏗️ 資本支出", g.get("capex", "—"),   "#f59e0b"),
    ]):
        col.markdown(f"""
        <div style="border-left:3px solid {clr};padding-left:12px;margin-bottom:8px;">
          <small style="color:#64748b;font-weight:700;font-size:0.72em;">{title}</small><br>
          <span style="font-size:0.86em;color:#1e293b;">{val}</span>
        </div>
        """, unsafe_allow_html=True)

    st.divider()
    l, r = st.columns([1.6, 1], gap="large")
    with l:
        st.markdown("**✨ 成長動能**")
        for h in outlook.get("growth_drivers", []):
            st.markdown(
                f"<div style='font-size:0.87em;margin-bottom:6px;color:#334155;"
                f"padding-left:10px;border-left:2px solid #e2e8f0;'>● {h}</div>",
                unsafe_allow_html=True)
        st.markdown("**🚩 法人風險**")
        risks = "、".join(outlook.get("analyst_concerns", [])) or "暫無重大風險"
        st.markdown(
            f"<div style='font-size:0.81em;color:#991b1b;background:#fff1f2;"
            f"padding:12px;border-radius:8px;'>{risks}</div>",
            unsafe_allow_html=True)
    with r:
        st.markdown("**🎯 估值邏輯**")
        st.markdown(
            f"""<div style="font-size:0.81em;color:#1e40af;background:#eff6ff;
                padding:14px;border-radius:8px;line-height:1.5;border:1px solid #dbeafe;">
                {outlook.get('valuation_anchor','—')}</div>""",
            unsafe_allow_html=True)
        fig = go.Figure()
        fig.add_trace(go.Scatterpolar(
            r=outlook.get("radar", [75]*5),
            theta=["訂單", "獲利", "地位", "技術", "財務"],
            fill="toself", line_color="#1e293b", fillcolor="rgba(30,41,59,0.08)",
        ))
        fig.update_layout(
            polar=dict(radialaxis=dict(visible=False, range=[0, 100]),
                       bgcolor="rgba(0,0,0,0)"),
            paper_bgcolor="rgba(0,0,0,0)", height=220,
            margin=dict(t=20, b=20, l=40, r=40), showlegend=False,
        )
        st.plotly_chart(fig, use_container_width=True)

    # Footnote
    st.caption(
        f"📌 法說分析基於 MOPS 摘要原文（hash: {content_hash}）。"
        "相同摘要內容將產生相同分析結果。"
        "如需更新，請等待 MOPS 發布新法說資料。"
    )
