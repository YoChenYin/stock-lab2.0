"""tabs/stock/overview.py — Tab 1: 公司概況

Real scores (no AI hallucination):
  - tech_pr       → engine/fundamentals.py calc_tech_barrier_score()
  - mkt_share     → engine/fundamentals.py calc_peer_market_share()
  - leader_score  → engine/fundamentals.py calc_leader_score()
  - foreign_pct   → engine/wall_street_engine.py fetch_real_chip_data()

AI is kept for: business_model description, moat narrative, diagnosis text.
"""

import streamlit as st
import plotly.graph_objects as go
from ui.cards import sms_gauge, sms_breakdown_chart
from engine.fundamentals import (
    calc_tech_barrier_score,
    calc_peer_market_share,
    calc_leader_score,
    get_sector_avg_margin,
)


def render(
    ai_data: dict,
    sms_result: dict,
    rev_accel: dict,
    fin_df,        # quarterly financials DataFrame
    df_chip,       # daily OHLCV + institutional DataFrame
    real_chip: dict,
    engine,
    sid: str,
    sector_stocks: list,
):
    # ── Compute real scores once ──
    with st.spinner("計算真實財務指標..."):
        sector_avg = get_sector_avg_margin(engine, sector_stocks)
        tech_score = calc_tech_barrier_score(fin_df, sector_avg_margin=sector_avg)
        peer_share = calc_peer_market_share(engine, sid, sector_stocks)
        leader     = calc_leader_score(engine, sid, sector_stocks, fin_df, df_chip)

    # ── Row 1: Business model + SMS ──
    col_main, col_sms = st.columns([1.4, 1], gap="large")

    with col_main:
        st.subheader("🎯 核心商業模式")
        st.markdown(f"> {ai_data['overview']['business_model']}")
        st.info(f"**💡 競業差異**：{ai_data['overview'].get('competitor_diff', '—')}")
        st.caption("⚠️ 以上為 AI 定性分析，僅供參考。")

    with col_sms:
        sms_gauge(sms_result["score"], sms_result["signal"])
        st.plotly_chart(sms_breakdown_chart(sms_result.get("breakdown", {})),
                        use_container_width=True)

    st.divider()

    # ── Row 2: Real moat metrics ──
    st.subheader("🏰 護城河指標（真實數據計算）")

    m1, m2, m3, m4 = st.columns(4)

    # Tech barrier — real calculation
    m1.metric(
        "技術門檻",
        f"PR {tech_score['tech_pr']}",
        help=f"R&D強度：{tech_score['notes'].get('rd_intensity_pct','—')}%  |  "
             f"毛利：{tech_score['notes'].get('latest_margin','—')}%  |  "
             f"穩定性：σ={tech_score['notes'].get('margin_std','—')}%"
    )
    st.caption(tech_score["label"]) if hasattr(m1, "caption") else None
    with m1:
        st.caption(tech_score["label"])
        if tech_score["data_quality"] == "partial":
            st.caption("⚠️ 無R&D數據，以毛利替代")

    # Peer market share — relative rank
    m2.metric(
        "同類股排名",
        f"第 {peer_share['rank']}/{peer_share['total']} 名",
        help=peer_share["note"],
    )
    with m2:
        st.caption(peer_share["label"])
        share_pct = peer_share.get("share_pct", 0)
        if share_pct > 0:
            st.caption(f"同類股內佔 {share_pct:.1f}%")

    # Leader score — real formula
    m3.metric(
        "領頭羊指數",
        f"{leader['leader_score']} / 100",
        help="四維計算：同類股營收排名 + 營收成長 vs 同類股 + 毛利 vs 同類股 + 法人累積方向"
    )
    with m3:
        st.caption(leader["label"])
        details = leader.get("details", {})
        if details.get("own_growth") is not None:
            st.caption(f"自身成長：{details['own_growth']:.1f}%  |  "
                       f"同類股中位：{details.get('sector_median_growth', '—'):.1f}%")

    # Real chip data
    fpct = real_chip.get("foreign_pct") if real_chip else None
    m4.metric(
        "外資持股",
        f"{fpct}%" if fpct is not None else "—",
        help="來源：FinMind taiwan_stock_holding_shares_per" if real_chip and real_chip.get("source") == "finmind_real"
             else "流量累積估算值"
    )
    with m4:
        if real_chip:
            src_label = "✅ 官方數據" if real_chip["source"] == "finmind_real" else "⚠️ 估算值"
            st.caption(src_label)

    # Tech barrier detail expander
    with st.expander("🔬 技術門檻計算明細"):
        breakdown = tech_score.get("breakdown", {})
        bc1, bc2, bc3 = st.columns(3)
        bc1.metric("R&D 強度分", f"{breakdown.get('rd_intensity', 0):.1f} / 40",
                   help="R&D費用 / 營收 × 100，滿分40分")
        bc2.metric("毛利溢價分", f"{breakdown.get('margin_premium', 0):.1f} / 30",
                   help=f"自身毛利 vs 同類股均值 {sector_avg:.1f}%，滿分30分")
        bc3.metric("毛利穩定性", f"{breakdown.get('margin_stability', 0):.1f} / 30",
                   help="8季毛利標準差越小越高分，滿分30分")

    # Leader score detail expander
    with st.expander("🏆 領頭羊指數計算明細"):
        lb = leader.get("breakdown", {})
        ld = leader.get("details", {})
        lc1, lc2, lc3, lc4 = st.columns(4)
        lc1.metric("規模排名", f"{lb.get('size_rank', 0):.1f} / 25", help="同類股營收排名")
        lc2.metric("成長溢價", f"{lb.get('growth_premium', 0):.1f} / 25",
                   help=f"自身 {ld.get('own_growth', '—')}% vs 同類股 {ld.get('sector_median_growth', '—')}%")
        lc3.metric("毛利領導力", f"{lb.get('margin_leadership', 0):.1f} / 25",
                   help=f"自身 {ld.get('own_margin', '—')}% vs 同類股 {ld.get('sector_median_margin', '—')}%")
        lc4.metric("法人方向", f"{lb.get('inst_accumulation', 0):.1f} / 25",
                   help=f"近20日法人集中度 {ld.get('f_net_20d_conc', '—')}%")

    st.divider()

    # ── Row 3: AI diagnostic (qualitative only) ──
    st.subheader("🩺 AI 基本面定性診斷")
    st.caption("以下為 Gemini 定性分析，不含數字預測，僅作輔助解讀。")
    d1, d2, d3 = st.columns(3)
    with d1:
        if rev_accel["is_accelerating"]:
            st.success(f"🔥 {rev_accel['label']}")
        else:
            st.info(f"📊 {rev_accel['label']}")
    with d2:
        st.markdown("**💰 獲利結構**")
        st.caption(ai_data["diagnosis"]["margin_trend"])
    with d3:
        st.markdown("**🌟 成長展望**")
        st.caption(ai_data["diagnosis"]["growth_status"])
