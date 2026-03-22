"""tabs/stock/strategy.py — Tab 6: 策略卡 ★"""

import streamlit as st
from ui.cards import sms_gauge, render_strategy_card


def render(engine, sid: str, name: str, df, df_ml,
           sms_result: dict, coil_result: dict, hit_rate: float):

    st.subheader("⚡ AI 策略卡")
    st.caption("整合 AI 分析 + 法說 + 籌碼 + 回測勝率 → 輸出可直接執行的交易策略")

    col_card, col_meta = st.columns([1.4, 1], gap="large")
    curr_close = df["close"].iloc[-1]

    with col_meta:
        st.markdown("**📊 策略輸入摘要**")
        sms_gauge(sms_result["score"], sms_result["signal"])
        st.markdown("")
        with st.container(border=True):
            m1, m2 = st.columns(2)
            m1.metric("回測勝率", f"{hit_rate:.1%}")
            m2.metric("SMS 評分", f"{sms_result['score']:.0f}/100")
            m1.metric("52W 低",   f"{df['close'].tail(252).min():.1f}")
            m2.metric("52W 高",   f"{df['close'].tail(252).max():.1f}")
        st.markdown(f"**蓄力狀態：** {coil_result['label']}")
        if coil_result["is_coiling"]:
            st.warning(f"壓力位：{coil_result['key_resistance']}")

    with col_card:
        if st.button("🎯 生成策略卡", type="primary", use_container_width=True):
            with st.spinner("AI 正在制定策略..."):
                f_cost_val = (df_ml["f_cost"].iloc[-1]
                              if "f_cost" in df_ml.columns and not df_ml.empty
                              else curr_close)
                # Fetch mops for context
                mops = engine.fetch_latest_mops_pdf_info(sid)
                card = engine.get_strategy_card_ai(
                    sid, name,
                    curr_close,
                    df["close"].tail(252).min(),
                    df["close"].tail(252).max(),
                    f_cost_val,
                    sms_result["score"],
                    mops.get("event", ""),
                    hit_rate,
                )
            if card:
                st.session_state[f"card_{sid}"] = card
            else:
                st.warning("策略卡生成失敗，請確認 Gemini API Key。")

        card_data = st.session_state.get(f"card_{sid}")
        if card_data:
            render_strategy_card(card_data, curr_close)
        else:
            st.markdown("""
            <div style="border:2px dashed #e2e8f0;border-radius:12px;padding:40px;
                        text-align:center;color:#94a3b8;">
              點擊「生成策略卡」<br>AI 將整合所有分析，輸出具體進場 · 停損 · 目標 · 部位建議
            </div>
            """, unsafe_allow_html=True)
