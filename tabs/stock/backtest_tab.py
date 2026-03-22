"""tabs/stock/backtest_tab.py — Tab 7: 回測室 ★"""

import streamlit as st
import pandas as pd
import numpy as np
import plotly.graph_objects as go
from plotly.subplots import make_subplots

from engine.backtest import (
    run_vbt_backtest, extract_vbt_stats, get_equity_curve,
    get_trade_log, simulate_signals_loop, compute_sharpe_beta, volume_profile, HAS_VBT
)
from config import COLORS, PLOTLY_BASE


def _prepare_signals(df_ml: pd.DataFrame) -> pd.DataFrame:
    """
    Add KD, MA, and SHIFT-CORRECTED buy/exit signals to df_ml.
    .shift(1) is applied here — this is the single source of truth for lookahead prevention.
    """
    df = df_ml.copy().sort_values("date").reset_index(drop=True)

    df["ma5"]  = df["close"].rolling(5).mean()
    df["ma10"] = df["close"].rolling(10).mean()

    hc = "high" if "high" in df.columns else "max"
    lc = "low"  if "low"  in df.columns else "min"
    low_9  = df[lc].rolling(9).min()
    high_9 = df[hc].rolling(9).max()
    rsv = (df["close"] - low_9) / (high_9 - low_9 + 1e-9) * 100
    df["k"] = rsv.ewm(com=2).mean()
    df["d"] = df["k"].ewm(com=2).mean()

    raw_entry = (
        (df["pred_potential"] > 0.10) &
        (df["close"] > df["ma20"]) &
        (df["k"] > df["d"])
    )
    raw_exit = (
        (df["pred_potential"] < 0.02) |
        (df["close"] < df["ma20"]) |
        (df["k"] < df["d"])
    )
    # CRITICAL: shift(1) — signal fires next bar, not current bar
    df["buy_cond"]  = raw_entry.shift(1).fillna(False)
    df["exit_cond"] = raw_exit.shift(1).fillna(False)
    return df


def render(df_ml: pd.DataFrame, hit_rate: float):
    st.subheader("🔬 策略回測室")

    df = _prepare_signals(df_ml)
    buy_idx, exit_idx = simulate_signals_loop(df)
    sharpe, beta      = compute_sharpe_beta(df)
    v_counts, v_prices = volume_profile(df)
    poc_price          = v_prices[np.argmax(v_counts)]

    # ── Metrics row ──
    with st.container(border=True):
        c1, c2, c3, c4, c5 = st.columns(5)
        c1.metric("回測勝率",     f"{hit_rate:.1%}",
                  help="AI 預測潛力 > 10% 且 20日後真的漲超 10% 的歷史機率")
        c2.metric("夏普比率",     f"{sharpe:.2f}",
                  delta="優" if sharpe > 1 else "一般")
        c3.metric("Beta",         f"{beta:.2f}",
                  delta="低波動" if beta < 1 else "高波動", delta_color="inverse")
        c4.metric("籌碼密集區",   f"{poc_price:.1f}")
        c5.metric("模擬交易次數", str(len(buy_idx)))

    # ── vectorbt (if available) ──
    if HAS_VBT and len(buy_idx) > 0:
        try:
            df_indexed = df.set_index("date")
            pf = run_vbt_backtest(
                df_indexed,
                pd.Series(df["buy_cond"].values,  index=df_indexed.index),
                pd.Series(df["exit_cond"].values, index=df_indexed.index),
            )
            if pf:
                st.session_state["last_backtest_pf"] = pf
                stats = extract_vbt_stats(pf)
                st.markdown("#### 📊 vectorbt 精確回測結果")
                vcols = st.columns(6)
                for col, (k, v) in zip(vcols, stats.items()):
                    col.metric(k, str(v))

                eq = get_equity_curve(pf)
                fig_eq = go.Figure()
                fig_eq.add_trace(go.Scatter(
                    x=eq.index, y=eq,
                    line=dict(color=COLORS["equity"], width=2),
                    fill="tozeroy", fillcolor="rgba(16,185,129,0.08)", name="策略",
                ))
                fig_eq.update_layout(**PLOTLY_BASE, height=260, yaxis_title="資產淨值 (NT$)")
                st.plotly_chart(fig_eq, use_container_width=True)

                with st.expander("📋 交易記錄"):
                    st.dataframe(get_trade_log(pf), use_container_width=True)
        except Exception as e:
            st.info(f"vectorbt 異常：{e}，顯示簡化版結果。")

    # ── Chart tabs ──
    tab_c1, tab_c2 = st.tabs(["📈 戰術與訊號分析", "📊 結構與壓力支撐"])

    with tab_c1:
        fig1 = make_subplots(rows=3, cols=1, shared_xaxes=True,
                             vertical_spacing=0.03, row_heights=[0.5, 0.25, 0.25],
                             subplot_titles=("價格路徑與 AI 訊號", "KD 動能", "法人籌碼流向"))
        fig1.add_trace(go.Scatter(x=df["date"], y=df["close"], name="現價",
                                  line=dict(color="#1e293b", width=2)), row=1, col=1)
        for col_name, color, dash in [
            ("ma20", COLORS["ma20"], "dash"),
            ("ma10", COLORS["ma10"], "dot"),
            ("ma5",  COLORS["ma5"],  "dot"),
        ]:
            if col_name in df.columns:
                fig1.add_trace(go.Scatter(x=df["date"], y=df[col_name], name=col_name.upper(),
                                          line=dict(color=color, width=1.2, dash=dash)), row=1, col=1)

        if buy_idx:
            fig1.add_trace(go.Scatter(
                x=df["date"].iloc[buy_idx], y=df["close"].iloc[buy_idx] * 0.97,
                mode="markers", name="進場",
                marker=dict(symbol="triangle-up", size=11, color=COLORS["buy_signal"])), row=1, col=1)
        if exit_idx:
            fig1.add_trace(go.Scatter(
                x=df["date"].iloc[exit_idx], y=df["close"].iloc[exit_idx] * 1.03,
                mode="markers", name="出場",
                marker=dict(symbol="triangle-down", size=11, color=COLORS["sell_signal"])), row=1, col=1)

        fig1.add_trace(go.Scatter(x=df["date"], y=df["k"], name="K",
                                  line=dict(color="#f59e0b", width=1.2)), row=2, col=1)
        fig1.add_trace(go.Scatter(x=df["date"], y=df["d"], name="D",
                                  line=dict(color="#03e3fc", width=1.2)), row=2, col=1)
        fig1.add_hrect(y0=80, y1=100, fillcolor="rgba(244,63,94,0.1)",  line_width=0, row=2, col=1)
        fig1.add_hrect(y0=0,  y1=20,  fillcolor="rgba(16,185,129,0.1)", line_width=0, row=2, col=1)

        if "f_net" in df.columns and "it_net" in df.columns:
            inst_net = df["f_net"] + df["it_net"]
            colors_b = [COLORS["buy_signal"] if v > 0 else COLORS["sell_signal"] for v in inst_net]
            fig1.add_trace(go.Bar(x=df["date"], y=inst_net, name="法人合買",
                                  marker_color=colors_b, opacity=0.7), row=3, col=1)

        fig1.update_layout(template="plotly_white", height=780,
                           margin=dict(t=30, b=0, l=0, r=0),
                           hovermode="x unified",
                           paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)")
        st.plotly_chart(fig1, use_container_width=True)

    with tab_c2:
        fig2 = make_subplots(rows=1, cols=2, shared_yaxes=True,
                             column_widths=[0.8, 0.2], horizontal_spacing=0.01)
        fig2.add_trace(go.Scatter(x=df["date"], y=df["close"],
                                  name="價格", line=dict(color="#0f172a")), row=1, col=1)
        fig2.add_hline(y=poc_price, line_dash="dash", line_color="#f43f5e",
                       annotation_text=f"籌碼密集區: {poc_price:.1f}", row=1, col=1)
        fig2.add_trace(go.Bar(x=v_counts, y=v_prices, orientation="h",
                              marker_color="#94a3b8", opacity=0.4, name="成交量分佈"), row=1, col=2)
        fig2.update_layout(template="plotly_white", height=480,
                           showlegend=False, margin=dict(t=10, b=0, l=0, r=0),
                           paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)")
        st.plotly_chart(fig2, use_container_width=True)

    # ── Notes ──
    st.markdown("#### 📓 分析師投資筆記")
    n1, n2 = st.columns(2)
    pred_val = df["pred_potential"].iloc[-1] if "pred_potential" in df.columns else 0
    k_val    = df["k"].iloc[-1] if "k" in df.columns else 50
    with n1:
        st.info(f"""
**策略摘要**
- AI 潛力預估：`{pred_val*100:.1f}%`
- K 值：`{k_val:.1f}` — {"強勢格局" if k_val > 50 else "偏弱震盪"}
- ✅ 已套用 `.shift(1)` 防止 lookahead bias
        """)
    with n2:
        st.success(f"""
**風險評估**
- 籌碼密集區：`{poc_price:.1f}`（支撐/壓力參考）
- Beta：`{beta:.2f}` — {"獨立行情" if beta < 0.8 else "跟隨大盤"}
        """)
