"""
chip_radar/scanner.py — 📡 籌碼雷達（每日選股排行 + 巨鯨警示）

頁面結構：
  1. 市場行情總覽（VIX / 10Y / Mag7 / 板塊輪動）
  2. 市場情緒條（SPY P/C + Z-Score）
  3. 巨鯨動向卡片（whale_alert 股票）
  4. 週期選擇 + 分數排行表（含技術面 Entry/Exit 分數）
  5. 使用指南
"""

import json
import streamlit as st
import pandas as pd

from ._db import (
    load_latest_scores, load_market_pulse, load_universe, COMPOSITE_KEY,
    load_insider_trades, load_options_flow, load_large_holders,
    load_market_env, load_tech_signals,
)
from ._ui import (
    market_pulse_bar, market_env_block, whale_card, section_header,
    guide_box, score_color, options_flow_chart,
)

# VIX 自動折扣係數
_VIX_DISCOUNT = {"low": 1.0, "medium": 0.95, "high": 0.80}

# 技術指標中文標籤對應
_TECH_FLAG_LABELS = {
    "ma_aligned":        ("多頭排列", "20MA>50MA>200MA，收盤在 200MA 之上"),
    "rsi_reversal":      ("RSI 超賣回升", "RSI(14) 近 5 日曾 ≤ 35，現已回升至 ≥ 40"),
    "macd_golden":       ("MACD 金叉", "MACD 柱狀體由負轉正"),
    "bb_breakout":       ("布林突破", "收盤突破布林上軌，量能 > 20 日均量 1.5 倍"),
    "double_bottom":     ("雙重底", "近 30 根 K 線偵測到雙重底型態"),
    "vcp_pattern":       ("VCP 波動收斂", "近 20 根 K 線振幅逐步縮小"),
    "hard_stop":         ("硬性止損警示", "從近 20 日高點回落 > 7%"),
    "below_20ma_3d":     ("跌破 20MA ×3", "連續 3 日收盤低於 20MA"),
    "rsi_divergence":    ("頂背離", "RSI > 75 後掉頭 或 價格新高但 RSI 未創新高"),
    "atr_trailing_stop": ("ATR 止盈線", "價格跌破近期高點 − ATR(14)×2"),
}


def render():
    # ── 1. 市場行情總覽 ───────────────────────────────────────────
    section_header("🌐 市場行情", "VIX / 10Y 殖利率 / Mag7 乖離 / 板塊輪動")
    env = load_market_env()
    market_env_block(env)

    # VIX 折扣係數（供下方 Entry Score 調整用）
    vix_level   = env.get("vix_level", "medium") if env else "medium"
    vix_discount = _VIX_DISCOUNT.get(vix_level, 0.95)
    if vix_level == "high":
        vix_val = env.get("vix") or "?"
        st.warning(
            f"⚠️ VIX={vix_val} 超過 25，市場恐慌！"
            "系統已對所有進場分數套用 **0.8 折扣**，請謹慎評估部位大小。",
            icon=None,
        )

    # ── 2. 市場情緒條（SPY P/C Ratio）────────────────────────────
    pulse = load_market_pulse()
    market_pulse_bar(
        pc_ratio=pulse.get("pc_ratio"),
        z_score=pulse.get("pc_zscore_20"),
        date=pulse.get("date"),
    )

    # ── 載入分數資料 ──────────────────────────────────────────────
    df = load_latest_scores()
    universe = load_universe()

    col_hdr, col_cache = st.columns([8, 2])
    with col_cache:
        if st.button("🔄 重新載入", key="chip_clear_cache", use_container_width=True,
                     help="清除快取，重新從資料庫讀取最新資料"):
            st.cache_data.clear()
            st.rerun()

    if df.empty:
        st.info("尚無資料。請先執行 `python -m chip_module.fetch_daily` 抓取資料。")
        _render_empty_guide()
        return

    # 解析 signal_flags JSON
    df["flags"] = df["signal_flags"].apply(
        lambda x: json.loads(x) if x else []
    )

    # 載入技術面信號並合併
    tech_df = load_tech_signals()
    if not tech_df.empty:
        tech_cols = [
            "ticker", "entry_score", "exit_risk",
            "ma_aligned", "rsi_reversal", "macd_golden", "bb_breakout",
            "double_bottom", "vcp_pattern",
            "hard_stop", "below_20ma_3d", "rsi_divergence", "atr_trailing_stop",
            "rsi_14", "macd_hist", "vol_ratio",
        ]
        available = [c for c in tech_cols if c in tech_df.columns]
        df = df.merge(tech_df[available], on="ticker", how="left")

        # VIX 折扣調整 Entry Score
        if "entry_score" in df.columns:
            df["entry_score"] = (df["entry_score"] * vix_discount).round(1)
    else:
        for col in ["entry_score", "exit_risk"]:
            df[col] = None

    # ── 3. 巨鯨動向 ──────────────────────────────────────────────
    whales = df[df["whale_alert"] == 1]
    if not whales.empty:
        section_header("🐋 巨鯨動向", "近期偵測到異常籌碼活動的股票")

        timeframe = st.session_state.get("chip_timeframe", "波段 (1–4週)")
        comp_col  = COMPOSITE_KEY.get(timeframe, "composite_swing")

        cols = st.columns(min(len(whales), 4))
        for i, (_, row) in enumerate(whales.iterrows()):
            with cols[i % 4]:
                whale_card(
                    ticker=row["ticker"],
                    composite=row.get(comp_col) or 50,
                    flags=row["flags"],
                    entry=bool(row.get("entry_timing")),
                    price=None,
                )
                with st.expander(f"📋 {row['ticker']} 詳情"):
                    _render_whale_detail(row["ticker"])

        st.markdown("<div style='margin-bottom:8px;'></div>", unsafe_allow_html=True)

    # ── 4. 週期 + 欄位 + 板塊篩選 ────────────────────────────────
    with col_hdr:
        section_header("📋 籌碼 × 技術分數排行")

    col_ctrl1, col_ctrl2, col_ctrl3 = st.columns([2, 3, 3])
    with col_ctrl1:
        timeframe = st.selectbox(
            "持倉週期",
            list(COMPOSITE_KEY.keys()),
            index=1,
            key="chip_timeframe",
            help="選擇你的預期持倉週期，分數排序會隨之改變",
        )
    with col_ctrl2:
        show_cols = st.multiselect(
            "顯示欄位",
            ["內部人", "空頭", "量能", "選擇權流", "機構"],
            default=["內部人", "空頭", "量能", "選擇權流"],
            help="選擇要顯示的信號維度",
        )
    with col_ctrl3:
        all_sectors = sorted({v.get("sector", "Unknown") for v in universe.values()}) if universe else []
        selected_sectors = st.multiselect(
            "板塊篩選",
            all_sectors,
            default=[],
            key="chip_sector_filter",
            help="留空 = 顯示全部；選擇板塊後只顯示該板塊股票",
        )

    comp_col = COMPOSITE_KEY[timeframe]

    # ── 建立顯示用 DataFrame ──────────────────────────────────────
    col_map = {
        "內部人":   "insider_score",
        "空頭":     "short_score",
        "量能":     "volume_score",
        "選擇權流": "options_flow_score",
        "機構":     "institutional_score",
    }
    display_cols = [comp_col] + [col_map[c] for c in show_cols if c in col_map]

    # 技術面欄位（若有資料則加入）
    tech_display = []
    if "entry_score" in df.columns and df["entry_score"].notna().any():
        tech_display += ["entry_score", "exit_risk"]

    base_cols = ["ticker", "date"] + display_cols + tech_display + ["whale_alert", "entry_timing", "flags"]
    # 技術旗標欄也帶入（用於 expander 詳情，不顯示在主表）
    flag_cols = [c for c in [
        "ma_aligned", "rsi_reversal", "macd_golden", "bb_breakout",
        "double_bottom", "vcp_pattern",
        "hard_stop", "below_20ma_3d", "rsi_divergence", "atr_trailing_stop",
        "rsi_14", "macd_hist", "vol_ratio",
    ] if c in df.columns]

    display = df[base_cols + flag_cols].copy()

    # 板塊篩選
    if selected_sectors and universe:
        in_sector = {t for t, info in universe.items() if info.get("sector") in selected_sectors}
        display = display[display["ticker"].isin(in_sector)].copy()

    display = display.sort_values(comp_col, ascending=False).reset_index(drop=True)

    # 日增減 delta
    from datetime import date as _date, timedelta
    yesterday = (_date.today() - timedelta(days=1)).isoformat()
    df_prev = load_latest_scores(as_of=yesterday)
    if not df_prev.empty:
        prev_map = df_prev.set_index("ticker")[comp_col].to_dict()
        def _delta(row):
            prev = prev_map.get(row["ticker"])
            if prev is None:
                return ""
            d = (row.get(comp_col) or 0) - prev
            return f"▲{d:.0f}" if d > 0.5 else (f"▼{abs(d):.0f}" if d < -0.5 else "—")
        display["日增減"] = [_delta(r) for _, r in display.iterrows()]
    else:
        display["日增減"] = ""

    # 信號 badge
    _BADGE_MAP = {
        "insider_cluster":    "insider ↑",
        "insider_selling":    "insider ↓",
        "unusual_options":    "options 異常",
        "volume_accumulation":"量能↑",
        "volume_distribution":"量能↓",
        "high_short_interest":"空頭高",
    }

    def fmt_flags(row):
        badges = []
        if row.get("whale_alert"):
            badges.append("🐋")
        if row.get("entry_timing"):
            badges.append("⚡")
        flags = row.get("flags", [])
        if not isinstance(flags, list):
            flags = []
        for f in flags:
            label = _BADGE_MAP.get(f)
            if label and label not in badges:
                badges.append(label)
        return "  ".join(badges)

    display["信號"] = [fmt_flags(r) for _, r in display.iterrows()]

    rename = {
        "ticker":              "股票",
        "date":                "更新日",
        comp_col:              f"綜合({timeframe[:2]})",
        "insider_score":       "內部人",
        "short_score":         "空頭",
        "volume_score":        "量能",
        "options_flow_score":  "選擇權流",
        "institutional_score": "機構",
        "entry_score":         "進場分",
        "exit_risk":           "出場風險",
    }

    table_cols = ["ticker", "date"] + display_cols + tech_display + ["日增減", "信號"]
    table = display[table_cols].rename(columns=rename)

    score_display_cols = [rename.get(c, c) for c in display_cols + tech_display]

    def color_score_cell(val):
        try:
            v = float(val)
        except (TypeError, ValueError):
            return ""
        color = score_color(v)
        bg = color + "12"
        return f"color:{color};font-weight:700;background:{bg};"

    styled = (
        table.style
        .applymap(color_score_cell, subset=score_display_cols)
        .format({c: "{:.0f}" for c in score_display_cols}, na_rep="—")
        .set_properties(**{"font-size": "13px", "text-align": "center"})
        .set_properties(subset=["股票"], **{"font-weight": "700", "text-align": "left"})
    )

    st.dataframe(styled, use_container_width=True, height=420)

    # ── 5. 點選查看詳情（含技術指標展開）──────────────────────────
    st.markdown("<div style='margin-top:4px;'></div>", unsafe_allow_html=True)
    selected = st.selectbox(
        "🔬 點選股票查看詳情",
        ["—"] + list(df["ticker"].unique()),
        key="chip_selected_ticker",
        help="選擇後在下方展開內部人、選擇權流、大戶持股、技術指標明細",
    )
    if selected != "—":
        st.session_state["chip_dive_ticker"] = selected
        t1, t2, t3, t4 = st.tabs([
            "👤 Form 4 內部人交易", "⚙️ 選擇權流量",
            "🏛️ 大戶持股 13D/13G", "📐 技術指標明細",
        ])
        with t1:
            _render_insider_detail(selected)
        with t2:
            _render_options_detail(selected)
        with t3:
            _render_holders_detail(selected)
        with t4:
            _render_tech_detail(selected, display, flag_cols)

    # ── 6. 使用指南 ──────────────────────────────────────────────
    with st.expander("? 如何解讀這張表格"):
        guide_box([
            "<b>市場行情區塊</b>：VIX > 25 時系統自動對所有進場分數套用 0.8 折扣，請縮小部位",
            "<b>進場分 (Entry Score)</b>：0–100，整合多頭排列 / RSI 超賣回升 / MACD 金叉 / 布林突破 / 型態分析",
            "<b>出場風險 (Exit Risk)</b>：0–100，整合止損警示 / 跌破 20MA / 頂背離 / ATR 止盈線",
            "<b>技術指標明細</b>：點選個股後切換到「📐 技術指標明細」標籤，查看每個條件的達標狀態與原始數值",
            "<b>綜合分數</b> 0–100：整合多個籌碼維度的加權分數，數字越高代表該週期看多信號越強",
            "<b>分數顏色</b>：🔵 75+ 強烈看多 ／ 🟢 55–74 偏多 ／ 🟡 35–54 中性 ／ 🔴 35以下 偏空",
            "<b>🐋 巨鯨動向</b>：內部人 Cluster 買入、13D 大戶進場、個股選擇權異常大單（任一觸發）",
            "<b>⚡ 進場時機</b>：巨鯨信號 + 量能確認 + 空頭壓力低，三條件同時滿足才觸發",
            "資料每日台灣時間 23:30 更新，反映美股當日收盤資料",
        ])


def _render_tech_detail(ticker: str, display: pd.DataFrame, flag_cols: list):
    """展示某股票的技術指標達標明細（讓 Entry/Exit Score 不是黑盒子）"""
    rows = display[display["ticker"] == ticker]
    if rows.empty:
        st.caption("無技術面信號資料")
        return

    row = rows.iloc[0]
    entry_score = row.get("entry_score")
    exit_risk   = row.get("exit_risk")

    # 分數卡
    c1, c2 = st.columns(2)
    with c1:
        es_val  = f"{entry_score:.0f}" if pd.notna(entry_score) else "—"
        es_color = score_color(entry_score) if pd.notna(entry_score) else "#94a3b8"
        st.markdown(f"""
        <div style="background:white;border:1px solid #e2e8f0;border-top:3px solid {es_color};
                    border-radius:10px;padding:14px 16px;text-align:center;">
          <div style="color:#64748b;font-size:0.7em;font-weight:600;">📈 進場信心分數</div>
          <div style="font-size:2.5em;font-weight:900;color:{es_color};">{es_val}</div>
          <div style="color:#94a3b8;font-size:0.72em;">/100（VIX 已折扣）</div>
        </div>
        """, unsafe_allow_html=True)
    with c2:
        er_val   = f"{exit_risk:.0f}" if pd.notna(exit_risk) else "—"
        er_color = score_color(100 - exit_risk) if pd.notna(exit_risk) else "#94a3b8"
        st.markdown(f"""
        <div style="background:white;border:1px solid #e2e8f0;border-top:3px solid {er_color};
                    border-radius:10px;padding:14px 16px;text-align:center;">
          <div style="color:#64748b;font-size:0.7em;font-weight:600;">📉 出場風險分數</div>
          <div style="font-size:2.5em;font-weight:900;color:{er_color};">{er_val}</div>
          <div style="color:#94a3b8;font-size:0.72em;">/100（越高越需要注意出場）</div>
        </div>
        """, unsafe_allow_html=True)

    st.markdown("<div style='margin:10px 0 6px;font-weight:700;color:#1e293b;'>📐 各指標達標狀況</div>",
                unsafe_allow_html=True)

    # 進場指標
    entry_flags = ["ma_aligned", "rsi_reversal", "macd_golden", "bb_breakout", "double_bottom", "vcp_pattern"]
    exit_flags  = ["hard_stop", "below_20ma_3d", "rsi_divergence", "atr_trailing_stop"]

    def _flag_row(flag: str, value, section: str):
        label, desc = _TECH_FLAG_LABELS.get(flag, (flag, ""))
        if value is None or pd.isna(value):
            icon, color, bg = "⚪", "#94a3b8", "#f8fafc"
        elif int(value) == 1:
            icon = "✅" if section == "entry" else "⚠️"
            color, bg = ("#10b981", "#f0fdf4") if section == "entry" else ("#ef4444", "#fef2f2")
        else:
            icon, color, bg = "❌", "#94a3b8", "#f8fafc"
        return (
            f'<div style="display:flex;align-items:flex-start;gap:10px;padding:6px 10px;'
            f'background:{bg};border-radius:8px;margin-bottom:4px;">'
            f'<span style="font-size:1em;">{icon}</span>'
            f'<div><div style="font-weight:600;font-size:0.82em;color:{color};">{label}</div>'
            f'<div style="font-size:0.72em;color:#94a3b8;">{desc}</div></div></div>'
        )

    ec1, ec2 = st.columns(2)
    with ec1:
        st.markdown("**進場條件**", unsafe_allow_html=False)
        html = "".join(_flag_row(f, row.get(f), "entry") for f in entry_flags if f in flag_cols)
        if html:
            st.markdown(html, unsafe_allow_html=True)
        else:
            st.caption("資料不足")
    with ec2:
        st.markdown("**出場警示**", unsafe_allow_html=False)
        html = "".join(_flag_row(f, row.get(f), "exit") for f in exit_flags if f in flag_cols)
        if html:
            st.markdown(html, unsafe_allow_html=True)
        else:
            st.caption("資料不足")

    # 原始指標數值表
    raw_map = {
        "rsi_14":   ("RSI(14)",            lambda v: f"{v:.1f}"),
        "macd_hist":("MACD 柱狀體",        lambda v: f"{v:+.3f}"),
        "vol_ratio":("量能倍數(vs 20MA)",  lambda v: f"{v:.2f}x"),
    }
    raw_rows = []
    for col, (label, fmt) in raw_map.items():
        val = row.get(col)
        raw_rows.append({
            "指標": label,
            "數值": fmt(float(val)) if val is not None and pd.notna(val) else "—",
        })
    for col in ["ma_20", "ma_50", "ma_200", "bb_upper", "bb_lower", "atr_14"]:
        if col in row.index and pd.notna(row.get(col)):
            labels = {
                "ma_20": "MA20", "ma_50": "MA50", "ma_200": "MA200",
                "bb_upper": "布林上軌", "bb_lower": "布林下軌", "atr_14": "ATR(14)",
            }
            raw_rows.append({"指標": labels[col], "數值": f"{float(row[col]):.2f}"})

    if raw_rows:
        st.markdown("<div style='margin-top:10px;font-size:0.78em;color:#64748b;font-weight:600;'>原始指標數值</div>",
                    unsafe_allow_html=True)
        st.dataframe(pd.DataFrame(raw_rows), use_container_width=True, hide_index=True, height=220)


def _render_whale_detail(ticker: str):
    t1, t2, t3 = st.tabs(["👤 內部人", "⚙️ 選擇權", "🏛️ 大戶"])
    with t1:
        _render_insider_detail(ticker)
    with t2:
        _render_options_detail(ticker)
    with t3:
        _render_holders_detail(ticker)


def _render_insider_detail(ticker: str):
    df = load_insider_trades(ticker)
    if df.empty:
        st.caption("近 90 天無內部人公開市場買賣記錄")
        return
    df = df.copy()
    df["total_value"] = df["total_value"].apply(
        lambda x: f"${x:,.0f}" if pd.notna(x) and x else "—"
    )
    df["shares"] = df["shares"].apply(
        lambda x: f"{x:,.0f}" if pd.notna(x) and x else "—"
    )
    df["price_per_share"] = df["price_per_share"].apply(
        lambda x: f"${x:.2f}" if pd.notna(x) and x else "—"
    )
    df["transaction_type"] = df["transaction_type"].map(
        {"P": "🟢 買入", "S": "🔴 賣出"}
    ).fillna(df["transaction_type"])
    df = df.rename(columns={
        "trade_date": "交易日", "insider_name": "內部人",
        "insider_title": "職位", "transaction_type": "類型",
        "shares": "股數", "price_per_share": "均價", "total_value": "總金額",
    })
    st.dataframe(df, use_container_width=True, hide_index=True, height=240)


def _render_options_detail(ticker: str):
    df = load_options_flow(ticker)
    if df.empty:
        st.caption("選擇權流量資料累積中（至少需要 1 天資料）")
        return
    st.plotly_chart(options_flow_chart(df), use_container_width=True)
    latest = df.iloc[-1]
    c1, c2, c3 = st.columns(3)
    with c1:
        cp = latest["call_volume"] / (latest["put_volume"] + 1e-9)
        st.metric("Call/Put 比", f"{cp:.2f}")
    with c2:
        total = latest["call_volume"] or 0
        otm   = latest["otm_call_volume"] or 0
        st.metric("OTM Call 佔比", f"{otm/total*100:.1f}%" if total else "—")
    with c3:
        st.metric("異常 Call Strike 數", f"{latest.get('unusual_call_strikes', 0):.0f}")


def _render_holders_detail(ticker: str):
    df = load_large_holders(ticker)
    if df.empty:
        st.caption("近期無 SC 13D/13G 大戶持股申報（持股 > 5% 才觸發）")
        return
    def fmt_form(v):
        if "13D" in str(v) and "/A" not in str(v):
            return f"🔴 {v}（主動持股）"
        if "13D/A" in str(v):
            return f"🟡 {v}（修正）"
        if "13G" in str(v):
            return f"🔵 {v}（被動持股）"
        return v
    df = df.rename(columns={
        "filed_date": "申報日", "form_type": "表單類型", "filer_name": "申報機構"
    })
    df["表單類型"] = df["表單類型"].apply(fmt_form)
    st.dataframe(df, use_container_width=True, hide_index=True)


def _render_empty_guide():
    with st.expander("? 快速開始"):
        guide_box([
            "在 stock_track 目錄執行：<code>python -m chip_module.fetch_daily --tickers NVDA AAPL TSLA MSFT AMD</code>",
            "首次執行約需 2–3 分鐘，之後每日排程自動更新",
            "個股選擇權歷史需累積 3 天以上，異常偵測才會啟用 Z-Score 比較",
        ])
