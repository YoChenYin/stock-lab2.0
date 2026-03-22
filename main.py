"""
main.py — application entry point and page router

Run with:  streamlit run main.py

Each page/tab is fully self-contained in its own module.
Add a new page by: (1) creating tabs/yourpage.py with a render(engine) function,
                   (2) adding it to the nav radio below.
"""

import streamlit as st
import xgboost as xgb
import numpy as np

from engine.wall_street_engine import WallStreetEngine
from engine.smart_money import calc_smart_money_score, calc_revenue_accel_score
from engine.rocket_detector import detect_coiling
from engine.scheduler import show_staleness_banner
from sector_data import STOCK_POOL, SECTOR_GROUPS

import tabs.screener   as screener_page
import tabs.heatmap    as heatmap_page
import tabs.guide      as guide_page
from tabs.stock import overview, financials, outlook, chips, valuation, strategy, backtest_tab


st.set_page_config(
    page_title="Stock Lab",
    page_icon="📈",
    layout="wide",
    initial_sidebar_state="expanded",
)

st.markdown("""
<style>
  .stTabs [data-baseweb="tab"] { font-size: 14px; font-weight: 500; }
  .block-container { padding-top: 1.2rem; }
  [data-testid="stMetricValue"] { font-size: 1.6em; }
</style>
""", unsafe_allow_html=True)

# ── Session state defaults ──
for k, v in [("selected_stock", None), ("last_backtest_pf", None), ("alert_queue", [])]:
    if k not in st.session_state:
        st.session_state[k] = v

# ── Sidebar ──
with st.sidebar:
    st.title("📈 Stock Lab")
    st.divider()
    nav = st.radio("功能", ["📖 使用指南", "1. 選股", "2. 族群資金熱圖", "3. 個股"])

engine = WallStreetEngine()

# Show stale-data banner after 14:30 Taipei on weekdays
show_staleness_banner()

# ── Page routing ──
if nav == "📖 使用指南":
    guide_page.render()

elif nav == "1. 選股":
    screener_page.render(engine)

elif nav == "2. 族群資金熱圖":
    heatmap_page.render(engine)

elif nav == "3. 個股":
    sid = st.sidebar.selectbox(
        "搜尋標的", list(STOCK_POOL.keys()),
        format_func=lambda x: f"{x} {STOCK_POOL[x]}",
    )
    name = STOCK_POOL[sid]

    # ── Shared data fetched once, passed to all tabs ──
    df, rev = engine.fetch_data(sid)
    if df.empty:
        st.warning("資料載入中，請稍候...")
        st.stop()

    with st.spinner("🤖 掃描五年籌碼規律並計算買賣訊號..."):
        df_ml = engine.fetch_ml_ready_data(sid)
        if not df_ml.empty:
            model = xgb.XGBRegressor(n_estimators=100, learning_rate=0.05,
                                      max_depth=5, random_state=42)
            features  = ["ma5", "ma20", "bias_f_cost", "conc", "f_streak", "volatility"]
            import numpy as np
            clean = df_ml.replace([np.inf, -np.inf], np.nan).dropna(
                subset=features + ["target_max_ret"])
            if len(clean) >= 100:
                model.fit(clean[features], clean["target_max_ret"])
                df_ml["pred_potential"] = model.predict(df_ml[features].fillna(0))
                high_conf = clean[model.predict(clean[features]) > 0.10]
                hit_rate  = (high_conf["target_max_ret"] > 0.10).mean() if not high_conf.empty else 0
            else:
                df_ml["pred_potential"] = 0
                hit_rate = 0
        else:
            st.error("歷史資料不足。")
            st.stop()

    # Pre-compute shared signals
    sms_result  = calc_smart_money_score(df) if "f_net" in df.columns else {"score": 0, "signal": "—", "breakdown": {}}
    coil_result = detect_coiling(df)
    rev_accel   = calc_revenue_accel_score(rev)
    real_chip   = engine.fetch_real_chip_data(sid)

    # Header
    curr        = df.iloc[-1]
    prev        = df.iloc[-2]
    change      = curr["close"] - prev["close"]
    change_pct  = (change / prev["close"]) * 100
    update_date = df["date"].iloc[-1].strftime("%Y/%m/%d")

    with st.spinner(f"AI 分析 {name}..."):
        ai_data = engine.get_ai_dashboard_data(sid, name, df, rev, real_chip)
    if not ai_data:
        st.error("AI 分析失敗，請檢查 Gemini API Key。")
        st.stop()

    from ui.cards import stock_header
    stock_header(sid, name, curr["close"], change, change_pct, update_date,
                 ai_data["header"]["category"], coil_result["label"], rev_accel["label"])

    # ── Tab router ──
    t1, t2, t3, t4, t5, t6, t7 = st.tabs([
        "1. 公司概況", "2. 財報分析", "3. 法說展望",
        "4. 籌碼面",   "5. 估值位置",
        "6. 策略卡 ★", "7. 回測室 ★",
    ])

    # Find which sector this stock belongs to (for peer comparisons)
    sector_stocks = []
    for sector, sids in SECTOR_GROUPS.items():
        if sid in sids:
            sector_stocks = sids
            break
    if not sector_stocks:
        sector_stocks = [sid]  # fallback: solo

    with t1: overview.render(
        ai_data, sms_result, rev_accel,
        fin_df=engine.fetch_quarterly_financials(sid),
        df_chip=df,
        real_chip=real_chip,
        engine=engine,
        sid=sid,
        sector_stocks=sector_stocks,
    )
    with t2: financials.render(engine, sid)
    with t3: outlook.render(engine, sid, name)
    with t4: chips.render(engine, sid, name, df, real_chip)
    with t5: valuation.render(ai_data, df, curr["close"])
    with t6: strategy.render(engine, sid, name, df, df_ml, sms_result, coil_result, hit_rate)
    with t7: backtest_tab.render(df_ml, hit_rate)
