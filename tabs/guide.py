"""
tabs/guide.py — 系統操作引導

互動式使用手冊，分兩條路徑：
  - 長期投資：財報品質 → 護城河 → 估值時機 → 持倉管理
  - 波段投資：蓄力掃描 → 籌碼確認 → 策略卡執行 → 回測驗證

所有說明基於平台的真實數據邏輯，並標明 AI 估算 vs 真實數字的邊界。
"""

import streamlit as st


# ─────────────────────────────────────────────────────────
# Shared helpers
# ─────────────────────────────────────────────────────────

def _badge(text: str, style: str = "both") -> str:
    colors = {
        "long":  ("background:#eff6ff;color:#1d4ed8;", "長期"),
        "swing": ("background:#f0fdf4;color:#15803d;", "波段"),
        "both":  ("background:#faf5ff;color:#6b21a8;", "共用"),
        "real":  ("background:#f0fdf4;color:#15803d;", "真實數字"),
        "ai":    ("background:#fefce8;color:#854d0e;", "AI估算"),
        "warn":  ("background:#fff7ed;color:#9a3412;", "注意"),
    }
    css, label = colors.get(style, colors["both"])
    return (f'<span style="{css}padding:2px 8px;border-radius:4px;'
            f'font-size:0.72em;font-weight:600;">{text or label}</span>')


def _section_header(icon: str, title: str, subtitle: str = ""):
    st.markdown(f"""
    <div style="padding:16px 0 8px;">
      <div style="font-size:1.1em;font-weight:600;color:var(--color-text-primary,#0f172a);">
        {icon} {title}</div>
      {"<div style='font-size:0.83em;color:#64748b;margin-top:2px;'>" + subtitle + "</div>" if subtitle else ""}
    </div>
    """, unsafe_allow_html=True)


def _step_card(num: int, title: str, body: str, badge_style: str = "both",
               badge_text: str = "", tip: str = "", signal_rule: str = ""):
    badge_html = _badge(badge_text or None, badge_style)
    tip_html = (
        f'<div style="background:#f0fdf4;border-left:3px solid #10b981;'
        f'padding:8px 12px;border-radius:0 6px 6px 0;font-size:0.8em;color:#166534;margin-top:8px;">'
        f'💡 {tip}</div>'
    ) if tip else ""
    rule_html = (
        f'<div style="background:#fff7ed;border-left:3px solid #f59e0b;'
        f'padding:8px 12px;border-radius:0 6px 6px 0;font-size:0.8em;color:#92400e;margin-top:6px;">'
        f'📐 操作規則：{signal_rule}</div>'
    ) if signal_rule else ""

    st.markdown(f"""
    <div style="border:1px solid #e2e8f0;border-radius:10px;padding:14px 16px;margin-bottom:10px;">
      <div style="display:flex;align-items:flex-start;gap:12px;">
        <div style="background:#1e293b;color:white;border-radius:6px;padding:3px 8px;
                    font-size:0.78em;font-weight:700;flex-shrink:0;margin-top:1px;">
          步驟 {num}</div>
        <div style="flex:1;">
          <div style="font-size:0.95em;font-weight:600;color:#1e293b;margin-bottom:4px;">
            {title} {badge_html}</div>
          <div style="font-size:0.83em;color:#475569;line-height:1.6;">{body}</div>
          {tip_html}
          {rule_html}
        </div>
      </div>
    </div>
    """, unsafe_allow_html=True)


def _signal_legend():
    st.markdown("""
    <div style="display:grid;grid-template-columns:repeat(4,1fr);gap:8px;margin:12px 0;">
      <div style="background:#f0fdf4;border-radius:8px;padding:10px;text-align:center;">
        <div style="font-size:1.2em;">🟢</div>
        <div style="font-size:0.72em;font-weight:600;color:#15803d;margin-top:4px;">SMS ≥ 75</div>
        <div style="font-size:0.68em;color:#166534;">強力買入</div>
      </div>
      <div style="background:#fefce8;border-radius:8px;padding:10px;text-align:center;">
        <div style="font-size:1.2em;">🟡</div>
        <div style="font-size:0.72em;font-weight:600;color:#854d0e;margin-top:4px;">SMS 50–74</div>
        <div style="font-size:0.68em;color:#92400e;">法人關注</div>
      </div>
      <div style="background:#fff7ed;border-radius:8px;padding:10px;text-align:center;">
        <div style="font-size:1.2em;">🟠</div>
        <div style="font-size:0.72em;font-weight:600;color:#9a3412;margin-top:4px;">SMS 25–49</div>
        <div style="font-size:0.68em;color:#9a3412;">觀望</div>
      </div>
      <div style="background:#fff1f2;border-radius:8px;padding:10px;text-align:center;">
        <div style="font-size:1.2em;">🔴</div>
        <div style="font-size:0.72em;font-weight:600;color:#b91c1c;margin-top:4px;">SMS &lt; 25</div>
        <div style="font-size:0.68em;color:#991b1b;">法人退場</div>
      </div>
    </div>
    """, unsafe_allow_html=True)


def _data_truth_table():
    rows = [
        ("OHLCV 股價、成交量", "FinMind 交易所", "real", "✅ 固定"),
        ("法人買賣超 f_net / it_net", "FinMind 法人申報", "real", "✅ 固定"),
        ("季度 EPS、毛利率", "FinMind 財報", "real", "✅ 固定（每季更新）"),
        ("月營收", "FinMind 公告", "real", "✅ 固定（每月 10 日後）"),
        ("SMS 評分 / 領頭羊指數 / tech_pr", "Python 計算", "real", "✅ 固定（同輸入同輸出）"),
        ("外資持股 %", "FinMind 持股表", "real", "✅ 固定（API 可取得時）"),
        ("法說摘要文字", "MOPS 官方爬蟲", "real", "✅ 固定（同份文件相同結果）"),
        ("business_model / moat 描述", "Gemini 定性分析", "ai", "⚠️ 措辭輕微變動"),
        ("法說分析 — 摘要豐富時", "Gemini + MOPS 原文", "ai", "⚠️ temperature=0 較穩定"),
        ("法說分析 — 摘要稀少時", "Gemini 推論", "warn", "❌ 每次可能不同"),
        ("策略卡 entry / stop 數字", "Gemini + 真實數字錨定", "ai", "⚠️ 輕微變動，數字有錨定"),
    ]
    header = """
    <table style="width:100%;border-collapse:collapse;font-size:11px;">
      <thead><tr style="background:var(--color-background-secondary,#f8fafc);">
        <th style="padding:7px 10px;text-align:left;color:#64748b;font-weight:500;
                   border-bottom:1px solid #e2e8f0;">欄位</th>
        <th style="padding:7px 10px;text-align:left;color:#64748b;font-weight:500;
                   border-bottom:1px solid #e2e8f0;">來源</th>
        <th style="padding:7px 10px;text-align:left;color:#64748b;font-weight:500;
                   border-bottom:1px solid #e2e8f0;">穩定性</th>
      </tr></thead><tbody>"""
    body = ""
    for field, source, style, stability in rows:
        colors = {"real": "#f0fdf4", "ai": "#fefce8", "warn": "#fff1f2"}
        bg = colors.get(style, "white")
        body += f"""<tr style="border-bottom:1px solid #f1f5f9;">
          <td style="padding:6px 10px;color:#1e293b;font-weight:500;">{field}</td>
          <td style="padding:6px 10px;color:#475569;">{source}</td>
          <td style="padding:6px 10px;background:{bg};border-radius:4px;">{stability}</td>
        </tr>"""
    st.markdown(header + body + "</tbody></table>", unsafe_allow_html=True)


# ─────────────────────────────────────────────────────────
# Main render
# ─────────────────────────────────────────────────────────

def render():
    st.header("📖 使用指南")
    st.caption("了解如何用這個平台做出更精準的投資決策 — 長期持有或波段操作都有專屬流程。")

    # ── 投資風格選擇 ──
    style = st.radio(
        "你主要的投資方式：",
        ["🔍 全部看（推薦第一次閱讀）", "📈 長期投資（持有 3 個月以上）", "⚡ 波段投資（持有 1–4 週）"],
        horizontal=True,
    )
    show_all   = "全部" in style
    show_long  = show_all or "長期" in style
    show_swing = show_all or "波段" in style

    st.divider()

    # ══════════════════════════════════════════════════════
    # PART 1 — 平台概覽（共用）
    # ══════════════════════════════════════════════════════
    _section_header("🧭", "Part 1 — 平台設計哲學",
                    "讀懂這三個核心，才能正確解讀所有數字")

    col_a, col_b, col_c = st.columns(3)
    for col, icon, title, desc in [
        (col_a, "🕵️", "跟單聰明錢",
         "外資、投信、神秘分點 — 他們在起漲前就悄悄進場。"
         "SMS 評分把這些訊號量化成 0–100 分，讓你一眼看出誰在偷偷買。"),
        (col_b, "🚀", "卡早期成長",
         "在財報爆發前找到「技術護城河高 + 營收加速」的公司。"
         "領頭羊指數和 tech_pr 都是用真實財報數字算出來的，不是 AI 猜的。"),
        (col_c, "💡", "直覺深度學習",
         "複雜數據 → 具體策略卡。進場區間、停損、目標、部位大小，"
         "AI 把分析轉成你可以直接執行的操作。"),
    ]:
        with col:
            st.markdown(f"""
            <div style="border:1px solid #e2e8f0;border-radius:10px;padding:14px;height:140px;">
              <div style="font-size:1.3em;margin-bottom:6px;">{icon}</div>
              <div style="font-size:0.9em;font-weight:600;color:#1e293b;margin-bottom:6px;">{title}</div>
              <div style="font-size:0.78em;color:#64748b;line-height:1.5;">{desc}</div>
            </div>
            """, unsafe_allow_html=True)

    st.divider()

    # ── SMS 評分解讀 ──
    _section_header("📊", "如何閱讀 SMS（Smart Money Score）",
                    "0–100 分，越高代表法人越積極佈局")

    with st.expander("展開 SMS 詳細說明", expanded=True):
        _signal_legend()
        st.markdown("""
        <div style="font-size:0.83em;color:#475569;line-height:1.7;margin-top:8px;">
        SMS 由四個維度組成，各佔 25 分：<br>
        <b>① 外資連買強度</b>：最近 5 日外資每天都買 = 25 分。
        連買代表外資有計畫性建倉，而非單日操作。<br>
        <b>② 集中度加速</b>：近 5 日法人集中度 > 近 20 日，代表法人正在加速買入。<br>
        <b>③ 外資成本安全墊</b>：現價接近外資平均成本，外資不會隨便賣出。
        乖離超過 30% 時分數歸零（外資隨時可能獲利了結）。<br>
        <b>④ 散戶退場訊號</b>：融資餘額連續減少 = 散戶怕了離場。
        散戶都走了，法人才能安靜拉抬。
        </div>
        """, unsafe_allow_html=True)

    st.divider()

    # ── 資料可信度 ──
    _section_header("✅", "資料可信度地圖",
                    "哪些數字是真實的，哪些是 AI 推算的 — 投資前必讀")

    with st.expander("展開資料可信度表"):
        _data_truth_table()
        st.caption("⚠️ 使用平台時，綠色欄位的數字才能作為進場依據。黃色/紅色欄位僅供輔助參考。")

    st.divider()

    # ══════════════════════════════════════════════════════
    # PART 2 — 長期投資
    # ══════════════════════════════════════════════════════
    if show_long:
        _section_header("📈", "Part 2 — 長期投資操作流程",
                        "選出有護城河的好公司，在合理位置低接，持有到基本面變化")

        _step_card(1, "財報品質初篩",
            "進入「個股」→「財報分析」，看「獲利含金量散點圖」。"
            "鎖定最近 3–4 季都在右上象限（營收增 + 毛利升）的標的。"
            "這類股票代表公司在賺錢的同時毛利也在擴張，是真正的成長。",
            badge_style="long",
            tip="右上象限 + 泡泡越來越大（EPS 持續增長）= 財報最健康的狀態",
            signal_rule="近 4 季中至少 3 季在右上象限，且 EPS YoY > 10%")

        _step_card(2, "護城河確認",
            "在「公司概況」確認三個真實計算的數字："
            "① tech_pr > 60（技術門檻中高以上）"
            "② 領頭羊指數 > 55（同類股中上游）"
            "③ 毛利穩定性分數 > 15（毛利不大幅波動）。"
            "這三個數字都是用財報算出來的，不是 AI 猜的。",
            badge_style="long",
            tip="展開「技術門檻計算明細」和「領頭羊指數計算明細」確認每個分項的來源",
            signal_rule="tech_pr > 60 且領頭羊 > 55，兩者至少滿足一個")

        _step_card(3, "估值位置確認",
            "進入「估值位置」，確認 52 週位置儀表盤在綠色區（< 40%）。"
            "這代表你以較低的歷史相對價格買入，安全邊際較高。"
            "同時確認「外資水位」（籌碼面 → 法人持倉水位）是向上趨勢。",
            badge_style="long",
            tip="52W 位置 < 30% + 外資水位從低點回升 = 長期最佳進場窗口",
            signal_rule="52W 百分位 < 40%，且外資水位近 20 日為正值")

        _step_card(4, "法說確認成長方向",
            "進入「法說展望」，確認 guidance_tone 是「穩健成長」或「強勁復甦」。"
            "重點看「具體營收指引」和「具體毛利指引」是否有數字。"
            "若法說摘要顯示「摘要不足」，點擊「查看簡報」閱讀完整 PDF。",
            badge_style="long",
            tip="法說有具體數字指引（如「全年營收成長 15–20%」）比模糊表態更可信",
            signal_rule="guidance_tone 非「短期受壓」，且至少一項指引有具體數字")

        _step_card(5, "進場執行與部位",
            "進入「策略卡」點擊生成，按卡片上的「進場區間」等待回落進場。"
            "部位建議按 Kelly 分數決定：kelly_fraction × 總資金。"
            "長期投資第一筆建議只用 50% 的建議部位，分兩次建倉。",
            badge_style="long",
            tip="長期投資不需要完美進場，用區間分批是比抄底更穩的方法",
            signal_rule="首次進場 50% 部位，法說確認後加至 100%")

        _step_card(6, "持倉管理與出場",
            "每個月盤後重新看一次 SMS 評分。若連續兩個月 SMS < 25 且"
            "52W 位置 > 70%，代表法人開始撤退 + 估值偏高，應考慮減倉。"
            "法說出現「毛利連續下滑」或「業績下修」是出場最強訊號。",
            badge_style="long",
            tip="長期持有的出場邏輯應該看基本面，不是看短期價格波動",
            signal_rule="SMS 兩個月連續 < 25 或法說業績下修 → 減倉 50%；再次確認 → 清倉")

        st.divider()

    # ══════════════════════════════════════════════════════
    # PART 3 — 波段投資
    # ══════════════════════════════════════════════════════
    if show_swing:
        _section_header("⚡", "Part 3 — 波段投資操作流程",
                        "找蓄力完成的標的，籌碼確認後跟進，策略卡嚴格執行")

        _step_card(7, "掃描蓄力標的",
            "進入「選股」頁，點擊「開始掃描」。"
            "找「蓄力」欄顯示「🔥 蓄力完成，等待突破」的標的。"
            "蓄力的定義：最近 5 日價格波動 < 5% + 成交量比前 10 日縮減 20% 以上。"
            "這代表籌碼正在整理，能量蓄積，一旦突破壓力位通常有較大波段。",
            badge_style="swing",
            tip="蓄力完成後等突破確認再進場，不要提前進場賭方向",
            signal_rule="蓄力欄顯示 🔥 + SMS > 50 = 第一道過濾")

        _step_card(8, "籌碼面確認",
            "點擊進入個股 →「籌碼面」，確認兩件事："
            "① 「法人持倉水位趨勢」圖中，外資水位近 20 日是上升的（正值且增加）。"
            "② 「實時籌碼診斷」顯示「外資增持」或「投信護盤」至少一個。"
            "若同時出現「散戶退場」，這是最佳籌碼組合。",
            badge_style="swing",
            tip="外資水位 + 散戶退場 + 蓄力型態三者同時出現 = 波段勝率最高的組合",
            signal_rule="外資水位 > 0 且近 5 日增加，散戶融資減少")

        _step_card(9, "分點追蹤",
            "在「籌碼面」下方查看「歷史起漲點：關鍵分點追蹤」。"
            "如果有資料，觀察同樣的分點是否在多次起漲前都出現。"
            "若有重複出現的分點（也可在選股掃描的分點 DNA 地圖確認），"
            "代表這個分點可能是固定的主力，可信度更高。",
            badge_style="swing",
            tip="分點資料需要 FinMind 付費帳號。若無資料，跳過此步驟即可",
            signal_rule="有重複分點 +1 信心分，無資料不扣分")

        _step_card(10, "讀策略卡執行",
            "進入「策略卡」，點擊生成。策略卡包含四個關鍵資訊："
            "① 進場區間 — 等價格回落到這個範圍才進場，不追高。"
            "② 停損位 — 這是硬性規則，不是參考線。跌破停損立刻執行，不猶豫。"
            "③ 目標一（減倉 50%）/ 目標二（清倉）。"
            "④ 部位建議 — Kelly 分數 × 總資金 = 本次最大部位。",
            badge_style="swing",
            tip="「風報比 1 : X」，X < 1.5 的策略建議不進場，期望值不划算",
            signal_rule="風報比 > 2 且信心度 > 50 才執行策略卡")

        _step_card(11, "回測室驗證信心",
            "進入「回測室」，查看三個數字："
            "① 回測勝率 > 55% — 歷史上多數時候這個訊號有效。"
            "② 夏普比率 > 1 — 每承擔一單位風險能獲得超過一單位的報酬。"
            "③ 最大回撤 — 告訴你歷史上最壞的狀況，確保你能承受這個虧損。",
            badge_style="swing",
            tip="回測勝率是歷史數字，不保證未來，但能幫助你建立正確的心理預期",
            signal_rule="勝率 > 55% + 夏普 > 1 = 可信的策略訊號")

        st.divider()

    # ══════════════════════════════════════════════════════
    # PART 4 — 風險管理（共用）
    # ══════════════════════════════════════════════════════
    _section_header("🛡️", "Part 4 — 風險管理（必讀）",
                    "賺多少靠運氣，虧多少靠紀律")

    _step_card(12, "停損是唯一的硬性規則",
        "策略卡給出的停損位是根據外資成本區和技術面計算的。"
        "跌破停損代表原始的多頭假設已經不成立。"
        "最常見的虧損不是進場錯，是進場對了但停損沒執行，從小虧變成大虧。",
        badge_style="both",
        signal_rule="跌破停損位 → 當日收盤前平倉，沒有例外",
        tip="設定手機價格警報，在停損位 -1% 就提醒你，不要等到大跌才反應")

    _step_card(13, "Kelly 部位不要超標",
        "Kelly 分數代表最大合理部位比例。超過 Kelly 建議的部位，"
        "即使勝率夠高，長期下來也會因為單次大虧把獲利吃掉。"
        "平台預設 Kelly 上限 0.25（最多用 25% 資金在單一標的），"
        "新手建議用 Half-Kelly（Kelly × 0.5），降低波動。",
        badge_style="both",
        signal_rule="單一標的部位 ≤ Kelly 分數 × 總資金，新手用 Half-Kelly",
        tip="分散在 4–6 個標的，每個標的平均 15–20% 是穩健的部位結構")

    _step_card(14, "三個最常見的操作錯誤",
        "① 追高進場：看到蓄力訊號但股價已漲 10%，仍然追高。"
        "等回落到策略卡的進場區間，沒有到不進場。\n"
        "② 忽視水位：法人水位一直下降還在持有。"
        "水位下降代表法人在賣出，不要跟法人對作。\n"
        "③ 信任 AI 編造數字：法說分析顯示「摘要不足」時，"
        "Gemini 給出的具體數字可能是推測的，不能當成真實指引引用。",
        badge_style="warn", badge_text="注意",
        tip="每次進場前問自己：這個進場依據來自真實數字，還是 AI 推算？")

    st.divider()

    # ══════════════════════════════════════════════════════
    # 快速參考卡
    # ══════════════════════════════════════════════════════
    _section_header("📋", "快速參考：進場核對清單")

    col_l, col_s = st.columns(2)
    with col_l:
        st.markdown("**📈 長期投資進場清單**")
        for item in [
            ("財報散點圖 3 季以上右上象限", True),
            ("tech_pr > 60 或領頭羊 > 55", True),
            ("52W 位置 < 40%", True),
            ("外資水位近 20 日上升", True),
            ("法說有具體指引數字", False),
            ("策略卡信心度 > 50", False),
        ]:
            icon  = "✅" if item[1] else "⬜"
            style = "font-weight:600;color:#1e293b;" if item[1] else "color:#64748b;"
            st.markdown(
                f"<div style='font-size:0.83em;{style}padding:3px 0;'>{icon} {item[0]}</div>",
                unsafe_allow_html=True)

    with col_s:
        st.markdown("**⚡ 波段投資進場清單**")
        for item in [
            ("蓄力完成（量縮價穩）", True),
            ("SMS > 50", True),
            ("外資水位正值且增加", True),
            ("策略卡風報比 > 2", True),
            ("回測勝率 > 55%", True),
            ("停損位已設定手機警報", False),
        ]:
            icon  = "✅" if item[1] else "⬜"
            style = "font-weight:600;color:#1e293b;" if item[1] else "color:#64748b;"
            st.markdown(
                f"<div style='font-size:0.83em;{style}padding:3px 0;'>{icon} {item[0]}</div>",
                unsafe_allow_html=True)

    st.divider()
    st.caption(
        "📌 本指南基於平台資料邏輯撰寫，不構成投資建議。"
        "所有操作決策由使用者自行負責。"
        "市場有風險，投資前請確認自身風險承受能力。"
    )
