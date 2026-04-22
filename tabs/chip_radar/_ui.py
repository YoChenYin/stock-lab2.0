"""chip_radar/_ui.py — reusable UI primitives, matching stock_lab style"""

import streamlit as st
import plotly.graph_objects as go


# ── Color helpers ─────────────────────────────────────────────────

def score_color(v: float) -> str:
    if v is None or v != v:
        return "#94a3b8"
    if v >= 75:
        return "#2563eb"
    if v >= 55:
        return "#10b981"
    if v >= 35:
        return "#f59e0b"
    return "#ef4444"


def score_label(v: float) -> str:
    if v is None or v != v:
        return "—"
    if v >= 75:
        return "強烈看多"
    if v >= 55:
        return "偏多"
    if v >= 45:
        return "中性"
    if v >= 25:
        return "偏空"
    return "強烈看空"


# ── Components ────────────────────────────────────────────────────

def score_card(label: str, score: float, icon: str = "",
               tooltip: str = "", key: str = ""):
    color  = score_color(score)
    s_str  = f"{score:.0f}" if score is not None else "—"
    bar_w  = int(score) if score is not None else 0
    tt_html = (
        f'<div style="color:#64748b;font-size:0.72em;margin-top:4px;">{tooltip}</div>'
        if tooltip else ""
    )
    st.markdown(f"""
    <div style="background:white;border:1px solid #e2e8f0;border-top:3px solid {color};
                border-radius:10px;padding:14px 16px;text-align:center;">
      <div style="color:#64748b;font-size:0.7em;font-weight:600;
                  letter-spacing:0.4px;margin-bottom:6px;">{icon} {label.upper()}</div>
      <div style="font-size:2em;font-weight:800;color:{color};line-height:1;">{s_str}</div>
      <div style="color:#94a3b8;font-size:0.72em;margin-bottom:8px;">/100</div>
      <div style="background:#f1f5f9;border-radius:4px;height:5px;">
        <div style="width:{bar_w}%;background:{color};height:5px;border-radius:4px;
                    transition:width .4s;"></div>
      </div>
      {tt_html}
    </div>
    """, unsafe_allow_html=True)


def whale_card(ticker: str, composite: float, flags: list,
               entry: bool = False, price: float = None):
    color  = score_color(composite)
    flags_html = " ".join(
        f'<span style="background:{color}18;color:{color};padding:2px 8px;'
        f'border-radius:10px;font-size:0.72em;font-weight:600;">{f}</span>'
        for f in flags
    )
    entry_badge = (
        '<span style="background:#fef3c7;color:#d97706;padding:2px 8px;'
        'border-radius:10px;font-size:0.72em;font-weight:700;margin-left:4px;">'
        '⚡ 進場時機</span>'
    ) if entry else ""
    price_str = f"${price:.2f}" if price else ""
    st.markdown(f"""
    <div style="background:white;border:1px solid #e2e8f0;border-left:4px solid {color};
                border-radius:10px;padding:16px;min-width:180px;">
      <div style="display:flex;justify-content:space-between;align-items:flex-start;">
        <div>
          <span style="font-size:1.1em;font-weight:800;color:#1e293b;">🐋 {ticker}</span>{entry_badge}
        </div>
        <div style="font-size:1.6em;font-weight:900;color:{color};">{composite:.0f}</div>
      </div>
      <div style="color:#94a3b8;font-size:0.75em;margin:2px 0 8px;">{price_str}</div>
      <div style="display:flex;flex-wrap:wrap;gap:4px;">{flags_html}</div>
    </div>
    """, unsafe_allow_html=True)


def market_pulse_bar(pc_ratio: float, z_score: float, date: str):
    """Top-of-page market sentiment strip"""
    if z_score is None:
        sentiment, s_color, s_bg = "資料累積中", "#64748b", "#f8fafc"
    elif z_score > 1.5:
        sentiment, s_color, s_bg = "極度恐慌（逆向偏多）", "#2563eb", "#eff6ff"
    elif z_score > 0.5:
        sentiment, s_color, s_bg = "輕度恐慌", "#10b981", "#f0fdf4"
    elif z_score < -1.5:
        sentiment, s_color, s_bg = "過度樂觀（警惕回調）", "#ef4444", "#fef2f2"
    elif z_score < -0.5:
        sentiment, s_color, s_bg = "偏樂觀", "#f59e0b", "#fffbeb"
    else:
        sentiment, s_color, s_bg = "中性", "#64748b", "#f8fafc"

    pc_str = f"{pc_ratio:.3f}" if pc_ratio else "—"
    z_str  = f"{z_score:+.2f}σ" if z_score is not None else "—"
    st.markdown(f"""
    <div style="background:{s_bg};border:1px solid {s_color}30;border-radius:10px;
                padding:12px 20px;margin-bottom:16px;
                display:flex;justify-content:space-between;align-items:center;flex-wrap:wrap;gap:8px;">
      <div style="display:flex;align-items:center;gap:16px;">
        <span style="color:#64748b;font-size:0.8em;font-weight:600;">📊 市場情緒</span>
        <span style="font-weight:700;color:{s_color};">{sentiment}</span>
        <span style="color:#94a3b8;font-size:0.8em;">SPY P/C = {pc_str}
          <span style="margin-left:6px;">{z_str}</span>
        </span>
      </div>
      <div style="color:#94a3b8;font-size:0.75em;">更新日期：{date or "—"}</div>
    </div>
    """, unsafe_allow_html=True)


def market_env_block(env: dict):
    """
    市場行情總覽區塊：VIX / 10Y 殖利率 / Mag7 乖離 / 板塊強弱
    env: dict from load_market_env()
    """
    if not env:
        st.info("市場環境指標尚未抓取，請執行 fetch_daily 更新資料。")
        return

    vix       = env.get("vix")
    vix_level = env.get("vix_level", "medium")
    tnx       = env.get("tnx_10y")
    tnx_slope = env.get("tnx_slope_20d")
    mag7_avg  = env.get("mag7_avg_deviation")
    mag7_risk = env.get("mag7_risk", "ok")
    mag7_devs = env.get("mag7_deviations") or {}
    date_str  = env.get("date", "")

    xlk1 = env.get("xlk_chg_1d")
    xlf1 = env.get("xlf_chg_1d")
    xlp1 = env.get("xlp_chg_1d")

    # ── VIX 顏色邏輯 ──
    vix_cfg = {
        "low":    ("#10b981", "#f0fdf4", "🟢 低風險",   "VIX < 15，市場情緒平穩"),
        "medium": ("#f59e0b", "#fffbeb", "🟡 中等風險", "VIX 15–25，保持警覺"),
        "high":   ("#ef4444", "#fef2f2", "🔴 警示",     "VIX > 25，市場恐慌！系統自動對進場分數折扣"),
    }
    vc, vbg, vlabel, vdesc = vix_cfg.get(vix_level, vix_cfg["medium"])

    # ── 10Y 斜率描述 ──
    if tnx_slope is not None:
        if tnx_slope > 0.02:
            slope_label = "⬆ 快速上升（成長股減分）"
            slope_color = "#ef4444"
        elif tnx_slope > 0.005:
            slope_label = "↗ 溫和上升"
            slope_color = "#f59e0b"
        elif tnx_slope < -0.005:
            slope_label = "↘ 下降"
            slope_color = "#10b981"
        else:
            slope_label = "→ 持平"
            slope_color = "#64748b"
    else:
        slope_label = "—"
        slope_color = "#94a3b8"

    # ── Mag7 乖離描述 ──
    mag7_color = "#ef4444" if mag7_risk == "caution" else "#10b981"
    mag7_label = f"⚠ {mag7_avg:+.1f}%（集體高乖離，回檔風險）" if mag7_risk == "caution" else f"{mag7_avg:+.1f}%（正常）" if mag7_avg is not None else "—"

    # ── 板塊強弱文字 ──
    def _chg_html(v):
        if v is None:
            return '<span style="color:#94a3b8">—</span>'
        color = "#10b981" if v > 0 else ("#ef4444" if v < 0 else "#64748b")
        arrow = "▲" if v > 0 else ("▼" if v < 0 else "")
        return f'<span style="color:{color};font-weight:700;">{arrow}{abs(v):.2f}%</span>'

    # ── 頂部橫幅（VIX 警示 bar）──
    st.markdown(f"""
    <div style="background:{vbg};border:1px solid {vc}40;border-left:4px solid {vc};
                border-radius:10px;padding:10px 18px;margin-bottom:6px;
                display:flex;align-items:center;justify-content:space-between;flex-wrap:wrap;gap:8px;">
      <div style="display:flex;align-items:center;gap:12px;">
        <span style="font-size:0.78em;font-weight:700;color:#64748b;letter-spacing:.3px;">📊 市場環境</span>
        <span style="font-weight:800;color:{vc};">{vlabel} — VIX {vix:.1f}</span>
        <span style="color:#94a3b8;font-size:0.75em;">{vdesc}</span>
      </div>
      <span style="color:#94a3b8;font-size:0.72em;">更新：{date_str}</span>
    </div>
    """, unsafe_allow_html=True)

    # ── 四欄指標卡 ──
    c1, c2, c3, c4 = st.columns(4)

    with c1:
        st.markdown(f"""
        <div style="background:white;border:1px solid #e2e8f0;border-top:3px solid {vc};
                    border-radius:10px;padding:14px 16px;">
          <div style="color:#64748b;font-size:0.7em;font-weight:600;letter-spacing:.4px;">😱 VIX 恐慌指數</div>
          <div style="font-size:1.9em;font-weight:900;color:{vc};line-height:1.1;">{f"{vix:.1f}" if vix else "—"}</div>
          <div style="color:#94a3b8;font-size:0.72em;">5日均: {f"{env.get('vix_ma5', 0):.1f}" if env.get("vix_ma5") else "—"}</div>
          <div style="margin-top:4px;font-size:0.72em;color:{vc};font-weight:600;">{vlabel}</div>
        </div>
        """, unsafe_allow_html=True)

    with c2:
        st.markdown(f"""
        <div style="background:white;border:1px solid #e2e8f0;border-top:3px solid {slope_color};
                    border-radius:10px;padding:14px 16px;">
          <div style="color:#64748b;font-size:0.7em;font-weight:600;letter-spacing:.4px;">🏦 10Y 美債殖利率</div>
          <div style="font-size:1.9em;font-weight:900;color:{slope_color};line-height:1.1;">{f"{tnx:.2f}%" if tnx else "—"}</div>
          <div style="color:#94a3b8;font-size:0.72em;">20日斜率</div>
          <div style="margin-top:4px;font-size:0.72em;color:{slope_color};font-weight:600;">{slope_label}</div>
        </div>
        """, unsafe_allow_html=True)

    with c3:
        st.markdown(f"""
        <div style="background:white;border:1px solid #e2e8f0;border-top:3px solid {mag7_color};
                    border-radius:10px;padding:14px 16px;">
          <div style="color:#64748b;font-size:0.7em;font-weight:600;letter-spacing:.4px;">🔭 Mag7 50MA 乖離</div>
          <div style="font-size:1.9em;font-weight:900;color:{mag7_color};line-height:1.1;">{f"{mag7_avg:+.1f}%" if mag7_avg is not None else "—"}</div>
          <div style="color:#94a3b8;font-size:0.72em;">七巨頭平均</div>
          <div style="margin-top:4px;font-size:0.72em;color:{mag7_color};font-weight:600;">{mag7_label}</div>
        </div>
        """, unsafe_allow_html=True)
        if mag7_devs:
            with st.expander("個股乖離明細", expanded=False):
                for t, d in sorted(mag7_devs.items(), key=lambda x: -x[1]):
                    dc = "#10b981" if d >= 0 else "#ef4444"
                    st.markdown(
                        f'<span style="font-size:0.82em;margin-right:12px;">'
                        f'<b>{t}</b> <span style="color:{dc};">{d:+.1f}%</span></span>',
                        unsafe_allow_html=True,
                    )

    with c4:
        def _sector_row(name, sym, val1):
            html = _chg_html(val1)
            return (
                f'<div style="display:flex;justify-content:space-between;align-items:center;'
                f'padding:2px 0;font-size:0.82em;">'
                f'<span style="color:#475569;">{name} ({sym})</span>{html}</div>'
            )
        rows = (
            _sector_row("科技", "XLK", xlk1)
            + _sector_row("金融", "XLF", xlf1)
            + _sector_row("防禦", "XLP", xlp1)
        )
        leader = max(
            [("科技", xlk1 or 0), ("金融", xlf1 or 0), ("防禦", xlp1 or 0)],
            key=lambda x: x[1],
        )
        st.markdown(f"""
        <div style="background:white;border:1px solid #e2e8f0;border-top:3px solid #2563eb;
                    border-radius:10px;padding:14px 16px;">
          <div style="color:#64748b;font-size:0.7em;font-weight:600;letter-spacing:.4px;">🔄 板塊輪動 (1日)</div>
          <div style="margin-top:8px;">{rows}</div>
          <div style="margin-top:6px;font-size:0.72em;color:#2563eb;font-weight:600;">
            領漲：{leader[0]} {_chg_html(leader[1])}
          </div>
        </div>
        """, unsafe_allow_html=True)

    st.markdown("<div style='margin-bottom:4px;'></div>", unsafe_allow_html=True)


def section_header(title: str, subtitle: str = ""):
    sub_html = (
        f'<div style="color:#64748b;font-size:0.82em;margin-top:2px;">{subtitle}</div>'
        if subtitle else ""
    )
    st.markdown(f"""
    <div style="margin:20px 0 10px;">
      <div style="font-size:1em;font-weight:700;color:#1e293b;">{title}</div>
      {sub_html}
    </div>
    """, unsafe_allow_html=True)


def guide_box(lines: list):
    """Collapsible guide/tooltip box"""
    items = "".join(
        f'<li style="margin-bottom:4px;">{ln}</li>' for ln in lines
    )
    st.markdown(f"""
    <div style="background:#f8fafc;border:1px solid #e2e8f0;border-radius:8px;
                padding:12px 16px;margin-top:6px;">
      <ul style="margin:0;padding-left:18px;color:#475569;font-size:0.82em;line-height:1.7;">
        {items}
      </ul>
    </div>
    """, unsafe_allow_html=True)


# ── Plotly charts ─────────────────────────────────────────────────

PLOTLY_BASE = dict(
    template="plotly_white",
    paper_bgcolor="rgba(0,0,0,0)",
    plot_bgcolor="rgba(0,0,0,0)",
    margin=dict(l=0, r=0, t=30, b=0),
    hovermode="x unified",
    font=dict(family="system-ui, sans-serif", size=12, color="#1e293b"),
)


def radar_chart(scores: dict) -> go.Figure:
    labels = ["內部人", "空頭動能", "量能", "選擇權流", "機構持倉"]
    keys   = ["insider_score", "short_score", "volume_score",
               "options_flow_score", "institutional_score"]
    values = [scores.get(k, 50) or 50 for k in keys]
    values_closed = values + [values[0]]
    labels_closed = labels + [labels[0]]

    fig = go.Figure()
    fig.add_trace(go.Scatterpolar(
        r=values_closed, theta=labels_closed,
        fill="toself",
        fillcolor="rgba(37,99,235,0.12)",
        line=dict(color="#2563eb", width=2),
        marker=dict(size=6, color="#2563eb"),
        hovertemplate="%{theta}: %{r:.0f}<extra></extra>",
    ))
    fig.update_layout(
        **{**PLOTLY_BASE,
           "polar": dict(
               radialaxis=dict(
                   visible=True, range=[0, 100],
                   tickvals=[25, 50, 75, 100],
                   tickfont=dict(size=10, color="#94a3b8"),
                   gridcolor="#e2e8f0",
                   linecolor="#e2e8f0",
               ),
               angularaxis=dict(
                   tickfont=dict(size=12, color="#1e293b"),
                   gridcolor="#e2e8f0",
                   linecolor="#e2e8f0",
               ),
               bgcolor="rgba(0,0,0,0)",
           ),
           "showlegend": False,
           "margin": dict(l=20, r=20, t=20, b=20),
        }
    )
    return fig


def score_history_chart(df, composite_col: str = "composite_swing") -> go.Figure:
    if df.empty:
        return go.Figure()

    fig = go.Figure()
    col_map = {
        "composite_short": ("短線", "#f59e0b"),
        "composite_swing": ("波段", "#2563eb"),
        "composite_mid":   ("中線", "#10b981"),
    }
    name, color = col_map.get(composite_col, ("分數", "#2563eb"))

    if composite_col in df.columns:
        fig.add_trace(go.Scatter(
            x=df["date"], y=df[composite_col],
            mode="lines+markers", name=name,
            line=dict(color=color, width=2.5),
            marker=dict(size=5),
            hovertemplate="%{x}: %{y:.1f}<extra></extra>",
        ))

    # 50 分基準線
    fig.add_hline(y=50, line_dash="dot", line_color="#94a3b8", line_width=1)

    fig.update_layout(**{
        **PLOTLY_BASE,
        "height": 220,
        "yaxis": dict(range=[0, 100], showgrid=True,
                      gridcolor="#f1f5f9", ticksuffix=""),
        "xaxis": dict(showgrid=False),
        "showlegend": False,
        "margin": dict(l=0, r=0, t=10, b=0),
    })
    return fig


def options_flow_chart(df) -> go.Figure:
    if df.empty:
        return go.Figure()

    fig = go.Figure()
    fig.add_trace(go.Bar(
        x=df["date"], y=df["call_volume"],
        name="Call", marker_color="#10b981", opacity=0.8,
        hovertemplate="Call: %{y:,}<extra></extra>",
    ))
    fig.add_trace(go.Bar(
        x=df["date"], y=[-v for v in df["put_volume"]],
        name="Put", marker_color="#ef4444", opacity=0.8,
        hovertemplate="Put: %{customdata:,}<extra></extra>",
        customdata=df["put_volume"],
    ))
    if "otm_call_volume" in df.columns:
        fig.add_trace(go.Scatter(
            x=df["date"], y=df["otm_call_volume"],
            name="OTM Call", mode="lines+markers",
            line=dict(color="#2563eb", dash="dot", width=1.5),
            marker=dict(size=4),
            hovertemplate="OTM Call: %{y:,}<extra></extra>",
        ))
    fig.update_layout(**{
        **PLOTLY_BASE,
        "barmode": "relative",
        "height": 250,
        "yaxis": dict(showgrid=True, gridcolor="#f1f5f9", tickformat=","),
        "xaxis": dict(showgrid=False),
        "legend": dict(orientation="h", yanchor="bottom", y=1.02),
        "margin": dict(l=0, r=0, t=30, b=0),
    })
    return fig
