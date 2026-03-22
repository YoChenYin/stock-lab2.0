"""
config.py — global constants, colours, Plotly base layout
Import from here everywhere else; never hardcode in tab files.
"""

COLORS = {
    "up":          "#ef4444",
    "down":        "#22c55e",
    "foreign":     "#2563eb",
    "trust":       "#16a34a",
    "dealer":      "#9333ea",
    "retail":      "#ef4444",
    "ma5":         "#fc8c03",
    "ma10":        "#03e3fc",
    "ma20":        "#6366f1",
    "buy_signal":  "#10b981",
    "sell_signal": "#f43f5e",
    "equity":      "#10b981",
    "benchmark":   "#94a3b8",
}

PLOTLY_BASE = dict(
    template="plotly_white",
    paper_bgcolor="rgba(0,0,0,0)",
    plot_bgcolor="rgba(0,0,0,0)",
    margin=dict(l=0, r=0, t=30, b=0),
    hovermode="x unified",
    legend=dict(orientation="h", yanchor="bottom", y=1.02),
    xaxis=dict(showgrid=False),
    yaxis=dict(showgrid=True, gridcolor="#f1f5f9"),
)

TWSE_FEES = 0.001425   # brokerage (buy + sell)
TWSE_TAX  = 0.003      # transaction tax (sell only)

# Alert thresholds
FOREIGN_STREAK_DAYS   = 5
COILING_SQUEEZE_RATIO = 0.3
COILING_VOL_SHRINK    = 0.2
ALERT_MAX_STOCKS      = 40


def plotly_layout(**overrides) -> dict:
    """
    Safe merge of PLOTLY_BASE with per-chart overrides.
    Use this instead of spreading **PLOTLY_BASE alongside explicit xaxis/yaxis kwargs —
    dict kwargs always win, never clash.

    Example:
        fig.update_layout(**plotly_layout(
            height=300,
            xaxis=dict(range=[0, 30], showgrid=False),
            yaxis=dict(showgrid=False),
        ))
    """
    return {**PLOTLY_BASE, **overrides}
