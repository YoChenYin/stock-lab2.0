"""
engine/backtest.py — vectorbt-based backtest with graceful fallback

CRITICAL RULE: entries and exits passed in must already be .shift(1).
This file does NOT apply shift — the caller is responsible.

Why shift(1)?
  We predict using today's close; we can only trade at tomorrow's open.
  Using the same bar for signal and execution = lookahead bias = fake returns.
"""

import pandas as pd
import numpy as np

try:
    import vectorbt as vbt
    HAS_VBT = True
except ImportError:
    HAS_VBT = False

from config import TWSE_FEES, TWSE_TAX


def run_vbt_backtest(
    df: pd.DataFrame,
    entries: pd.Series,
    exits: pd.Series,
    initial_cash: float = 1_000_000,
) -> object | None:
    """
    Run a vectorbt backtest.
    Returns vbt.Portfolio or None if vectorbt is not installed.

    df must have a DatetimeIndex and a 'close' column.
    entries / exits are boolean Series aligned to df.index.
    Both must already be .shift(1)'d by the caller.
    """
    if not HAS_VBT:
        return None

    pf = vbt.Portfolio.from_signals(
        df["close"],
        entries=entries,
        exits=exits,
        init_cash=initial_cash,
        fees=TWSE_FEES,
        slippage=TWSE_TAX / 2,
        size=1.0,
        size_type="percent",
        freq="D",
    )
    return pf


def extract_vbt_stats(pf) -> dict:
    """Pull the 6 key metrics from a vbt.Portfolio."""
    s = pf.stats()
    return {
        "總報酬 (%)":   round(s["Total Return [%]"], 2),
        "年化報酬 (%)": round(s["Annualized Return [%]"], 2),
        "夏普比率":     round(s["Sharpe Ratio"], 2),
        "最大回撤 (%)": round(s["Max Drawdown [%]"], 2),
        "勝率 (%)":     round(s["Win Rate [%]"], 2),
        "交易次數":     int(s["Total Trades"]),
    }


def get_equity_curve(pf) -> pd.Series:
    return pf.value()


def get_trade_log(pf) -> pd.DataFrame:
    return pf.trades.records_readable


def simulate_signals_loop(df: pd.DataFrame) -> tuple[list, list]:
    """
    Fallback for-loop simulation when vectorbt is unavailable.
    Input df must have buy_cond and exit_cond boolean columns (already shifted).
    Returns (buy_indices, exit_indices).
    """
    pos = False
    buy_indices, exit_indices = [], []
    for i in range(len(df)):
        if not pos and df["buy_cond"].iloc[i]:
            pos = True
            buy_indices.append(i)
        elif pos and df["exit_cond"].iloc[i]:
            pos = False
            exit_indices.append(i)
    return buy_indices, exit_indices


def compute_sharpe_beta(df: pd.DataFrame) -> tuple[float, float]:
    """Quick Sharpe and simplified Beta from close price series."""
    returns    = df["close"].pct_change().dropna()
    rf_daily   = 0.015 / 252
    avg_excess = returns.mean() - rf_daily
    std        = returns.std()
    sharpe     = (avg_excess / std) * np.sqrt(252) if std != 0 else 0.0
    beta       = std / (0.15 / np.sqrt(252))
    return round(sharpe, 2), round(beta, 2)


def volume_profile(df: pd.DataFrame, bins: int = 30) -> tuple[np.ndarray, np.ndarray]:
    """
    Returns (v_counts, v_prices) for the last 252 bars.
    POC = v_prices[argmax(v_counts)]
    """
    tail = df.tail(252)
    v_counts, bin_edges = np.histogram(
        tail["close"], bins=bins, weights=tail["trading_volume"]
    )
    v_prices = (bin_edges[:-1] + bin_edges[1:]) / 2
    return v_counts, v_prices
