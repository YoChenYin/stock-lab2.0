"""
engine/rocket_detector.py — pre-breakout pattern detection

Two-stage rocket model:
  Stage 1 — Coiling:   price stable + volume shrinking = energy accumulating
  Stage 2 — Breakout:  price closes above resistance + volume surge = launch

Both functions accept a normalised DataFrame (lowercase columns, high/max both supported).
"""

import pandas as pd
import numpy as np


def _high_col(df: pd.DataFrame) -> str:
    return "high" if "high" in df.columns else "max"


def _low_col(df: pd.DataFrame) -> str:
    return "low" if "low" in df.columns else "min"


def detect_coiling(df: pd.DataFrame, lookback: int = 20) -> dict:
    """
    Detect 'price-stable + volume-shrinking' coiling pattern.

    Returns:
      is_coiling      bool
      squeeze_ratio   % — how much price range has compressed
      vol_shrink_pct  % — how much volume has dropped
      key_resistance  float — highest high in the lookback window
      label           display string
    """
    if len(df) < lookback:
        return {"is_coiling": False, "label": "資料不足",
                "key_resistance": 0, "squeeze_ratio": 0, "vol_shrink_pct": 0}

    recent = df.tail(lookback).copy()
    hc, lc = _high_col(df), _low_col(df)

    # Price range compression
    price_range   = (recent[hc] - recent[lc]) / (recent["close"] + 1e-9)
    vol_now       = price_range.tail(5).mean()
    vol_before    = price_range.head(10).mean()
    squeeze_ratio = 1 - (vol_now / (vol_before + 1e-9))

    # Volume shrink
    shares_now    = recent["trading_volume"].tail(5).mean()
    shares_before = recent["trading_volume"].head(10).mean()
    vol_shrink    = 1 - (shares_now / (shares_before + 1e-9))

    # Price stability (last 5 days range < 5% of close)
    price_stable  = (
        recent["close"].tail(5).max() - recent["close"].tail(5).min()
    ) / recent["close"].iloc[-1] < 0.05

    key_resistance = recent[hc].max()
    is_coiling     = squeeze_ratio > 0.3 and vol_shrink > 0.2 and price_stable

    return {
        "is_coiling":    is_coiling,
        "squeeze_ratio": round(squeeze_ratio * 100, 1),
        "vol_shrink_pct":round(vol_shrink * 100, 1),
        "key_resistance":round(key_resistance, 2),
        "label": "🔥 蓄力完成，等待突破" if is_coiling else "📊 尚未蓄力",
    }


