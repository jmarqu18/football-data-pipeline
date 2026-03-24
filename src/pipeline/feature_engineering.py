"""Feature engineering: construcción de métricas derivadas (CLEAN → FEATURES)."""
from __future__ import annotations

import logging
import math
from datetime import date
from pathlib import Path

import numpy as np
import pandas as pd
from sqlalchemy import Engine, text

logger = logging.getLogger(__name__)

_MIN_MINUTES = 450
_SEASON_TOTAL_MINUTES = 3420  # 38 matchdays × 90 min


def _safe_divide(num: pd.Series, denom: pd.Series) -> pd.Series:
    """Divide num/denom, returning NaN where denom is 0 or NaN."""
    return np.where(denom > 0, num / denom, np.nan).astype(float)


def compute_per90_features(
    df: pd.DataFrame,
    min_minutes: int = _MIN_MINUTES,
    matchdays_played: int | None = None,
) -> pd.DataFrame:
    """Compute per-90 and contextual metrics from player_season_stats data.

    Keeps one row per (player_id, team_id) stint. Does NOT aggregate mid-season
    transfers — each stint is an independent observation. Filters to stints with
    >= min_minutes.

    minutes_pct denominator:
    - If matchdays_played is provided (from standings Parquet): uses matchdays_played * 90.
    - Otherwise falls back to max(starts) * 90 as a proxy (season ongoing, no match data).

    Args:
        df: DataFrame with columns from player_season_stats JOIN players.
            Required columns: player_id, canonical_name, known_name, team_id, season,
            position, appearances, starts, minutes, goals, assists, shots_total,
            shots_on_target, key_passes, tackles, dribbles_attempted,
            dribbles_successful, duels_total, duels_won.
        min_minutes: Minimum minutes threshold per stint (default 450).
        matchdays_played: Exact matchdays played in the season (from standings).
                          If None, uses max(starts) * 90 as proxy.

    Returns:
        DataFrame with one row per (player_id, team_id) stint, per-90 and contextual features.
    """
    df = df[df["minutes"] >= min_minutes].copy()

    if matchdays_played is not None:
        season_max_minutes = matchdays_played * 90
    else:
        # Proxy: the player who started the most games ≈ matchdays played
        season_max_minutes = int(df["starts"].max()) * 90 if not df.empty else _SEASON_TOTAL_MINUTES
        logger.warning(
            "minutes_pct: standings not available, using max(starts)*90=%d as proxy",
            season_max_minutes,
        )

    nineties = df["minutes"] / 90.0
    df["minutes_pct"] = df["minutes"] / season_max_minutes
    df["games_started_pct"] = _safe_divide(df["starts"], df["appearances"])
    df["goals_per_90"] = df["goals"] / nineties
    df["assists_per_90"] = df["assists"] / nineties
    df["shots_per_90"] = df["shots_total"] / nineties
    df["key_passes_per_90"] = df["key_passes"] / nineties
    df["tackles_per_90"] = df["tackles"] / nineties
    df["shots_on_target_pct"] = _safe_divide(df["shots_on_target"], df["shots_total"])
    df["dribble_success_rate"] = _safe_divide(df["dribbles_successful"], df["dribbles_attempted"])
    df["duels_won_pct"] = _safe_divide(df["duels_won"], df["duels_total"])

    logger.info("Per-90 features: %d stints (player+team) with >= %d minutes", len(df), min_minutes)
    return df
