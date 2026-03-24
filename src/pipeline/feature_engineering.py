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


def compute_xg_features(per90_df: pd.DataFrame, advanced_df: pd.DataFrame) -> pd.DataFrame:
    """Compute xG-derived features by joining per90 base with Understat advanced stats.

    Each row in per90_df is one (player_id, team_id) stint. Merge on (player_id, team_id)
    so transferred players get the correct team's xg_chain_share.

    xg_chain_share = player_xg_chain / sum(xg_chain for all players in same team+season).

    Args:
        per90_df: Output of compute_per90_features (one row per player+team stint).
                  Must have columns: player_id, team_id, minutes, goals.
        advanced_df: Raw player_season_advanced rows. Required columns: player_id,
                     team_id, season, xg, xa, npxg, xg_chain, xg_buildup.

    Returns:
        per90_df with added columns: xg_overperformance, npxg_per_90, xa_per_90,
        xg_chain_share, xg_buildup_per_90.
    """
    xg_cols = ("xg_overperformance", "npxg_per_90", "xa_per_90", "xg_chain_share", "xg_buildup_per_90")

    if advanced_df.empty:
        result = per90_df.copy()
        for col in xg_cols:
            result[col] = np.nan
        return result

    # Compute team_xg_chain BEFORE any player-level grouping (needed for xg_chain_share)
    team_xg_chain = (
        advanced_df.groupby(["team_id", "season"])["xg_chain"]
        .sum()
        .reset_index()
        .rename(columns={"xg_chain": "team_xg_chain"})
    )

    # Attach team_xg_chain to each player row
    adv = advanced_df.merge(team_xg_chain, on=["team_id", "season"], how="left")

    # Merge per90_df with advanced on (player_id, team_id)
    merged = per90_df.merge(
        adv[["player_id", "team_id", "xg", "xa", "npxg", "xg_chain", "xg_buildup", "team_xg_chain"]],
        on=["player_id", "team_id"],
        how="left",
    )

    nineties = merged["minutes"] / 90.0
    merged["xg_overperformance"] = np.where(merged["xg"].notna(), merged["goals"] - merged["xg"], np.nan)
    merged["npxg_per_90"] = np.where(merged["npxg"].notna(), merged["npxg"] / nineties, np.nan)
    merged["xa_per_90"] = np.where(merged["xa"].notna(), merged["xa"] / nineties, np.nan)
    merged["xg_chain_share"] = _safe_divide(merged["xg_chain"], merged["team_xg_chain"])
    merged["xg_buildup_per_90"] = np.where(merged["xg_buildup"].notna(), merged["xg_buildup"] / nineties, np.nan)

    merged = merged.drop(columns=["xg", "xa", "npxg", "xg_chain", "xg_buildup", "team_xg_chain"])

    resolved = merged["xg_overperformance"].notna().sum()
    logger.info("xG features: %d/%d stints with Understat data", resolved, len(merged))
    return merged


def compute_shot_features(shots_df: pd.DataFrame) -> pd.DataFrame:
    """Compute shot quality aggregations from player_shots.

    Distance formula: goal at (1.0, 0.5) normalized, pitch 105m × 68m.
    distance = sqrt(((1-x)*105)^2 + ((y-0.5)*68)^2)

    Args:
        shots_df: DataFrame from player_shots. Required columns: player_id, x, y, xg,
            result, situation, body_part.

    Returns:
        DataFrame with one row per player_id: xg_per_shot, avg_shot_distance,
        shot_conversion_rate, open_play_shot_pct, headed_shot_pct.
    """
    if shots_df.empty:
        return pd.DataFrame(
            columns=["player_id", "xg_per_shot", "avg_shot_distance",
                     "shot_conversion_rate", "open_play_shot_pct", "headed_shot_pct"]
        )

    df = shots_df.copy()
    df["is_goal"] = (df["result"] == "Goal").astype(int)
    df["is_open_play"] = (df["situation"] == "OpenPlay").astype(int)
    df["is_header"] = (df["body_part"] == "Head").astype(int)
    df["shot_distance"] = np.sqrt(
        ((1.0 - df["x"]) * 105) ** 2 + ((df["y"] - 0.5) * 68) ** 2
    )

    agg = df.groupby("player_id").agg(
        total_shots=("xg", "count"),
        total_xg=("xg", "sum"),
        total_goals=("is_goal", "sum"),
        total_open_play=("is_open_play", "sum"),
        total_headers=("is_header", "sum"),
        avg_shot_distance=("shot_distance", "mean"),
    ).reset_index()

    agg["xg_per_shot"] = _safe_divide(agg["total_xg"], agg["total_shots"])
    agg["shot_conversion_rate"] = _safe_divide(agg["total_goals"], agg["total_shots"])
    agg["open_play_shot_pct"] = _safe_divide(agg["total_open_play"], agg["total_shots"])
    agg["headed_shot_pct"] = _safe_divide(agg["total_headers"], agg["total_shots"])

    logger.info("Shot features: %d players with shot data", len(agg))
    return agg[["player_id", "xg_per_shot", "avg_shot_distance", "shot_conversion_rate",
                "open_play_shot_pct", "headed_shot_pct"]]


def compute_scouting_features(
    injuries_df: pd.DataFrame,
    transfers_df: pd.DataFrame,
    reference_date: date,
) -> pd.DataFrame:
    """Compute injury and transfer history features.

    Note: total_injury_days is NOT computable — player_injuries only has injury_date
    (report date), not end_date. Use injury_count + days_since_last_injury instead.

    Args:
        injuries_df: DataFrame from player_injuries. Columns: player_id, injury_date.
        transfers_df: DataFrame from player_transfers. Column: player_id.
        reference_date: Date to compute days_since_last_injury from (typically today).

    Returns:
        DataFrame with columns: player_id, injury_count, transfer_count,
        days_since_last_injury. One row per unique player across both inputs.
    """
    transfer_counts = transfers_df.groupby("player_id").size().reset_index(name="transfer_count")

    if not injuries_df.empty:
        inj = injuries_df.copy()
        inj["injury_date"] = pd.to_datetime(inj["injury_date"])
        inj_agg = inj.groupby("player_id").agg(
            injury_count=("injury_date", "count"),
            last_injury_date=("injury_date", "max"),
        ).reset_index()
        ref_ts = pd.Timestamp(reference_date)
        inj_agg["days_since_last_injury"] = (ref_ts - inj_agg["last_injury_date"]).dt.days
        inj_agg = inj_agg.drop(columns=["last_injury_date"])
    else:
        inj_agg = pd.DataFrame(columns=["player_id", "injury_count", "days_since_last_injury"])

    result = transfer_counts.merge(inj_agg, on="player_id", how="outer")
    result["transfer_count"] = result["transfer_count"].fillna(0).astype(int)
    result["injury_count"] = result["injury_count"].fillna(0).astype(int)

    logger.info("Scouting features: %d players", len(result))
    return result[["player_id", "injury_count", "transfer_count", "days_since_last_injury"]]
