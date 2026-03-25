"""Feature engineering: construcción de métricas derivadas (CLEAN → FEATURES)."""

from __future__ import annotations

import logging
import math
from datetime import date
from pathlib import Path

import numpy as np
import pandas as pd
from sqlalchemy import Engine, text

from pipeline.db import get_engine
from pipeline.models.features import PlayerSeasonFeatures

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
    xg_cols = (
        "xg_overperformance",
        "npxg_per_90",
        "xa_per_90",
        "xg_chain_share",
        "xg_buildup_per_90",
    )

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
        adv[
            ["player_id", "team_id", "xg", "xa", "npxg", "xg_chain", "xg_buildup", "team_xg_chain"]
        ],
        on=["player_id", "team_id"],
        how="left",
    )

    nineties = merged["minutes"] / 90.0
    merged["xg_overperformance"] = np.where(
        merged["xg"].notna(), merged["goals"] - merged["xg"], np.nan
    )
    merged["npxg_per_90"] = np.where(merged["npxg"].notna(), merged["npxg"] / nineties, np.nan)
    merged["xa_per_90"] = np.where(merged["xa"].notna(), merged["xa"] / nineties, np.nan)
    merged["xg_chain_share"] = _safe_divide(merged["xg_chain"], merged["team_xg_chain"])
    merged["xg_buildup_per_90"] = np.where(
        merged["xg_buildup"].notna(), merged["xg_buildup"] / nineties, np.nan
    )

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
            columns=[
                "player_id",
                "xg_per_shot",
                "avg_shot_distance",
                "shot_conversion_rate",
                "open_play_shot_pct",
                "headed_shot_pct",
            ]
        )

    df = shots_df.copy()
    df["is_goal"] = (df["result"] == "Goal").astype(int)
    df["is_open_play"] = (df["situation"] == "OpenPlay").astype(int)
    df["is_header"] = (df["body_part"] == "Head").astype(int)
    df["shot_distance"] = np.sqrt(((1.0 - df["x"]) * 105) ** 2 + ((df["y"] - 0.5) * 68) ** 2)

    agg = (
        df.groupby("player_id")
        .agg(
            total_shots=("xg", "count"),
            total_xg=("xg", "sum"),
            total_goals=("is_goal", "sum"),
            total_open_play=("is_open_play", "sum"),
            total_headers=("is_header", "sum"),
            avg_shot_distance=("shot_distance", "mean"),
        )
        .reset_index()
    )

    agg["xg_per_shot"] = _safe_divide(agg["total_xg"], agg["total_shots"])
    agg["shot_conversion_rate"] = _safe_divide(agg["total_goals"], agg["total_shots"])
    agg["open_play_shot_pct"] = _safe_divide(agg["total_open_play"], agg["total_shots"])
    agg["headed_shot_pct"] = _safe_divide(agg["total_headers"], agg["total_shots"])

    logger.info("Shot features: %d players with shot data", len(agg))
    return agg[
        [
            "player_id",
            "xg_per_shot",
            "avg_shot_distance",
            "shot_conversion_rate",
            "open_play_shot_pct",
            "headed_shot_pct",
        ]
    ]


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
        inj_agg = (
            inj.groupby("player_id")
            .agg(
                injury_count=("injury_date", "count"),
                last_injury_date=("injury_date", "max"),
            )
            .reset_index()
        )
        ref_ts = pd.Timestamp(reference_date)
        inj_agg["days_since_last_injury"] = (ref_ts - inj_agg["last_injury_date"]).dt.days.astype(
            float
        )
        inj_agg = inj_agg.drop(columns=["last_injury_date"])
    else:
        inj_agg = pd.DataFrame(
            {
                "player_id": pd.Series(dtype=int),
                "injury_count": pd.Series(dtype=float),
                "days_since_last_injury": pd.Series(dtype=float),
            }
        )

    result = transfer_counts.merge(inj_agg, on="player_id", how="outer")
    result["transfer_count"] = result["transfer_count"].fillna(0).astype(int)
    result["injury_count"] = result["injury_count"].fillna(0).astype(int)

    logger.info("Scouting features: %d players", len(result))
    return result[["player_id", "injury_count", "transfer_count", "days_since_last_injury"]]


_PERCENTILE_METRICS: list[tuple[str, str]] = [
    ("goals_per_90", "pct_goals_per_90"),
    ("assists_per_90", "pct_assists_per_90"),
    ("xg_overperformance", "pct_xg_overperformance"),
    ("npxg_per_90", "pct_npxg_per_90"),
    ("xg_per_shot", "pct_xg_per_shot"),
    ("shot_conversion_rate", "pct_shot_conversion_rate"),
    ("tackles_per_90", "pct_tackles_per_90"),
]


def compute_percentiles(df: pd.DataFrame) -> pd.DataFrame:
    """Add percentile rank columns for key metrics, grouped by position.

    Only non-null values participate in ranking. Players with a null metric
    receive NaN for that percentile column.

    Args:
        df: DataFrame with columns: player_id, position, and all metric columns
            listed in _PERCENTILE_METRICS.

    Returns:
        df with added pct_* columns (0-100 scale).
    """
    result = df.copy()
    for metric, pct_col in _PERCENTILE_METRICS:
        if metric not in result.columns:
            result[pct_col] = np.nan
            continue
        result[pct_col] = (
            result.groupby("position", group_keys=False)[metric].rank(pct=True, na_option="keep")
            * 100
        )
    logger.info(
        "Percentiles computed for %d stints across %d metrics",
        len(result),
        len(_PERCENTILE_METRICS),
    )
    return result


def _load_matchdays_played(raw_dir: Path | None, season_year: int) -> int | None:
    """Read matchdays_played from standings Parquet if available.

    Args:
        raw_dir: Root of data/raw/ directory. If None, standings not loaded.
        season_year: API-Football season year (e.g. 2024 for 2024/25).

    Returns:
        Number of matchdays played, or None if standings file not found.
    """
    if raw_dir is None:
        return None
    standings_path = Path(raw_dir) / "api_football" / "standings.parquet"
    if not standings_path.exists():
        # The standings endpoint is not yet active in ingestion.yaml (planned for a
        # future sprint). Until then, this warning fires on every run and the
        # max(starts)*90 proxy is always used — this is the expected behaviour.
        logger.warning(
            "Standings file not found at %s — using max(starts) proxy for minutes_pct",
            standings_path,
        )
        return None
    df = pd.read_parquet(standings_path)
    season_df = df[df["season"] == season_year]
    if season_df.empty:
        return None
    matchdays = int(season_df["played_total"].max())
    logger.info("Standings: %d matchdays played (season %d)", matchdays, season_year)
    return matchdays


def load_clean_data(
    engine: Engine,
    season: str,
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """Load all 5 relevant CLEAN tables from PostgreSQL for a given season.

    Returns:
        Tuple: (stats_df, advanced_df, shots_df, injuries_df, transfers_df).
        stats_df includes canonical_name, known_name via JOIN with players table.
    """
    with engine.connect() as conn:
        stats_df = pd.read_sql(
            text("""
                SELECT pss.player_id, p.canonical_name, p.known_name,
                    pss.team_id, pss.season, pss.position,
                    pss.appearances, pss.starts, pss.minutes,
                    pss.goals, pss.assists, pss.shots_total, pss.shots_on_target,
                    pss.key_passes, pss.tackles,
                    pss.dribbles_attempted, pss.dribbles_successful,
                    pss.duels_total, pss.duels_won
                FROM player_season_stats pss
                JOIN players p ON pss.player_id = p.player_id
                WHERE pss.season = :season
            """),
            conn,
            params={"season": season},
        )
        advanced_df = pd.read_sql(
            text(
                "SELECT player_id, team_id, season, xg, xa, npxg, xg_chain, xg_buildup"
                " FROM player_season_advanced WHERE season = :season"
            ),
            conn,
            params={"season": season},
        )
        shots_df = pd.read_sql(
            text(
                "SELECT player_id, team_id, season, x, y, xg, result, situation, body_part"
                " FROM player_shots WHERE season = :season"
            ),
            conn,
            params={"season": season},
        )
        injuries_df = pd.read_sql(
            text("SELECT player_id, injury_date FROM player_injuries"),
            conn,
        )
        transfers_df = pd.read_sql(
            text("SELECT player_id FROM player_transfers"),
            conn,
        )

    logger.info(
        "Loaded: %d stat rows, %d advanced, %d shots, %d injuries, %d transfers",
        len(stats_df),
        len(advanced_df),
        len(shots_df),
        len(injuries_df),
        len(transfers_df),
    )
    return stats_df, advanced_df, shots_df, injuries_df, transfers_df


def run_feature_engineering(
    output_path: Path,
    season: str,
    engine: Engine | None = None,
    raw_dir: Path | None = None,
) -> dict[str, int]:
    """Orchestrate CLEAN → FEATURES pipeline.

    Reads from PostgreSQL, computes all feature groups, validates via Pydantic v2,
    writes to Parquet. Only stints with >= 450 minutes get feature records.

    Args:
        output_path: Destination Parquet file path.
        season: Season string, e.g. "2024/2025".
        engine: Optional SQLAlchemy engine. If None, reads DATABASE_URL env var.
        raw_dir: Optional path to data/raw/ for loading standings Parquet.
                 If None, minutes_pct falls back to max(starts)*90 proxy.

    Returns:
        Dict with stats: players_total, players_with_xg, players_written.
    """
    if engine is None:
        engine = get_engine()

    # Derive season_year for standings lookup (e.g. "2024/2025" → 2024)
    season_year = int(season.split("/")[0])
    matchdays_played = _load_matchdays_played(raw_dir, season_year)

    stats_df, advanced_df, shots_df, injuries_df, transfers_df = load_clean_data(engine, season)

    # Compute feature groups
    base = compute_per90_features(stats_df, matchdays_played=matchdays_played)
    base = compute_xg_features(base, advanced_df)

    shot_feats = compute_shot_features(shots_df)
    base = base.merge(shot_feats, on="player_id", how="left")

    scouting = compute_scouting_features(injuries_df, transfers_df, reference_date=date.today())
    base = base.merge(scouting, on="player_id", how="left")

    # Fill scouting defaults for players not in injuries/transfers tables
    base["injury_count"] = base["injury_count"].fillna(0).astype(int)
    base["transfer_count"] = base["transfer_count"].fillna(0).astype(int)

    base = compute_percentiles(base)

    # Clamp minutes_pct to [0, 1] — proxy denominator may undercount true matchdays
    if "minutes_pct" in base.columns:
        base["minutes_pct"] = base["minutes_pct"].clip(upper=1.0)

    # Select only columns declared in PlayerSeasonFeatures to avoid extra-field rejections
    model_fields = set(PlayerSeasonFeatures.model_fields.keys())
    cols_to_keep = [c for c in base.columns if c in model_fields]
    base = base[cols_to_keep]

    # Replace NaN with None so Pydantic Optional[float] fields receive None, not nan
    records = [
        {k: (None if isinstance(v, float) and math.isnan(v) else v) for k, v in row.items()}
        for row in base.to_dict(orient="records")
    ]
    validated: list[dict] = []
    rejected = 0
    for rec in records:
        try:
            validated.append(PlayerSeasonFeatures(**rec).model_dump())
        except Exception as exc:
            logger.warning("Rejected feature record player_id=%s: %s", rec.get("player_id"), exc)
            rejected += 1

    if not validated:
        logger.warning("No feature records to write for season %s", season)
        return {"players_total": len(base), "players_with_xg": 0, "players_written": 0}

    output_path = Path(output_path)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    pd.DataFrame(validated).to_parquet(output_path, index=False)

    players_with_xg = sum(1 for r in validated if r.get("xg_overperformance") is not None)
    logger.info(
        "Features written: %d stints (%d with xG data, %d rejected) → %s",
        len(validated),
        players_with_xg,
        rejected,
        output_path,
    )
    return {
        "players_total": len(base),
        "players_with_xg": players_with_xg,
        "players_written": len(validated),
    }
