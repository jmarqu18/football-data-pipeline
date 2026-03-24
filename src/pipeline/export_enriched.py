"""Export FEATURES → ENRICHED: builds SQLite database for Datasette.

Reads player_season_features.parquet (FEATURES layer) and supplements it with
player/team metadata from PostgreSQL (CLEAN layer) to produce a fully
denormalized SQLite database with two tables:

- ``player_season_stats_flat``: one row per (player, team, season) stint with
  all metrics, profile data, photo/logo URLs, and resolution confidence.
- ``player_shots``: shot-level data with Juego de Posición zone labels.
"""

from __future__ import annotations

import logging
import sqlite3
from pathlib import Path

import pandas as pd
from sqlalchemy import text
from sqlalchemy.engine import Engine

from pipeline.db import get_engine

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Zone assignment — Juego de Posición grid
# ---------------------------------------------------------------------------

_ZONE_X_INSIDE_BOX = 0.83  # ~17m from goal on 105m pitch
_ZONE_X_ZONA_14 = 0.70  # edge of zona 14 (space in front of box)
_ZONE_Y_LEFT_FLANK = 0.20
_ZONE_Y_LEFT_HALFSPACE = 0.35
_ZONE_Y_RIGHT_HALFSPACE = 0.65
_ZONE_Y_RIGHT_FLANK = 0.80


def _assign_zone(x: float, y: float) -> str:
    """Map Understat normalized (x, y) coordinates to a Juego de Posición zone.

    Coordinates are normalized [0, 1] where x=1.0 is the attacking goal and
    y=0 is the left flank (from the attacking team's perspective).

    Args:
        x: Normalized pitch length coordinate (0 = own goal, 1 = opponent goal).
        y: Normalized pitch width coordinate (0 = left, 1 = right).

    Returns:
        Zone label in the format ``"{depth}_{lane}"``, e.g. ``"inside_box_center"``.
    """
    if x > _ZONE_X_INSIDE_BOX:
        depth = "inside_box"
    elif x > _ZONE_X_ZONA_14:
        depth = "zona_14"
    else:
        depth = "outside"

    if y < _ZONE_Y_LEFT_FLANK:
        lane = "left_flank"
    elif y < _ZONE_Y_LEFT_HALFSPACE:
        lane = "left_halfspace"
    elif y < _ZONE_Y_RIGHT_HALFSPACE:
        lane = "center"
    elif y < _ZONE_Y_RIGHT_FLANK:
        lane = "right_halfspace"
    else:
        lane = "right_flank"

    return f"{depth}_{lane}"


# ---------------------------------------------------------------------------
# Data assembly
# ---------------------------------------------------------------------------


def build_flat_view(engine: Engine, features_path: Path, season: str) -> pd.DataFrame:
    """Assemble the denormalized player_season_stats_flat DataFrame.

    Reads the FEATURES Parquet file and left-joins supplementary columns from
    PostgreSQL: player identity/metadata, team name/logo, and physical profile.

    Args:
        engine: SQLAlchemy engine connected to the CLEAN PostgreSQL database.
        features_path: Path to ``player_season_features.parquet``.
        season: Season string to filter on, e.g. ``"2024/2025"``.

    Returns:
        DataFrame with ~55 columns ready to be written to SQLite.
    """
    logger.info("Reading features Parquet: %s", features_path)
    features_df = pd.read_parquet(features_path)
    features_df = features_df[features_df["season"] == season].copy()
    logger.info("Features rows for season %s: %d", season, len(features_df))

    players_df = pd.read_sql(
        """
        SELECT player_id,
               photo_url,
               resolution_confidence,
               resolution_method,
               api_football_id,
               understat_id,
               birth_date,
               nationality
        FROM players
        """,
        engine,
    )

    teams_df = pd.read_sql(
        "SELECT team_id, canonical_name AS team_name, logo_url FROM teams",
        engine,
    )

    profile_df = pd.read_sql(
        "SELECT player_id, height_cm, weight_kg FROM player_profile",
        engine,
    )

    flat_df = (
        features_df.merge(players_df, on="player_id", how="left")
        .merge(profile_df, on="player_id", how="left")
        .merge(teams_df, on="team_id", how="left")
    )

    logger.info("Flat view assembled: %d rows, %d columns", len(flat_df), len(flat_df.columns))
    return flat_df


def build_shots_table(engine: Engine, season: str) -> pd.DataFrame:
    """Load player_shots from PostgreSQL, add canonical names and zone labels.

    Args:
        engine: SQLAlchemy engine connected to the CLEAN PostgreSQL database.
        season: Season string to filter on, e.g. ``"2024/2025"``.

    Returns:
        DataFrame with shot-level data including Juego de Posición ``zone`` column.
    """
    shots_df = pd.read_sql(
        text(
            """
            SELECT
                ps.shot_id,
                ps.player_id,
                pl.canonical_name,
                ps.team_id,
                t.canonical_name AS team_name,
                ps.season,
                ps.minute,
                ps.result,
                ps.x,
                ps.y,
                ps.xg,
                ps.situation,
                ps.body_part
            FROM player_shots ps
            JOIN players pl ON ps.player_id = pl.player_id
            JOIN teams t ON ps.team_id = t.team_id
            WHERE ps.season = :season
            """
        ),
        engine,
        params={"season": season},
    )

    shots_df["zone"] = shots_df.apply(
        lambda r: _assign_zone(r["x"], r["y"]), axis=1
    )

    logger.info("Shots loaded for season %s: %d rows", season, len(shots_df))
    return shots_df


# ---------------------------------------------------------------------------
# Orchestrator
# ---------------------------------------------------------------------------


def run_export_enriched(
    output_path: Path,
    features_path: Path,
    season: str,
    engine: Engine | None = None,
) -> dict:
    """Export FEATURES + CLEAN data to a SQLite database for Datasette.

    Creates (or replaces) the SQLite file at ``output_path`` with two tables:
    ``player_season_stats_flat`` and ``player_shots``.

    Args:
        output_path: Destination path for the ``.db`` file.
        features_path: Path to ``player_season_features.parquet``.
        season: Season string, e.g. ``"2024/2025"``.
        engine: Optional SQLAlchemy engine. Defaults to ``get_engine()``.
            Pass an explicit engine in tests to avoid needing a live database.

    Returns:
        Stats dict with keys ``players_written``, ``shots_written``, ``output_path``.
    """
    output_path.parent.mkdir(parents=True, exist_ok=True)
    if engine is None:
        engine = get_engine()

    flat_df = build_flat_view(engine, features_path, season)
    shots_df = build_shots_table(engine, season)

    logger.info("Writing SQLite database to %s", output_path)
    with sqlite3.connect(output_path) as conn:
        flat_df.to_sql("player_season_stats_flat", conn, if_exists="replace", index=False)
        shots_df.to_sql("player_shots", conn, if_exists="replace", index=False)

    stats = {
        "players_written": len(flat_df),
        "shots_written": len(shots_df),
        "output_path": str(output_path),
    }
    logger.info(
        "Export complete — players: %d, shots: %d → %s",
        stats["players_written"],
        stats["shots_written"],
        stats["output_path"],
    )
    return stats
