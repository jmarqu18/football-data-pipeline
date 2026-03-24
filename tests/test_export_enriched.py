"""Unit and integration tests for the ENRICHED export module.

Tests validate zone assignment logic, flat view assembly, and end-to-end
SQLite output without requiring a live PostgreSQL database.
"""

from __future__ import annotations

import sqlite3

import pandas as pd
import pytest
from sqlalchemy import create_engine, text

from pipeline.export_enriched import _assign_zone, build_flat_view, build_shots_table, run_export_enriched


# ---------------------------------------------------------------------------
# Zone assignment (_assign_zone)
# ---------------------------------------------------------------------------


def test_assign_zone_inside_box_center():
    """Central shot inside the penalty area maps to inside_box_center."""
    assert _assign_zone(0.9, 0.5) == "inside_box_center"


def test_assign_zone_boundaries():
    """Zone boundaries map correctly across depth and lane dimensions."""
    assert _assign_zone(0.75, 0.5) == "zona_14_center"
    assert _assign_zone(0.75, 0.15) == "zona_14_left_flank"
    assert _assign_zone(0.75, 0.28) == "zona_14_left_halfspace"
    assert _assign_zone(0.75, 0.72) == "zona_14_right_halfspace"
    assert _assign_zone(0.75, 0.85) == "zona_14_right_flank"
    assert _assign_zone(0.50, 0.5) == "outside_center"
    # Edge of inside_box threshold
    assert _assign_zone(0.84, 0.5) == "inside_box_center"
    assert _assign_zone(0.83, 0.5) == "zona_14_center"


# ---------------------------------------------------------------------------
# build_flat_view
# ---------------------------------------------------------------------------


def _make_enriched_engine():
    """Create an in-memory SQLite engine with minimal CLEAN tables."""
    engine = create_engine("sqlite:///:memory:")
    with engine.begin() as conn:
        conn.execute(text("""
            CREATE TABLE players (
                player_id INTEGER PRIMARY KEY,
                canonical_name TEXT,
                photo_url TEXT,
                resolution_confidence REAL,
                resolution_method TEXT,
                api_football_id INTEGER,
                understat_id INTEGER,
                birth_date TEXT,
                nationality TEXT
            )
        """))
        conn.execute(text("""
            CREATE TABLE teams (
                team_id INTEGER PRIMARY KEY,
                canonical_name TEXT,
                logo_url TEXT
            )
        """))
        conn.execute(text("""
            CREATE TABLE player_profile (
                player_id INTEGER PRIMARY KEY,
                height_cm INTEGER,
                weight_kg INTEGER
            )
        """))
        conn.execute(text("""
            CREATE TABLE player_shots (
                shot_id INTEGER PRIMARY KEY,
                player_id INTEGER,
                team_id INTEGER,
                season TEXT,
                minute INTEGER,
                result TEXT,
                x REAL,
                y REAL,
                xg REAL,
                situation TEXT,
                body_part TEXT
            )
        """))
        conn.execute(text(
            "INSERT INTO players VALUES (1, 'Pedri', 'https://photo.url/pedri.jpg', 0.95, 'fuzzy', 101, 201, '2002-11-25', 'Spanish')"
        ))
        conn.execute(text(
            "INSERT INTO teams VALUES (3, 'FC Barcelona', 'https://logo.url/barca.png')"
        ))
        conn.execute(text("INSERT INTO player_profile VALUES (1, 174, 68)"))
        conn.execute(text(
            "INSERT INTO player_shots VALUES (1, 1, 3, '2024/2025', 65, 'Goal', 0.90, 0.50, 0.35, 'OpenPlay', 'Right Foot')"
        ))
    return engine


def _make_features_parquet(tmp_path, season: str = "2024/2025") -> object:
    """Write a minimal player_season_features.parquet to tmp_path."""
    features_path = tmp_path / "player_season_features.parquet"
    df = pd.DataFrame([{
        "player_id": 1,
        "canonical_name": "Pedri",
        "known_name": "Pedri",
        "season": season,
        "team_id": 3,
        "position": "Midfielder",
        "minutes": 2700,
        "appearances": 30,
        "starts": 28,
        "minutes_pct": 0.789,
        "games_started_pct": 0.933,
        "goals_per_90": 0.17,
        "assists_per_90": 0.27,
        "shots_per_90": 1.67,
        "key_passes_per_90": 1.5,
        "tackles_per_90": 0.83,
        "shots_on_target_pct": 0.4,
        "dribble_success_rate": 0.65,
        "duels_won_pct": 0.52,
        "xg_overperformance": 0.5,
        "npxg_per_90": 0.14,
        "xa_per_90": 0.23,
        "xg_chain_share": 0.12,
        "xg_buildup_per_90": 0.38,
        "xg_per_shot": 0.12,
        "avg_shot_distance": 18.5,
        "shot_conversion_rate": 0.10,
        "open_play_shot_pct": 0.8,
        "headed_shot_pct": 0.1,
        "injury_count": 1,
        "transfer_count": 0,
        "days_since_last_injury": 120,
        "pct_goals_per_90": 75.0,
        "pct_assists_per_90": 82.0,
        "pct_xg_overperformance": 68.0,
        "pct_npxg_per_90": 72.0,
        "pct_xg_per_shot": 60.0,
        "pct_shot_conversion_rate": 55.0,
        "pct_tackles_per_90": 40.0,
    }])
    df.to_parquet(features_path, index=False)
    return features_path


def test_build_flat_view_merges_columns(tmp_path):
    """Flat view joins player metadata, team name/logo and physical profile."""
    engine = _make_enriched_engine()
    features_path = _make_features_parquet(tmp_path)

    result = build_flat_view(engine, features_path, season="2024/2025")

    assert len(result) == 1
    row = result.iloc[0]
    # From features Parquet
    assert row["canonical_name"] == "Pedri"
    assert row["goals_per_90"] == pytest.approx(0.17)
    # From players table
    assert row["resolution_confidence"] == pytest.approx(0.95)
    assert row["photo_url"] == "https://photo.url/pedri.jpg"
    assert row["nationality"] == "Spanish"
    # From teams table
    assert row["team_name"] == "FC Barcelona"
    assert row["logo_url"] == "https://logo.url/barca.png"
    # From player_profile table
    assert row["height_cm"] == 174
    assert row["weight_kg"] == 68


# ---------------------------------------------------------------------------
# run_export_enriched (integration)
# ---------------------------------------------------------------------------


def test_run_export_enriched_writes_tables(tmp_path):
    """End-to-end: enriched.db is created with both tables and correct row counts."""
    engine = _make_enriched_engine()
    features_path = _make_features_parquet(tmp_path)
    output_path = tmp_path / "enriched" / "enriched.db"

    stats = run_export_enriched(
        output_path=output_path,
        features_path=features_path,
        season="2024/2025",
        engine=engine,
    )

    assert output_path.exists()
    assert stats["players_written"] == 1
    assert stats["shots_written"] == 1

    with sqlite3.connect(output_path) as conn:
        tables = {r[0] for r in conn.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()}
        assert "player_season_stats_flat" in tables
        assert "player_shots" in tables

        flat_count = conn.execute("SELECT COUNT(*) FROM player_season_stats_flat").fetchone()[0]
        shots_count = conn.execute("SELECT COUNT(*) FROM player_shots").fetchone()[0]
        assert flat_count == 1
        assert shots_count == 1

        zone = conn.execute("SELECT zone FROM player_shots LIMIT 1").fetchone()[0]
        assert zone == "inside_box_center"
