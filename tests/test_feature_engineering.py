"""Unit tests for feature engineering and the PlayerSeasonFeatures model.

Tests validate the Pydantic model for derived metrics at the player-season level.
"""

from __future__ import annotations

import pytest

from pipeline.models.features import PlayerSeasonFeatures


def test_player_season_features_complete_record():
    """Test a complete PlayerSeasonFeatures record with all fields populated."""
    f = PlayerSeasonFeatures(
        player_id=1,
        canonical_name="Pedri",
        known_name="Pedri",
        season="2024/2025",
        team_id=3,
        position="Midfielder",
        minutes=2700,
        appearances=30,
        starts=30,
        minutes_pct=0.789,
        games_started_pct=1.0,
        goals_per_90=0.17,
        assists_per_90=0.27,
        shots_per_90=1.67,
        key_passes_per_90=1.5,
        tackles_per_90=0.83,
        shots_on_target_pct=0.40,
        dribble_success_rate=0.65,
        duels_won_pct=0.52,
        xg_overperformance=0.5,
        npxg_per_90=0.14,
        xa_per_90=0.23,
        xg_chain_share=0.12,
        xg_buildup_per_90=0.38,
        xg_per_shot=0.12,
        avg_shot_distance=18.5,
        shot_conversion_rate=0.10,
        open_play_shot_pct=0.80,
        headed_shot_pct=0.10,
        injury_count=1,
        transfer_count=0,
        days_since_last_injury=120,
        pct_goals_per_90=75.0,
        pct_assists_per_90=82.0,
        pct_xg_overperformance=68.0,
        pct_npxg_per_90=72.0,
        pct_xg_per_shot=60.0,
        pct_shot_conversion_rate=55.0,
        pct_tackles_per_90=40.0,
    )
    assert f.goals_per_90 == pytest.approx(0.17)
    assert f.minutes_pct == pytest.approx(0.789)


def test_player_season_features_null_advanced_fields():
    """Player with no Understat match: xG/shot fields should be None."""
    f = PlayerSeasonFeatures(
        player_id=2,
        canonical_name="Unknown",
        known_name=None,
        season="2024/2025",
        team_id=1,
        position="Defender",
        minutes=900,
        appearances=10,
        starts=10,
        minutes_pct=0.263,
        games_started_pct=1.0,
        goals_per_90=0.0,
        assists_per_90=0.0,
        shots_per_90=0.5,
        key_passes_per_90=0.3,
        tackles_per_90=2.1,
        shots_on_target_pct=0.3,
        dribble_success_rate=0.5,
        duels_won_pct=0.48,
        xg_overperformance=None,
        npxg_per_90=None,
        xa_per_90=None,
        xg_chain_share=None,
        xg_buildup_per_90=None,
        xg_per_shot=None,
        avg_shot_distance=None,
        shot_conversion_rate=None,
        open_play_shot_pct=None,
        headed_shot_pct=None,
        injury_count=0,
        transfer_count=0,
        days_since_last_injury=None,
        pct_goals_per_90=None,
        pct_assists_per_90=None,
        pct_xg_overperformance=None,
        pct_npxg_per_90=None,
        pct_xg_per_shot=None,
        pct_shot_conversion_rate=None,
        pct_tackles_per_90=None,
    )
    assert f.xg_overperformance is None
    assert f.injury_count == 0


import pandas as pd
import numpy as np
from pipeline.feature_engineering import compute_per90_features


def _make_stats_row(**overrides) -> dict:
    base = {
        "player_id": 1, "canonical_name": "Pedri", "known_name": "Pedri",
        "team_id": 3, "season": "2024/2025", "position": "Midfielder",
        "appearances": 30, "starts": 28, "minutes": 2700,
        "goals": 5, "assists": 8, "shots_total": 50, "shots_on_target": 20,
        "key_passes": 45, "tackles": 25,
        "dribbles_attempted": 60, "dribbles_successful": 40,
        "duels_total": 100, "duels_won": 52,
    }
    base.update(overrides)
    return base


def test_per90_basic_calculation():
    """Each (player, team) row gets its own per-90 metrics."""
    df = pd.DataFrame([_make_stats_row(starts=28)])
    # max(starts) = 28 → season_max_minutes = 28 * 90 = 2520
    result = compute_per90_features(df)
    assert len(result) == 1
    row = result.iloc[0]
    assert row["goals_per_90"] == pytest.approx(5 / (2700 / 90), abs=1e-4)
    assert row["assists_per_90"] == pytest.approx(8 / (2700 / 90), abs=1e-4)
    assert row["minutes_pct"] == pytest.approx(2700 / (28 * 90), abs=1e-4)
    assert row["games_started_pct"] == pytest.approx(28 / 30.0, abs=1e-4)
    assert row["shots_on_target_pct"] == pytest.approx(20 / 50.0, abs=1e-4)
    assert row["dribble_success_rate"] == pytest.approx(40 / 60.0, abs=1e-4)
    assert row["duels_won_pct"] == pytest.approx(52 / 100.0, abs=1e-4)


def test_per90_filters_min_minutes():
    df = pd.DataFrame([
        _make_stats_row(player_id=1, minutes=2700),
        _make_stats_row(player_id=2, minutes=300, team_id=2),  # Below 450 min
    ])
    result = compute_per90_features(df)
    assert len(result) == 1
    assert result.iloc[0]["player_id"] == 1


def test_per90_keeps_both_rows_for_transferred_player():
    """Two stints for the same player (different teams) stay as two rows."""
    df = pd.DataFrame([
        _make_stats_row(player_id=1, team_id=1, minutes=1000, goals=3, starts=11),
        _make_stats_row(player_id=1, team_id=2, minutes=1500, goals=5, starts=17),
    ])
    result = compute_per90_features(df)
    assert len(result) == 2
    # Each row has its own per-90
    row1 = result[result["team_id"] == 1].iloc[0]
    assert row1["goals_per_90"] == pytest.approx(3 / (1000 / 90), abs=1e-4)
    row2 = result[result["team_id"] == 2].iloc[0]
    assert row2["goals_per_90"] == pytest.approx(5 / (1500 / 90), abs=1e-4)


def test_per90_zero_division_safety():
    """Player with 0 shots_total should have shots_on_target_pct = NaN."""
    df = pd.DataFrame([_make_stats_row(shots_total=0, shots_on_target=0)])
    result = compute_per90_features(df)
    assert pd.isna(result.iloc[0]["shots_on_target_pct"])


# ---------------------------------------------------------------------------
# Task 3: compute_xg_features
# ---------------------------------------------------------------------------
from pipeline.feature_engineering import compute_xg_features


def _make_advanced_row(**overrides) -> dict:
    base = {
        "player_id": 1, "team_id": 3, "season": "2024/2025",
        "xg": 4.5, "xa": 7.2, "npxg": 4.0,
        "xg_chain": 12.0, "xg_buildup": 6.0,
    }
    base.update(overrides)
    return base


def test_xg_features_basic():
    per90_df = pd.DataFrame([_make_stats_row(goals=5, minutes=2700, starts=30)])
    per90_df = compute_per90_features(per90_df)
    # Team has 2 players: player 1 with xg_chain=12, player 99 with xg_chain=8
    advanced_df = pd.DataFrame([
        _make_advanced_row(player_id=1, xg=4.5, xg_chain=12.0, xg_buildup=6.0, xa=7.2, npxg=4.0),
        _make_advanced_row(player_id=99, team_id=3, xg_chain=8.0, xg=0.5, xa=0.2, npxg=0.4, xg_buildup=2.0),
    ])
    result = compute_xg_features(per90_df, advanced_df)
    row = result[result["player_id"] == 1].iloc[0]
    assert row["xg_overperformance"] == pytest.approx(5 - 4.5, abs=1e-4)
    assert row["npxg_per_90"] == pytest.approx(4.0 / (2700 / 90), abs=1e-4)
    assert row["xg_chain_share"] == pytest.approx(12.0 / 20.0, abs=1e-4)  # 12/(12+8)
    assert row["xg_buildup_per_90"] == pytest.approx(6.0 / (2700 / 90), abs=1e-4)
    assert row["xa_per_90"] == pytest.approx(7.2 / (2700 / 90), abs=1e-4)


def test_xg_features_missing_understat():
    """Player with no Understat row should have None for all xG fields."""
    per90_df = pd.DataFrame([_make_stats_row(player_id=1, goals=5, minutes=2700, starts=30)])
    per90_df = compute_per90_features(per90_df)
    advanced_df = pd.DataFrame(columns=["player_id", "team_id", "season", "xg", "xa", "npxg", "xg_chain", "xg_buildup"])
    result = compute_xg_features(per90_df, advanced_df)
    row = result[result["player_id"] == 1].iloc[0]
    assert pd.isna(row["xg_overperformance"])
    assert pd.isna(row["npxg_per_90"])


# ---------------------------------------------------------------------------
# Task 4: compute_shot_features
# ---------------------------------------------------------------------------
import math
from pipeline.feature_engineering import compute_shot_features


def _make_shot(**overrides) -> dict:
    base = {
        "player_id": 1, "team_id": 3, "season": "2024/2025",
        "x": 0.85, "y": 0.5, "xg": 0.15, "result": "SavedShot",
        "situation": "OpenPlay", "body_part": "Right Foot",
    }
    base.update(overrides)
    return base


def test_shot_features_basic():
    shots = pd.DataFrame([
        _make_shot(xg=0.2, result="Goal", situation="OpenPlay", body_part="Right Foot", x=0.9, y=0.5),
        _make_shot(xg=0.1, result="SavedShot", situation="SetPiece", body_part="Head", x=0.8, y=0.5),
        _make_shot(xg=0.3, result="Goal", situation="OpenPlay", body_part="Right Foot", x=0.85, y=0.5),
    ])
    result = compute_shot_features(shots)
    assert len(result) == 1
    row = result.iloc[0]
    assert row["xg_per_shot"] == pytest.approx((0.2 + 0.1 + 0.3) / 3, abs=1e-4)
    assert row["shot_conversion_rate"] == pytest.approx(2 / 3, abs=1e-4)
    assert row["open_play_shot_pct"] == pytest.approx(2 / 3, abs=1e-4)
    assert row["headed_shot_pct"] == pytest.approx(1 / 3, abs=1e-4)
    # avg_shot_distance: distance from goal (1.0, 0.5) using pitch 105x68m
    expected_dist = (
        math.sqrt(((1 - 0.9) * 105) ** 2 + ((0.5 - 0.5) * 68) ** 2)
        + math.sqrt(((1 - 0.8) * 105) ** 2 + ((0.5 - 0.5) * 68) ** 2)
        + math.sqrt(((1 - 0.85) * 105) ** 2 + ((0.5 - 0.5) * 68) ** 2)
    ) / 3
    assert row["avg_shot_distance"] == pytest.approx(expected_dist, abs=0.01)


def test_shot_features_empty_returns_empty():
    shots = pd.DataFrame(columns=["player_id", "team_id", "season", "x", "y", "xg", "result", "situation", "body_part"])
    result = compute_shot_features(shots)
    assert len(result) == 0
