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
