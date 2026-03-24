"""Pydantic models for the FEATURES layer of the pipeline.

These models represent derived metrics and percentiles calculated from
the CLEAN layer data. Feature engineering happens at the player-season level
(player_id + team_id + season).
"""

from __future__ import annotations

from pydantic import BaseModel, ConfigDict, Field


class PlayerSeasonFeatures(BaseModel):
    """Derived metrics for a player-season stint (player_id + team_id + season).

    Only produced for stints with >= 450 minutes played.
    """

    model_config = ConfigDict(frozen=True, extra="forbid")

    # ─────────────────────────────────────────────────────────────
    # Identity
    # ─────────────────────────────────────────────────────────────
    player_id: int
    canonical_name: str
    known_name: str | None
    season: str
    team_id: int
    position: str | None

    # ─────────────────────────────────────────────────────────────
    # Contextual (appearance-level aggregates)
    # ─────────────────────────────────────────────────────────────
    minutes: int = Field(ge=0)
    appearances: int = Field(ge=0)
    starts: int = Field(ge=0)
    minutes_pct: float | None = Field(None, ge=0.0, le=1.0)
    games_started_pct: float | None = Field(None, ge=0.0, le=1.0)

    # ─────────────────────────────────────────────────────────────
    # Per-90 metrics (from player_season_stats)
    # ─────────────────────────────────────────────────────────────
    goals_per_90: float | None = Field(None, ge=0.0)
    assists_per_90: float | None = Field(None, ge=0.0)
    shots_per_90: float | None = Field(None, ge=0.0)
    key_passes_per_90: float | None = Field(None, ge=0.0)
    tackles_per_90: float | None = Field(None, ge=0.0)
    shots_on_target_pct: float | None = Field(None, ge=0.0, le=1.0)
    dribble_success_rate: float | None = Field(None, ge=0.0, le=1.0)
    duels_won_pct: float | None = Field(None, ge=0.0, le=1.0)

    # ─────────────────────────────────────────────────────────────
    # xG advanced (from player_season_advanced — None if no Understat match)
    # ─────────────────────────────────────────────────────────────
    xg_overperformance: float | None  # goals - xg; can be negative, no bound constraint
    npxg_per_90: float | None = Field(None, ge=0.0)
    xa_per_90: float | None = Field(None, ge=0.0)
    xg_chain_share: float | None = Field(None, ge=0.0)
    xg_buildup_per_90: float | None = Field(None, ge=0.0)

    # ─────────────────────────────────────────────────────────────
    # Shot quality (from player_shots — None if no Understat shots)
    # ─────────────────────────────────────────────────────────────
    xg_per_shot: float | None = Field(None, ge=0.0, le=1.0)
    avg_shot_distance: float | None = Field(None, ge=0.0)
    shot_conversion_rate: float | None = Field(None, ge=0.0, le=1.0)
    open_play_shot_pct: float | None = Field(None, ge=0.0, le=1.0)
    headed_shot_pct: float | None = Field(None, ge=0.0, le=1.0)

    # ─────────────────────────────────────────────────────────────
    # Scouting (from player_injuries + player_transfers)
    # ─────────────────────────────────────────────────────────────
    injury_count: int = Field(ge=0)
    transfer_count: int = Field(ge=0)
    days_since_last_injury: int | None = Field(None, ge=0)

    # ─────────────────────────────────────────────────────────────
    # Percentiles by position (None if underlying metric is None)
    # ─────────────────────────────────────────────────────────────
    pct_goals_per_90: float | None = Field(None, ge=0.0, le=100.0)
    pct_assists_per_90: float | None = Field(None, ge=0.0, le=100.0)
    pct_xg_overperformance: float | None = Field(None, ge=0.0, le=100.0)
    pct_npxg_per_90: float | None = Field(None, ge=0.0, le=100.0)
    pct_xg_per_shot: float | None = Field(None, ge=0.0, le=100.0)
    pct_shot_conversion_rate: float | None = Field(None, ge=0.0, le=100.0)
    pct_tackles_per_90: float | None = Field(None, ge=0.0, le=100.0)
