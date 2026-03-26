"""Pydantic models for the CLEAN layer — entity resolution outputs.

These models represent the resolved entities (teams and players) after
cross-source matching between API-Football and Understat.
"""

from __future__ import annotations

from datetime import date, datetime
from typing import Literal

from pydantic import BaseModel, ConfigDict, Field

ResolutionMethod = Literal["exact", "fuzzy", "contextual", "statistical", "unresolved"]


class ResolvedTeam(BaseModel):
    """A team resolved across API-Football and Understat."""

    model_config = ConfigDict(frozen=True)

    canonical_name: str
    api_football_id: int = Field(ge=1)
    api_football_name: str
    understat_name: str | None = None
    # Team identity metadata
    country: str | None = None
    logo_url: str | None = None
    code: str | None = None
    founded: int | None = None
    # Venue metadata
    venue_name: str | None = None
    venue_address: str | None = None
    venue_city: str | None = None
    venue_capacity: int | None = None
    venue_surface: str | None = None
    venue_image_url: str | None = None
    resolution_confidence: float | None = Field(default=None, ge=0.0, le=1.0)
    resolution_method: ResolutionMethod | None = None
    resolved_at: datetime | None = None


class ResolvedPlayer(BaseModel):
    """A player resolved across API-Football and Understat."""

    model_config = ConfigDict(frozen=True)

    canonical_name: str
    known_name: str | None = None
    api_football_id: int | None = Field(default=None, ge=1)
    understat_id: int | None = Field(default=None, ge=1)
    birth_date: date | None = None
    nationality: str | None = None
    photo_url: str | None = None
    resolution_confidence: float | None = Field(default=None, ge=0.0, le=1.0)
    resolution_method: ResolutionMethod | None = None
    resolved_at: datetime | None = None


class CandidateMatch(BaseModel):
    """A potential match candidate for an unresolved player."""

    model_config = ConfigDict(frozen=True)

    candidate_name: str
    candidate_source: str
    candidate_source_id: int
    fuzzy_score: float = Field(ge=0.0, le=1.0)


class UnresolvedPlayer(BaseModel):
    """A player that could not be resolved, with its top candidates."""

    model_config = ConfigDict(frozen=True)

    source: str
    player_id: int
    player_name: str
    team: str | None = None
    top_candidates: list[CandidateMatch] = Field(default_factory=list)


class ResolutionResult(BaseModel):
    """Complete output of the entity resolution process."""

    model_config = ConfigDict(frozen=True)

    resolved_players: list[ResolvedPlayer] = Field(default_factory=list)
    unresolved: list[UnresolvedPlayer] = Field(default_factory=list)
