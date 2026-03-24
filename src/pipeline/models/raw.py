"""Pydantic models for the RAW layer of the pipeline.

These models validate the minimum schema and basic types of the data
as it arrives from each source, **without transformation**.

Supported sources:
- API-Football (players, injuries, transfers)
- Understat (shots with xG + season-level advanced stats)
- FBref (player season statistics)

Reference JSON Schema: https://docs.pydantic.dev/latest/concepts/json_schema/
"""

from __future__ import annotations

from pydantic import BaseModel, ConfigDict, Field

# ─────────────────────────────────────────────────────────────
# API-Football — internal sub-models for nested stats categories
# ─────────────────────────────────────────────────────────────


class _APIFootballGames(BaseModel):
    """Games sub-object from API-Football statistics entry."""

    model_config = ConfigDict(frozen=True, extra="forbid")

    appearances: int | None = Field(default=None, ge=0)
    lineups: int | None = Field(default=None, ge=0)
    minutes: int | None = Field(default=None, ge=0)
    number: int | None = None
    position: str | None = None
    rating: str | None = None  # API returns "7.342857" as string
    captain: bool = False


class _APIFootballShots(BaseModel):
    """Shots sub-object from API-Football statistics entry."""

    model_config = ConfigDict(frozen=True, extra="forbid")

    total: int | None = Field(default=None, ge=0)
    on: int | None = Field(default=None, ge=0)


class _APIFootballGoals(BaseModel):
    """Goals sub-object from API-Football statistics entry."""

    model_config = ConfigDict(frozen=True, extra="forbid")

    total: int | None = Field(default=None, ge=0)
    conceded: int | None = Field(default=None, ge=0)
    assists: int | None = Field(default=None, ge=0)
    saves: int | None = Field(default=None, ge=0)


class _APIFootballPasses(BaseModel):
    """Passes sub-object from API-Football statistics entry."""

    model_config = ConfigDict(frozen=True, extra="forbid")

    total: int | None = Field(default=None, ge=0)
    key: int | None = Field(default=None, ge=0)
    accuracy: int | None = Field(default=None, ge=0, le=100)


class _APIFootballTackles(BaseModel):
    """Tackles sub-object from API-Football statistics entry."""

    model_config = ConfigDict(frozen=True, extra="forbid")

    total: int | None = Field(default=None, ge=0)
    blocks: int | None = Field(default=None, ge=0)
    interceptions: int | None = Field(default=None, ge=0)


class _APIFootballDuels(BaseModel):
    """Duels sub-object from API-Football statistics entry."""

    model_config = ConfigDict(frozen=True, extra="forbid")

    total: int | None = Field(default=None, ge=0)
    won: int | None = Field(default=None, ge=0)


class _APIFootballDribbles(BaseModel):
    """Dribbles sub-object from API-Football statistics entry."""

    model_config = ConfigDict(frozen=True, extra="forbid")

    attempts: int | None = Field(default=None, ge=0)
    success: int | None = Field(default=None, ge=0)
    past: int | None = Field(default=None, ge=0)


class _APIFootballFouls(BaseModel):
    """Fouls sub-object from API-Football statistics entry."""

    model_config = ConfigDict(frozen=True, extra="forbid")

    drawn: int | None = Field(default=None, ge=0)
    committed: int | None = Field(default=None, ge=0)


class _APIFootballCards(BaseModel):
    """Cards sub-object from API-Football statistics entry."""

    model_config = ConfigDict(frozen=True, extra="forbid")

    yellow: int | None = Field(default=None, ge=0)
    yellowred: int | None = Field(default=None, ge=0)
    red: int | None = Field(default=None, ge=0)


class _APIFootballPenalty(BaseModel):
    """Penalty sub-object from API-Football statistics entry."""

    model_config = ConfigDict(frozen=True, extra="forbid")

    won: int | None = Field(default=None, ge=0)
    committed: int | None = Field(default=None, ge=0)
    scored: int | None = Field(default=None, ge=0)
    missed: int | None = Field(default=None, ge=0)
    saved: int | None = Field(default=None, ge=0)


# ─────────────────────────────────────────────────────────────
# API-Football — public models
# ─────────────────────────────────────────────────────────────


class RawAPIFootballPlayer(BaseModel):
    """Biographical data from the API-Football ``/players`` endpoint.

    Height and weight arrive as strings with units (``"174 cm"``,
    ``"60 kg"``).  Birth date is an ISO date string.  All of these
    are parsed into typed values in the CLEAN layer.
    """

    model_config = ConfigDict(frozen=True, extra="forbid")

    player_id: int = Field(ge=1)
    name: str
    firstname: str | None = None
    lastname: str | None = None
    age: int | None = Field(default=None, ge=0, le=100)
    birth_date: str | None = None
    nationality: str | None = None
    height: str | None = None
    weight: str | None = None
    photo_url: str | None = None


class RawAPIFootballPlayerStats(BaseModel):
    """Season statistics from one ``statistics[]`` entry in API-Football.

    Each entry corresponds to a single player-team-league-season
    combination.  The nested sub-models mirror the JSON structure
    returned by the API to stay faithful to the RAW layer contract.
    """

    model_config = ConfigDict(frozen=True, extra="forbid")

    player_id: int = Field(ge=1)
    team_id: int = Field(ge=1)
    team_name: str
    league_id: int = Field(ge=1)
    season: int = Field(ge=2000, le=2100)
    games: _APIFootballGames
    shots: _APIFootballShots
    goals: _APIFootballGoals
    passes: _APIFootballPasses
    tackles: _APIFootballTackles
    duels: _APIFootballDuels
    dribbles: _APIFootballDribbles
    fouls: _APIFootballFouls
    cards: _APIFootballCards
    penalty: _APIFootballPenalty


class RawAPIFootballInjury(BaseModel):
    """Injury record from the API-Football ``/injuries`` endpoint.

    ``fixture_id`` is nullable because some injuries (e.g. training
    injuries) are not tied to a specific match.
    """

    model_config = ConfigDict(frozen=True, extra="forbid")

    player_id: int = Field(ge=1)
    player_name: str
    team_id: int = Field(ge=1)
    team_name: str
    fixture_id: int | None = Field(default=None, ge=1)
    league_id: int = Field(ge=1)
    reason: str
    type: str
    date: str


class RawAPIFootballStandings(BaseModel):
    """One team's standing in a league from the API-Football ``/standings`` endpoint."""

    model_config = ConfigDict(frozen=True, extra="forbid")

    league_id: int = Field(ge=1)
    season: int = Field(ge=2000)
    team_id: int = Field(ge=1)
    team_name: str
    rank: int = Field(ge=1)
    points: int = Field(ge=0)
    played_total: int = Field(ge=0)
    wins: int = Field(ge=0)
    draws: int = Field(ge=0)
    losses: int = Field(ge=0)
    goals_for: int = Field(ge=0)
    goals_against: int = Field(ge=0)
    goal_diff: int
    form: str | None = None  # e.g. "WWDLW" — may be None early in season


class RawAPIFootballTransfer(BaseModel):
    """Transfer record from the API-Football ``/transfers`` endpoint.

    The ``type`` field is overloaded by API-Football: it can contain
    the mechanism (``"Loan"``, ``"Free"``, ``"N/A"``) or the fee
    amount (``"€ 222M"``).  Parsing happens in the CLEAN layer.
    """

    model_config = ConfigDict(frozen=True, extra="forbid")

    player_id: int = Field(ge=1)
    player_name: str
    date: str | None = None
    team_in_id: int | None = Field(default=None, ge=1)
    team_in_name: str | None = None
    team_out_id: int | None = Field(default=None, ge=1)
    team_out_name: str | None = None
    type: str | None = None


# ─────────────────────────────────────────────────────────────
# Understat
# ─────────────────────────────────────────────────────────────


class RawUnderstatShot(BaseModel):
    """Raw shot from Understat.

    Coordinates (x, y) are normalised between 0 and 1 as provided by
    Understat.  The ``xg`` field is the expected-goal probability for
    that single shot.
    """

    model_config = ConfigDict(frozen=True, extra="forbid")

    id: int
    minute: int = Field(ge=0)
    result: str
    x: float = Field(ge=0.0, le=1.0)
    y: float = Field(ge=0.0, le=1.0)
    xg: float = Field(ge=0.0, le=1.0)
    player: str
    player_id: int
    situation: str
    body_part: str | None = None


class RawUnderstatPlayerSeason(BaseModel):
    """Season-level advanced stats from Understat.

    These metrics are Understat's own calculations at the season level
    and are **not** derived from individual shots.  The xG-family
    fields (``xg``, ``xa``, ``npxg``, ``xg_chain``, ``xg_buildup``)
    are season totals and can exceed 1.0.
    """

    model_config = ConfigDict(frozen=True, extra="forbid")

    player_id: int
    player_name: str
    team: str
    season: str
    games: int = Field(ge=0)
    minutes: int = Field(ge=0)
    goals: int = Field(ge=0)
    assists: int = Field(ge=0)
    xg: float = Field(ge=0.0)
    xa: float = Field(ge=0.0)
    npxg: float = Field(ge=0.0)
    xg_chain: float = Field(ge=0.0)
    xg_buildup: float = Field(ge=0.0)
    shots: int = Field(ge=0)
    key_passes: int = Field(ge=0)
    yellow_cards: int = Field(ge=0)
    red_cards: int = Field(ge=0)


