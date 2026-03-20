"""Modelos Pydantic para la capa RAW del pipeline.

Estos modelos validan el esquema mínimo y tipos básicos de los datos
tal como llegan de cada fuente, **sin transformación**.

Fuentes soportadas:
- StatsBomb (eventos + partidos)
- Understat (tiros con xG)
- FBref (estadísticas de jugador por temporada)

Referencia JSON Schema: https://docs.pydantic.dev/latest/concepts/json_schema/
"""

from __future__ import annotations

from pydantic import BaseModel, ConfigDict, Field

# ─────────────────────────────────────────────────────────────
# StatsBomb
# ─────────────────────────────────────────────────────────────


class RawStatsBombEvent(BaseModel):
    """Evento crudo de StatsBomb.

    Representa una acción individual dentro de un partido
    (pase, tiro, presión, etc.).  Los datos llegan vía `statsbombpy`.

    ``period`` distingue tiempo regular (1-2), prórroga (3-4) y
    penaltis (5).  ``player_id`` se incluye por trazabilidad: en
    entity resolution no se utiliza directamente porque los IDs de
    StatsBomb no comparten espacio con Understat ni FBref.
    """

    model_config = ConfigDict(frozen=True, extra="forbid")

    id: str
    type: str
    player: str | None = None
    player_id: int | None = None
    team: str
    location: list[float] | None = None
    period: int = Field(ge=1, le=5)
    minute: int = Field(ge=0)
    second: int = Field(ge=0, le=59)


class RawStatsBombMatch(BaseModel):
    """Partido crudo de StatsBomb.

    Contiene la información básica de un partido incluyendo
    equipos, resultado, competición y temporada.
    """

    model_config = ConfigDict(frozen=True, extra="forbid")

    match_id: int
    home_team: str
    away_team: str
    home_score: int = Field(ge=0)
    away_score: int = Field(ge=0)
    competition: str
    season: str


# ─────────────────────────────────────────────────────────────
# Understat
# ─────────────────────────────────────────────────────────────


class RawUnderstatShot(BaseModel):
    """Tiro crudo de Understat.

    Las coordenadas (x, y) están normalizadas entre 0 y 1
    tal como las proporciona Understat.  El campo ``xg`` es la
    probabilidad de gol esperado para ese tiro.
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


# ─────────────────────────────────────────────────────────────
# FBref
# ─────────────────────────────────────────────────────────────


class RawFBrefPlayerSeason(BaseModel):
    """Estadísticas de jugador por temporada de FBref.

    Resumen estándar que incluye apariciones, minutos, goles,
    asistencias y tarjetas.  ``competition`` y ``season`` identifican
    unívocamente el contexto del registro; sin ellos, datos de
    diferentes temporadas serían indistinguibles en el pipeline.
    Algunos campos pueden ser ``None`` cuando FBref no dispone del
    dato (ej. nacionalidad, año de nacimiento).
    """

    model_config = ConfigDict(frozen=True, extra="forbid")

    player: str
    competition: str
    season: str
    nation: str | None = None
    pos: str
    squad: str
    born: int | None = None
    matches_played: int = Field(ge=0)
    minutes: int = Field(ge=0)
    goals: int = Field(ge=0)
    assists: int = Field(ge=0)
    cards_yellow: int = Field(ge=0)
    cards_red: int = Field(ge=0)
