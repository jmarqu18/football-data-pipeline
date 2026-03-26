"""Transform RAW layer (Parquet) → CLEAN layer (PostgreSQL).

Reads Parquet files from data/raw/, runs entity resolution (teams then
players), and inserts the resolved data into the 8 PostgreSQL CLEAN tables.

Idempotent: truncates all tables before re-inserting on every run.
"""

from __future__ import annotations

import logging
import re
import time
from datetime import date
from decimal import Decimal, InvalidOperation
from pathlib import Path

import pyarrow.parquet as pq
from pydantic import BaseModel, ValidationError
from sqlalchemy import text
from sqlalchemy.engine import Engine

from pipeline.db import get_engine
from pipeline.entity_resolution import (
    resolve_players,
    resolve_teams,
    write_unresolved_report,
)
from pipeline.models.clean import ResolvedPlayer, ResolvedTeam
from pipeline.models.raw import (
    RawAPIFootballInjury,
    RawAPIFootballPlayer,
    RawAPIFootballPlayerStats,
    RawAPIFootballTeam,
    RawAPIFootballTransfer,
    RawUnderstatPlayerSeason,
    RawUnderstatShot,
)

logger = logging.getLogger(__name__)

_DEFAULT_REPORT_PATH = "data/reports/unresolved_candidates.csv"

# ─────────────────────────────────────────────────────────────
# Parquet → Pydantic
# ─────────────────────────────────────────────────────────────


def read_parquet_models[T: BaseModel](path: Path, model_class: type[T]) -> list[T]:
    """Deserialize a Parquet file into a list of Pydantic model instances.

    Invalid rows are logged as warnings and skipped — never raises for
    individual record failures.

    Args:
        path: Path to the ``.parquet`` file.
        model_class: Pydantic model class to validate each row against.

    Returns:
        List of validated model instances.
    """
    table = pq.read_table(path)
    rows = table.to_pylist()
    models: list[T] = []
    rejected = 0
    for row in rows:
        try:
            models.append(model_class(**row))
        except ValidationError as exc:
            rejected += 1
            logger.warning(
                "Rejected row in %s: %s — %s",
                path.name,
                row,
                exc.errors(),
            )
    logger.info(
        "Read %s: %d total, %d valid, %d rejected",
        path.name,
        len(rows),
        len(models),
        rejected,
    )
    return models


def load_raw_api_football(
    raw_dir: Path,
) -> tuple[
    list[RawAPIFootballPlayer],
    list[RawAPIFootballPlayerStats],
    list[RawAPIFootballInjury],
    list[RawAPIFootballTransfer],
    list[RawAPIFootballTeam],
]:
    """Load all API-Football RAW Parquet files from a directory."""
    players = read_parquet_models(raw_dir / "players.parquet", RawAPIFootballPlayer)
    stats = read_parquet_models(raw_dir / "player_stats.parquet", RawAPIFootballPlayerStats)
    injuries = read_parquet_models(raw_dir / "injuries.parquet", RawAPIFootballInjury)
    transfers = read_parquet_models(raw_dir / "transfers.parquet", RawAPIFootballTransfer)

    # Load teams
    teams_path = raw_dir / "teams.parquet"
    raw_teams: list[RawAPIFootballTeam] = []
    if teams_path.exists():
        teams_df = pq.read_table(teams_path).to_pandas()
        for row in teams_df.to_dict("records"):
            try:
                raw_teams.append(RawAPIFootballTeam.model_validate(row))
            except ValidationError as exc:
                logger.warning("Rejected raw team record: %s", exc)
    else:
        logger.warning(
            "teams.parquet not found at %s — team metadata will be empty", teams_path
        )

    return players, stats, injuries, transfers, raw_teams


def load_raw_understat(
    raw_dir: Path,
) -> tuple[list[RawUnderstatShot], list[RawUnderstatPlayerSeason]]:
    """Load all Understat RAW Parquet files from a directory."""
    shots = read_parquet_models(raw_dir / "shots.parquet", RawUnderstatShot)
    player_season = read_parquet_models(raw_dir / "player_season.parquet", RawUnderstatPlayerSeason)
    return shots, player_season


# ─────────────────────────────────────────────────────────────
# Parsing helpers
# ─────────────────────────────────────────────────────────────

_MEASUREMENT_RE = re.compile(r"(\d+)")
_CURRENCY_RE = re.compile(r"[€$£¥]")


def parse_measurement(value: str | None) -> int | None:
    """Parse height/weight strings like '174 cm' or '60 kg' into integers."""
    if not value:
        return None
    match = _MEASUREMENT_RE.search(value)
    return int(match.group(1)) if match else None


def parse_rating(value: str | None) -> Decimal | None:
    """Parse a rating string like '7.342857' into a Decimal."""
    if not value:
        return None
    try:
        return Decimal(value)
    except InvalidOperation:
        return None


def parse_date(value: str | None) -> date | None:
    """Parse an ISO date string into a date object."""
    if not value:
        return None
    try:
        return date.fromisoformat(value)
    except ValueError:
        return None


def parse_transfer_type(value: str | None) -> tuple[str | None, str | None]:
    """Parse API-Football transfer type field into (transfer_type, fee_text).

    If the value contains a currency symbol it is treated as a fee amount;
    otherwise it is the transfer mechanism (Loan, Free, etc.).
    """
    if not value:
        return None, None
    if _CURRENCY_RE.search(value):
        return None, value
    return value, None


# ─────────────────────────────────────────────────────────────
# PostgreSQL helpers
# ─────────────────────────────────────────────────────────────


def _truncate_all(engine: Engine) -> None:
    """Truncate all CLEAN tables in reverse FK order."""
    with engine.begin() as conn:
        conn.execute(
            text(
                "TRUNCATE player_transfers, player_injuries, player_profile, "
                "player_shots, player_season_advanced, player_season_stats, "
                "players, teams CASCADE"
            )
        )
    logger.info("Truncated all CLEAN tables")


# ─────────────────────────────────────────────────────────────
# Insert functions
# ─────────────────────────────────────────────────────────────


def insert_teams(
    engine: Engine,
    resolved_teams: list[ResolvedTeam],
) -> dict[int, int]:
    """Insert resolved teams and return mapping {api_football_id → team_id (SERIAL)}."""
    mapping: dict[int, int] = {}
    with engine.begin() as conn:
        for team in resolved_teams:
            result = conn.execute(
                text(
                    "INSERT INTO teams "
                    "(api_football_id, understat_name, canonical_name, "
                    "country, logo_url, code, founded, "
                    "venue_name, venue_address, venue_city, "
                    "venue_capacity, venue_surface, venue_image_url, "
                    "resolution_confidence, resolution_method, resolved_at) "
                    "VALUES (:af_id, :u_name, :canon, "
                    ":country, :logo_url, :code, :founded, "
                    ":venue_name, :venue_address, :venue_city, "
                    ":venue_capacity, :venue_surface, :venue_image_url, "
                    ":conf, :method, :resolved_at) "
                    "RETURNING team_id"
                ),
                {
                    "af_id": team.api_football_id,
                    "u_name": team.understat_name,
                    "canon": team.canonical_name,
                    "country": team.country,
                    "logo_url": team.logo_url,
                    "code": team.code,
                    "founded": team.founded,
                    "venue_name": team.venue_name,
                    "venue_address": team.venue_address,
                    "venue_city": team.venue_city,
                    "venue_capacity": team.venue_capacity,
                    "venue_surface": team.venue_surface,
                    "venue_image_url": team.venue_image_url,
                    "conf": team.resolution_confidence,
                    "method": team.resolution_method,
                    "resolved_at": team.resolved_at,
                },
            )
            team_id = result.scalar_one()
            mapping[team.api_football_id] = team_id
    logger.info("Inserted %d teams", len(mapping))
    return mapping


def insert_players(
    engine: Engine,
    resolved_players: list[ResolvedPlayer],
) -> tuple[dict[int, int], dict[int, int]]:
    """Insert resolved players and return two mappings.

    Returns:
        Tuple of (api_football_id → player_id, understat_id → player_id).
        Each mapping only contains entries where the source ID is not None.
    """
    af_map: dict[int, int] = {}
    us_map: dict[int, int] = {}
    with engine.begin() as conn:
        for player in resolved_players:
            result = conn.execute(
                text(
                    "INSERT INTO players "
                    "(api_football_id, understat_id, canonical_name, known_name, "
                    "birth_date, nationality, photo_url, "
                    "resolution_confidence, resolution_method, resolved_at) "
                    "VALUES (:af_id, :u_id, :canon, :known, :bdate, :nat, :photo, "
                    ":conf, :method, :resolved_at) "
                    "RETURNING player_id"
                ),
                {
                    "af_id": player.api_football_id,
                    "u_id": player.understat_id,
                    "canon": player.canonical_name,
                    "known": player.known_name,
                    "bdate": player.birth_date,  # already a date | None from Pydantic
                    "nat": player.nationality,
                    "photo": player.photo_url,
                    "conf": player.resolution_confidence,
                    "method": player.resolution_method,
                    "resolved_at": player.resolved_at,
                },
            )
            player_id = result.scalar_one()
            if player.api_football_id is not None:
                af_map[player.api_football_id] = player_id
            if player.understat_id is not None:
                us_map[player.understat_id] = player_id
    unique_count = len(set(af_map.values()) | set(us_map.values()))
    logger.info("Inserted %d players", unique_count)
    return af_map, us_map


def insert_player_season_stats(
    engine: Engine,
    stats: list[RawAPIFootballPlayerStats],
    af_player_map: dict[int, int],
    team_id_map: dict[int, int],
    season: str,
) -> int:
    """Insert player season stats. Returns count of inserted rows."""
    inserted = 0
    with engine.begin() as conn:
        for s in stats:
            pid = af_player_map.get(s.player_id)
            tid = team_id_map.get(s.team_id)
            if pid is None or tid is None:
                logger.warning(
                    "Skipping stats for player_id=%d team_id=%d: missing FK mapping",
                    s.player_id,
                    s.team_id,
                )
                continue
            conn.execute(
                text(
                    "INSERT INTO player_season_stats "
                    "(player_id, team_id, season, league_id, "
                    "appearances, starts, minutes, shirt_number, position, rating, captain, "
                    "shots_total, shots_on_target, "
                    "goals, assists, goals_conceded, saves, "
                    "passes_total, key_passes, pass_accuracy, "
                    "tackles, blocks, interceptions, "
                    "duels_total, duels_won, "
                    "dribbles_attempted, dribbles_successful, dribbles_past, "
                    "fouls_drawn, fouls_committed, "
                    "cards_yellow, cards_yellow_red, cards_red, "
                    "penalties_won, penalties_committed, penalties_scored, "
                    "penalties_missed, penalties_saved) "
                    "VALUES (:pid, :tid, :season, :lid, "
                    ":appearances, :starts, :minutes, :shirt_number, :position, "
                    ":rating, :captain, "
                    ":shots_total, :shots_on_target, "
                    ":goals, :assists, :goals_conceded, :saves, "
                    ":passes_total, :key_passes, :pass_accuracy, "
                    ":tackles, :blocks, :interceptions, "
                    ":duels_total, :duels_won, "
                    ":dribbles_attempted, :dribbles_successful, :dribbles_past, "
                    ":fouls_drawn, :fouls_committed, "
                    ":cards_yellow, :cards_yellow_red, :cards_red, "
                    ":penalties_won, :penalties_committed, :penalties_scored, "
                    ":penalties_missed, :penalties_saved)"
                ),
                {
                    "pid": pid,
                    "tid": tid,
                    "season": season,
                    "lid": s.league_id,
                    "appearances": s.games.appearances,
                    "starts": s.games.lineups,
                    "minutes": s.games.minutes,
                    "shirt_number": s.games.number,
                    "position": s.games.position,
                    "rating": parse_rating(s.games.rating),
                    "captain": s.games.captain,
                    "shots_total": s.shots.total,
                    "shots_on_target": s.shots.on,
                    "goals": s.goals.total,
                    "assists": s.goals.assists,
                    "goals_conceded": s.goals.conceded,
                    "saves": s.goals.saves,
                    "passes_total": s.passes.total,
                    "key_passes": s.passes.key,
                    "pass_accuracy": s.passes.accuracy,
                    "tackles": s.tackles.total,
                    "blocks": s.tackles.blocks,
                    "interceptions": s.tackles.interceptions,
                    "duels_total": s.duels.total,
                    "duels_won": s.duels.won,
                    "dribbles_attempted": s.dribbles.attempts,
                    "dribbles_successful": s.dribbles.success,
                    "dribbles_past": s.dribbles.past,
                    "fouls_drawn": s.fouls.drawn,
                    "fouls_committed": s.fouls.committed,
                    "cards_yellow": s.cards.yellow,
                    "cards_yellow_red": s.cards.yellowred,
                    "cards_red": s.cards.red,
                    "penalties_won": s.penalty.won,
                    "penalties_committed": s.penalty.committed,
                    "penalties_scored": s.penalty.scored,
                    "penalties_missed": s.penalty.missed,
                    "penalties_saved": s.penalty.saved,
                },
            )
            inserted += 1
    logger.info("Inserted %d player_season_stats rows", inserted)
    return inserted


def insert_player_season_advanced(
    engine: Engine,
    understat_players: list[RawUnderstatPlayerSeason],
    us_player_map: dict[int, int],
    team_id_map: dict[int, int],
    season: str,
) -> int:
    """Insert Understat advanced season stats. Returns count of inserted rows."""
    inserted = 0
    with engine.begin() as conn:
        for up in understat_players:
            pid = us_player_map.get(up.player_id)
            tid = _find_team_id_by_understat(up.team, team_id_map)
            if pid is None:
                logger.warning(
                    "Skipping advanced stats for Understat player '%s' (id=%d): no player mapping",
                    up.player_name,
                    up.player_id,
                )
                continue
            conn.execute(
                text(
                    "INSERT INTO player_season_advanced "
                    "(player_id, team_id, season, xg, xa, npxg, xg_chain, xg_buildup, "
                    "shots, key_passes) "
                    "VALUES (:pid, :tid, :season, :xg, :xa, :npxg, :xg_chain, :xg_buildup, "
                    ":shots, :key_passes)"
                ),
                {
                    "pid": pid,
                    "tid": tid,
                    "season": season,
                    "xg": up.xg,
                    "xa": up.xa,
                    "npxg": up.npxg,
                    "xg_chain": up.xg_chain,
                    "xg_buildup": up.xg_buildup,
                    "shots": up.shots,
                    "key_passes": up.key_passes,
                },
            )
            inserted += 1
    logger.info("Inserted %d player_season_advanced rows", inserted)
    return inserted


def insert_player_shots(
    engine: Engine,
    shots: list[RawUnderstatShot],
    us_player_map: dict[int, int],
    season: str,
    league_id: int | None = None,
    us_player_team_map: dict[int, int | None] | None = None,
) -> int:
    """Insert Understat shot-level data. Returns count of inserted rows.

    Args:
        us_player_team_map: Optional mapping of understat player_id → SERIAL team_id.
            When provided, populates the ``team_id`` column on each shot row.
    """
    team_map = us_player_team_map or {}
    inserted = 0
    with engine.begin() as conn:
        for shot in shots:
            pid = us_player_map.get(shot.player_id)
            if pid is None:
                logger.warning(
                    "Skipping shot id=%d for Understat player '%s' (id=%d): no player mapping",
                    shot.id,
                    shot.player,
                    shot.player_id,
                )
                continue
            conn.execute(
                text(
                    "INSERT INTO player_shots "
                    "(player_id, team_id, season, league_id, understat_id, minute, result, "
                    "x, y, xg, situation, body_part) "
                    "VALUES (:pid, :tid, :season, :lid, :uid, :minute, :result, "
                    ":x, :y, :xg, :situation, :body_part)"
                ),
                {
                    "pid": pid,
                    "tid": team_map.get(shot.player_id),
                    "season": season,
                    "lid": league_id,
                    "uid": shot.id,
                    "minute": shot.minute,
                    "result": shot.result,
                    "x": shot.x,
                    "y": shot.y,
                    "xg": shot.xg,
                    "situation": shot.situation,
                    "body_part": shot.body_part,
                },
            )
            inserted += 1
    logger.info("Inserted %d player_shots rows", inserted)
    return inserted


def insert_player_profile(
    engine: Engine,
    players: list[RawAPIFootballPlayer],
    af_player_map: dict[int, int],
) -> int:
    """Insert player profile (biographical) data. Returns count of inserted rows."""
    inserted = 0
    with engine.begin() as conn:
        for p in players:
            pid = af_player_map.get(p.player_id)
            if pid is None:
                continue
            height_cm = parse_measurement(p.height)
            weight_kg = parse_measurement(p.weight)
            if height_cm is None and weight_kg is None:
                continue
            conn.execute(
                text(
                    "INSERT INTO player_profile (player_id, height_cm, weight_kg) "
                    "VALUES (:pid, :height, :weight)"
                ),
                {"pid": pid, "height": height_cm, "weight": weight_kg},
            )
            inserted += 1
    logger.info("Inserted %d player_profile rows", inserted)
    return inserted


def insert_player_injuries(
    engine: Engine,
    injuries: list[RawAPIFootballInjury],
    af_player_map: dict[int, int],
    team_id_map: dict[int, int],
) -> int:
    """Insert player injury records. Returns count of inserted rows."""
    inserted = 0
    with engine.begin() as conn:
        for inj in injuries:
            pid = af_player_map.get(inj.player_id)
            if pid is None:
                logger.warning(
                    "Skipping injury for player_id=%d: no player mapping",
                    inj.player_id,
                )
                continue
            tid = team_id_map.get(inj.team_id)
            conn.execute(
                text(
                    "INSERT INTO player_injuries "
                    "(player_id, team_id, league_id, fixture_id, injury_date, type, reason) "
                    "VALUES (:pid, :tid, :lid, :fid, :idate, :type, :reason)"
                ),
                {
                    "pid": pid,
                    "tid": tid,
                    "lid": inj.league_id,
                    "fid": inj.fixture_id,
                    "idate": parse_date(inj.date),
                    "type": inj.type,
                    "reason": inj.reason,
                },
            )
            inserted += 1
    logger.info("Inserted %d player_injuries rows", inserted)
    return inserted


def insert_player_transfers(
    engine: Engine,
    transfers: list[RawAPIFootballTransfer],
    af_player_map: dict[int, int],
    team_id_map: dict[int, int],
) -> int:
    """Insert player transfer records. Returns count of inserted rows."""
    inserted = 0
    with engine.begin() as conn:
        for tr in transfers:
            pid = af_player_map.get(tr.player_id)
            if pid is None:
                logger.warning(
                    "Skipping transfer for player_id=%d: no player mapping",
                    tr.player_id,
                )
                continue
            transfer_type, fee_text = parse_transfer_type(tr.type)
            conn.execute(
                text(
                    "INSERT INTO player_transfers "
                    "(player_id, from_team_id, to_team_id, from_team_name, to_team_name, "
                    "transfer_date, transfer_type, fee_text) "
                    "VALUES (:pid, :from_tid, :to_tid, :from_name, :to_name, "
                    ":tdate, :ttype, :fee)"
                ),
                {
                    "pid": pid,
                    "from_tid": team_id_map.get(tr.team_out_id) if tr.team_out_id else None,
                    "to_tid": team_id_map.get(tr.team_in_id) if tr.team_in_id else None,
                    "from_name": tr.team_out_name,
                    "to_name": tr.team_in_name,
                    "tdate": parse_date(tr.date),
                    "ttype": transfer_type,
                    "fee": fee_text,
                },
            )
            inserted += 1
    logger.info("Inserted %d player_transfers rows", inserted)
    return inserted


# ─────────────────────────────────────────────────────────────
# Team lookup helper
# ─────────────────────────────────────────────────────────────

# Module-level cache populated during run_transform_clean
_understat_team_to_api_id: dict[str, int] = {}


def _find_team_id_by_understat(
    understat_team_name: str,
    team_id_map: dict[int, int],
) -> int | None:
    """Look up the SERIAL team_id for an Understat team name."""
    api_id = _understat_team_to_api_id.get(understat_team_name)
    if api_id is not None:
        return team_id_map.get(api_id)
    return None


# ─────────────────────────────────────────────────────────────
# Orchestrator
# ─────────────────────────────────────────────────────────────


def run_transform_clean(
    raw_dir: Path,
    database_url: str | None = None,
    report_path: str | Path | None = None,
) -> dict:
    """Orchestrate the full RAW → CLEAN transformation.

    Args:
        raw_dir: Root of the RAW data directory (contains ``api_football/`` and ``understat/``).
        database_url: PostgreSQL connection string; defaults to DATABASE_URL env or localhost.
        report_path: Where to write the unresolved candidates CSV.

    Returns:
        Dict with counts of inserted records per table (for XCom / logging).
    """
    start = time.monotonic()
    report_path = report_path or _DEFAULT_REPORT_PATH

    # 1. Load RAW data
    logger.info("Loading RAW Parquet files from %s", raw_dir)
    players, stats, injuries, transfers, raw_teams = load_raw_api_football(
        raw_dir / "api_football"
    )
    shots, player_season = load_raw_understat(raw_dir / "understat")

    # 2. Extract unique teams
    if raw_teams:
        api_teams = raw_teams
    else:
        logger.warning("No teams.parquet — falling back to team names from player stats")
        seen: dict[int, str] = {}
        for s in stats:
            if s.team_id not in seen:
                seen[s.team_id] = s.team_name
        api_teams = [RawAPIFootballTeam(team_id=tid, name=name) for tid, name in seen.items()]
    understat_teams = sorted({p.team for p in player_season})

    # 3. Entity resolution
    logger.info("Running team resolution...")
    resolved_teams = resolve_teams(api_teams, understat_teams)

    logger.info("Running player resolution...")
    resolution_result = resolve_players(players, stats, player_season, resolved_teams, transfers)

    # 4. Write unresolved report
    write_unresolved_report(resolution_result.unresolved, report_path)

    # 5. Build understat team lookup for insert helpers
    _understat_team_to_api_id.clear()
    for team in resolved_teams:
        if team.understat_name is not None:
            _understat_team_to_api_id[team.understat_name] = team.api_football_id

    # 6. Insert into PostgreSQL
    engine = get_engine(database_url)
    _truncate_all(engine)

    team_id_map = insert_teams(engine, resolved_teams)
    af_player_map, us_player_map = insert_players(engine, resolution_result.resolved_players)

    # Build season string from config or stats
    season_str = f"{stats[0].season}/{stats[0].season + 1}" if stats else "unknown"

    # Count unique players (some appear in both maps)
    all_player_ids = set(af_player_map.values()) | set(us_player_map.values())
    counts: dict[str, int] = {
        "teams": len(team_id_map),
        "players": len(all_player_ids),
    }
    counts["player_season_stats"] = insert_player_season_stats(
        engine, stats, af_player_map, team_id_map, season_str
    )
    counts["player_season_advanced"] = insert_player_season_advanced(
        engine, player_season, us_player_map, team_id_map, season_str
    )
    # Build understat_player_id → SERIAL team_id for shot-level inserts.
    # player_season has one row per player with their team (Understat name).
    us_player_team_map: dict[int, int | None] = {}
    for up in player_season:
        api_id = _understat_team_to_api_id.get(up.team)
        us_player_team_map[up.player_id] = team_id_map.get(api_id) if api_id else None

    counts["player_shots"] = insert_player_shots(
        engine,
        shots,
        us_player_map,
        season_str,
        league_id=stats[0].league_id if stats else None,
        us_player_team_map=us_player_team_map,
    )
    counts["player_profile"] = insert_player_profile(engine, players, af_player_map)
    counts["player_injuries"] = insert_player_injuries(engine, injuries, af_player_map, team_id_map)
    counts["player_transfers"] = insert_player_transfers(
        engine, transfers, af_player_map, team_id_map
    )

    elapsed = time.monotonic() - start
    logger.info(
        "Transform RAW → CLEAN complete in %.1fs. Counts: %s",
        elapsed,
        counts,
    )
    return counts
