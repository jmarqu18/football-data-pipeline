"""Entity resolution: cross-source identity matching between API-Football and Understat.

Resolves teams and players across the two data sources using a multi-pass
strategy that progresses from high-confidence exact matches to lower-confidence
statistical fingerprinting.

See docs/entity-resolution-spec.md for the full design specification.
"""

from __future__ import annotations

import csv
import logging
from collections.abc import Iterable, Sequence
from datetime import UTC, datetime
from pathlib import Path

from rapidfuzz import fuzz

from pipeline.candidate_pool import CandidatePool
from pipeline.match_scoring import MatchScorer
from pipeline.models.clean import (
    CandidateMatch,
    ResolutionMethod,
    ResolutionResult,
    ResolvedTeam,
    UnresolvedPlayer,
)
from pipeline.models.raw import (
    RawAPIFootballPlayer,
    RawAPIFootballPlayerStats,
    RawAPIFootballTeam,
    RawAPIFootballTransfer,
    RawUnderstatPlayerSeason,
)
from pipeline.name_normalization import decode_api_name, normalize_name
from pipeline.resolution_ledger import ResolutionLedger
from pipeline.resolution_passes import (
    ContextualPass,
    ExactPass,
    FuzzyPass,
    ResolutionPass,
    ResolutionSubject,
    StatisticalPass,
)

logger = logging.getLogger(__name__)

# ─────────────────────────────────────────────────────────────
# Team resolution
# ─────────────────────────────────────────────────────────────

_TEAM_FUZZY_THRESHOLD = 80  # token_sort_ratio minimum for team fuzzy match


def _build_resolved_team(
    api_team: RawAPIFootballTeam,
    understat_name: str | None,
    confidence: float | None,
    method: ResolutionMethod | None,
    now: datetime,
) -> ResolvedTeam:
    """Construct a ResolvedTeam from a RawAPIFootballTeam, propagating all metadata fields."""
    decoded_name = decode_api_name(api_team.name)
    return ResolvedTeam(
        canonical_name=decoded_name,
        api_football_id=api_team.team_id,
        api_football_name=decoded_name,
        understat_name=understat_name,
        country=api_team.country,
        logo_url=api_team.logo_url,
        code=api_team.code,
        founded=api_team.founded,
        venue_name=api_team.venue_name,
        venue_address=api_team.venue_address,
        venue_city=api_team.venue_city,
        venue_capacity=api_team.venue_capacity,
        venue_surface=api_team.venue_surface,
        venue_image_url=api_team.venue_image_url,
        resolution_confidence=confidence,
        resolution_method=method,
        resolved_at=now,
    )


def resolve_teams(
    api_teams: list[RawAPIFootballTeam],
    understat_teams: list[str],
) -> list[ResolvedTeam]:
    """Resolve teams between API-Football and Understat.

    Args:
        api_teams: List of RawAPIFootballTeam objects from API-Football.
        understat_teams: List of unique team names from Understat.

    Returns:
        List of ResolvedTeam with cross-source identifiers.
    """
    now = datetime.now(tz=UTC)
    resolved: list[ResolvedTeam] = []
    unmatched_understat = set(understat_teams)

    # Build normalized lookup for API-Football teams.
    # Decode HTML entities once here so that api_name is always clean
    # when used as canonical_name or api_football_name downstream.
    api_normalized: dict[str, RawAPIFootballTeam] = {}
    for team in api_teams:
        decoded_name = decode_api_name(team.name)
        api_normalized[normalize_name(decoded_name)] = team

    # Pass 1: Exact match on normalized name
    for u_team in list(unmatched_understat):
        norm_u = normalize_name(u_team)
        if norm_u in api_normalized:
            api_team = api_normalized[norm_u]
            resolved.append(_build_resolved_team(api_team, u_team, 1.0, "exact", now))
            unmatched_understat.discard(u_team)
            del api_normalized[norm_u]
            logger.debug("Team exact match: '%s' ↔ '%s'", u_team, decode_api_name(api_team.name))

    # Pass 2: Fuzzy match
    for u_team in list(unmatched_understat):
        norm_u = normalize_name(u_team)
        best_score = 0.0
        best_api_key: str | None = None
        for api_key in api_normalized:
            score = fuzz.token_sort_ratio(norm_u, api_key)
            if score > best_score:
                best_score = score
                best_api_key = api_key
        if best_api_key is not None and best_score >= _TEAM_FUZZY_THRESHOLD:
            api_team = api_normalized[best_api_key]
            resolved.append(_build_resolved_team(api_team, u_team, 0.85, "fuzzy", now))
            unmatched_understat.discard(u_team)
            del api_normalized[best_api_key]
            logger.debug(
                "Team fuzzy match: '%s' ↔ '%s' (score=%.1f)",
                u_team,
                decode_api_name(api_team.name),
                best_score,
            )

    # Log unresolved teams
    for u_team in unmatched_understat:
        logger.warning("Team unresolved: Understat '%s' has no match in API-Football", u_team)

    resolved_api_ids = {r.api_football_id for r in resolved}
    for _api_key, api_team in api_normalized.items():
        if api_team.team_id not in resolved_api_ids:
            resolved.append(_build_resolved_team(api_team, None, None, None, now))

    exact_count = sum(1 for r in resolved if r.resolution_method == "exact")
    fuzzy_count = sum(1 for r in resolved if r.resolution_method == "fuzzy")
    logger.info(
        "Team resolution complete: %d resolved (exact=%d, fuzzy=%d), %d unresolved Understat teams",
        exact_count + fuzzy_count,
        exact_count,
        fuzzy_count,
        len(unmatched_understat),
    )
    return resolved


# ─────────────────────────────────────────────────────────────
# Player resolution
# ─────────────────────────────────────────────────────────────


def _build_team_mapping(
    resolved_teams: list[ResolvedTeam],
) -> dict[str, int]:
    """Build a mapping from normalized Understat team name to API-Football team_id."""
    mapping: dict[str, int] = {}
    for team in resolved_teams:
        if team.understat_name is not None:
            mapping[normalize_name(team.understat_name)] = team.api_football_id
    return mapping


def _run_passes(
    passes: Sequence[ResolutionPass],
    subjects: Sequence[ResolutionSubject],
    ledger: ResolutionLedger,
) -> None:
    """Run every subject through each pass in turn, recording what matches.

    The ordering is pass-major and load-bearing: all subjects go through pass 1
    before any reaches pass 2. Running player-major instead would let a
    low-confidence match on an early subject claim a candidate that a later
    subject would have matched exactly, which is precisely what the descending
    confidence order exists to prevent.

    Args:
        passes: The passes, in descending order of confidence.
        subjects: The Understat players to resolve.
        ledger: Receives every match; also tells the driver who is already done.
    """
    for resolution_pass in passes:
        for subject in subjects:
            understat_player = subject.understat_player
            if ledger.has_understat(understat_player.player_id):
                continue

            match = resolution_pass.attempt(subject)
            if match is None:
                continue

            ledger.record_match(match.api_player, understat_player, match.confidence, match.method)
            logger.debug(
                "%s: '%s' ↔ '%s' (%s)",
                resolution_pass.name,
                understat_player.player_name,
                match.api_player.name,
                match.detail,
            )


def _get_top_candidates(
    scorer: MatchScorer,
    pool: CandidatePool,
    understat_name: str,
    candidate_ids: Iterable[int],
    preferred_team_id: int | None = None,
    n: int = 3,
) -> list[CandidateMatch]:
    """Get top-N best fuzzy match candidates for an unresolved Understat player.

    When ``preferred_team_id`` is provided, candidates from the player's own
    team are ranked before cross-team candidates, making the diagnostic report
    easier to act on.
    """
    same_team_ids: set[int] = set()
    if preferred_team_id is not None:
        same_team_ids = pool.in_team(preferred_team_id)

    norm_understat = normalize_name(understat_name)
    same_team: list[tuple[float, int, str]] = []
    cross_team: list[tuple[float, int, str]] = []

    for api_id in candidate_ids:
        score = scorer.fuzzy_score(norm_understat, pool.variants(api_id))
        entry = (score, api_id, pool.player(api_id).name)
        if api_id in same_team_ids:
            same_team.append(entry)
        else:
            cross_team.append(entry)

    same_team.sort(reverse=True)
    cross_team.sort(reverse=True)

    combined = same_team[:n] + cross_team[: max(0, n - len(same_team))]
    return [
        CandidateMatch(
            candidate_name=name,
            candidate_source="api_football",
            candidate_source_id=api_id,
            fuzzy_score=round(score, 4),
        )
        for score, api_id, name in combined[:n]
    ]


def resolve_players(
    api_players: list[RawAPIFootballPlayer],
    api_stats: list[RawAPIFootballPlayerStats],
    understat_players: list[RawUnderstatPlayerSeason],
    resolved_teams: list[ResolvedTeam],
    raw_transfers: list[RawAPIFootballTransfer] | None = None,
) -> ResolutionResult:
    """Resolve players between API-Football and Understat using 4 passes.

    Pass 1: Exact name + same team → confidence 1.0
    Pass 2: Fuzzy name ≥ 0.85 + same team → confidence 0.90
    Pass 3: Fuzzy name ≥ 0.75 cross-team + transfer history → confidence 0.70
    Pass 4: Statistical fingerprint (games/minutes) + same team → confidence 0.60

    Args:
        api_players: Biographical data from API-Football.
        api_stats: Season statistics from API-Football.
        understat_players: Season-level data from Understat.
        resolved_teams: Previously resolved team mappings.
        raw_transfers: Raw transfer records for Pass 3 verification.

    Returns:
        ResolutionResult with resolved players and unresolved candidates.
    """
    now = datetime.now(tz=UTC)
    raw_transfers = raw_transfers or []
    team_mapping = _build_team_mapping(resolved_teams)

    # Owns the API-Football side: identity, name variants, team membership and
    # season stats, all indexed once for the whole run.
    pool = CandidatePool(api_players, api_stats)

    # All candidate scoring (name similarity, position compatibility, statistical
    # fingerprinting, ambiguity detection) goes through this one interface.
    scorer = MatchScorer()

    # Owns which players are already matched and accumulates the resolved
    # records, so no pass mutates matching state directly.
    ledger = ResolutionLedger(now=now)

    # ── Build one subject per Understat player, normalizing once ──
    subjects = [
        ResolutionSubject(
            understat_player=u_player,
            normalized_name=normalize_name(u_player.player_name),
            api_team_id=team_mapping.get(normalize_name(u_player.team)),
        )
        for u_player in understat_players
    ]

    # ── The 4 passes, in descending order of confidence (ADR-004) ──
    passes: list[ResolutionPass] = [
        ExactPass(pool, ledger),
        FuzzyPass(pool, scorer, ledger),
        ContextualPass(pool, scorer, ledger, raw_transfers, resolved_teams),
        StatisticalPass(pool, scorer, ledger),
    ]
    _run_passes(passes, subjects, ledger)

    # ── Collect unresolved ──
    unresolved: list[UnresolvedPlayer] = []
    remaining_api = ledger.unmatched_among(pool.all_ids())

    for u_player in understat_players:
        if not ledger.has_understat(u_player.player_id):
            u_team_id_for_report = team_mapping.get(normalize_name(u_player.team))
            top = _get_top_candidates(
                scorer,
                pool,
                u_player.player_name,
                remaining_api,
                preferred_team_id=u_team_id_for_report,
            )
            unresolved.append(
                UnresolvedPlayer(
                    source="understat",
                    player_id=u_player.player_id,
                    player_name=u_player.player_name,
                    team=u_player.team,
                    top_candidates=top,
                )
            )
            logger.warning(
                "Unresolved Understat player: '%s' (id=%d, team=%s). Best candidate: %s (%.3f)",
                u_player.player_name,
                u_player.player_id,
                u_player.team,
                top[0].candidate_name if top else "none",
                top[0].fuzzy_score if top else 0.0,
            )

    # Single-source API-Football players still reach the CLEAN players table.
    for api_id in ledger.unmatched_among(pool.all_ids()):
        ledger.record_single_source(pool.player(api_id))

    resolved = ledger.resolved_players()

    # ── Log summary ──
    method_counts: dict[str, int] = {}
    confidences: list[float] = []
    for p in resolved:
        if p.resolution_method and p.resolution_method != "unresolved":
            method_counts[p.resolution_method] = method_counts.get(p.resolution_method, 0) + 1
            if p.resolution_confidence is not None:
                confidences.append(p.resolution_confidence)

    total_resolved = sum(method_counts.values())
    avg_conf = sum(confidences) / len(confidences) if confidences else 0.0
    logger.info(
        "Player resolution complete: %d resolved (exact=%d, fuzzy=%d, contextual=%d, "
        "statistical=%d), %d unresolved, avg confidence=%.3f",
        total_resolved,
        method_counts.get("exact", 0),
        method_counts.get("fuzzy", 0),
        method_counts.get("contextual", 0),
        method_counts.get("statistical", 0),
        len(unresolved),
        avg_conf,
    )

    return ResolutionResult(resolved_players=resolved, unresolved=unresolved)


# ─────────────────────────────────────────────────────────────
# Unresolved candidates report
# ─────────────────────────────────────────────────────────────


def write_unresolved_report(
    unresolved: list[UnresolvedPlayer],
    output_path: str | Path = "data/reports/unresolved_candidates.csv",
) -> Path:
    """Write a CSV report of unresolved players with their top candidates.

    Returns the path to the written file.
    """
    path = Path(output_path)
    path.parent.mkdir(parents=True, exist_ok=True)

    with path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(
            [
                "source",
                "player_id",
                "player_name",
                "team",
                "candidate_name",
                "candidate_source_id",
                "fuzzy_score",
            ]
        )
        for player in unresolved:
            if player.top_candidates:
                for candidate in player.top_candidates:
                    writer.writerow(
                        [
                            player.source,
                            player.player_id,
                            player.player_name,
                            player.team or "",
                            candidate.candidate_name,
                            candidate.candidate_source_id,
                            f"{candidate.fuzzy_score:.4f}",
                        ]
                    )
            else:
                writer.writerow(
                    [
                        player.source,
                        player.player_id,
                        player.player_name,
                        player.team or "",
                        "",
                        "",
                        "",
                    ]
                )

    logger.info("Unresolved candidates report written to %s (%d players)", path, len(unresolved))
    return path
