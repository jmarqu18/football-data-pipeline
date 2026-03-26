"""Entity resolution: cross-source identity matching between API-Football and Understat.

Resolves teams and players across the two data sources using a multi-pass
strategy that progresses from high-confidence exact matches to lower-confidence
statistical fingerprinting.

See docs/entity-resolution-spec.md for the full design specification.
"""

from __future__ import annotations

import csv
import html
import logging
import re
from datetime import UTC, datetime
from pathlib import Path

from rapidfuzz import fuzz
from unidecode import unidecode

from pipeline.models.clean import (
    CandidateMatch,
    ResolutionResult,
    ResolvedPlayer,
    ResolvedTeam,
    UnresolvedPlayer,
)
from pipeline.models.raw import (
    RawAPIFootballPlayer,
    RawAPIFootballPlayerStats,
    RawAPIFootballTransfer,
    RawUnderstatPlayerSeason,
)

logger = logging.getLogger(__name__)

# ─────────────────────────────────────────────────────────────
# Name normalization utilities
# ─────────────────────────────────────────────────────────────

_WHITESPACE_RE = re.compile(r"\s+")


def decode_api_name(name: str) -> str:
    """Decode HTML entities in an API-Football name string.

    API-Football occasionally returns names with HTML entities
    (e.g. ``"E. Eto&apos;o Pineda"``).  This function decodes them to
    their Unicode equivalents before the value is stored in the CLEAN layer.

    Examples:
        >>> decode_api_name("E. Eto&apos;o Pineda")
        "E. Eto'o Pineda"
        >>> decode_api_name("Marcelo &amp; Silva")
        'Marcelo & Silva'
    """
    return html.unescape(name)


def normalize_name(name: str) -> str:
    """Normalize a player or team name for comparison.

    Applies: HTML unescape → unidecode (strip diacritics) → lowercase →
    strip → collapse multiple whitespace into single space.

    HTML entities are unescaped first so that ``"Eto&apos;o"`` and
    ``"Eto'o"`` compare equal after normalization.

    Examples:
        >>> normalize_name("Vinícius Júnior")
        'vinicius junior'
        >>> normalize_name("  Pedro  González   López  ")
        'pedro gonzalez lopez'
        >>> normalize_name("E. Eto&apos;o Pineda")
        "e. eto'o pineda"
    """
    return _WHITESPACE_RE.sub(" ", unidecode(html.unescape(name)).lower().strip())


def build_name_variants(
    name: str,
    firstname: str | None = None,
    lastname: str | None = None,
) -> list[str]:
    """Generate normalized name variants from API-Football player fields.

    Returns a deduplicated list of all meaningful name forms to maximize
    the chance of matching against Understat's single player_name field.
    """
    variants: set[str] = set()
    norm_name = normalize_name(name)
    if norm_name:
        variants.add(norm_name)
    if firstname:
        norm_first = normalize_name(firstname)
        if norm_first:
            variants.add(norm_first)
    if lastname:
        norm_last = normalize_name(lastname)
        if norm_last:
            variants.add(norm_last)
    if firstname and lastname:
        combined = normalize_name(f"{firstname} {lastname}")
        if combined:
            variants.add(combined)
    return list(variants)


def best_match_score(understat_name: str, api_variants: list[str]) -> float:
    """Return the best fuzzy match score between an Understat name and API-Football variants.

    Uses the maximum of token_sort_ratio and partial_ratio across all variants.
    Returns a value in [0.0, 1.0].
    """
    norm = normalize_name(understat_name)
    if not norm or not api_variants:
        return 0.0
    best = 0.0
    for variant in api_variants:
        if not variant:
            continue
        score_token = fuzz.token_sort_ratio(norm, variant)
        score_partial = fuzz.partial_ratio(norm, variant)
        best = max(best, score_token, score_partial)
    return best / 100.0


# ─────────────────────────────────────────────────────────────
# Team resolution
# ─────────────────────────────────────────────────────────────

_TEAM_FUZZY_THRESHOLD = 80  # token_sort_ratio minimum for team fuzzy match


def resolve_teams(
    api_teams: list[tuple[int, str]],
    understat_teams: list[str],
) -> list[ResolvedTeam]:
    """Resolve teams between API-Football and Understat.

    Args:
        api_teams: List of (team_id, team_name) from API-Football.
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
    api_normalized: dict[str, tuple[int, str]] = {}
    for team_id, team_name in api_teams:
        decoded = decode_api_name(team_name)
        api_normalized[normalize_name(decoded)] = (team_id, decoded)

    # Pass 1: Exact match on normalized name
    for u_team in list(unmatched_understat):
        norm_u = normalize_name(u_team)
        if norm_u in api_normalized:
            team_id, api_name = api_normalized[norm_u]
            resolved.append(
                ResolvedTeam(
                    canonical_name=api_name,
                    api_football_id=team_id,
                    api_football_name=api_name,
                    understat_name=u_team,
                    resolution_confidence=1.0,
                    resolution_method="exact",
                    resolved_at=now,
                )
            )
            unmatched_understat.discard(u_team)
            del api_normalized[norm_u]
            logger.debug("Team exact match: '%s' ↔ '%s'", u_team, api_name)

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
            team_id, api_name = api_normalized[best_api_key]
            resolved.append(
                ResolvedTeam(
                    canonical_name=api_name,
                    api_football_id=team_id,
                    api_football_name=api_name,
                    understat_name=u_team,
                    resolution_confidence=0.85,
                    resolution_method="fuzzy",
                    resolved_at=now,
                )
            )
            unmatched_understat.discard(u_team)
            del api_normalized[best_api_key]
            logger.debug(
                "Team fuzzy match: '%s' ↔ '%s' (score=%.1f)",
                u_team,
                api_name,
                best_score,
            )

    # Log unresolved teams
    for u_team in unmatched_understat:
        logger.error("Team unresolved: Understat '%s' has no match in API-Football", u_team)

    resolved_api_ids = {r.api_football_id for r in resolved}
    for _api_key, (team_id, api_name) in api_normalized.items():
        if team_id not in resolved_api_ids:
            resolved.append(
                ResolvedTeam(
                    canonical_name=api_name,
                    api_football_id=team_id,
                    api_football_name=api_name,
                    understat_name=None,
                    resolution_confidence=None,
                    resolution_method=None,
                    resolved_at=now,
                )
            )

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

_PLAYER_FUZZY_THRESHOLD = 0.85
_PLAYER_CROSS_TEAM_THRESHOLD = 0.75
_CONFLICT_THRESHOLD = 0.05
_STAT_GAMES_TOLERANCE = 3
_STAT_MINUTES_TOLERANCE_PCT = 0.20


def _build_team_mapping(
    resolved_teams: list[ResolvedTeam],
) -> dict[str, int]:
    """Build a mapping from normalized Understat team name to API-Football team_id."""
    mapping: dict[str, int] = {}
    for team in resolved_teams:
        if team.understat_name is not None:
            mapping[normalize_name(team.understat_name)] = team.api_football_id
    return mapping


def _has_conflict(scores: list[float]) -> bool:
    """Check if top two scores are too close, indicating ambiguity."""
    if len(scores) < 2:
        return False
    sorted_scores = sorted(scores, reverse=True)
    return (sorted_scores[0] - sorted_scores[1]) < _CONFLICT_THRESHOLD


def _check_transfer_history(
    api_player_id: int,
    understat_team_name: str,
    raw_transfers: list[RawAPIFootballTransfer],
    resolved_teams: list[ResolvedTeam],
) -> bool:
    """Check if an API-Football player has transfer history linking them to a team."""
    norm_u_team = normalize_name(understat_team_name)

    # Build set of API-Football team IDs that map to the Understat team
    team_ids: set[int] = set()
    for team in resolved_teams:
        if team.understat_name and normalize_name(team.understat_name) == norm_u_team:
            team_ids.add(team.api_football_id)

    for transfer in raw_transfers:
        if transfer.player_id != api_player_id:
            continue
        if transfer.team_in_id in team_ids or transfer.team_out_id in team_ids:
            return True
        # Also check by name if IDs don't match
        if transfer.team_in_name and normalize_name(transfer.team_in_name) == norm_u_team:
            return True
        if transfer.team_out_name and normalize_name(transfer.team_out_name) == norm_u_team:
            return True
    return False


def _stats_match(
    api_appearances: int | None,
    api_minutes: int | None,
    understat_games: int,
    understat_minutes: int,
) -> bool:
    """Check if two players have similar enough game/minute stats."""
    if api_appearances is None or api_minutes is None:
        return False
    if abs(api_appearances - understat_games) > _STAT_GAMES_TOLERANCE:
        return False
    if understat_minutes == 0 and api_minutes == 0:
        return True
    max_minutes = max(api_minutes, understat_minutes)
    if max_minutes == 0:
        return False
    minutes_diff = abs(api_minutes - understat_minutes) / max_minutes
    return minutes_diff <= _STAT_MINUTES_TOLERANCE_PCT


def _get_top_candidates(
    understat_name: str,
    api_players: dict[int, tuple[RawAPIFootballPlayer, list[str]]],
    n: int = 3,
) -> list[CandidateMatch]:
    """Get top-N best fuzzy match candidates for an unresolved Understat player."""
    scores: list[tuple[float, int, str]] = []
    for api_id, (api_player, variants) in api_players.items():
        score = best_match_score(understat_name, variants)
        scores.append((score, api_id, api_player.name))
    scores.sort(reverse=True)
    return [
        CandidateMatch(
            candidate_name=name,
            candidate_source="api_football",
            candidate_source_id=api_id,
            fuzzy_score=round(score, 4),
        )
        for score, api_id, name in scores[:n]
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

    # Index API-Football data
    api_player_map: dict[int, RawAPIFootballPlayer] = {p.player_id: p for p in api_players}
    api_variants_map: dict[int, list[str]] = {
        p.player_id: build_name_variants(p.name, p.firstname, p.lastname) for p in api_players
    }
    api_stats_by_player: dict[int, list[RawAPIFootballPlayerStats]] = {}
    for stat in api_stats:
        api_stats_by_player.setdefault(stat.player_id, []).append(stat)

    # Group API-Football players by team_id
    api_by_team: dict[int, set[int]] = {}
    for stat in api_stats:
        api_by_team.setdefault(stat.team_id, set()).add(stat.player_id)

    # Track which players have been matched
    matched_api: set[int] = set()
    matched_understat: set[int] = set()
    resolved: list[ResolvedPlayer] = []

    def _make_resolved(
        api_p: RawAPIFootballPlayer,
        u_p: RawUnderstatPlayerSeason,
        confidence: float,
        method: str,
    ) -> ResolvedPlayer:
        decoded_name = decode_api_name(api_p.name)
        return ResolvedPlayer(
            canonical_name=decoded_name,
            known_name=u_p.player_name if u_p.player_name != decoded_name else None,
            api_football_id=api_p.player_id,
            understat_id=u_p.player_id,
            birth_date=api_p.birth_date,
            nationality=api_p.nationality,
            photo_url=api_p.photo_url,
            resolution_confidence=confidence,
            resolution_method=method,
            resolved_at=now,
        )

    # ── Pass 1: Exact name + same team ──
    for u_player in understat_players:
        if u_player.player_id in matched_understat:
            continue
        norm_u = normalize_name(u_player.player_name)
        u_team_id = team_mapping.get(normalize_name(u_player.team))
        if u_team_id is None:
            continue

        candidates_in_team = api_by_team.get(u_team_id, set()) - matched_api
        for api_id in candidates_in_team:
            variants = api_variants_map.get(api_id, [])
            if norm_u in variants:
                api_p = api_player_map[api_id]
                resolved.append(_make_resolved(api_p, u_player, 1.0, "exact"))
                matched_api.add(api_id)
                matched_understat.add(u_player.player_id)
                logger.debug(
                    "Pass 1 exact: '%s' ↔ '%s' (team=%s)",
                    u_player.player_name,
                    api_p.name,
                    u_player.team,
                )
                break

    # ── Pass 2: Fuzzy name + same team ──
    for u_player in understat_players:
        if u_player.player_id in matched_understat:
            continue
        u_team_id = team_mapping.get(normalize_name(u_player.team))
        if u_team_id is None:
            continue

        candidates_in_team = api_by_team.get(u_team_id, set()) - matched_api
        best_score = 0.0
        best_api_id: int | None = None
        all_scores: list[float] = []
        for api_id in candidates_in_team:
            variants = api_variants_map.get(api_id, [])
            score = best_match_score(u_player.player_name, variants)
            all_scores.append(score)
            if score > best_score:
                best_score = score
                best_api_id = api_id

        if (
            best_api_id is not None
            and best_score >= _PLAYER_FUZZY_THRESHOLD
            and not _has_conflict(all_scores)
        ):
            api_p = api_player_map[best_api_id]
            resolved.append(_make_resolved(api_p, u_player, 0.90, "fuzzy"))
            matched_api.add(best_api_id)
            matched_understat.add(u_player.player_id)
            logger.debug(
                "Pass 2 fuzzy: '%s' ↔ '%s' (score=%.3f, team=%s)",
                u_player.player_name,
                api_p.name,
                best_score,
                u_player.team,
            )

    # ── Pass 3: Cross-team fuzzy + transfer history ──
    for u_player in understat_players:
        if u_player.player_id in matched_understat:
            continue
        all_unmatched_api = set(api_player_map.keys()) - matched_api
        best_score = 0.0
        best_api_id = None
        all_scores = []
        for api_id in all_unmatched_api:
            variants = api_variants_map.get(api_id, [])
            score = best_match_score(u_player.player_name, variants)
            all_scores.append(score)
            if score > best_score:
                best_score = score
                best_api_id = api_id

        if (
            best_api_id is not None
            and best_score >= _PLAYER_CROSS_TEAM_THRESHOLD
            and not _has_conflict(all_scores)
        ):
            # Verify transfer history
            if _check_transfer_history(best_api_id, u_player.team, raw_transfers, resolved_teams):
                api_p = api_player_map[best_api_id]
                resolved.append(_make_resolved(api_p, u_player, 0.70, "contextual"))
                matched_api.add(best_api_id)
                matched_understat.add(u_player.player_id)
                logger.debug(
                    "Pass 3 contextual: '%s' ↔ '%s' (score=%.3f, transfer confirmed)",
                    u_player.player_name,
                    api_p.name,
                    best_score,
                )

    # ── Pass 4: Statistical fingerprint + same team ──
    for u_player in understat_players:
        if u_player.player_id in matched_understat:
            continue
        u_team_id = team_mapping.get(normalize_name(u_player.team))
        if u_team_id is None:
            continue

        candidates_in_team = api_by_team.get(u_team_id, set()) - matched_api
        stat_matches: list[int] = []
        for api_id in candidates_in_team:
            stats_list = api_stats_by_player.get(api_id, [])
            for stat in stats_list:
                if stat.team_id == u_team_id and _stats_match(
                    stat.games.appearances,
                    stat.games.minutes,
                    u_player.games,
                    u_player.minutes,
                ):
                    stat_matches.append(api_id)
                    break

        if len(stat_matches) == 1:
            api_id = stat_matches[0]
            api_p = api_player_map[api_id]
            resolved.append(_make_resolved(api_p, u_player, 0.60, "statistical"))
            matched_api.add(api_id)
            matched_understat.add(u_player.player_id)
            logger.debug(
                "Pass 4 statistical: '%s' ↔ '%s' (team=%s, games=%d, minutes=%d)",
                u_player.player_name,
                api_p.name,
                u_player.team,
                u_player.games,
                u_player.minutes,
            )
        elif len(stat_matches) > 1:
            logger.debug(
                "Pass 4 conflict: '%s' has %d stat matches in team, skipping",
                u_player.player_name,
                len(stat_matches),
            )

    # ── Collect unresolved ──
    unresolved: list[UnresolvedPlayer] = []
    remaining_api = {
        api_id: (api_player_map[api_id], api_variants_map.get(api_id, []))
        for api_id in set(api_player_map.keys()) - matched_api
    }

    for u_player in understat_players:
        if u_player.player_id not in matched_understat:
            top = _get_top_candidates(u_player.player_name, remaining_api)
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

    for api_id in set(api_player_map.keys()) - matched_api:
        api_p = api_player_map[api_id]
        # Single-source API-Football player → add as resolved with only api_football_id
        resolved.append(
            ResolvedPlayer(
                canonical_name=decode_api_name(api_p.name),
                known_name=None,
                api_football_id=api_p.player_id,
                understat_id=None,
                birth_date=api_p.birth_date,
                nationality=api_p.nationality,
                photo_url=api_p.photo_url,
                resolution_confidence=None,
                resolution_method="unresolved",
                resolved_at=now,
            )
        )

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
