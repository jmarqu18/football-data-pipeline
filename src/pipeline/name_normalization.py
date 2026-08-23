"""Name preparation for cross-source matching.

Turns raw source names into the normalized forms that candidate scoring
compares. Both team resolution and player resolution depend on this module,
and so does ``pipeline.resolution_ledger`` when it builds canonical names —
keeping these helpers here is what stops those modules importing each other.

This module produces prepared text; it does not compare it. Comparison lives
in ``pipeline.match_scoring`` (see ADR-004).
"""

from __future__ import annotations

import html
import re

from unidecode import unidecode

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
