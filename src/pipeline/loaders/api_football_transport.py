"""Transport for API-Football: cache-first HTTP with rate limiting, retry and pagination.

Answers one question — how do I get a JSON response for this endpoint,
respecting the daily call budget and the cache TTL? — behind one interface.
It holds no endpoint knowledge: response shapes, extraction and season/league
semantics live in ``pipeline.loaders.api_football_loader`` and
``pipeline.loaders.api_football_recovery``, both of which depend on this
module rather than the other way around.
"""

from __future__ import annotations

import json
import logging
import time
from pathlib import Path
from typing import Any

import httpx

from pipeline.config import ApiFootballConfig

logger = logging.getLogger(__name__)

_BASE_URL = "https://v3.football.api-sports.io"
_MAX_RETRIES = 3
_RETRY_BACKOFF_SECONDS = (1.0, 2.0, 4.0)


class APIFootballError(Exception):
    """Raised when API-Football returns an error or is unreachable."""


class APIFootballPlanRestricted(APIFootballError):
    """Raised when the configured season is blocked by the API-Football plan.

    Unlike a generic :class:`APIFootballError`, this signals a known, expected
    limitation of the free tier (seasons are capped, e.g. 2022-2024) rather than
    a transient failure.  Callers should surface the message to the operator
    instead of retrying, since retrying cannot succeed for the same season.
    """


class ApiFootballTransport:
    """Cache-first, rate-limited, retried HTTP GET against API-Football.

    Args:
        config: API-Football configuration from ``ingestion.yaml``.
        api_key: API key for the ``x-apisports-key`` header.
        client: Optional injectable ``httpx.Client`` (for testing).
    """

    def __init__(
        self,
        config: ApiFootballConfig,
        api_key: str,
        client: httpx.Client | None = None,
    ) -> None:
        self._config = config
        self._client = client or httpx.Client(
            base_url=_BASE_URL,
            headers={"x-apisports-key": api_key},
            timeout=30.0,
        )
        self._owns_client = client is None
        self._cache_ttl_seconds = config.cache_ttl_hours * 3600
        self._calls_made = 0
        self._cache_hits = 0
        self._last_call_time = 0.0

    @property
    def calls_made(self) -> int:
        """Number of real HTTP calls made so far (excludes cache hits)."""
        return self._calls_made

    @property
    def cache_hits(self) -> int:
        """Number of requests served from cache so far."""
        return self._cache_hits

    def close(self) -> None:
        """Close the HTTP client if it was created internally."""
        if self._owns_client:
            self._client.close()

    def __enter__(self) -> ApiFootballTransport:
        return self

    def __exit__(self, *exc: object) -> None:
        self.close()

    # ─────────────────────────────────────────────────────────
    # Cache
    # ─────────────────────────────────────────────────────────

    def _cache_path(self, endpoint: str, params: dict[str, str | int]) -> Path:
        """Build a human-readable cache file path from endpoint and params.

        Examples::

            players/league_140_season_2024_page_1.json
            injuries/league_140_season_2024_page_1.json
            transfers/team_529.json
        """
        parts = "_".join(f"{k}_{v}" for k, v in sorted(params.items()))
        return Path(self._config.cache_dir) / endpoint / f"{parts}.json"

    def _read_cache(self, path: Path) -> dict[str, Any] | None:
        """Return cached JSON if the file exists and TTL has not expired."""
        if not path.exists():
            return None
        age = time.time() - path.stat().st_mtime
        if age > self._cache_ttl_seconds:
            logger.debug("Cache expired: %s (age=%.0fs)", path, age)
            return None
        with path.open(encoding="utf-8") as f:
            data: dict[str, Any] = json.load(f)
        self._cache_hits += 1
        logger.debug("Cache hit: %s", path)
        return data

    def _write_cache(self, path: Path, data: dict[str, Any]) -> None:
        """Write raw API response JSON to cache."""
        path.parent.mkdir(parents=True, exist_ok=True)
        with path.open("w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False)

    # ─────────────────────────────────────────────────────────
    # Request + pagination
    # ─────────────────────────────────────────────────────────

    def request(
        self,
        endpoint: str,
        params: dict[str, str | int],
        *,
        force_refresh: bool = False,
    ) -> dict[str, Any]:
        """Execute a cache-first HTTP GET against API-Football.

        Args:
            endpoint: API endpoint name (e.g. ``"players"``).
            params: Query parameters for the request.
            force_refresh: If ``True``, skip the cache lookup.

        Returns:
            The full parsed JSON response dict (envelope included).

        Raises:
            APIFootballError: On rate limit exhaustion, HTTP errors after
                retries, or API-level errors.
        """
        cache_file = self._cache_path(endpoint, params)

        if not force_refresh:
            cached = self._read_cache(cache_file)
            if cached is not None:
                return cached

        # Rate limit check
        max_calls = self._config.rate_limit.max_calls_per_day
        if self._calls_made >= max_calls:
            msg = f"Daily rate limit exhausted ({max_calls} calls). Cannot request {endpoint} {params}"
            logger.error(msg)
            raise APIFootballError(msg)

        # Enforce delay between calls
        delay = self._config.rate_limit.delay_between_calls
        elapsed = time.time() - self._last_call_time
        if elapsed < delay and self._last_call_time > 0:
            time.sleep(delay - elapsed)

        # Retry loop
        last_error: Exception | None = None
        for attempt, backoff in enumerate(_RETRY_BACKOFF_SECONDS):
            try:
                response = self._client.get(f"/{endpoint}", params=params)
                response.raise_for_status()
                break
            except (httpx.HTTPStatusError, httpx.RequestError) as exc:
                last_error = exc
                if attempt < _MAX_RETRIES - 1:
                    logger.warning(
                        "Retry %d/%d for %s %s: %s",
                        attempt + 1,
                        _MAX_RETRIES,
                        endpoint,
                        params,
                        exc,
                    )
                    time.sleep(backoff)
        else:
            msg = f"Failed after {_MAX_RETRIES} retries: {endpoint} {params}"
            logger.error(msg)
            raise APIFootballError(msg) from last_error

        self._last_call_time = time.time()
        self._calls_made += 1

        data: dict[str, Any] = response.json()

        # Check API-level errors
        api_errors = data.get("errors")
        if api_errors and len(api_errors) > 0:
            errors_dict = api_errors if isinstance(api_errors, dict) else {}
            # Rate limit: wait 65s and retry once (rolling 1-minute window)
            if "rateLimit" in errors_dict:
                logger.warning("Rate limit hit for %s %s — sleeping 65s then retrying", endpoint, params)
                time.sleep(65)
                return self.request(endpoint, params, force_refresh=force_refresh)
            # Plan restriction: the free tier caps either the accessible season
            # range (e.g. 2022-2024) or the ``page`` parameter (max 3 for
            # /players).  This is permanent for the given request — raise a
            # clear, actionable error instead of a cryptic API envelope dump.
            # Note: pagination-triggered page limits are normally caught and
            # tolerated by ``paginate`` (partial data returned), but the
            # message below still helps operators understand *why*.
            plan_msg = errors_dict.get("plan")
            if plan_msg:
                is_page_limit = "page" in plan_msg.lower()
                if is_page_limit:
                    guidance = (
                        "Free tier caps the 'page' parameter (max 3). "
                        "Player data is truncated to the first 3 pages per query; "
                        "this is expected on the free plan and the loader degrades gracefully. "
                        "For complete player coverage, upgrade to a paid plan."
                    )
                else:
                    guidance = (
                        f"The configured season {self._config.season} is outside the "
                        f"accessible range for the current plan. Use a season within the "
                        f"allowed range, or upgrade to a paid plan."
                    )
                msg = f"API-Football plan restriction: {plan_msg} {guidance} (request: {endpoint} {params})"
                logger.error(msg)
                raise APIFootballPlanRestricted(msg)
            # errors can be a list or a dict depending on the error type
            msg = f"API-Football error for {endpoint} {params}: {api_errors}"
            logger.error(msg)
            raise APIFootballError(msg)

        # Log rate limit headers
        remaining = response.headers.get("x-ratelimit-requests-remaining")
        if remaining is not None:
            remaining_int = int(remaining)
            if remaining_int < 20:
                logger.warning("API rate limit: %d calls remaining today", remaining_int)
            else:
                logger.debug("API rate limit: %d calls remaining today", remaining_int)

        paging = data.get("paging", {})
        results = data.get("results", 0)
        logger.debug(
            "GET /%s %s → %d results (page %d/%d)",
            endpoint,
            params,
            results,
            paging.get("current", 1),
            paging.get("total", 1),
        )

        self._write_cache(cache_file, data)
        return data

    def paginate(
        self,
        endpoint: str,
        params: dict[str, str | int],
        *,
        force_refresh: bool = False,
    ) -> tuple[list[dict[str, Any]], int]:
        """Fetch all pages for a paginated endpoint.

        Returns:
            A tuple of ``(items, total_pages)`` where ``items`` is the flat
            list of all ``response`` entries collected and ``total_pages`` is
            the ``paging.total`` reported by the API (the last value observed
            before any early stop).  ``total_pages`` lets callers detect
            truncation when the free-tier page cap kicks in.
        """
        all_items: list[dict[str, Any]] = []
        page = 1
        total_pages = 1

        while True:
            page_params = {**params, "page": page}
            try:
                data = self.request(endpoint, page_params, force_refresh=force_refresh)
            except APIFootballError as exc:
                if page > 1:
                    # Free plan limits pagination (e.g. max 3 pages). Return
                    # whatever we have collected so far rather than crashing.
                    logger.warning("Stopping pagination at page %d for %s: %s", page, endpoint, exc)
                    break
                raise
            all_items.extend(data.get("response", []))

            paging = data.get("paging", {})
            total_pages = paging.get("total", 1)
            if page >= total_pages:
                break
            page += 1

        return all_items, total_pages
