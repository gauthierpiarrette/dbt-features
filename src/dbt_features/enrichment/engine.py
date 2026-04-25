"""Orchestration: catalog + profile -> per-group warehouse snapshots.

Top-level callable for the rest of the codebase. Manages cache, adapter
lifecycle, and the read-through pattern. Stays simple on purpose — the
adapters do the warehouse-specific work; this module is plumbing.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from dbt_features.enrichment.adapters import get_adapter
from dbt_features.enrichment.cache import EnrichmentCache
from dbt_features.enrichment.exceptions import EnrichmentError
from dbt_features.enrichment.models import FreshnessSnapshot
from dbt_features.enrichment.profiles import load_profile

if TYPE_CHECKING:
    from pathlib import Path

    from dbt_features.catalog import Catalog


def enrich_catalog(
    catalog: Catalog,
    *,
    profile_name: str,
    target: str | None = None,
    profiles_dir: Path | None = None,
    cache: EnrichmentCache | None = None,
) -> dict[str, FreshnessSnapshot]:
    """Fetch warehouse facts for every feature group in ``catalog``.

    Returns a map keyed by ``unique_id``. If a cache is provided and the
    cached results are within TTL, the warehouse is not contacted at all.
    Per-group failures land on the snapshot's ``error`` field — they don't
    raise — so a missing or permission-denied table degrades to "freshness
    check failed" in the UI rather than aborting the catalog build.
    """

    if cache is not None:
        cached = cache.read()
        if cached is not None:
            # Only honor the cache if it covers every current group.
            # If the user added a new feature table since last fetch, we
            # need to re-query. (Cache invalidation by addition.)
            cached_uids = set(cached.keys())
            current_uids = {g.unique_id for g in catalog.feature_groups}
            if current_uids.issubset(cached_uids):
                return {uid: cached[uid] for uid in current_uids}

    profile = load_profile(profile_name, target=target, profiles_dir=profiles_dir)
    adapter = get_adapter(profile)
    try:
        results: dict[str, FreshnessSnapshot] = {}
        for group in catalog.feature_groups:
            results[group.unique_id] = adapter.fetch_group_stats(group)
    finally:
        adapter.close()

    if cache is not None:
        cache.write(results)
    return results


__all__ = ["EnrichmentError", "EnrichmentCache", "FreshnessSnapshot", "enrich_catalog"]
