"""TTL-bounded JSON cache for enrichment data.

Why a cache: a typical project has 10-50 feature tables; running freshness
queries on every catalog rebuild during development hammers the warehouse
for no reason. Cached results expire after the TTL (default 1 hour) so
manual refreshes still work — pass ``--cache-ttl 0`` or ``--no-cache``.

Format is plain JSON so users can poke at it, diff it, or delete it.
"""

from __future__ import annotations

import json
from dataclasses import asdict
from datetime import datetime, timezone
from pathlib import Path

from dbt_features.enrichment.models import ColumnStats, FreshnessSnapshot

CACHE_FORMAT_VERSION = "1"


class EnrichmentCache:
    """Load/store a map of ``unique_id -> FreshnessSnapshot``."""

    def __init__(self, path: Path, ttl_seconds: int = 3600):
        self.path = path
        self.ttl_seconds = ttl_seconds

    def read(self) -> dict[str, FreshnessSnapshot] | None:
        """Return cached snapshots if the file exists and isn't expired.

        Returns ``None`` on miss, expiry, or any read error — the caller
        treats all three identically (re-fetch from the warehouse). We
        don't surface read errors because they're never the user's
        problem; a corrupt cache file should just be regenerated.
        """

        if not self.path.exists():
            return None
        try:
            data = json.loads(self.path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            return None

        if data.get("format") != CACHE_FORMAT_VERSION:
            return None
        try:
            fetched_at = _parse_dt(data["fetched_at"])
        except (KeyError, ValueError):
            return None

        age = (datetime.now(timezone.utc) - fetched_at).total_seconds()
        if age > self.ttl_seconds:
            return None

        out: dict[str, FreshnessSnapshot] = {}
        for uid, raw in (data.get("groups") or {}).items():
            try:
                out[uid] = _deserialize_snapshot(raw)
            except (KeyError, ValueError):
                # One bad entry shouldn't invalidate the whole cache —
                # but we also don't want to silently drop it. Skip it
                # and let the caller re-fetch only what's missing if it
                # wants. (Engine currently re-fetches all on miss; that's
                # fine because fetch is the rare path.)
                continue
        return out

    def write(self, snapshots: dict[str, FreshnessSnapshot]) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        payload = {
            "format": CACHE_FORMAT_VERSION,
            "fetched_at": datetime.now(timezone.utc).isoformat(),
            "ttl_seconds": self.ttl_seconds,
            "groups": {uid: _serialize_snapshot(s) for uid, s in snapshots.items()},
        }
        self.path.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")

    def clear(self) -> None:
        if self.path.exists():
            self.path.unlink()


def _serialize_snapshot(s: FreshnessSnapshot) -> dict[str, object]:
    return {
        "queried_at": s.queried_at.isoformat(),
        "max_timestamp": s.max_timestamp.isoformat() if s.max_timestamp else None,
        "row_count": s.row_count,
        "error": s.error,
        "columns": {name: asdict(stats) for name, stats in s.columns.items()},
    }


def _deserialize_snapshot(raw: dict[str, object]) -> FreshnessSnapshot:
    columns = {}
    for name, stats_raw in (raw.get("columns") or {}).items():
        if not isinstance(stats_raw, dict):
            continue
        columns[name] = ColumnStats(
            null_count=stats_raw.get("null_count"),
            distinct_count=stats_raw.get("distinct_count"),
        )
    return FreshnessSnapshot(
        queried_at=_parse_dt(raw["queried_at"]),  # type: ignore[arg-type]
        max_timestamp=_parse_dt(raw["max_timestamp"]) if raw.get("max_timestamp") else None,
        row_count=raw.get("row_count"),  # type: ignore[arg-type]
        error=raw.get("error"),  # type: ignore[arg-type]
        columns=columns,
    )


def _parse_dt(value: str) -> datetime:
    """Parse an ISO-8601 datetime, normalizing to aware UTC.

    Older Python ``fromisoformat`` doesn't accept the ``Z`` suffix; replace
    it with ``+00:00`` first. Naïve datetimes get ``UTC`` attached so all
    arithmetic in the rest of the codebase is timezone-aware.
    """

    if value.endswith("Z"):
        value = value[:-1] + "+00:00"
    dt = datetime.fromisoformat(value)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt
