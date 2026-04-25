"""Helpers that turn raw enrichment snapshots into render-ready strings.

Kept separate from the rendering layer because the same logic needs to
run from templates and (later) JSON exports. Pure functions, no I/O.
"""

from __future__ import annotations

from datetime import datetime, timezone
from typing import TYPE_CHECKING

from dbt_features.enrichment.models import FreshnessStatus
from dbt_features.schema import Freshness, FreshnessPeriod, FreshnessThreshold

if TYPE_CHECKING:
    from dbt_features.enrichment.models import FreshnessSnapshot

_PERIOD_SECONDS = {
    FreshnessPeriod.MINUTE: 60,
    FreshnessPeriod.HOUR: 3600,
    FreshnessPeriod.DAY: 86_400,
}


def threshold_to_seconds(threshold: FreshnessThreshold) -> int:
    return threshold.count * _PERIOD_SECONDS[threshold.period]


def humanize_duration(seconds: int) -> str:
    """Render a duration in the granularity that fits its size.

    "2 hours ago" beats "7,317 seconds ago" every time. We bias toward
    fewer significant digits — "1 day ago" rather than "1.04 days ago" —
    because the precision isn't real (cache TTL alone introduces ±1h of
    noise).
    """

    if seconds < 0:
        seconds = 0
    if seconds < 60:
        return "just now"
    if seconds < 3600:
        m = seconds // 60
        return f"{m} minute{'s' if m != 1 else ''} ago"
    if seconds < 86_400:
        h = seconds // 3600
        return f"{h} hour{'s' if h != 1 else ''} ago"
    d = seconds // 86_400
    return f"{d} day{'s' if d != 1 else ''} ago"


def humanize_count(n: int | None) -> str:
    """Format a row/distinct count with thousands separators.

    Compact suffixes (K/M) are tempting but lose information at the small
    end. Engineers want the exact number when triaging a freshness issue.
    """

    if n is None:
        return "—"
    return f"{n:,}"


def humanize_percent(numerator: int | None, denominator: int | None) -> str:
    if numerator is None or denominator is None or denominator == 0:
        return "—"
    pct = (numerator / denominator) * 100
    if pct == 0:
        return "0%"
    if pct < 0.01:
        return "<0.01%"
    if pct < 1:
        return f"{pct:.2f}%"
    return f"{pct:.1f}%"


def compute_freshness_status(
    snapshot: FreshnessSnapshot | None,
    freshness: Freshness | None,
    *,
    now: datetime | None = None,
) -> FreshnessStatus:
    """Resolve a green/yellow/red status for one feature group.

    The result drives both a coarse classifier (the badge color) and a
    human-readable age string. Decoupled from the templates so it can be
    unit-tested without rendering HTML.
    """

    now = now or datetime.now(timezone.utc)

    if snapshot is None:
        return FreshnessStatus(label="unknown", age_seconds=None, age_human="—")
    if snapshot.error:
        return FreshnessStatus(label="error", age_seconds=None, age_human="check failed")
    if snapshot.max_timestamp is None:
        # No timestamp column declared — we have row_count but can't reason
        # about age. Treat as "fresh" only if the table isn't empty; else
        # flag it.
        if snapshot.row_count and snapshot.row_count > 0:
            return FreshnessStatus(label="fresh", age_seconds=None, age_human="not time-anchored")
        return FreshnessStatus(label="warn", age_seconds=None, age_human="empty")

    max_ts = snapshot.max_timestamp
    if max_ts.tzinfo is None:
        max_ts = max_ts.replace(tzinfo=timezone.utc)
    age_seconds = max(0, int((now - max_ts).total_seconds()))
    age_human = humanize_duration(age_seconds)

    if freshness is not None:
        if freshness.error_after and age_seconds >= threshold_to_seconds(freshness.error_after):
            return FreshnessStatus(label="error", age_seconds=age_seconds, age_human=age_human)
        if freshness.warn_after and age_seconds >= threshold_to_seconds(freshness.warn_after):
            return FreshnessStatus(label="warn", age_seconds=age_seconds, age_human=age_human)

    return FreshnessStatus(label="fresh", age_seconds=age_seconds, age_human=age_human)
