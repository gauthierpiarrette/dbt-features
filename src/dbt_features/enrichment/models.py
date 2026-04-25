"""Data classes representing warehouse-derived enrichment data.

Kept frozen + slotted so the renderer can assume immutability and so
serialization to/from JSON is deterministic. Per-feature-group snapshots
are independent: any one can fail without aborting the whole run.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime


@dataclass(frozen=True, slots=True)
class ColumnStats:
    """Cheap stats for a single feature column.

    Both fields are optional because (a) some warehouses can't compute one
    or the other for certain types, and (b) we may skip the stats query
    when the table is empty (saves a round trip).
    """

    null_count: int | None = None
    distinct_count: int | None = None


@dataclass(frozen=True, slots=True)
class FreshnessSnapshot:
    """Per-feature-group warehouse facts, captured at a single moment.

    ``error`` is set when the query failed (table missing, permission
    denied, etc.). When set, the other fields will be ``None``. The renderer
    surfaces the error inline instead of pretending the data is current.
    """

    queried_at: datetime
    max_timestamp: datetime | None = None
    row_count: int | None = None
    error: str | None = None
    columns: dict[str, ColumnStats] = field(default_factory=dict)


@dataclass(frozen=True, slots=True)
class FreshnessStatus:
    """Computed status for a feature group, derived from snapshot + SLA.

    Three states map to the green/yellow/red badge in the UI. Computed at
    render time, not at fetch time, so changing the SLA in YAML doesn't
    require a fresh warehouse query.
    """

    label: str  # "fresh" | "warn" | "error" | "unknown"
    age_seconds: int | None
    age_human: str  # e.g. "2 hours ago"
