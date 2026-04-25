"""DuckDB adapter.

The simplest warehouse to support — a single Python dep, a file on disk.
Used in tests and for local development against ``dbt-duckdb`` projects.
The structure here is the template the other adapters (postgres, redshift,
snowflake, bigquery) will follow.
"""

from __future__ import annotations

from datetime import datetime, timezone
from typing import TYPE_CHECKING, Any

from dbt_features.enrichment.exceptions import EnrichmentError
from dbt_features.enrichment.models import ColumnStats, FreshnessSnapshot

if TYPE_CHECKING:
    from dbt_features.catalog import FeatureGroup


class DuckDBAdapter:
    """Reads from a DuckDB database file (or in-memory).

    Read-only by default — we never need to write, and read-only mode
    plays nicely with concurrent dbt processes that may have the file
    open. If the file isn't writable, DuckDB silently falls back to
    read-only anyway, so this is belt-and-suspenders.
    """

    def __init__(self, profile: dict[str, Any]):
        try:
            import duckdb
        except ImportError as e:
            raise EnrichmentError(
                "DuckDB driver not installed. Install with: pip install dbt-features[duckdb]"
            ) from e

        path = profile.get("path") or ":memory:"
        try:
            self._conn = duckdb.connect(database=str(path), read_only=path != ":memory:")
        except duckdb.Error as e:  # type: ignore[attr-defined]
            raise EnrichmentError(f"Could not open DuckDB at {path}: {e}") from e

        # Stash so fetch_group_stats can build fully-qualified names.
        self._default_schema = profile.get("schema") or "main"
        self._default_database = profile.get("database")

    def fetch_group_stats(self, group: FeatureGroup) -> FreshnessSnapshot:
        queried_at = datetime.now(timezone.utc)
        try:
            fqn = self._fqn(group)

            ts_col = group.timestamp_column
            if ts_col:
                row = self._conn.execute(
                    f'SELECT MAX("{ts_col}") AS mx, COUNT(*) AS cnt FROM {fqn}'
                ).fetchone()
                max_ts_raw, row_count = row
            else:
                row = self._conn.execute(f"SELECT COUNT(*) AS cnt FROM {fqn}").fetchone()
                max_ts_raw, row_count = None, row[0]

            max_ts = _coerce_dt(max_ts_raw)

            columns: dict[str, ColumnStats] = {}
            feature_cols = [f.name for f in group.features]
            if feature_cols and row_count and row_count > 0:
                # One round trip per group: SELECT all the null/distinct
                # counts together. Keeps the warehouse hit predictable
                # (one query per group rather than 1 + N).
                clauses: list[str] = []
                for col in feature_cols:
                    safe = col.replace('"', '""')
                    clauses.append(
                        f'COUNT(*) - COUNT("{safe}") AS "{safe}__nulls", '
                        f'COUNT(DISTINCT "{safe}") AS "{safe}__distinct"'
                    )
                stats_sql = f"SELECT {', '.join(clauses)} FROM {fqn}"
                cursor = self._conn.execute(stats_sql)
                stats_row = cursor.fetchone()
                col_names = [d[0] for d in cursor.description]
                values = dict(zip(col_names, stats_row, strict=False))
                for col in feature_cols:
                    columns[col] = ColumnStats(
                        null_count=_int_or_none(values.get(f"{col}__nulls")),
                        distinct_count=_int_or_none(values.get(f"{col}__distinct")),
                    )

            return FreshnessSnapshot(
                queried_at=queried_at,
                max_timestamp=max_ts,
                row_count=int(row_count) if row_count is not None else None,
                columns=columns,
            )
        except Exception as e:  # noqa: BLE001 - we want one bad table to not abort the whole build
            return FreshnessSnapshot(queried_at=queried_at, error=f"{type(e).__name__}: {e}")

    def close(self) -> None:
        import contextlib

        with contextlib.suppress(Exception):
            self._conn.close()

    def _fqn(self, group: FeatureGroup) -> str:
        """Build a quoted ``[db.]schema.table`` reference.

        DuckDB uses double quotes for identifiers and is case-sensitive
        when quoted. We prefer the catalog's stored values (from
        manifest.json) but fall back to the profile's defaults so a
        partially-populated manifest still resolves.
        """

        database = group.database or self._default_database
        schema = group.schema_name or self._default_schema
        parts = [p for p in (database, schema, group.name) if p]
        return ".".join(f'"{p}"' for p in parts)


def _coerce_dt(value: object) -> datetime | None:
    if value is None:
        return None
    if isinstance(value, datetime):
        return value if value.tzinfo else value.replace(tzinfo=timezone.utc)
    # DuckDB returns date as datetime.date — promote it.
    try:
        from datetime import date as _date

        if isinstance(value, _date):
            return datetime(value.year, value.month, value.day, tzinfo=timezone.utc)
    except ImportError:  # pragma: no cover
        pass
    return None


def _int_or_none(value: object) -> int | None:
    if value is None:
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None
