"""Shared query logic for DBAPI 2.0 adapters (Postgres, Redshift, Snowflake).

These warehouses all expose a standard cursor with ``execute``, ``fetchone``,
and ``description``. The actual SQL is portable across them: double-quoted
identifiers, ``MAX``, ``COUNT``, ``COUNT(DISTINCT)``. Only connection setup
differs.

DuckDB has a slightly different cursor model (the connection itself acts as
the cursor) and lives in its own adapter file.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from dbt_features.enrichment.models import ColumnStats

if TYPE_CHECKING:
    from dbt_features.catalog import FeatureGroup


def fully_qualified_name(
    group: FeatureGroup,
    default_schema: str | None,
    default_database: str | None,
) -> str:
    """Build a quoted ``[db.]schema.table`` reference for postgres-family SQL.

    Falls back to the profile's defaults if the catalog doesn't have a
    schema or database recorded — common when the manifest was generated
    against a different target.
    """

    schema = group.schema_name or default_schema
    database = group.database or default_database
    parts = [p for p in (database, schema, group.name) if p]
    if not parts:
        raise ValueError(f"Cannot qualify {group.name}: no schema or database available")
    return ".".join(f'"{_quote_inner(p)}"' for p in parts)


def run_group_query(
    cursor: Any,
    group: FeatureGroup,
    *,
    default_schema: str | None,
    default_database: str | None,
) -> tuple[Any, int | None, dict[str, ColumnStats]]:
    """Run the freshness + per-column stats queries against ``cursor``.

    Returns ``(max_timestamp_raw, row_count, columns)``. The caller is
    responsible for type-coercing ``max_timestamp_raw`` (drivers vary on
    whether they return ``datetime``, ``date``, or driver-specific types).

    Two round trips per group: one for ``MAX(ts)`` + ``COUNT(*)``, one for
    all columns' null/distinct counts. Avoids 1+N query patterns.
    """

    fqn = fully_qualified_name(group, default_schema, default_database)
    ts_col = group.timestamp_column

    if ts_col:
        cursor.execute(f'SELECT MAX("{_quote_inner(ts_col)}"), COUNT(*) FROM {fqn}')
        row = cursor.fetchone()
        max_ts_raw, row_count = row[0], row[1]
    else:
        cursor.execute(f"SELECT COUNT(*) FROM {fqn}")
        row = cursor.fetchone()
        max_ts_raw, row_count = None, row[0]

    columns: dict[str, ColumnStats] = {}
    feature_cols = [f.name for f in group.features]
    if feature_cols and row_count and row_count > 0:
        clauses: list[str] = []
        for col in feature_cols:
            safe = _quote_inner(col)
            clauses.append(
                f'COUNT(*) - COUNT("{safe}") AS "{safe}__nulls", '
                f'COUNT(DISTINCT "{safe}") AS "{safe}__distinct"'
            )
        cursor.execute(f"SELECT {', '.join(clauses)} FROM {fqn}")
        stats_row = cursor.fetchone()
        col_names = [d[0] for d in cursor.description]
        values = dict(zip(col_names, stats_row, strict=False))
        for col in feature_cols:
            # Postgres lower-cases unquoted identifiers in column names from
            # `description`. We quoted them in the SELECT, so the case
            # should round-trip — but be defensive in case some driver
            # normalizes anyway.
            n = values.get(f"{col}__nulls")
            d = values.get(f"{col}__distinct")
            if n is None and d is None:
                lc = col.lower()
                n = values.get(f"{lc}__nulls")
                d = values.get(f"{lc}__distinct")
            columns[col] = ColumnStats(
                null_count=_int_or_none(n),
                distinct_count=_int_or_none(d),
            )

    return max_ts_raw, _int_or_none(row_count), columns


def _quote_inner(identifier: str) -> str:
    """Escape any inner double-quotes for use inside a quoted identifier."""

    return identifier.replace('"', '""')


def _int_or_none(value: object) -> int | None:
    if value is None:
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None
