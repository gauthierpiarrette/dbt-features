"""Postgres adapter via ``psycopg2``.

Maps dbt-postgres profile keys (``host``, ``port``, ``user``, ``password``,
``dbname``, ``schema``) onto ``psycopg2.connect``. Keeps the auth surface
minimal: password and user. Advanced auth (``sslcert``, certificate-based
auth) goes through psycopg2 if present in the profile via the
``connect_kwargs`` passthrough.
"""

from __future__ import annotations

import contextlib
from datetime import datetime, timezone
from typing import TYPE_CHECKING, Any

from dbt_features.enrichment.adapters._dbapi import run_group_query
from dbt_features.enrichment.exceptions import EnrichmentError
from dbt_features.enrichment.models import FreshnessSnapshot

if TYPE_CHECKING:
    from dbt_features.catalog import FeatureGroup


class PostgresAdapter:
    def __init__(self, profile: dict[str, Any]):
        try:
            import psycopg2
        except ImportError as e:
            raise EnrichmentError(
                "Postgres driver not installed. "
                "Install with: pip install dbt-features[postgres]"
            ) from e

        host = profile.get("host")
        if not host:
            raise EnrichmentError("Postgres profile missing 'host'.")

        # dbt's postgres profile uses 'dbname'; tolerate 'database' as an
        # alias (some users come from non-dbt configs).
        database = profile.get("dbname") or profile.get("database")
        if not database:
            raise EnrichmentError("Postgres profile missing 'dbname' (or 'database').")

        connect_kwargs = {
            "host": host,
            "port": int(profile.get("port", 5432)),
            "user": profile.get("user"),
            "password": profile.get("password"),
            "dbname": database,
            "connect_timeout": int(profile.get("connect_timeout", 10)),
        }
        # Pass through SSL options if set — common in production setups.
        for k in ("sslmode", "sslrootcert", "sslcert", "sslkey"):
            if k in profile:
                connect_kwargs[k] = profile[k]

        try:
            self._conn = psycopg2.connect(**{k: v for k, v in connect_kwargs.items() if v is not None})
        except psycopg2.Error as e:
            raise EnrichmentError(f"Could not connect to Postgres: {e}") from e

        # Treat the session as read-only as a defense-in-depth: the queries
        # we run are SELECTs only, but a misconfigured profile pointing at
        # the wrong database deserves an explicit "we won't write" assurance.
        with contextlib.suppress(Exception):
            self._conn.set_session(readonly=True, autocommit=True)

        self._default_schema = profile.get("schema") or "public"
        self._default_database = database

    def fetch_group_stats(self, group: FeatureGroup) -> FreshnessSnapshot:
        queried_at = datetime.now(timezone.utc)
        try:
            with self._conn.cursor() as cursor:
                max_ts_raw, row_count, columns = run_group_query(
                    cursor,
                    group,
                    default_schema=self._default_schema,
                    default_database=self._default_database,
                )
            return FreshnessSnapshot(
                queried_at=queried_at,
                max_timestamp=_coerce_dt(max_ts_raw),
                row_count=row_count,
                columns=columns,
            )
        except Exception as e:  # noqa: BLE001
            # Postgres aborts the whole transaction after an error; rollback
            # so subsequent groups can still query on the same connection.
            with contextlib.suppress(Exception):
                self._conn.rollback()
            return FreshnessSnapshot(queried_at=queried_at, error=f"{type(e).__name__}: {e}")

    def close(self) -> None:
        with contextlib.suppress(Exception):
            self._conn.close()


def _coerce_dt(value: object) -> datetime | None:
    """Normalize whatever psycopg2 returns into an aware UTC ``datetime``.

    psycopg2 returns ``datetime`` for ``timestamp`` / ``timestamptz`` and
    ``date`` for ``date``. We promote ``date`` to a UTC datetime at midnight
    so all downstream arithmetic is uniform.
    """

    if value is None:
        return None
    if isinstance(value, datetime):
        return value if value.tzinfo else value.replace(tzinfo=timezone.utc)
    from datetime import date as _date

    if isinstance(value, _date):
        return datetime(value.year, value.month, value.day, tzinfo=timezone.utc)
    return None
