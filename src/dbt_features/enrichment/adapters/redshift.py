"""Redshift adapter via ``redshift-connector``.

We use AWS's official ``redshift-connector`` rather than psycopg2 so IAM
auth works without additional dependencies. The wire protocol is
postgres-compatible, so query SQL is identical to ``postgres.py`` — only
connection setup differs.

dbt-redshift profile keys we honor:
- Password auth: ``host``, ``port``, ``user``, ``password``, ``dbname``, ``schema``
- IAM auth: ``method: iam`` (or ``iam: true``), plus ``cluster_id`` and
  optionally ``region``, ``iam_profile``
- Serverless auth: ``cluster_id`` omitted, ``region`` set, ``host`` is the
  serverless endpoint

Anything fancier (SSO, federated identity) routes through the underlying
driver via ``connect_kwargs`` passthrough — we don't reimplement it.
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


class RedshiftAdapter:
    def __init__(self, profile: dict[str, Any]):
        try:
            import redshift_connector
        except ImportError as e:
            raise EnrichmentError(
                "Redshift driver not installed. "
                "Install with: pip install dbt-features[redshift]"
            ) from e

        database = profile.get("dbname") or profile.get("database")
        if not database:
            raise EnrichmentError("Redshift profile missing 'dbname' (or 'database').")

        # dbt-redshift's profile uses ``method: iam`` to flip auth modes;
        # also tolerate the boolean ``iam: true`` shorthand we've seen in
        # community configs.
        is_iam = profile.get("method") == "iam" or bool(profile.get("iam"))

        connect_kwargs: dict[str, Any] = {
            "database": database,
            "user": profile.get("user"),
            "host": profile.get("host"),
            "port": int(profile.get("port", 5439)),
            "timeout": int(profile.get("connect_timeout", 30)),
        }

        if is_iam:
            connect_kwargs["iam"] = True
            cluster_id = profile.get("cluster_id") or profile.get("cluster_identifier")
            if cluster_id:
                connect_kwargs["cluster_identifier"] = cluster_id
            region = profile.get("region") or profile.get("region_name")
            if region:
                connect_kwargs["region"] = region
            if profile.get("iam_profile") or profile.get("profile"):
                connect_kwargs["profile"] = profile.get("iam_profile") or profile.get("profile")
            if profile.get("access_key_id"):
                connect_kwargs["access_key_id"] = profile["access_key_id"]
            if profile.get("secret_access_key"):
                connect_kwargs["secret_access_key"] = profile["secret_access_key"]
            if profile.get("session_token"):
                connect_kwargs["session_token"] = profile["session_token"]
        else:
            password = profile.get("password")
            if not password:
                raise EnrichmentError(
                    "Redshift profile missing 'password' (and IAM auth not enabled). "
                    "Set password, or add `method: iam` to the profile."
                )
            connect_kwargs["password"] = password

        if profile.get("ssl") is not None:
            connect_kwargs["ssl"] = profile["ssl"]
        if profile.get("autocommit") is not None:
            connect_kwargs["autocommit"] = profile["autocommit"]

        try:
            self._conn = redshift_connector.connect(
                **{k: v for k, v in connect_kwargs.items() if v is not None}
            )
        except Exception as e:  # noqa: BLE001 - any driver-internal error is fatal
            raise EnrichmentError(f"Could not connect to Redshift: {e}") from e

        # Read-only session — same defense-in-depth as the Postgres adapter.
        with contextlib.suppress(Exception):
            self._conn.autocommit = True

        self._default_schema = profile.get("schema") or "public"
        self._default_database = database

    def fetch_group_stats(self, group: FeatureGroup) -> FreshnessSnapshot:
        queried_at = datetime.now(timezone.utc)
        try:
            cursor = self._conn.cursor()
            try:
                max_ts_raw, row_count, columns = run_group_query(
                    cursor,
                    group,
                    default_schema=self._default_schema,
                    default_database=self._default_database,
                )
            finally:
                with contextlib.suppress(Exception):
                    cursor.close()
            return FreshnessSnapshot(
                queried_at=queried_at,
                max_timestamp=_coerce_dt(max_ts_raw),
                row_count=row_count,
                columns=columns,
            )
        except Exception as e:  # noqa: BLE001
            with contextlib.suppress(Exception):
                self._conn.rollback()
            return FreshnessSnapshot(queried_at=queried_at, error=f"{type(e).__name__}: {e}")

    def close(self) -> None:
        with contextlib.suppress(Exception):
            self._conn.close()


def _coerce_dt(value: object) -> datetime | None:
    if value is None:
        return None
    if isinstance(value, datetime):
        return value if value.tzinfo else value.replace(tzinfo=timezone.utc)
    from datetime import date as _date

    if isinstance(value, _date):
        return datetime(value.year, value.month, value.day, tzinfo=timezone.utc)
    return None
