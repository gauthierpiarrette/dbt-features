"""Tests for the postgres-flavored adapter family.

Real Postgres/Redshift servers are out of scope for unit tests — we mock
the DBAPI driver modules. The shared ``_dbapi.run_group_query`` helper is
exercised by a fake cursor; the per-warehouse adapters are tested for
correct connection setup.
"""

from __future__ import annotations

import sys
from datetime import datetime, timezone
from typing import Any
from unittest.mock import MagicMock

import pytest

from dbt_features.catalog import Catalog, Feature, FeatureGroup
from dbt_features.enrichment.adapters._dbapi import (
    fully_qualified_name,
    run_group_query,
)
from dbt_features.enrichment.exceptions import EnrichmentError
from dbt_features.schema import FeatureTableMeta, FeatureType

# ---------- fixtures -----------------------------------------------------------


def _make_group(
    name: str = "customer_features_daily",
    *,
    schema: str = "analytics",
    database: str | None = None,
    timestamp_column: str | None = "feature_date",
    feature_names: tuple[str, ...] = ("orders_count_7d", "is_repeat_customer"),
) -> FeatureGroup:
    features = tuple(
        Feature(
            name=n,
            description="",
            column_type=None,
            feature_type=FeatureType.NUMERIC,
            null_behavior=None,
            used_by=(),
            tags=(),
        )
        for n in feature_names
    )
    meta_data = {
        "is_feature_table": True,
        "entity": "customer_id",
    }
    if timestamp_column:
        meta_data["timestamp_column"] = timestamp_column
    meta = FeatureTableMeta.model_validate(meta_data)
    return FeatureGroup(
        name=name,
        unique_id=f"model.demo.{name}",
        description="",
        schema_name=schema,
        database=database,
        materialization="table",
        package_name="demo",
        file_path="x.sql",
        meta=meta,
        features=features,
        upstream=(),
        downstream=(),
    )


class FakeCursor:
    """Minimal DBAPI cursor stand-in.

    Returns canned results for the two queries ``run_group_query`` issues,
    in the order it issues them. Records the SQL for assertion.
    """

    def __init__(self, results: list[Any], description_per_query: list[list[str]] | None = None):
        self._results = list(results)
        self._descriptions = list(description_per_query or [])
        self.description: list[tuple[str]] = []
        self.executed: list[str] = []
        self._next: Any = None

    def execute(self, sql: str) -> None:
        self.executed.append(sql)
        self._next = self._results.pop(0) if self._results else None
        if self._descriptions:
            cols = self._descriptions.pop(0)
            self.description = [(c,) for c in cols]
        else:
            self.description = []

    def fetchone(self) -> Any:
        return self._next

    def __enter__(self) -> FakeCursor:
        return self

    def __exit__(self, *exc: Any) -> None:
        pass

    def close(self) -> None:
        pass


# ---------- _dbapi shared helper -----------------------------------------------


class TestFullyQualifiedName:
    def test_three_part_with_database(self) -> None:
        group = _make_group(database="warehouse", schema="analytics")
        assert fully_qualified_name(group, None, None) == '"warehouse"."analytics"."customer_features_daily"'

    def test_falls_back_to_defaults(self) -> None:
        group = _make_group(database=None, schema="")
        assert fully_qualified_name(group, "public", "prod_db") == '"prod_db"."public"."customer_features_daily"'

    def test_two_part_when_no_database(self) -> None:
        group = _make_group(database=None, schema="analytics")
        assert fully_qualified_name(group, None, None) == '"analytics"."customer_features_daily"'

    def test_quotes_escape_inner(self) -> None:
        group = _make_group(name='weird"table', schema='weird"schema')
        result = fully_qualified_name(group, None, None)
        # Inner quotes should be doubled per SQL standard
        assert '"weird""schema"' in result
        assert '"weird""table"' in result


class TestRunGroupQuery:
    def test_with_timestamp_column(self) -> None:
        group = _make_group()
        max_ts = datetime(2024, 12, 31, 10, 0, tzinfo=timezone.utc)
        cursor = FakeCursor(
            results=[
                (max_ts, 1000),  # MAX(ts), COUNT(*)
                (5, 250, 0, 2),  # nulls, distinct, nulls, distinct
            ],
            description_per_query=[
                [],  # MAX/COUNT query — description not needed
                [
                    "orders_count_7d__nulls",
                    "orders_count_7d__distinct",
                    "is_repeat_customer__nulls",
                    "is_repeat_customer__distinct",
                ],
            ],
        )
        out_max_ts, row_count, columns = run_group_query(
            cursor, group, default_schema="analytics", default_database=None
        )
        assert out_max_ts == max_ts
        assert row_count == 1000
        assert columns["orders_count_7d"].null_count == 5
        assert columns["orders_count_7d"].distinct_count == 250
        assert columns["is_repeat_customer"].null_count == 0
        assert columns["is_repeat_customer"].distinct_count == 2

        # Verify the SQL we issued
        assert any("MAX(" in sql for sql in cursor.executed)
        assert any("COUNT(DISTINCT" in sql for sql in cursor.executed)

    def test_without_timestamp_column_skips_max(self) -> None:
        group = _make_group(timestamp_column=None)
        cursor = FakeCursor(
            results=[
                (500,),  # COUNT(*) only
                (10, 100, 1, 2),
            ],
            description_per_query=[
                [],
                [
                    "orders_count_7d__nulls",
                    "orders_count_7d__distinct",
                    "is_repeat_customer__nulls",
                    "is_repeat_customer__distinct",
                ],
            ],
        )
        max_ts, row_count, columns = run_group_query(
            cursor, group, default_schema="analytics", default_database=None
        )
        assert max_ts is None
        assert row_count == 500
        assert "MAX(" not in cursor.executed[0]
        assert columns["orders_count_7d"].null_count == 10

    def test_empty_table_skips_stats_query(self) -> None:
        group = _make_group()
        cursor = FakeCursor(results=[(None, 0)])  # no max, no rows
        max_ts, row_count, columns = run_group_query(
            cursor, group, default_schema="analytics", default_database=None
        )
        assert row_count == 0
        # Stats query should be skipped — only one execution.
        assert len(cursor.executed) == 1
        assert columns == {}

    def test_lowercase_column_name_fallback(self) -> None:
        """Some drivers (mysql variants etc.) lowercase column names. The
        helper has a defensive fallback for that case."""

        group = _make_group(feature_names=("MIXEDCASE",))
        cursor = FakeCursor(
            results=[
                (datetime(2024, 12, 31, tzinfo=timezone.utc), 100),
                (3, 50),
            ],
            description_per_query=[
                [],
                ["mixedcase__nulls", "mixedcase__distinct"],  # forced lowercase
            ],
        )
        _, _, columns = run_group_query(
            cursor, group, default_schema="analytics", default_database=None
        )
        assert columns["MIXEDCASE"].null_count == 3
        assert columns["MIXEDCASE"].distinct_count == 50


# ---------- Postgres adapter ---------------------------------------------------


class FakePsycopg2Connection:
    def __init__(self) -> None:
        self.closed = False
        self.rolled_back = False
        self._cursor = FakeCursor(
            results=[
                (datetime(2024, 12, 31, 10, 0, tzinfo=timezone.utc), 1000),
                (5, 250, 0, 2),
            ],
            description_per_query=[
                [],
                [
                    "orders_count_7d__nulls",
                    "orders_count_7d__distinct",
                    "is_repeat_customer__nulls",
                    "is_repeat_customer__distinct",
                ],
            ],
        )

    def cursor(self) -> FakeCursor:
        return self._cursor

    def close(self) -> None:
        self.closed = True

    def rollback(self) -> None:
        self.rolled_back = True

    def set_session(self, **kwargs: Any) -> None:
        pass


@pytest.fixture()
def fake_psycopg2(monkeypatch: pytest.MonkeyPatch) -> Any:
    """Install a fake psycopg2 module so PostgresAdapter constructs without
    needing a real Postgres."""

    fake = MagicMock()
    fake.Error = Exception
    fake_conn = FakePsycopg2Connection()
    fake.connect = MagicMock(return_value=fake_conn)
    monkeypatch.setitem(sys.modules, "psycopg2", fake)
    return fake


class TestPostgresAdapter:
    def test_constructs_with_password_auth(self, fake_psycopg2: Any) -> None:
        from dbt_features.enrichment.adapters.postgres import PostgresAdapter

        adapter = PostgresAdapter(
            {
                "type": "postgres",
                "host": "warehouse.example.com",
                "user": "analytics",
                "password": "secret",
                "dbname": "prod",
                "schema": "analytics",
            }
        )
        fake_psycopg2.connect.assert_called_once()
        kwargs = fake_psycopg2.connect.call_args.kwargs
        assert kwargs["host"] == "warehouse.example.com"
        assert kwargs["dbname"] == "prod"
        assert kwargs["password"] == "secret"
        assert adapter._default_schema == "analytics"

    def test_passes_through_ssl_options(self, fake_psycopg2: Any) -> None:
        from dbt_features.enrichment.adapters.postgres import PostgresAdapter

        PostgresAdapter(
            {
                "type": "postgres",
                "host": "h",
                "dbname": "d",
                "sslmode": "require",
                "sslrootcert": "/etc/ssl/ca.crt",
            }
        )
        kwargs = fake_psycopg2.connect.call_args.kwargs
        assert kwargs["sslmode"] == "require"
        assert kwargs["sslrootcert"] == "/etc/ssl/ca.crt"

    def test_missing_host_raises(self, fake_psycopg2: Any) -> None:
        from dbt_features.enrichment.adapters.postgres import PostgresAdapter

        with pytest.raises(EnrichmentError, match="missing 'host'"):
            PostgresAdapter({"type": "postgres", "dbname": "d"})

    def test_missing_dbname_raises(self, fake_psycopg2: Any) -> None:
        from dbt_features.enrichment.adapters.postgres import PostgresAdapter

        with pytest.raises(EnrichmentError, match="missing 'dbname'"):
            PostgresAdapter({"type": "postgres", "host": "h"})

    def test_database_alias_accepted(self, fake_psycopg2: Any) -> None:
        from dbt_features.enrichment.adapters.postgres import PostgresAdapter

        PostgresAdapter({"type": "postgres", "host": "h", "database": "x"})
        kwargs = fake_psycopg2.connect.call_args.kwargs
        assert kwargs["dbname"] == "x"

    def test_fetch_group_stats_returns_snapshot(self, fake_psycopg2: Any) -> None:
        from dbt_features.enrichment.adapters.postgres import PostgresAdapter

        adapter = PostgresAdapter({"type": "postgres", "host": "h", "dbname": "d"})
        snap = adapter.fetch_group_stats(_make_group())
        assert snap.error is None
        assert snap.row_count == 1000
        assert snap.columns["orders_count_7d"].null_count == 5

    def test_per_group_failure_captured_not_raised(
        self, fake_psycopg2: Any, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """A query failure must land on the snapshot, not bubble out — and
        the connection must be rolled back so subsequent groups can query."""

        from dbt_features.enrichment.adapters.postgres import PostgresAdapter

        adapter = PostgresAdapter({"type": "postgres", "host": "h", "dbname": "d"})

        def boom(*_: Any, **__: Any) -> None:
            raise RuntimeError("permission denied")

        adapter._conn._cursor.execute = boom  # type: ignore[assignment]
        snap = adapter.fetch_group_stats(_make_group())
        assert snap.error is not None
        assert "permission denied" in snap.error
        assert adapter._conn.rolled_back  # type: ignore[attr-defined]


# ---------- Redshift adapter ---------------------------------------------------


class FakeRedshiftConnection:
    def __init__(self) -> None:
        self.closed = False
        self.rolled_back = False
        self.autocommit = False
        self._cursor = FakeCursor(
            results=[
                (datetime(2024, 12, 31, 10, 0, tzinfo=timezone.utc), 1000),
                (5, 250),
            ],
            description_per_query=[
                [],
                ["orders_count_7d__nulls", "orders_count_7d__distinct"],
            ],
        )

    def cursor(self) -> FakeCursor:
        return self._cursor

    def close(self) -> None:
        self.closed = True

    def rollback(self) -> None:
        self.rolled_back = True


@pytest.fixture()
def fake_redshift_connector(monkeypatch: pytest.MonkeyPatch) -> Any:
    fake = MagicMock()
    fake_conn = FakeRedshiftConnection()
    fake.connect = MagicMock(return_value=fake_conn)
    monkeypatch.setitem(sys.modules, "redshift_connector", fake)
    return fake


class TestRedshiftAdapter:
    def test_constructs_with_password_auth(self, fake_redshift_connector: Any) -> None:
        from dbt_features.enrichment.adapters.redshift import RedshiftAdapter

        RedshiftAdapter(
            {
                "type": "redshift",
                "host": "cluster.abc.us-east-1.redshift.amazonaws.com",
                "port": 5439,
                "user": "admin",
                "password": "secret",
                "dbname": "prod",
                "schema": "analytics",
            }
        )
        kwargs = fake_redshift_connector.connect.call_args.kwargs
        assert kwargs["host"] == "cluster.abc.us-east-1.redshift.amazonaws.com"
        assert kwargs["port"] == 5439
        assert kwargs["password"] == "secret"
        assert kwargs["database"] == "prod"
        # `iam` must not be set in password mode
        assert "iam" not in kwargs

    def test_constructs_with_iam_auth(self, fake_redshift_connector: Any) -> None:
        from dbt_features.enrichment.adapters.redshift import RedshiftAdapter

        RedshiftAdapter(
            {
                "type": "redshift",
                "method": "iam",
                "host": "cluster.abc.us-east-1.redshift.amazonaws.com",
                "user": "iam-user",
                "dbname": "prod",
                "cluster_id": "my-cluster",
                "region": "us-east-1",
                "iam_profile": "default",
            }
        )
        kwargs = fake_redshift_connector.connect.call_args.kwargs
        assert kwargs["iam"] is True
        assert kwargs["cluster_identifier"] == "my-cluster"
        assert kwargs["region"] == "us-east-1"
        assert kwargs["profile"] == "default"
        # No password should be sent in IAM mode
        assert "password" not in kwargs

    def test_iam_shorthand_boolean(self, fake_redshift_connector: Any) -> None:
        """``iam: true`` should also work (community-config shorthand)."""

        from dbt_features.enrichment.adapters.redshift import RedshiftAdapter

        RedshiftAdapter(
            {
                "type": "redshift",
                "iam": True,
                "host": "h",
                "user": "u",
                "dbname": "d",
                "cluster_id": "c",
            }
        )
        kwargs = fake_redshift_connector.connect.call_args.kwargs
        assert kwargs["iam"] is True

    def test_password_required_when_not_iam(self, fake_redshift_connector: Any) -> None:
        from dbt_features.enrichment.adapters.redshift import RedshiftAdapter

        with pytest.raises(EnrichmentError, match="missing 'password'"):
            RedshiftAdapter(
                {
                    "type": "redshift",
                    "host": "h",
                    "user": "u",
                    "dbname": "d",
                    # no password, no iam
                }
            )

    def test_missing_dbname_raises(self, fake_redshift_connector: Any) -> None:
        from dbt_features.enrichment.adapters.redshift import RedshiftAdapter

        with pytest.raises(EnrichmentError, match="missing 'dbname'"):
            RedshiftAdapter({"type": "redshift", "host": "h"})

    def test_fetch_group_stats_returns_snapshot(self, fake_redshift_connector: Any) -> None:
        from dbt_features.enrichment.adapters.redshift import RedshiftAdapter

        adapter = RedshiftAdapter(
            {"type": "redshift", "host": "h", "user": "u", "password": "p", "dbname": "d"}
        )
        snap = adapter.fetch_group_stats(
            _make_group(feature_names=("orders_count_7d",))
        )
        assert snap.error is None
        assert snap.row_count == 1000
        assert snap.columns["orders_count_7d"].null_count == 5


# ---------- adapter dispatch ---------------------------------------------------


class TestDispatch:
    def test_postgres_dispatched(self, fake_psycopg2: Any) -> None:
        from dbt_features.enrichment.adapters import get_adapter
        from dbt_features.enrichment.adapters.postgres import PostgresAdapter

        adapter = get_adapter(
            {"type": "postgres", "host": "h", "dbname": "d", "user": "u", "password": "p"}
        )
        assert isinstance(adapter, PostgresAdapter)

    def test_redshift_dispatched(self, fake_redshift_connector: Any) -> None:
        from dbt_features.enrichment.adapters import get_adapter
        from dbt_features.enrichment.adapters.redshift import RedshiftAdapter

        adapter = get_adapter(
            {"type": "redshift", "host": "h", "user": "u", "password": "p", "dbname": "d"}
        )
        assert isinstance(adapter, RedshiftAdapter)

    def test_unknown_warehouse_message_lists_supported(self) -> None:
        from dbt_features.enrichment.adapters import get_adapter

        with pytest.raises(EnrichmentError, match="duckdb, postgres, redshift"):
            get_adapter({"type": "oracle"})


# ---------- end-to-end via engine ---------------------------------------------


def test_engine_dispatches_to_postgres_adapter(
    fake_psycopg2: Any, tmp_path: Any
) -> None:
    """Smoke-test the full path: profile -> get_adapter -> Postgres."""

    import yaml

    from dbt_features.enrichment.engine import enrich_catalog

    profiles_dir = tmp_path / ".dbt"
    profiles_dir.mkdir()
    (profiles_dir / "profiles.yml").write_text(
        yaml.safe_dump(
            {
                "demo": {
                    "target": "dev",
                    "outputs": {
                        "dev": {
                            "type": "postgres",
                            "host": "h",
                            "user": "u",
                            "password": "p",
                            "dbname": "d",
                            "schema": "analytics",
                        }
                    },
                }
            }
        )
    )

    cat = Catalog(project_name="demo", feature_groups=(_make_group(),))
    results = enrich_catalog(cat, profile_name="demo", profiles_dir=profiles_dir)
    assert len(results) == 1
    snap = next(iter(results.values()))
    assert snap.error is None
    assert snap.row_count == 1000
