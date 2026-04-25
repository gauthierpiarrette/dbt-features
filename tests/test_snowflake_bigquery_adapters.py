"""Tests for the Snowflake and BigQuery adapters.

Both warehouse drivers are mocked at the ``sys.modules`` level — running
real Snowflake/BigQuery in CI would require credentials we shouldn't
ship. The shared ``_dbapi.run_group_query`` helper is already covered by
the Postgres/Redshift tests, so here we focus on per-warehouse setup
quirks (auth modes, identifier quoting, Job-vs-cursor API).
"""

from __future__ import annotations

import sys
from datetime import datetime, timezone
from typing import Any
from unittest.mock import MagicMock

import pytest

from dbt_features.catalog import Feature, FeatureGroup
from dbt_features.enrichment.exceptions import EnrichmentError
from dbt_features.schema import FeatureTableMeta, FeatureType


def _make_group(
    name: str = "customer_features_daily",
    *,
    schema: str = "analytics",
    database: str | None = None,
    timestamp_column: str | None = "feature_date",
    feature_names: tuple[str, ...] = ("orders_count_7d",),
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
    meta_data: dict[str, Any] = {"is_feature_table": True, "entity": "customer_id"}
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


# =============================================================================
# Snowflake
# =============================================================================


class FakeSnowflakeCursor:
    """Mimics ``snowflake.connector`` cursor — DBAPI-shaped."""

    def __init__(self, results: list[Any], descriptions: list[list[str]]):
        self._results = list(results)
        self._descriptions = list(descriptions)
        self.description: list[tuple[str]] = []
        self.executed: list[str] = []

    def execute(self, sql: str) -> None:
        self.executed.append(sql)
        self._next = self._results.pop(0)
        if self._descriptions:
            cols = self._descriptions.pop(0)
            self.description = [(c,) for c in cols]
        else:
            self.description = []

    def fetchone(self) -> Any:
        return self._next

    def close(self) -> None:
        pass


class FakeSnowflakeConnection:
    def __init__(self) -> None:
        self.closed = False
        self._cursor = FakeSnowflakeCursor(
            results=[
                (datetime(2024, 12, 31, 10, 0, tzinfo=timezone.utc), 1000),
                (5, 250),
            ],
            descriptions=[[], ["orders_count_7d__nulls", "orders_count_7d__distinct"]],
        )

    def cursor(self) -> FakeSnowflakeCursor:
        return self._cursor

    def close(self) -> None:
        self.closed = True

    def autocommit(self, _flag: bool) -> None:
        pass


@pytest.fixture()
def fake_snowflake(monkeypatch: pytest.MonkeyPatch) -> Any:
    """Install fake ``snowflake`` and ``snowflake.connector`` modules."""

    fake_pkg = MagicMock()
    fake_connector = MagicMock()
    fake_connector.connect = MagicMock(return_value=FakeSnowflakeConnection())
    fake_pkg.connector = fake_connector

    monkeypatch.setitem(sys.modules, "snowflake", fake_pkg)
    monkeypatch.setitem(sys.modules, "snowflake.connector", fake_connector)
    return fake_connector


class TestSnowflakeAdapter:
    def test_password_auth(self, fake_snowflake: Any) -> None:
        from dbt_features.enrichment.adapters.snowflake import SnowflakeAdapter

        SnowflakeAdapter(
            {
                "type": "snowflake",
                "account": "myorg-myaccount",
                "user": "ANALYTICS",
                "password": "secret",
                "database": "PROD",
                "warehouse": "COMPUTE_WH",
                "role": "ANALYTICS_RO",
                "schema": "ANALYTICS",
            }
        )
        kwargs = fake_snowflake.connect.call_args.kwargs
        assert kwargs["account"] == "myorg-myaccount"
        assert kwargs["user"] == "ANALYTICS"
        assert kwargs["password"] == "secret"
        assert kwargs["database"] == "PROD"
        assert kwargs["warehouse"] == "COMPUTE_WH"
        assert kwargs["role"] == "ANALYTICS_RO"
        # Query tag is set so DBAs can spot us in QUERY_HISTORY
        assert kwargs["session_parameters"]["QUERY_TAG"] == "dbt-features-enrichment"
        # No key-pair fields in password mode
        assert "private_key" not in kwargs
        assert "authenticator" not in kwargs

    def test_authenticator_passes_through(self, fake_snowflake: Any) -> None:
        from dbt_features.enrichment.adapters.snowflake import SnowflakeAdapter

        SnowflakeAdapter(
            {
                "type": "snowflake",
                "account": "a",
                "user": "u",
                "database": "d",
                "authenticator": "externalbrowser",
            }
        )
        kwargs = fake_snowflake.connect.call_args.kwargs
        assert kwargs["authenticator"] == "externalbrowser"
        assert "password" not in kwargs

    def test_oauth_token_passed(self, fake_snowflake: Any) -> None:
        from dbt_features.enrichment.adapters.snowflake import SnowflakeAdapter

        SnowflakeAdapter(
            {
                "type": "snowflake",
                "account": "a",
                "user": "u",
                "database": "d",
                "authenticator": "oauth",
                "token": "ya29...",
            }
        )
        kwargs = fake_snowflake.connect.call_args.kwargs
        assert kwargs["token"] == "ya29..."

    def test_no_auth_raises(self, fake_snowflake: Any) -> None:
        from dbt_features.enrichment.adapters.snowflake import SnowflakeAdapter

        with pytest.raises(EnrichmentError, match="no auth configured"):
            SnowflakeAdapter(
                {"type": "snowflake", "account": "a", "user": "u", "database": "d"}
            )

    def test_missing_account_raises(self, fake_snowflake: Any) -> None:
        from dbt_features.enrichment.adapters.snowflake import SnowflakeAdapter

        with pytest.raises(EnrichmentError, match="missing 'account'"):
            SnowflakeAdapter(
                {"type": "snowflake", "user": "u", "password": "p", "database": "d"}
            )

    def test_fetch_returns_snapshot(self, fake_snowflake: Any) -> None:
        from dbt_features.enrichment.adapters.snowflake import SnowflakeAdapter

        adapter = SnowflakeAdapter(
            {
                "type": "snowflake",
                "account": "a",
                "user": "u",
                "password": "p",
                "database": "d",
            }
        )
        snap = adapter.fetch_group_stats(_make_group())
        assert snap.error is None
        assert snap.row_count == 1000
        assert snap.columns["orders_count_7d"].null_count == 5

    def test_naive_timestamp_normalized_to_utc(self, fake_snowflake: Any) -> None:
        """Snowflake returns naive datetimes for TIMESTAMP_NTZ. They must
        be normalized to aware UTC so freshness arithmetic works."""

        from dbt_features.enrichment.adapters.snowflake import _coerce_dt

        naive = datetime(2024, 12, 31, 10, 0)  # no tzinfo
        coerced = _coerce_dt(naive)
        assert coerced is not None
        assert coerced.tzinfo is timezone.utc


# =============================================================================
# BigQuery
# =============================================================================


class FakeBigQueryRow:
    """Mimics ``google.cloud.bigquery.Row``: dict-like and has ``items``."""

    def __init__(self, data: dict[str, Any]):
        self._data = dict(data)

    def items(self) -> Any:
        return self._data.items()

    def __getitem__(self, key: str) -> Any:
        return self._data[key]


class FakeBigQueryJob:
    def __init__(self, rows: list[dict[str, Any]]):
        self._rows = [FakeBigQueryRow(r) for r in rows]

    def result(self, timeout: float | None = None) -> Any:
        return iter(self._rows)


class FakeBigQueryClient:
    def __init__(self, project: str | None = None, **_: Any) -> None:
        self.project = project
        self.queries: list[str] = []
        self._row_queue: list[list[dict[str, Any]]] = []

    def queue_rows(self, rows: list[dict[str, Any]]) -> None:
        self._row_queue.append(rows)

    def query(self, sql: str) -> FakeBigQueryJob:
        self.queries.append(sql)
        rows = self._row_queue.pop(0) if self._row_queue else []
        return FakeBigQueryJob(rows)

    def close(self) -> None:
        pass


@pytest.fixture()
def fake_bigquery(monkeypatch: pytest.MonkeyPatch) -> Any:
    """Install fake ``google.cloud.bigquery`` module + ``google.oauth2``."""

    fake_client_instance = FakeBigQueryClient(project="demo-project")
    fake_client_instance.queue_rows([{"mx": datetime(2024, 12, 31, 10, 0, tzinfo=timezone.utc), "cnt": 1000}])
    fake_client_instance.queue_rows([{"orders_count_7d__nulls": 5, "orders_count_7d__distinct": 250}])

    fake_bq = MagicMock()
    fake_bq.Client = MagicMock(return_value=fake_client_instance)
    fake_bq.Client.from_service_account_json = MagicMock(return_value=fake_client_instance)

    fake_google = MagicMock()
    fake_cloud = MagicMock()
    fake_cloud.bigquery = fake_bq
    fake_google.cloud = fake_cloud

    fake_oauth2 = MagicMock()
    fake_service_account = MagicMock()
    fake_service_account.Credentials.from_service_account_info = MagicMock(return_value="fake-creds")
    fake_oauth2.service_account = fake_service_account

    monkeypatch.setitem(sys.modules, "google", fake_google)
    monkeypatch.setitem(sys.modules, "google.cloud", fake_cloud)
    monkeypatch.setitem(sys.modules, "google.cloud.bigquery", fake_bq)
    monkeypatch.setitem(sys.modules, "google.oauth2", fake_oauth2)
    monkeypatch.setitem(sys.modules, "google.oauth2.service_account", fake_service_account)
    return fake_bq, fake_client_instance


class TestBigQueryAdapter:
    def test_oauth_method_default(self, fake_bigquery: Any) -> None:
        from dbt_features.enrichment.adapters.bigquery import BigQueryAdapter

        bq, client = fake_bigquery
        BigQueryAdapter(
            {"type": "bigquery", "project": "demo-project", "dataset": "analytics"}
        )
        bq.Client.assert_called_once_with(project="demo-project")

    def test_service_account_method(self, fake_bigquery: Any) -> None:
        from dbt_features.enrichment.adapters.bigquery import BigQueryAdapter

        bq, _ = fake_bigquery
        BigQueryAdapter(
            {
                "type": "bigquery",
                "method": "service-account",
                "project": "demo-project",
                "dataset": "analytics",
                "keyfile": "/path/to/key.json",
            }
        )
        bq.Client.from_service_account_json.assert_called_once_with(
            "/path/to/key.json", project="demo-project"
        )

    def test_service_account_method_missing_keyfile(self, fake_bigquery: Any) -> None:
        from dbt_features.enrichment.adapters.bigquery import BigQueryAdapter

        with pytest.raises(EnrichmentError, match="requires 'keyfile'"):
            BigQueryAdapter(
                {
                    "type": "bigquery",
                    "method": "service-account",
                    "project": "p",
                    "dataset": "a",
                }
            )

    def test_service_account_json_method(self, fake_bigquery: Any) -> None:
        from dbt_features.enrichment.adapters.bigquery import BigQueryAdapter

        BigQueryAdapter(
            {
                "type": "bigquery",
                "method": "service-account-json",
                "project": "demo-project",
                "dataset": "analytics",
                "keyfile_json": {"type": "service_account", "project_id": "demo-project"},
            }
        )

    def test_unknown_method_raises(self, fake_bigquery: Any) -> None:
        from dbt_features.enrichment.adapters.bigquery import BigQueryAdapter

        with pytest.raises(EnrichmentError, match="Unsupported BigQuery auth method"):
            BigQueryAdapter(
                {
                    "type": "bigquery",
                    "method": "external-oauth",
                    "project": "p",
                    "dataset": "a",
                }
            )

    def test_missing_project_raises(self, fake_bigquery: Any) -> None:
        from dbt_features.enrichment.adapters.bigquery import BigQueryAdapter

        with pytest.raises(EnrichmentError, match="missing 'project'"):
            BigQueryAdapter({"type": "bigquery", "dataset": "a"})

    def test_missing_dataset_raises(self, fake_bigquery: Any) -> None:
        from dbt_features.enrichment.adapters.bigquery import BigQueryAdapter

        with pytest.raises(EnrichmentError, match="missing 'dataset'"):
            BigQueryAdapter({"type": "bigquery", "project": "p"})

    def test_dataset_alias_schema_works(self, fake_bigquery: Any) -> None:
        """``schema`` should be honored as an alias for ``dataset``."""

        from dbt_features.enrichment.adapters.bigquery import BigQueryAdapter

        adapter = BigQueryAdapter(
            {"type": "bigquery", "project": "p", "schema": "analytics"}
        )
        assert adapter._default_schema == "analytics"

    def test_fetch_returns_snapshot_with_backtick_sql(self, fake_bigquery: Any) -> None:
        from dbt_features.enrichment.adapters.bigquery import BigQueryAdapter

        _, client = fake_bigquery
        adapter = BigQueryAdapter(
            {"type": "bigquery", "project": "demo-project", "dataset": "analytics"}
        )
        snap = adapter.fetch_group_stats(_make_group())
        assert snap.error is None
        assert snap.row_count == 1000
        assert snap.columns["orders_count_7d"].null_count == 5
        # Verify the SQL uses backticks (not double quotes) and a fully
        # qualified three-part name.
        assert any("`demo-project`.`analytics`." in q for q in client.queries)
        assert all('"' not in q for q in client.queries)

    def test_fqn_uses_default_project_when_missing(self, fake_bigquery: Any) -> None:
        from dbt_features.enrichment.adapters.bigquery import BigQueryAdapter

        _, client = fake_bigquery
        adapter = BigQueryAdapter(
            {"type": "bigquery", "project": "demo-project", "dataset": "analytics"}
        )
        # Group has no database/project recorded — should fall back to
        # the profile's project.
        adapter.fetch_group_stats(_make_group(database=None))
        assert any("`demo-project`" in q for q in client.queries)


# =============================================================================
# Dispatch
# =============================================================================


class TestDispatch:
    def test_snowflake_dispatched(self, fake_snowflake: Any) -> None:
        from dbt_features.enrichment.adapters import get_adapter
        from dbt_features.enrichment.adapters.snowflake import SnowflakeAdapter

        adapter = get_adapter(
            {
                "type": "snowflake",
                "account": "a",
                "user": "u",
                "password": "p",
                "database": "d",
            }
        )
        assert isinstance(adapter, SnowflakeAdapter)

    def test_bigquery_dispatched(self, fake_bigquery: Any) -> None:
        from dbt_features.enrichment.adapters import get_adapter
        from dbt_features.enrichment.adapters.bigquery import BigQueryAdapter

        adapter = get_adapter(
            {"type": "bigquery", "project": "p", "dataset": "a"}
        )
        assert isinstance(adapter, BigQueryAdapter)

    def test_supported_list_in_error(self) -> None:
        from dbt_features.enrichment.adapters import get_adapter

        with pytest.raises(EnrichmentError, match="duckdb, postgres, redshift, snowflake, bigquery"):
            get_adapter({"type": "oracle"})
