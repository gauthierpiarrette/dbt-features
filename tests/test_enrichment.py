"""Tests for the enrichment subsystem.

Covers profile loading, cache TTL semantics, the DuckDB adapter, and the
end-to-end engine path. Uses an on-disk DuckDB so we exercise real driver
code rather than mocks.
"""

from __future__ import annotations

import json
import os
import time
from datetime import datetime, timezone
from pathlib import Path

import duckdb
import pytest

from dbt_features.catalog import Catalog, Feature, FeatureGroup
from dbt_features.enrichment import (
    ColumnStats,
    EnrichmentCache,
    EnrichmentError,
    FreshnessSnapshot,
    enrich_catalog,
)
from dbt_features.enrichment.profiles import _render_env_vars, load_profile
from dbt_features.schema import FeatureTableMeta, FeatureType

# ---------- profile loading ----------------------------------------------------


def _write_profiles(profiles_dir: Path, content: str) -> None:
    profiles_dir.mkdir(parents=True, exist_ok=True)
    (profiles_dir / "profiles.yml").write_text(content)


class TestProfiles:
    def test_load_default_target(self, tmp_path: Path) -> None:
        _write_profiles(
            tmp_path,
            """
demo:
  target: dev
  outputs:
    dev:
      type: duckdb
      path: ./demo.duckdb
""",
        )
        profile = load_profile("demo", profiles_dir=tmp_path)
        assert profile["type"] == "duckdb"
        assert profile["path"] == "./demo.duckdb"

    def test_explicit_target_override(self, tmp_path: Path) -> None:
        _write_profiles(
            tmp_path,
            """
demo:
  target: dev
  outputs:
    dev:
      type: duckdb
      path: ./dev.duckdb
    prod:
      type: duckdb
      path: ./prod.duckdb
""",
        )
        profile = load_profile("demo", target="prod", profiles_dir=tmp_path)
        assert profile["path"] == "./prod.duckdb"

    def test_missing_profile_raises(self, tmp_path: Path) -> None:
        _write_profiles(
            tmp_path,
            """
other:
  target: dev
  outputs:
    dev: {type: duckdb, path: x}
""",
        )
        with pytest.raises(EnrichmentError, match="Profile 'demo' not found"):
            load_profile("demo", profiles_dir=tmp_path)

    def test_missing_target_raises(self, tmp_path: Path) -> None:
        _write_profiles(
            tmp_path,
            """
demo:
  target: dev
  outputs:
    dev: {type: duckdb, path: x}
""",
        )
        with pytest.raises(EnrichmentError, match="Target 'staging' not found"):
            load_profile("demo", target="staging", profiles_dir=tmp_path)

    def test_missing_profiles_yml_raises(self, tmp_path: Path) -> None:
        with pytest.raises(EnrichmentError, match="profiles.yml not found"):
            load_profile("demo", profiles_dir=tmp_path)

    def test_env_var_rendering(self) -> None:
        os.environ["TEST_DBTFEAT_HOST"] = "warehouse.example.com"
        try:
            rendered = _render_env_vars(
                {"host": "{{ env_var('TEST_DBTFEAT_HOST') }}", "port": 5432}
            )
        finally:
            del os.environ["TEST_DBTFEAT_HOST"]
        assert rendered == {"host": "warehouse.example.com", "port": 5432}

    def test_env_var_with_default(self) -> None:
        rendered = _render_env_vars(
            {"host": "{{ env_var('NEVER_SET_DBTFEAT', 'fallback.example.com') }}"}
        )
        assert rendered["host"] == "fallback.example.com"

    def test_env_var_missing_no_default_raises(self) -> None:
        with pytest.raises(EnrichmentError, match="NEVER_SET_DBTFEAT"):
            _render_env_vars("{{ env_var('NEVER_SET_DBTFEAT') }}")


# ---------- cache --------------------------------------------------------------


def _make_snapshot(uid: str, max_ts: datetime | None = None) -> FreshnessSnapshot:
    return FreshnessSnapshot(
        queried_at=datetime.now(timezone.utc),
        max_timestamp=max_ts,
        row_count=42,
        columns={"col_a": ColumnStats(null_count=1, distinct_count=10)},
    )


class TestCache:
    def test_round_trip(self, tmp_path: Path) -> None:
        cache = EnrichmentCache(tmp_path / "c.json", ttl_seconds=3600)
        ts = datetime(2024, 12, 31, 10, 0, tzinfo=timezone.utc)
        snapshots = {"model.x.foo": _make_snapshot("model.x.foo", ts)}
        cache.write(snapshots)
        out = cache.read()
        assert out is not None
        assert out["model.x.foo"].row_count == 42
        assert out["model.x.foo"].max_timestamp == ts
        assert out["model.x.foo"].columns["col_a"].null_count == 1

    def test_miss_returns_none(self, tmp_path: Path) -> None:
        cache = EnrichmentCache(tmp_path / "c.json", ttl_seconds=3600)
        assert cache.read() is None

    def test_corrupt_returns_none(self, tmp_path: Path) -> None:
        path = tmp_path / "c.json"
        path.write_text("not json {{")
        cache = EnrichmentCache(path, ttl_seconds=3600)
        assert cache.read() is None

    def test_expired_returns_none(self, tmp_path: Path) -> None:
        path = tmp_path / "c.json"
        cache = EnrichmentCache(path, ttl_seconds=1)
        cache.write({"x": _make_snapshot("x")})
        time.sleep(1.1)
        assert cache.read() is None

    def test_clear(self, tmp_path: Path) -> None:
        path = tmp_path / "c.json"
        cache = EnrichmentCache(path, ttl_seconds=3600)
        cache.write({"x": _make_snapshot("x")})
        cache.clear()
        assert not path.exists()
        # Idempotent: clearing a non-existent cache must not crash.
        cache.clear()

    def test_format_version_mismatch_returns_none(self, tmp_path: Path) -> None:
        path = tmp_path / "c.json"
        path.write_text(
            json.dumps(
                {
                    "format": "999",
                    "fetched_at": datetime.now(timezone.utc).isoformat(),
                    "groups": {},
                }
            )
        )
        cache = EnrichmentCache(path, ttl_seconds=3600)
        assert cache.read() is None

    def test_error_snapshot_round_trips(self, tmp_path: Path) -> None:
        cache = EnrichmentCache(tmp_path / "c.json", ttl_seconds=3600)
        snapshots = {
            "model.x.foo": FreshnessSnapshot(
                queried_at=datetime.now(timezone.utc),
                error="permission denied",
            )
        }
        cache.write(snapshots)
        out = cache.read()
        assert out is not None
        assert out["model.x.foo"].error == "permission denied"
        assert out["model.x.foo"].max_timestamp is None


# ---------- DuckDB adapter end-to-end ------------------------------------------


@pytest.fixture()
def duckdb_warehouse(tmp_path: Path) -> Path:
    """Build a real DuckDB file with one feature-table-shaped table."""

    db_path = tmp_path / "warehouse.duckdb"
    conn = duckdb.connect(str(db_path))
    conn.execute("CREATE SCHEMA IF NOT EXISTS analytics")
    conn.execute(
        """
        CREATE TABLE analytics.customer_features_daily (
            feature_date DATE,
            customer_id VARCHAR,
            orders_count_7d INTEGER
        )
        """
    )
    conn.executemany(
        "INSERT INTO analytics.customer_features_daily VALUES (?, ?, ?)",
        [
            ("2024-12-30", "c1", 3),
            ("2024-12-30", "c2", 0),
            ("2024-12-30", "c3", None),  # null
            ("2024-12-31", "c1", 5),
        ],
    )
    conn.close()
    return db_path


def _make_catalog(group_name: str = "customer_features_daily") -> Catalog:
    feature = Feature(
        name="orders_count_7d",
        description="",
        column_type="INTEGER",
        feature_type=FeatureType.NUMERIC,
        null_behavior=None,
        used_by=(),
        tags=(),
    )
    meta = FeatureTableMeta.model_validate(
        {
            "is_feature_table": True,
            "entity": "customer_id",
            "timestamp_column": "feature_date",
        }
    )
    group = FeatureGroup(
        name=group_name,
        unique_id=f"model.demo.{group_name}",
        description="",
        schema_name="analytics",
        database=None,
        materialization="table",
        package_name="demo",
        file_path="x.sql",
        meta=meta,
        features=(feature,),
        upstream=(),
        downstream=(),
    )
    return Catalog(project_name="demo", feature_groups=(group,))


def _write_profile_for(db_path: Path, profiles_dir: Path) -> None:
    _write_profiles(
        profiles_dir,
        f"""
demo:
  target: dev
  outputs:
    dev:
      type: duckdb
      path: {db_path}
      schema: analytics
""",
    )


class TestDuckDBAdapterEndToEnd:
    def test_fetches_freshness_and_stats(self, tmp_path: Path, duckdb_warehouse: Path) -> None:
        profiles_dir = tmp_path / "dbt"
        _write_profile_for(duckdb_warehouse, profiles_dir)

        cat = _make_catalog()
        results = enrich_catalog(cat, profile_name="demo", profiles_dir=profiles_dir)
        assert len(results) == 1

        snap = next(iter(results.values()))
        assert snap.error is None
        assert snap.row_count == 4
        # max(feature_date) was 2024-12-31
        assert snap.max_timestamp is not None
        assert snap.max_timestamp.date().isoformat() == "2024-12-31"
        # 1 null, 3 distinct (c1 appears twice with non-null values 3 and 5)
        col = snap.columns["orders_count_7d"]
        assert col.null_count == 1
        assert col.distinct_count == 3

    def test_missing_table_captured_as_per_group_error(
        self, tmp_path: Path, duckdb_warehouse: Path
    ) -> None:
        """A table that doesn't exist must NOT abort the whole build —
        the failure must be on the snapshot."""

        profiles_dir = tmp_path / "dbt"
        _write_profile_for(duckdb_warehouse, profiles_dir)

        cat = _make_catalog(group_name="this_table_does_not_exist")
        results = enrich_catalog(cat, profile_name="demo", profiles_dir=profiles_dir)
        snap = next(iter(results.values()))
        assert snap.error is not None
        assert snap.row_count is None

    def test_cache_hit_skips_warehouse(
        self, tmp_path: Path, duckdb_warehouse: Path
    ) -> None:
        """Second call within TTL must return cached results without
        touching the warehouse — verified by deleting the DB after the
        first call."""

        profiles_dir = tmp_path / "dbt"
        _write_profile_for(duckdb_warehouse, profiles_dir)

        cache = EnrichmentCache(tmp_path / "cache.json", ttl_seconds=3600)
        cat = _make_catalog()
        first = enrich_catalog(cat, profile_name="demo", profiles_dir=profiles_dir, cache=cache)

        # Nuke the warehouse so a re-query would error out.
        duckdb_warehouse.unlink()

        second = enrich_catalog(cat, profile_name="demo", profiles_dir=profiles_dir, cache=cache)
        assert first == second

    def test_cache_invalidation_on_new_group(
        self, tmp_path: Path, duckdb_warehouse: Path
    ) -> None:
        """When the user adds a new feature table, the cache must NOT
        return stale results that miss it."""

        profiles_dir = tmp_path / "dbt"
        _write_profile_for(duckdb_warehouse, profiles_dir)
        cache = EnrichmentCache(tmp_path / "cache.json", ttl_seconds=3600)

        cat_v1 = _make_catalog(group_name="customer_features_daily")
        enrich_catalog(cat_v1, profile_name="demo", profiles_dir=profiles_dir, cache=cache)

        # Add a *new* group not in cache. We expect it to be queried even
        # though the cache file exists. (Will fail because table missing,
        # but the point is enrich_catalog tried, so we get a per-group
        # error rather than a silent cache hit.)
        feature = Feature(
            name="x",
            description="",
            column_type=None,
            feature_type=None,
            null_behavior=None,
            used_by=(),
            tags=(),
        )
        meta = FeatureTableMeta.model_validate({"is_feature_table": True, "entity": "x"})
        new_group = FeatureGroup(
            name="brand_new_table",
            unique_id="model.demo.brand_new_table",
            description="",
            schema_name="analytics",
            database=None,
            materialization="view",
            package_name="demo",
            file_path="x.sql",
            meta=meta,
            features=(feature,),
            upstream=(),
            downstream=(),
        )
        cat_v2 = Catalog(
            project_name="demo",
            feature_groups=cat_v1.feature_groups + (new_group,),
        )
        results = enrich_catalog(
            cat_v2, profile_name="demo", profiles_dir=profiles_dir, cache=cache
        )
        assert "model.demo.brand_new_table" in results

    def test_unsupported_warehouse_type_raises(self, tmp_path: Path) -> None:
        profiles_dir = tmp_path / "dbt"
        _write_profiles(
            profiles_dir,
            """
demo:
  target: dev
  outputs:
    dev:
      type: redshift
      host: x
      user: y
""",
        )
        cat = _make_catalog()
        with pytest.raises(EnrichmentError, match="Unsupported warehouse type"):
            enrich_catalog(cat, profile_name="demo", profiles_dir=profiles_dir)

    def test_table_with_no_timestamp_column(
        self, tmp_path: Path, duckdb_warehouse: Path
    ) -> None:
        """Slowly-changing tables without a timestamp_column should still
        report row count + per-column stats."""

        profiles_dir = tmp_path / "dbt"
        _write_profile_for(duckdb_warehouse, profiles_dir)

        # Build a catalog without timestamp_column declared
        feature = Feature(
            name="orders_count_7d",
            description="",
            column_type="INTEGER",
            feature_type=FeatureType.NUMERIC,
            null_behavior=None,
            used_by=(),
            tags=(),
        )
        meta = FeatureTableMeta.model_validate(
            {"is_feature_table": True, "entity": "customer_id"}
        )
        group = FeatureGroup(
            name="customer_features_daily",
            unique_id="model.demo.customer_features_daily",
            description="",
            schema_name="analytics",
            database=None,
            materialization="table",
            package_name="demo",
            file_path="x.sql",
            meta=meta,
            features=(feature,),
            upstream=(),
            downstream=(),
        )
        cat = Catalog(project_name="demo", feature_groups=(group,))
        results = enrich_catalog(cat, profile_name="demo", profiles_dir=profiles_dir)
        snap = next(iter(results.values()))
        assert snap.error is None
        assert snap.max_timestamp is None
        assert snap.row_count == 4


# ---------- CLI integration ----------------------------------------------------


def test_build_with_connection_writes_cache(
    tmp_path: Path, duckdb_warehouse: Path, project_dir: Path
) -> None:
    """End-to-end CLI: --connection produces an enrichment cache."""

    from click.testing import CliRunner

    from dbt_features.cli import main

    profiles_dir = tmp_path / "dbt"
    # The fixture's manifest references a real schema/table that doesn't
    # exist in our test warehouse — that's fine, we expect per-group errors.
    # The point is the cache file gets written.
    _write_profile_for(duckdb_warehouse, profiles_dir)

    runner = CliRunner()
    out = tmp_path / "site"
    result = runner.invoke(
        main,
        [
            "build",
            "--project-dir",
            str(project_dir),
            "--output",
            str(out),
            "--connection",
            "demo",
            "--profiles-dir",
            str(profiles_dir),
        ],
    )
    assert result.exit_code == 0, result.output
    assert "Enrichment:" in result.output
    cache_path = out / ".cache" / "enrichment.json"
    assert cache_path.exists()
    payload = json.loads(cache_path.read_text())
    assert payload["format"] == "1"
    assert "groups" in payload


def test_build_without_connection_skips_enrichment(
    project_dir: Path, tmp_path: Path
) -> None:
    """The catalog still builds normally when no --connection is passed."""

    from click.testing import CliRunner

    from dbt_features.cli import main

    runner = CliRunner()
    out = tmp_path / "site"
    result = runner.invoke(
        main,
        ["build", "--project-dir", str(project_dir), "--output", str(out)],
    )
    assert result.exit_code == 0, result.output
    assert "Enrichment:" not in result.output
    assert not (out / ".cache").exists()


def test_build_with_bad_profile_fails_helpfully(
    tmp_path: Path, project_dir: Path
) -> None:
    from click.testing import CliRunner

    from dbt_features.cli import main

    profiles_dir = tmp_path / "dbt"
    _write_profiles(profiles_dir, "other:\n  target: dev\n  outputs:\n    dev: {type: duckdb, path: x}\n")

    runner = CliRunner()
    result = runner.invoke(
        main,
        [
            "build",
            "--project-dir",
            str(project_dir),
            "--output",
            str(tmp_path / "site"),
            "--connection",
            "missing-profile",
            "--profiles-dir",
            str(profiles_dir),
        ],
    )
    assert result.exit_code != 0
    assert "Profile 'missing-profile' not found" in result.output


def test_build_no_cache_flag(
    tmp_path: Path, duckdb_warehouse: Path, project_dir: Path
) -> None:
    """--no-cache must skip cache reads/writes (no .cache directory)."""

    from click.testing import CliRunner

    from dbt_features.cli import main

    profiles_dir = tmp_path / "dbt"
    _write_profile_for(duckdb_warehouse, profiles_dir)

    runner = CliRunner()
    out = tmp_path / "site"
    result = runner.invoke(
        main,
        [
            "build",
            "--project-dir",
            str(project_dir),
            "--output",
            str(out),
            "--connection",
            "demo",
            "--profiles-dir",
            str(profiles_dir),
            "--no-cache",
        ],
    )
    assert result.exit_code == 0, result.output
    assert not (out / ".cache" / "enrichment.json").exists()


def test_age_is_aware_of_naive_timestamps(tmp_path: Path) -> None:
    """The cache must normalize naive datetimes to UTC so TTL math works
    even when a future warehouse driver returns naive datetimes."""

    from dbt_features.enrichment.cache import _parse_dt

    aware = _parse_dt("2024-12-31T10:00:00+00:00")
    z_form = _parse_dt("2024-12-31T10:00:00Z")
    naive = _parse_dt("2024-12-31T10:00:00")
    assert aware == z_form == naive.replace(tzinfo=timezone.utc)
    assert aware.tzinfo is not None
