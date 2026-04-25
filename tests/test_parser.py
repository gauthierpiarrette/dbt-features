from __future__ import annotations

import json
from pathlib import Path

import pytest

from dbt_features.parser import parse_project
from dbt_features.schema import FeatureType, NullBehavior, SchemaError


def test_parses_feature_groups(project_dir: Path) -> None:
    cat = parse_project(project_dir)
    names = [g.name for g in cat.feature_groups]
    assert names == ["customer_features_daily", "customer_features_lifetime"]
    assert cat.project_name == "jaffle_features"
    assert cat.feature_count == 5  # 3 features in daily, 2 in lifetime


def test_feature_metadata_parsed(project_dir: Path) -> None:
    cat = parse_project(project_dir)
    daily = next(g for g in cat.feature_groups if g.name == "customer_features_daily")
    assert daily.entity_columns == ["customer_id"]
    assert daily.grain == ["feature_date", "customer_id"]
    assert daily.timestamp_column == "feature_date"
    assert daily.owner == "growth-team@jaffle.com"
    assert "customer" in daily.tags
    assert daily.freshness is not None
    assert daily.freshness.warn_after.count == 36


def test_only_marked_columns_become_features(project_dir: Path) -> None:
    cat = parse_project(project_dir)
    daily = next(g for g in cat.feature_groups if g.name == "customer_features_daily")
    feature_names = {f.name for f in daily.features}
    # feature_date is a column but is NOT marked is_feature, so it should be excluded
    assert "feature_date" not in feature_names
    assert feature_names == {"customer_id", "orders_count_7d", "is_repeat_customer"}


def test_entity_columns_sort_first(project_dir: Path) -> None:
    cat = parse_project(project_dir)
    daily = next(g for g in cat.feature_groups if g.name == "customer_features_daily")
    assert daily.features[0].name == "customer_id"


def test_feature_type_and_null_behavior(project_dir: Path) -> None:
    cat = parse_project(project_dir)
    daily = next(g for g in cat.feature_groups if g.name == "customer_features_daily")
    orders = next(f for f in daily.features if f.name == "orders_count_7d")
    assert orders.feature_type == FeatureType.NUMERIC
    assert orders.null_behavior == NullBehavior.ZERO
    assert "churn_model_v2" in orders.used_by


def test_warehouse_type_from_catalog_json(project_dir: Path) -> None:
    cat = parse_project(project_dir)
    daily = next(g for g in cat.feature_groups if g.name == "customer_features_daily")
    orders = next(f for f in daily.features if f.name == "orders_count_7d")
    # data_type from manifest takes precedence
    assert orders.column_type == "integer"


def test_warehouse_type_falls_back_to_catalog_json(project_dir: Path) -> None:
    """If manifest has no data_type, we should look in catalog.json (case-insensitively)."""

    manifest = json.loads((project_dir / "target" / "manifest.json").read_text())
    daily_node = manifest["nodes"]["model.jaffle_features.customer_features_daily"]
    daily_node["columns"]["orders_count_7d"]["data_type"] = None
    (project_dir / "target" / "manifest.json").write_text(json.dumps(manifest))

    cat = parse_project(project_dir)
    daily = next(g for g in cat.feature_groups if g.name == "customer_features_daily")
    orders = next(f for f in daily.features if f.name == "orders_count_7d")
    assert orders.column_type == "INTEGER"  # from catalog.json


def test_lineage_upstream_downstream(project_dir: Path) -> None:
    cat = parse_project(project_dir)
    daily = next(g for g in cat.feature_groups if g.name == "customer_features_daily")
    upstream_names = {r.name for r in daily.upstream}
    downstream_names = {r.name for r in daily.downstream}
    assert "stg_orders" in upstream_names
    # customer_features_lifetime depends on this — it's a feature table
    assert "customer_features_lifetime" in downstream_names
    # consumer_dashboard_metrics also consumes it (non-feature)
    assert "consumer_dashboard_metrics" in downstream_names


def test_feature_table_lineage_flag(project_dir: Path) -> None:
    cat = parse_project(project_dir)
    daily = next(g for g in cat.feature_groups if g.name == "customer_features_daily")
    refs = {r.name: r for r in daily.downstream}
    assert refs["customer_features_lifetime"].is_feature_table is True
    assert refs["consumer_dashboard_metrics"].is_feature_table is False


def test_missing_manifest_raises_helpful(tmp_path: Path) -> None:
    with pytest.raises(FileNotFoundError, match="manifest.json"):
        parse_project(tmp_path)


def test_missing_catalog_json_is_ok(project_dir: Path) -> None:
    (project_dir / "target" / "catalog.json").unlink()
    cat = parse_project(project_dir)
    assert len(cat.feature_groups) == 2


def test_invalid_metadata_raises_schema_error(tmp_path: Path) -> None:
    bad_manifest = {
        "metadata": {"project_name": "broken"},
        "nodes": {
            "model.broken.x": {
                "name": "x",
                "unique_id": "model.broken.x",
                "resource_type": "model",
                "package_name": "broken",
                "path": "x.sql",
                "original_file_path": "models/x.sql",
                "schema": "s",
                "database": "d",
                "config": {"materialized": "view"},
                "meta": {
                    "feature_catalog": {
                        "is_feature_table": True,
                        "freshness": {}
                    }
                },
                "depends_on": {"nodes": []},
                "columns": {}
            }
        },
        "sources": {}
    }
    target = tmp_path / "target"
    target.mkdir()
    (target / "manifest.json").write_text(json.dumps(bad_manifest))

    with pytest.raises(SchemaError) as exc_info:
        parse_project(tmp_path)
    assert exc_info.value.node_id == "model.broken.x"


def test_meta_under_config_meta_picked_up(tmp_path: Path) -> None:
    """dbt nests meta under config.meta in newer manifests."""

    manifest = {
        "metadata": {"project_name": "p"},
        "nodes": {
            "model.p.foo": {
                "name": "foo",
                "unique_id": "model.p.foo",
                "resource_type": "model",
                "package_name": "p",
                "path": "foo.sql",
                "original_file_path": "models/foo.sql",
                "description": "",
                "schema": "s",
                "database": "d",
                "config": {
                    "materialized": "table",
                    "meta": {"feature_catalog": {"is_feature_table": True, "entity": "id"}},
                },
                "meta": {},
                "depends_on": {"nodes": []},
                "columns": {}
            }
        },
        "sources": {}
    }
    target = tmp_path / "target"
    target.mkdir()
    (target / "manifest.json").write_text(json.dumps(manifest))

    cat = parse_project(tmp_path)
    assert len(cat.feature_groups) == 1
    assert cat.feature_groups[0].entity_columns == ["id"]


def test_all_tags_aggregated(project_dir: Path) -> None:
    cat = parse_project(project_dir)
    assert cat.all_tags == ["customer", "daily", "lifetime"]


def test_groups_by_tag(project_dir: Path) -> None:
    cat = parse_project(project_dir)
    by_tag = cat.feature_groups_by_tag()
    assert "customer" in by_tag
    # both groups are tagged "customer"
    assert {g.name for g in by_tag["customer"]} == {"customer_features_daily", "customer_features_lifetime"}
    assert {g.name for g in by_tag["daily"]} == {"customer_features_daily"}
