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
    # daily: orders_count_7d, is_repeat_customer, total_value_usd, preferred_category (4)
    # lifetime: lifetime_order_count (1)
    # Excluded: entity/grain/timestamp cols, _loaded_at via exclude_columns,
    # debug_score via is_feature: false.
    assert cat.feature_count == 5


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


def test_entity_grain_and_timestamp_columns_excluded_from_features(project_dir: Path) -> None:
    """Columns named in entity / grain / timestamp_column are auto-excluded.

    They describe *the row*, not properties *of the entity* — so they
    aren't features. This is the heart of the v0.2 inclusion rule.
    """

    cat = parse_project(project_dir)
    daily = next(g for g in cat.feature_groups if g.name == "customer_features_daily")
    feature_names = {f.name for f in daily.features}
    assert "customer_id" not in feature_names  # entity + grain
    assert "feature_date" not in feature_names  # grain + timestamp_column


def test_exclude_columns_at_table_level(project_dir: Path) -> None:
    """Columns listed in `exclude_columns` are dropped without per-column blocks."""

    cat = parse_project(project_dir)
    daily = next(g for g in cat.feature_groups if g.name == "customer_features_daily")
    feature_names = {f.name for f in daily.features}
    assert "_loaded_at" not in feature_names


def test_per_column_is_feature_false_excludes(project_dir: Path) -> None:
    """`is_feature: false` on a column is the per-column escape hatch."""

    cat = parse_project(project_dir)
    daily = next(g for g in cat.feature_groups if g.name == "customer_features_daily")
    feature_names = {f.name for f in daily.features}
    assert "debug_score" not in feature_names


def test_columns_without_meta_block_are_auto_included(project_dir: Path) -> None:
    """v0.2 core: a non-key column with no `feature_catalog` block is still a feature."""

    cat = parse_project(project_dir)
    daily = next(g for g in cat.feature_groups if g.name == "customer_features_daily")
    feature_names = {f.name for f in daily.features}
    # `total_value_usd` has no meta.feature_catalog block at all.
    assert "total_value_usd" in feature_names


def test_feature_type_inferred_when_not_overridden(project_dir: Path) -> None:
    """Inference fills in `feature_type` from warehouse type when user didn't say."""

    cat = parse_project(project_dir)
    daily = next(g for g in cat.feature_groups if g.name == "customer_features_daily")

    # No block at all -> inferred from DECIMAL(10, 2) -> numeric.
    total = next(f for f in daily.features if f.name == "total_value_usd")
    assert total.feature_type == FeatureType.NUMERIC

    # Block present but no feature_type -> inferred from BOOLEAN -> boolean.
    repeat = next(f for f in daily.features if f.name == "is_repeat_customer")
    assert repeat.feature_type == FeatureType.BOOLEAN

    # Block present with explicit feature_type -> override wins (varchar
    # would otherwise be left unspecified by inference).
    pref = next(f for f in daily.features if f.name == "preferred_category")
    assert pref.feature_type == FeatureType.CATEGORICAL


def test_inference_on_column_with_no_feature_type_override(project_dir: Path) -> None:
    """Column has a meta block with null_behavior + used_by but no feature_type.

    Inference should fill it in from the warehouse data_type (INTEGER -> numeric).
    """

    cat = parse_project(project_dir)
    lifetime = next(g for g in cat.feature_groups if g.name == "customer_features_lifetime")
    lto = next(f for f in lifetime.features if f.name == "lifetime_order_count")
    assert lto.feature_type == FeatureType.NUMERIC


def test_feature_columns_preserve_yaml_order(project_dir: Path) -> None:
    """No more 'entity columns first' resorting — entity cols aren't features.

    We surface columns in the order dbt presents them (which round-trips
    YAML declaration order), so users can control layout from their YAML.
    """

    cat = parse_project(project_dir)
    daily = next(g for g in cat.feature_groups if g.name == "customer_features_daily")
    names = [f.name for f in daily.features]
    assert names == [
        "orders_count_7d",
        "is_repeat_customer",
        "total_value_usd",
        "preferred_category",
    ]


def test_used_by_and_null_behavior_overrides(project_dir: Path) -> None:
    cat = parse_project(project_dir)
    daily = next(g for g in cat.feature_groups if g.name == "customer_features_daily")
    orders = next(f for f in daily.features if f.name == "orders_count_7d")
    assert orders.null_behavior == NullBehavior.ZERO
    assert "churn_model_v2" in orders.used_by


def test_warehouse_type_from_manifest_takes_precedence(project_dir: Path) -> None:
    cat = parse_project(project_dir)
    daily = next(g for g in cat.feature_groups if g.name == "customer_features_daily")
    orders = next(f for f in daily.features if f.name == "orders_count_7d")
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
    assert "customer_features_lifetime" in downstream_names
    assert "consumer_dashboard_metrics" in downstream_names


def test_feature_table_lineage_flag(project_dir: Path) -> None:
    cat = parse_project(project_dir)
    daily = next(g for g in cat.feature_groups if g.name == "customer_features_daily")
    refs = {r.name: r for r in daily.downstream}
    assert refs["customer_features_lifetime"].is_feature_table is True
    assert refs["consumer_dashboard_metrics"].is_feature_table is False


def test_test_nodes_excluded_from_downstream(project_dir: Path) -> None:
    """dbt test nodes (not_null_*, unique_*) should not appear in downstream lineage."""

    cat = parse_project(project_dir)
    daily = next(g for g in cat.feature_groups if g.name == "customer_features_daily")
    downstream_types = {r.resource_type for r in daily.downstream}
    downstream_names = {r.name for r in daily.downstream}
    assert "test" not in downstream_types
    assert "not_null_customer_features_daily_customer_id" not in downstream_names
    assert "unique_customer_features_daily_customer_id" not in downstream_names
    assert len(daily.downstream) == 2


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


def test_feature_table_with_no_structural_metadata_includes_all_columns(tmp_path: Path) -> None:
    """Edge case: no entity/grain/timestamp/exclude declared.

    Every column becomes a feature. This is permissive on purpose — the
    user opted in at the table level; if they want stricter exclusion
    they have four mechanisms to use.
    """

    manifest = {
        "metadata": {"project_name": "p"},
        "nodes": {
            "model.p.flat": {
                "name": "flat",
                "unique_id": "model.p.flat",
                "resource_type": "model",
                "package_name": "p",
                "path": "flat.sql",
                "original_file_path": "models/flat.sql",
                "description": "",
                "schema": "s",
                "database": "d",
                "config": {"materialized": "table"},
                "meta": {"feature_catalog": {"is_feature_table": True}},
                "depends_on": {"nodes": []},
                "columns": {
                    "a": {"name": "a", "data_type": "int", "tags": [], "meta": {}},
                    "b": {"name": "b", "data_type": "varchar", "tags": [], "meta": {}},
                },
            }
        },
        "sources": {},
    }
    target = tmp_path / "target"
    target.mkdir()
    (target / "manifest.json").write_text(json.dumps(manifest))

    cat = parse_project(tmp_path)
    assert {f.name for f in cat.feature_groups[0].features} == {"a", "b"}


def test_exposure_tracing_populates_used_by(project_dir: Path) -> None:
    """ML exposures auto-derive used_by on features via graph traversal.

    The fixture has churn_scoring_model (type: ml) depending on
    consumer_dashboard_metrics, which depends on customer_features_daily.
    Every feature in customer_features_daily should gain churn_scoring_model
    in its used_by.
    """

    cat = parse_project(project_dir)
    daily = next(g for g in cat.feature_groups if g.name == "customer_features_daily")
    for feature in daily.features:
        assert "churn_scoring_model" in feature.used_by, (
            f"{feature.name} missing exposure-derived used_by"
        )


def test_exposure_tracing_preserves_manual_used_by(project_dir: Path) -> None:
    """Manual used_by entries are preserved alongside auto-derived ones."""

    cat = parse_project(project_dir)
    daily = next(g for g in cat.feature_groups if g.name == "customer_features_daily")
    orders = next(f for f in daily.features if f.name == "orders_count_7d")
    assert "churn_model_v2" in orders.used_by
    assert "ltv_model_v3" in orders.used_by
    assert "churn_scoring_model" in orders.used_by


def test_non_ml_exposures_ignored(project_dir: Path) -> None:
    """Exposures with type != 'ml' should not affect feature used_by."""

    cat = parse_project(project_dir)
    daily = next(g for g in cat.feature_groups if g.name == "customer_features_daily")
    for feature in daily.features:
        assert "bi_dashboard" not in feature.used_by


def test_exposure_info_on_catalog(project_dir: Path) -> None:
    """ML exposure metadata is stored on the catalog for model pages."""

    cat = parse_project(project_dir)
    assert "churn_scoring_model" in cat.exposure_info
    info = cat.exposure_info["churn_scoring_model"]
    assert info.owner_name == "ML Platform Team"
    assert info.owner_email == "ml-platform@jaffle.com"
    assert info.maturity == "high"
    assert info.url == "https://mlflow.jaffle.com/models/churn"
    assert "bi_dashboard" not in cat.exposure_info


def test_exposure_derived_models_appear_in_all_models(project_dir: Path) -> None:
    """Exposure-derived used_by entries appear in catalog.all_models."""

    cat = parse_project(project_dir)
    assert "churn_scoring_model" in cat.all_models


def test_all_tags_aggregated(project_dir: Path) -> None:
    cat = parse_project(project_dir)
    assert cat.all_tags == ["customer", "daily", "lifetime"]


def test_groups_by_tag(project_dir: Path) -> None:
    cat = parse_project(project_dir)
    by_tag = cat.feature_groups_by_tag()
    assert "customer" in by_tag
    assert {g.name for g in by_tag["customer"]} == {
        "customer_features_daily",
        "customer_features_lifetime",
    }
    assert {g.name for g in by_tag["daily"]} == {"customer_features_daily"}
