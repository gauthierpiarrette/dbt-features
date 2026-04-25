from __future__ import annotations

import pytest
from pydantic import ValidationError

from dbt_features.schema import (
    FeatureMeta,
    FeatureTableMeta,
    FeatureType,
    Freshness,
    FreshnessThreshold,
    Lifecycle,
    NullBehavior,
)


class TestFeatureTableMeta:
    def test_minimal(self) -> None:
        m = FeatureTableMeta.model_validate({"is_feature_table": True})
        assert m.is_feature_table is True
        assert m.entity is None
        assert m.tags == []

    def test_full(self) -> None:
        m = FeatureTableMeta.model_validate(
            {
                "is_feature_table": True,
                "entity": "user_id",
                "grain": ["dt", "user_id"],
                "timestamp_column": "dt",
                "freshness": {
                    "warn_after": {"count": 24, "period": "hour"},
                    "error_after": {"count": 48, "period": "hour"},
                },
                "owner": "team@x.com",
                "tags": ["users", "daily"],
            }
        )
        assert m.entity_columns == ["user_id"]
        assert m.freshness is not None
        assert m.freshness.warn_after is not None
        assert m.freshness.warn_after.count == 24

    def test_entity_list_normalized(self) -> None:
        m = FeatureTableMeta.model_validate(
            {"is_feature_table": True, "entity": ["a", "b"]}
        )
        assert m.entity_columns == ["a", "b"]

    def test_entity_string_normalized_to_list(self) -> None:
        m = FeatureTableMeta.model_validate({"is_feature_table": True, "entity": "x"})
        assert m.entity_columns == ["x"]

    def test_extra_fields_rejected(self) -> None:
        with pytest.raises(ValidationError):
            FeatureTableMeta.model_validate(
                {"is_feature_table": True, "totally_not_a_field": "boom"}
            )

    def test_strips_empty_tags(self) -> None:
        m = FeatureTableMeta.model_validate(
            {"is_feature_table": True, "tags": [" foo ", "", "bar"]}
        )
        assert m.tags == ["foo", "bar"]

    def test_exclude_columns_defaults_empty(self) -> None:
        m = FeatureTableMeta.model_validate({"is_feature_table": True})
        assert m.exclude_columns == []

    def test_exclude_columns_strips_blanks(self) -> None:
        m = FeatureTableMeta.model_validate(
            {"is_feature_table": True, "exclude_columns": [" _loaded_at ", "", "_batch_id"]}
        )
        assert m.exclude_columns == ["_loaded_at", "_batch_id"]


class TestFreshness:
    def test_requires_at_least_one_threshold(self) -> None:
        with pytest.raises(ValidationError):
            Freshness.model_validate({})

    def test_warn_only_ok(self) -> None:
        f = Freshness.model_validate({"warn_after": {"count": 1, "period": "hour"}})
        assert f.warn_after is not None
        assert f.error_after is None

    def test_count_must_be_positive(self) -> None:
        with pytest.raises(ValidationError):
            FreshnessThreshold.model_validate({"count": 0, "period": "hour"})


class TestFeatureMeta:
    def test_default_is_feature_true(self) -> None:
        """v0.2: presence of a column meta block does not opt-in.

        Inclusion is decided at the table level. The block exists to
        carry overrides; ``is_feature`` defaults to True so an empty
        block is equivalent to no block at all.
        """

        m = FeatureMeta.model_validate({})
        assert m.is_feature is True
        assert m.feature_type is None

    def test_explicit_false_opts_out(self) -> None:
        """The one new use of ``is_feature`` in v0.2 is per-column exclude."""

        m = FeatureMeta.model_validate({"is_feature": False})
        assert m.is_feature is False

    def test_full(self) -> None:
        m = FeatureMeta.model_validate(
            {
                "feature_type": "numeric",
                "null_behavior": "zero",
                "used_by": ["model_a", "model_b"],
            }
        )
        assert m.feature_type == FeatureType.NUMERIC
        assert m.null_behavior == NullBehavior.ZERO
        assert m.used_by == ["model_a", "model_b"]

    def test_strips_used_by(self) -> None:
        m = FeatureMeta.model_validate({"used_by": [" a ", "", "b"]})
        assert m.used_by == ["a", "b"]

    def test_invalid_feature_type_rejected(self) -> None:
        with pytest.raises(ValidationError):
            FeatureMeta.model_validate({"feature_type": "complex_number"})

    def test_extra_fields_rejected(self) -> None:
        with pytest.raises(ValidationError):
            FeatureMeta.model_validate({"secret_field": True})


class TestLifecycleAndVersion:
    """Schema fields for lifecycle / definition_version. Used by the renderer."""

    def test_defaults(self) -> None:
        f = FeatureMeta.model_validate({})
        assert f.definition_version == 1
        assert f.lifecycle == Lifecycle.ACTIVE
        assert f.replacement is None

        t = FeatureTableMeta.model_validate({"is_feature_table": True})
        assert t.definition_version == 1
        assert t.lifecycle == Lifecycle.ACTIVE
        assert t.replacement is None

    def test_definition_version_must_be_positive(self) -> None:
        with pytest.raises(ValidationError):
            FeatureMeta.model_validate({"definition_version": 0})

    def test_lifecycle_enum(self) -> None:
        f = FeatureMeta.model_validate(
            {"lifecycle": "deprecated", "replacement": "new_feature"}
        )
        assert f.lifecycle == Lifecycle.DEPRECATED
        assert f.replacement == "new_feature"

    def test_invalid_lifecycle_rejected(self) -> None:
        with pytest.raises(ValidationError):
            FeatureMeta.model_validate({"lifecycle": "retired"})

    def test_table_level_preview(self) -> None:
        t = FeatureTableMeta.model_validate(
            {"is_feature_table": True, "lifecycle": "preview"}
        )
        assert t.lifecycle == Lifecycle.PREVIEW
