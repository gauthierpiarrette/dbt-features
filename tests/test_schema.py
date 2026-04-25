from __future__ import annotations

import pytest
from pydantic import ValidationError

from dbt_features.schema import (
    FeatureMeta,
    FeatureTableMeta,
    FeatureType,
    Freshness,
    FreshnessThreshold,
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
    def test_minimal(self) -> None:
        m = FeatureMeta.model_validate({"is_feature": True})
        assert m.is_feature is True
        assert m.feature_type is None

    def test_full(self) -> None:
        m = FeatureMeta.model_validate(
            {
                "is_feature": True,
                "feature_type": "numeric",
                "null_behavior": "zero",
                "used_by": ["model_a", "model_b"],
            }
        )
        assert m.feature_type == FeatureType.NUMERIC
        assert m.null_behavior == NullBehavior.ZERO
        assert m.used_by == ["model_a", "model_b"]

    def test_strips_used_by(self) -> None:
        m = FeatureMeta.model_validate(
            {"is_feature": True, "used_by": [" a ", "", "b"]}
        )
        assert m.used_by == ["a", "b"]

    def test_invalid_feature_type_rejected(self) -> None:
        with pytest.raises(ValidationError):
            FeatureMeta.model_validate({"is_feature": True, "feature_type": "complex_number"})

    def test_extra_fields_rejected(self) -> None:
        with pytest.raises(ValidationError):
            FeatureMeta.model_validate({"is_feature": True, "secret_field": True})
