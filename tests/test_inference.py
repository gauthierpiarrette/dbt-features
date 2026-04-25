from __future__ import annotations

import pytest

from dbt_features.inference import infer_feature_type
from dbt_features.schema import FeatureType


@pytest.mark.parametrize(
    "data_type",
    ["INT", "INTEGER", "BIGINT", "SMALLINT", "TINYINT", "INT64", "int", "Integer"],
)
def test_integer_types_map_to_numeric(data_type: str) -> None:
    assert infer_feature_type(data_type) == FeatureType.NUMERIC


@pytest.mark.parametrize(
    "data_type",
    ["FLOAT", "DOUBLE", "REAL", "DECIMAL", "NUMERIC", "NUMBER", "FLOAT64", "DOUBLE PRECISION"],
)
def test_floating_types_map_to_numeric(data_type: str) -> None:
    assert infer_feature_type(data_type) == FeatureType.NUMERIC


@pytest.mark.parametrize("data_type", ["BOOL", "BOOLEAN", "bool", "Boolean"])
def test_boolean_types(data_type: str) -> None:
    assert infer_feature_type(data_type) == FeatureType.BOOLEAN


@pytest.mark.parametrize(
    "data_type",
    [
        "DATE",
        "TIMESTAMP",
        "DATETIME",
        "TIMESTAMP_NTZ",
        "TIMESTAMP_LTZ",
        "TIMESTAMPTZ",
        "timestamp",
    ],
)
def test_timestamp_types(data_type: str) -> None:
    assert infer_feature_type(data_type) == FeatureType.TIMESTAMP


@pytest.mark.parametrize(
    "data_type",
    ["ARRAY<FLOAT64>", "ARRAY[DOUBLE]", "ARRAY", "VECTOR(768)", "VECTOR"],
)
def test_array_and_vector_types_map_to_embedding(data_type: str) -> None:
    assert infer_feature_type(data_type) == FeatureType.EMBEDDING


@pytest.mark.parametrize("data_type", ["VARCHAR", "TEXT", "STRING", "CHAR", "VARCHAR(255)"])
def test_string_types_left_unspecified(data_type: str) -> None:
    """Categorical vs text is ambiguous without cardinality — user overrides."""

    assert infer_feature_type(data_type) is None


def test_parens_stripped() -> None:
    assert infer_feature_type("DECIMAL(10, 2)") == FeatureType.NUMERIC
    assert infer_feature_type("NUMERIC(38,9)") == FeatureType.NUMERIC


def test_unknown_type_returns_none() -> None:
    assert infer_feature_type("GEOGRAPHY") is None
    assert infer_feature_type("JSON") is None
    assert infer_feature_type("VARIANT") is None


def test_none_and_empty() -> None:
    assert infer_feature_type(None) is None
    assert infer_feature_type("") is None
    assert infer_feature_type("   ") is None
