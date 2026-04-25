"""Infer ``FeatureType`` from a warehouse column type.

Conservative on purpose. We only map the unambiguous cases — anything we
can't confidently classify returns ``None`` and the renderer shows "—".
A wrong inferred type would be worse than no inference at all (users would
silently consume mislabeled features), so the bias is towards leaving it
unspecified and asking the user to override when it matters.

Notably, ``VARCHAR`` / ``TEXT`` / ``STRING`` are *not* inferred. The
distinction between ``categorical`` and ``text`` requires cardinality data
we don't have at parse time. Cardinality-based inference would couple this
module to the enrichment layer, which we want to avoid.
"""

from __future__ import annotations

import re

from dbt_features.schema import FeatureType

_PARENS_RE = re.compile(r"\(.*?\)")


def _normalize(data_type: str) -> str:
    """Strip ``(precision, scale)`` and uppercase for matching.

    Examples:
        ``VARCHAR(255)`` -> ``VARCHAR``
        ``decimal(10, 2)`` -> ``DECIMAL``
        ``ARRAY<FLOAT64>`` -> ``ARRAY<FLOAT64>`` (stays uppercase)
    """

    return _PARENS_RE.sub("", data_type).strip().upper()


_NUMERIC_TYPES = {
    "INT",
    "INT2",
    "INT4",
    "INT8",
    "INT16",
    "INT32",
    "INT64",
    "INTEGER",
    "BIGINT",
    "SMALLINT",
    "TINYINT",
    "FLOAT",
    "FLOAT4",
    "FLOAT8",
    "FLOAT32",
    "FLOAT64",
    "DOUBLE",
    "DOUBLE PRECISION",
    "REAL",
    "DECIMAL",
    "NUMERIC",
    "NUMBER",
}

_BOOLEAN_TYPES = {"BOOL", "BOOLEAN"}

_NUMERIC_ARRAY_RE = re.compile(
    r"ARRAY[<\[]("
    r"FLOAT|FLOAT4|FLOAT8|FLOAT32|FLOAT64|"
    r"DOUBLE|DOUBLE PRECISION|REAL|"
    r"INT|INT2|INT4|INT8|INT16|INT32|INT64|INTEGER|BIGINT|SMALLINT|TINYINT|"
    r"DECIMAL|NUMERIC|NUMBER"
    r")[>\]]",
)

_TIMESTAMP_TYPES = {
    "DATE",
    "DATETIME",
    "TIMESTAMP",
    "TIMESTAMP_NTZ",
    "TIMESTAMP_LTZ",
    "TIMESTAMP_TZ",
    "TIMESTAMPTZ",
}


def infer_feature_type(data_type: str | None) -> FeatureType | None:
    """Best-effort guess at a column's ``feature_type`` from its warehouse type.

    Returns ``None`` for unknown / ambiguous types — the caller should treat
    that as "no inference available" rather than as an error.
    """

    if not data_type:
        return None

    normalized = _normalize(data_type)

    if normalized in _NUMERIC_TYPES:
        return FeatureType.NUMERIC
    if normalized in _BOOLEAN_TYPES:
        return FeatureType.BOOLEAN
    if normalized in _TIMESTAMP_TYPES:
        return FeatureType.TIMESTAMP

    # Vector types are unambiguous — ``VECTOR(768)`` etc.
    if normalized.startswith("VECTOR"):
        return FeatureType.EMBEDDING

    # Array types are only inferred as embedding when the element type is
    # numeric (``ARRAY<FLOAT64>``, ``ARRAY[DOUBLE]``). A bare ``ARRAY``
    # without element info or an ``ARRAY<STRING>`` is ambiguous and left
    # unspecified.
    if normalized.startswith("ARRAY") and _NUMERIC_ARRAY_RE.search(normalized):
        return FeatureType.EMBEDDING

    # VARCHAR / TEXT / STRING / CHAR intentionally not inferred — see module
    # docstring. User must override if they care.
    return None


__all__ = ["infer_feature_type"]
