"""User-facing metadata schema for `feature_catalog` blocks in dbt YAML.

These Pydantic models define what a user can declare under
``meta.feature_catalog`` in their dbt ``schema.yml`` files. The schema is
versioned and intentionally minimal — the goal is discovery metadata, not a
full feature-store spec.

This module is the load-bearing contract of the project. Changes here are
breaking changes.
"""

from __future__ import annotations

from enum import Enum
from typing import Annotated

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

SCHEMA_VERSION = "0.2"


class FeatureType(str, Enum):
    """The semantic type of a feature.

    Distinct from the warehouse column type (e.g. ``DOUBLE``) — this captures
    how the feature is *used* downstream, which is what consumers care about.
    """

    NUMERIC = "numeric"
    CATEGORICAL = "categorical"
    BOOLEAN = "boolean"
    EMBEDDING = "embedding"
    TIMESTAMP = "timestamp"
    TEXT = "text"
    IDENTIFIER = "identifier"


class NullBehavior(str, Enum):
    """How nulls in this feature are handled by downstream consumers.

    Documentation only — not enforced by the catalog. Consumers can use this
    to decide on imputation strategies.
    """

    ZERO = "zero"
    MEAN = "mean"
    PROPAGATE = "propagate"
    ERROR = "error"
    IGNORE = "ignore"


class FreshnessPeriod(str, Enum):
    MINUTE = "minute"
    HOUR = "hour"
    DAY = "day"


class Lifecycle(str, Enum):
    """Where this feature/feature-table is in its lifecycle.

    The catalog renders the state (badge + deprecation banner) but does not
    block usage. Enforcement is left to downstream consumers.
    """

    ACTIVE = "active"
    PREVIEW = "preview"
    DEPRECATED = "deprecated"


class FreshnessThreshold(BaseModel):
    """Same shape as dbt source freshness thresholds, on purpose.

    Users already know this shape from ``dbt-core``, so we reuse it.
    """

    model_config = ConfigDict(extra="forbid")

    count: Annotated[int, Field(ge=1)]
    period: FreshnessPeriod


class Freshness(BaseModel):
    model_config = ConfigDict(extra="forbid")

    warn_after: FreshnessThreshold | None = None
    error_after: FreshnessThreshold | None = None

    @model_validator(mode="after")
    def _at_least_one(self) -> Freshness:
        if self.warn_after is None and self.error_after is None:
            raise ValueError("freshness must declare at least one of warn_after / error_after")
        return self


class FeatureMeta(BaseModel):
    """Column-level ``feature_catalog`` metadata.

    Lives under ``columns[].meta.feature_catalog`` in dbt YAML.

    In v0.2+ columns are auto-included in the catalog when their parent
    table is marked ``is_feature_table: true`` — except those listed in
    ``entity`` / ``grain`` / ``timestamp_column`` / ``exclude_columns``.
    A column block is therefore an *override*, not an opt-in. Set
    ``is_feature: false`` to exclude a single column without listing it
    at the table level.
    """

    model_config = ConfigDict(extra="forbid")

    is_feature: bool = True
    feature_type: FeatureType | None = None
    null_behavior: NullBehavior | None = None
    used_by: list[str] = Field(default_factory=list)
    description: str | None = None
    definition_version: Annotated[int, Field(ge=1)] = 1
    lifecycle: Lifecycle = Lifecycle.ACTIVE
    replacement: str | None = None

    @field_validator("used_by")
    @classmethod
    def _strip_used_by(cls, v: list[str]) -> list[str]:
        return [s.strip() for s in v if s and s.strip()]


class FeatureTableMeta(BaseModel):
    """Model-level ``feature_catalog`` metadata.

    Lives under ``models[].meta.feature_catalog`` in dbt YAML. A model is
    only picked up by the catalog if ``is_feature_table`` is true.
    """

    model_config = ConfigDict(extra="forbid")

    is_feature_table: bool = False
    version: str = SCHEMA_VERSION
    entity: str | list[str] | None = None
    grain: list[str] = Field(default_factory=list)
    timestamp_column: str | None = None
    exclude_columns: list[str] = Field(default_factory=list)
    freshness: Freshness | None = None
    owner: str | None = None
    tags: list[str] = Field(default_factory=list)
    description: str | None = None
    definition_version: Annotated[int, Field(ge=1)] = 1
    lifecycle: Lifecycle = Lifecycle.ACTIVE
    replacement: str | None = None

    @field_validator("entity")
    @classmethod
    def _normalize_entity(cls, v: str | list[str] | None) -> list[str] | None:
        if v is None:
            return None
        if isinstance(v, str):
            return [v]
        return list(v)

    @field_validator("tags", "grain", "exclude_columns")
    @classmethod
    def _strip_strings(cls, v: list[str]) -> list[str]:
        return [s.strip() for s in v if s and s.strip()]

    @property
    def entity_columns(self) -> list[str]:
        if self.entity is None:
            return []
        return self.entity if isinstance(self.entity, list) else [self.entity]


class SchemaError(ValueError):
    """Raised when a user's ``feature_catalog`` block fails validation.

    Carries the dbt node id so the CLI can produce a useful error message
    pointing back to the offending model.
    """

    def __init__(self, message: str, node_id: str | None = None):
        super().__init__(message)
        self.node_id = node_id
