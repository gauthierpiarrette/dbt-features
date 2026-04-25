"""Internal catalog data model — what the renderer consumes.

These are deliberately kept separate from the user-facing schema in
``schema.py``: the user's declared metadata gets normalized, joined with
manifest/catalog data, and resolved into these objects before rendering.
That separation keeps validation errors close to the user's input and keeps
the rendering layer simple.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone

from dbt_features.schema import (
    FeatureTableMeta,
    FeatureType,
    Freshness,
    Lifecycle,
    NullBehavior,
)


@dataclass(frozen=True, slots=True)
class Feature:
    name: str
    description: str
    column_type: str | None
    feature_type: FeatureType | None
    null_behavior: NullBehavior | None
    used_by: tuple[str, ...]
    tags: tuple[str, ...]
    definition_version: int = 1
    lifecycle: Lifecycle = Lifecycle.ACTIVE
    replacement: str | None = None


@dataclass(frozen=True, slots=True)
class LineageRef:
    """A reference to another node in the dbt graph.

    ``unique_id`` is dbt's identifier (e.g. ``model.jaffle.foo``). ``name``
    is the short name used in the UI. ``is_feature_table`` lets the renderer
    link to a catalog page if the dependency is itself a feature table.
    """

    unique_id: str
    name: str
    resource_type: str
    is_feature_table: bool


@dataclass(frozen=True, slots=True)
class FeatureGroup:
    """A dbt model marked as a feature table.

    Composed of: the user-declared ``FeatureTableMeta`` (validated upstream),
    plus model facts pulled from ``manifest.json`` (description, schema,
    materialization, lineage), plus the columns the user marked as features.
    Non-feature columns (keys, timestamps) are intentionally excluded.
    """

    name: str
    unique_id: str
    description: str
    schema_name: str
    database: str | None
    materialization: str
    package_name: str
    file_path: str
    meta: FeatureTableMeta
    features: tuple[Feature, ...]
    upstream: tuple[LineageRef, ...]
    downstream: tuple[LineageRef, ...]

    @property
    def entity_columns(self) -> list[str]:
        return self.meta.entity_columns

    @property
    def grain(self) -> list[str]:
        return list(self.meta.grain)

    @property
    def timestamp_column(self) -> str | None:
        return self.meta.timestamp_column

    @property
    def freshness(self) -> Freshness | None:
        return self.meta.freshness

    @property
    def owner(self) -> str | None:
        return self.meta.owner

    @property
    def tags(self) -> list[str]:
        return list(self.meta.tags)

    @property
    def definition_version(self) -> int:
        return self.meta.definition_version

    @property
    def lifecycle(self) -> Lifecycle:
        return self.meta.lifecycle

    @property
    def replacement(self) -> str | None:
        return self.meta.replacement

    @property
    def fully_qualified_name(self) -> str:
        parts = [p for p in (self.database, self.schema_name, self.name) if p]
        return ".".join(parts)


@dataclass(frozen=True, slots=True)
class Catalog:
    project_name: str
    feature_groups: tuple[FeatureGroup, ...]
    generated_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))

    @property
    def feature_count(self) -> int:
        return sum(len(g.features) for g in self.feature_groups)

    @property
    def all_tags(self) -> list[str]:
        seen: dict[str, None] = {}
        for g in self.feature_groups:
            for tag in g.tags:
                seen[tag] = None
        return sorted(seen)

    def by_unique_id(self, unique_id: str) -> FeatureGroup | None:
        for g in self.feature_groups:
            if g.unique_id == unique_id:
                return g
        return None

    def feature_groups_by_tag(self) -> dict[str, list[FeatureGroup]]:
        """Group feature groups by tag.

        Groups with no tags fall under ``"untagged"``. A group with multiple
        tags appears under each one. Kept for backward compatibility and
        for callers (e.g. exporters) that want a tag-faceted view; the
        index page itself groups by entity now to avoid card duplication.
        """

        out: dict[str, list[FeatureGroup]] = {}
        for g in self.feature_groups:
            tags = g.tags or ["untagged"]
            for tag in tags:
                out.setdefault(tag, []).append(g)
        for groups in out.values():
            groups.sort(key=lambda g: g.name)
        return dict(sorted(out.items()))

    def feature_groups_by_entity(self) -> dict[str, list[FeatureGroup]]:
        """Group feature groups by their primary entity, no duplication.

        Entity is the join key — the question every feature consumer asks
        first ("what features can I join to a customer?"). We use the
        first declared entity column as the section. Multi-entity groups
        land in ``"Cross-entity"`` so they're discoverable but not
        duplicated. Groups without an entity fall under ``"Other"``.
        """

        out: dict[str, list[FeatureGroup]] = {}
        for g in self.feature_groups:
            entities = g.entity_columns
            if not entities:
                key = "Other"
            elif len(entities) > 1:
                key = "Cross-entity"
            else:
                key = entities[0]
            out.setdefault(key, []).append(g)
        for groups in out.values():
            groups.sort(key=lambda g: g.name)
        # Surface "Cross-entity" and "Other" last; everything else alpha.
        def _order(k: str) -> tuple[int, str]:
            if k == "Cross-entity":
                return (1, k)
            if k == "Other":
                return (2, k)
            return (0, k)

        return {k: out[k] for k in sorted(out.keys(), key=_order)}

    @property
    def all_entities(self) -> list[str]:
        seen: dict[str, None] = {}
        for g in self.feature_groups:
            for e in g.entity_columns:
                seen[e] = None
        return sorted(seen)

    @property
    def all_owners(self) -> list[str]:
        seen: dict[str, None] = {}
        for g in self.feature_groups:
            if g.owner:
                seen[g.owner] = None
        return sorted(seen)

    @property
    def all_models(self) -> list[str]:
        """Distinct model names declared via column-level ``used_by``.

        These are typically ML/analytics consumers that don't appear in
        the dbt graph. Sorted, deduped.
        """

        seen: dict[str, None] = {}
        for g in self.feature_groups:
            for f in g.features:
                for m in f.used_by:
                    seen[m] = None
        return sorted(seen)

    def features_by_model(self) -> dict[str, list[tuple[FeatureGroup, Feature]]]:
        """Inverted index: model name -> list of (group, feature) pairs.

        Powers the ``/models/<name>/`` pages — the consumer-centric view
        that was missing from v0.2.
        """

        out: dict[str, list[tuple[FeatureGroup, Feature]]] = {}
        for g in self.feature_groups:
            for f in g.features:
                for m in f.used_by:
                    out.setdefault(m, []).append((g, f))
        for entries in out.values():
            entries.sort(key=lambda gf: (gf[0].name, gf[1].name))
        return dict(sorted(out.items()))
