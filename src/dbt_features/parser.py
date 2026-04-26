"""Parser layer — read dbt artifacts and assemble a ``Catalog``.

We deliberately don't depend on ``dbt-core``: importing it pulls in a heavy
tree of database adapters, which would force users into version-pinning
fights with their actual dbt installation. Instead, we read ``manifest.json``
and ``catalog.json`` as plain JSON. These artifacts are stable, well-known,
and versioned by dbt itself.

We touch only the small subset of fields we need (model nodes, columns,
depends_on, meta, materialization). Anything else can change without
breaking us.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from pydantic import ValidationError

from dbt_features.catalog import Catalog, Feature, FeatureGroup, LineageRef
from dbt_features.inference import infer_feature_type
from dbt_features.schema import (
    FeatureMeta,
    FeatureTableMeta,
    SchemaError,
)

DEFAULT_TARGET_DIR = "target"
MANIFEST_FILE = "manifest.json"
CATALOG_FILE = "catalog.json"


def parse_project(
    project_dir: str | Path,
    *,
    manifest_path: str | Path | None = None,
    catalog_path: str | Path | None = None,
) -> Catalog:
    """Parse a dbt project directory into a ``Catalog``.

    Looks for ``target/manifest.json`` and ``target/catalog.json`` by default.
    ``catalog.json`` is optional; if absent, columns won't have warehouse
    types but everything else still renders.
    """

    project_dir = Path(project_dir)
    manifest_path = Path(manifest_path) if manifest_path else project_dir / DEFAULT_TARGET_DIR / MANIFEST_FILE
    catalog_path = Path(catalog_path) if catalog_path else project_dir / DEFAULT_TARGET_DIR / CATALOG_FILE

    if not manifest_path.exists():
        raise FileNotFoundError(
            f"manifest.json not found at {manifest_path}. "
            f"Run `dbt parse` or `dbt compile` in your dbt project first."
        )

    manifest = _load_json(manifest_path)
    dbt_catalog = _load_json(catalog_path) if catalog_path.exists() else None

    return _build_catalog(manifest, dbt_catalog)


def _load_json(path: Path) -> dict[str, Any]:
    with path.open("r", encoding="utf-8") as f:
        return json.load(f)


def _build_catalog(manifest: dict[str, Any], dbt_catalog: dict[str, Any] | None) -> Catalog:
    nodes: dict[str, Any] = manifest.get("nodes", {})
    sources: dict[str, Any] = manifest.get("sources", {})
    project_name = manifest.get("metadata", {}).get("project_name") or "dbt-project"

    catalog_nodes: dict[str, Any] = {}
    if dbt_catalog is not None:
        catalog_nodes = dbt_catalog.get("nodes", {})

    # Pass 1: identify every model that's been declared a feature table, and
    # validate its metadata. We collect everything before resolving lineage
    # so a later step can cross-reference upstreams.
    feature_table_nodes: dict[str, dict[str, Any]] = {}
    feature_table_metas: dict[str, FeatureTableMeta] = {}

    for unique_id, node in nodes.items():
        if node.get("resource_type") != "model":
            continue
        meta_block = _feature_catalog_meta(node)
        if meta_block is None:
            continue
        try:
            ft_meta = FeatureTableMeta.model_validate(meta_block)
        except ValidationError as e:
            raise SchemaError(_format_validation_error(e, node), node_id=unique_id) from e
        if not ft_meta.is_feature_table:
            continue
        feature_table_nodes[unique_id] = node
        feature_table_metas[unique_id] = ft_meta

    # Pass 2: build a reverse lineage index so we can resolve "downstream
    # consumers" — every node that depends on this feature table.
    # Test nodes (not_null_*, unique_*, etc.) are excluded: they drown the
    # 2–3 real downstream models in 20–40 test references per table.
    children_of: dict[str, list[str]] = {}
    for unique_id, node in nodes.items():
        if node.get("resource_type") == "test":
            continue
        for dep in node.get("depends_on", {}).get("nodes", []) or []:
            children_of.setdefault(dep, []).append(unique_id)

    # Pass 3: assemble FeatureGroup objects.
    feature_groups: list[FeatureGroup] = []
    for unique_id, node in feature_table_nodes.items():
        meta = feature_table_metas[unique_id]
        features = _build_features(unique_id, node, catalog_nodes, meta)
        upstream = _resolve_lineage(
            node.get("depends_on", {}).get("nodes", []) or [],
            nodes,
            sources,
            feature_table_nodes,
        )
        downstream = _resolve_lineage(
            children_of.get(unique_id, []),
            nodes,
            sources,
            feature_table_nodes,
        )
        description = meta.description or node.get("description") or ""
        feature_groups.append(
            FeatureGroup(
                name=node.get("name", unique_id),
                unique_id=unique_id,
                description=description,
                schema_name=node.get("schema") or "",
                database=node.get("database"),
                materialization=node.get("config", {}).get("materialized") or "view",
                package_name=node.get("package_name") or "",
                file_path=node.get("original_file_path") or node.get("path") or "",
                meta=meta,
                features=features,
                upstream=upstream,
                downstream=downstream,
            )
        )

    feature_groups.sort(key=lambda g: g.name)
    return Catalog(project_name=project_name, feature_groups=tuple(feature_groups))


def _feature_catalog_meta(node: dict[str, Any]) -> dict[str, Any] | None:
    """Extract the ``feature_catalog`` block from a node's ``meta``.

    dbt allows two locations for arbitrary metadata: ``node.meta`` (top
    level) and ``node.config.meta`` (under config). We check both — modern
    dbt prefers the latter but the former is still common in older projects.
    """

    meta = node.get("meta") or {}
    if "feature_catalog" in meta:
        return meta["feature_catalog"]
    config_meta = (node.get("config") or {}).get("meta") or {}
    if "feature_catalog" in config_meta:
        return config_meta["feature_catalog"]
    return None


def _column_feature_catalog_meta(column: dict[str, Any]) -> dict[str, Any] | None:
    meta = column.get("meta") or {}
    if "feature_catalog" in meta:
        return meta["feature_catalog"]
    return None


def _build_features(
    unique_id: str,
    node: dict[str, Any],
    catalog_nodes: dict[str, Any],
    table_meta: FeatureTableMeta,
) -> tuple[Feature, ...]:
    """Derive features from a feature-table node.

    Inclusion rule (v0.2): every column on the model is a feature *unless*
    it appears in the table's structural metadata (entity / grain /
    timestamp_column / exclude_columns) or its column-level meta block sets
    ``is_feature: false``. Column blocks are pure overrides — their absence
    is not an opt-out.

    ``feature_type`` falls back to ``infer_feature_type(column_type)`` when
    the user hasn't specified one. The inference is conservative (numeric /
    boolean / timestamp / embedding only); ambiguous warehouse types like
    VARCHAR stay unspecified.
    """

    columns: dict[str, Any] = node.get("columns") or {}
    catalog_columns: dict[str, Any] = (
        catalog_nodes.get(unique_id, {}).get("columns", {}) if catalog_nodes else {}
    )

    excluded = (
        set(table_meta.entity_columns)
        | set(table_meta.grain)
        | ({table_meta.timestamp_column} if table_meta.timestamp_column else set())
        | set(table_meta.exclude_columns)
    )

    features: list[Feature] = []
    for col_name, col in columns.items():
        if col_name in excluded:
            continue

        meta_block = _column_feature_catalog_meta(col)
        if meta_block is not None:
            try:
                f_meta = FeatureMeta.model_validate(meta_block)
            except ValidationError as e:
                raise SchemaError(
                    _format_validation_error(e, col, label=f"column `{col_name}`"),
                    node_id=unique_id,
                ) from e
        else:
            f_meta = FeatureMeta()

        if not f_meta.is_feature:
            # Explicit per-column opt-out. Same effect as listing the column
            # under ``exclude_columns``, just colocated with the column.
            continue

        column_type = col.get("data_type")
        if not column_type:
            cat_col = _ci_lookup(catalog_columns, col_name)
            if cat_col:
                column_type = cat_col.get("type")

        feature_type = f_meta.feature_type or infer_feature_type(column_type)
        description = f_meta.description or col.get("description") or ""
        column_tags = list(col.get("tags") or [])

        features.append(
            Feature(
                name=col_name,
                description=description,
                column_type=column_type,
                feature_type=feature_type,
                null_behavior=f_meta.null_behavior,
                used_by=tuple(f_meta.used_by),
                tags=tuple(column_tags),
                definition_version=f_meta.definition_version,
                lifecycle=f_meta.lifecycle,
                replacement=f_meta.replacement,
            )
        )

    # Preserve YAML declaration order (Python dicts are insertion-ordered
    # since 3.7, and dbt round-trips that order through manifest.json).
    return tuple(features)


def _ci_lookup(d: dict[str, Any], key: str) -> dict[str, Any] | None:
    """Case-insensitive dict lookup.

    catalog.json sometimes upper-cases column names depending on adapter
    (Snowflake especially). manifest.json uses whatever case the user
    declared. We tolerate the mismatch.
    """

    if key in d:
        return d[key]
    lower = key.lower()
    for k, v in d.items():
        if k.lower() == lower:
            return v
    return None


def _resolve_lineage(
    unique_ids: list[str],
    nodes: dict[str, Any],
    sources: dict[str, Any],
    feature_table_nodes: dict[str, dict[str, Any]],
) -> tuple[LineageRef, ...]:
    refs: list[LineageRef] = []
    for uid in unique_ids:
        if uid in nodes:
            n = nodes[uid]
            refs.append(
                LineageRef(
                    unique_id=uid,
                    name=n.get("name") or uid,
                    resource_type=n.get("resource_type") or "model",
                    is_feature_table=uid in feature_table_nodes,
                )
            )
        elif uid in sources:
            s = sources[uid]
            refs.append(
                LineageRef(
                    unique_id=uid,
                    name=f"{s.get('source_name','')}.{s.get('name', uid)}".strip("."),
                    resource_type="source",
                    is_feature_table=False,
                )
            )
        else:
            # Unknown node id — surface it rather than dropping silently, in
            # case a downstream artifact references a node not in this
            # manifest. Won't render as a link.
            refs.append(
                LineageRef(
                    unique_id=uid,
                    name=uid.split(".")[-1],
                    resource_type="unknown",
                    is_feature_table=False,
                )
            )
    refs.sort(key=lambda r: (not r.is_feature_table, r.name))
    return tuple(refs)


def _format_validation_error(
    e: ValidationError, source: dict[str, Any], label: str | None = None
) -> str:
    name = source.get("name") or label or "<unknown>"
    lines = [f"Invalid feature_catalog metadata on {label or name}:"]
    for err in e.errors():
        loc = ".".join(str(p) for p in err["loc"]) or "<root>"
        lines.append(f"  - {loc}: {err['msg']}")
    return "\n".join(lines)
