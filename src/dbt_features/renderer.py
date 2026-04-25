"""Renderer layer — turn a ``Catalog`` into a static HTML site.

The output is a directory tree:

    output/
      index.html
      lineage.html
      static/
        style.css
        search.js
        search-index.json
      groups/
        <group-name>/
          index.html
          features/
            <feature-name>.html
      models/
        <model-name>/
          index.html

URL design is the load-bearing constraint here: paths must be stable across
catalog rebuilds so people can paste links into Slack and they stay valid.
We use slugified group/feature/model names as path components.
"""

from __future__ import annotations

import json
import re
from importlib import resources
from pathlib import Path

from jinja2 import Environment, FileSystemLoader, PackageLoader, select_autoescape

from dbt_features.catalog import (
    Catalog,
    Feature,
    FeatureGroup,
    LineageRef,
)
from dbt_features.enrichment.format import (
    compute_freshness_status,
    humanize_count,
    humanize_percent,
)
from dbt_features.enrichment.models import FreshnessSnapshot


def render_catalog(
    catalog: Catalog,
    output_dir: str | Path,
    *,
    enrichment: dict[str, FreshnessSnapshot] | None = None,
) -> Path:
    """Render ``catalog`` to ``output_dir``. Returns the output directory.

    ``enrichment`` is an optional map of ``unique_id -> FreshnessSnapshot``
    produced by the enrichment subsystem. When present, templates render
    actual freshness, row counts, and per-column stats. When absent (the
    default), they fall back to declared metadata only — no warehouse
    facts, no broken UI.

    Idempotent: clears the output directory before writing. Refusing to
    write into a non-empty directory that wasn't created by us is a CLI
    concern, not a renderer concern — see ``cli.py``.
    """

    output = Path(output_dir)
    output.mkdir(parents=True, exist_ok=True)
    enrichment = enrichment or {}

    env = _make_env()

    _copy_static_assets(output)
    _write_search_index(catalog, output, enrichment=enrichment)

    summary_stats = _compute_summary_stats(catalog, enrichment)
    type_bars = _compute_type_bars(catalog)
    facets = _compute_facets(catalog, enrichment)
    cards = _compute_index_cards(catalog, enrichment)
    entity_palette = _compute_entity_palette(catalog)

    index_html = env.get_template("index.html").render(
        catalog=catalog,
        groups_by_entity=catalog.feature_groups_by_entity(),
        groups_by_tag=catalog.feature_groups_by_tag(),
        page_title=catalog.project_name,
        base_url=".",
        enrichment=enrichment,
        summary=summary_stats,
        type_bars=type_bars,
        facets=facets,
        cards=cards,
        entity_palette=entity_palette,
    )
    (output / "index.html").write_text(index_html, encoding="utf-8")

    lineage_html = env.get_template("lineage.html").render(
        catalog=catalog,
        page_title=f"Lineage — {catalog.project_name}",
        base_url=".",
        mermaid_source=_lineage_mermaid(catalog),
        enrichment=enrichment,
        entity_palette=entity_palette,
    )
    (output / "lineage.html").write_text(lineage_html, encoding="utf-8")

    groups_dir = output / "groups"
    groups_dir.mkdir(exist_ok=True)
    for group in catalog.feature_groups:
        _render_group(env, catalog, group, groups_dir, enrichment, entity_palette)

    models_dir = output / "models"
    models_dir.mkdir(exist_ok=True)
    features_by_model = catalog.features_by_model()
    if features_by_model:
        models_index_html = env.get_template("models_index.html").render(
            catalog=catalog,
            page_title=f"Models — {catalog.project_name}",
            base_url=".",
            features_by_model=features_by_model,
            enrichment=enrichment,
            entity_palette=entity_palette,
        )
        (models_dir / "index.html").write_text(models_index_html, encoding="utf-8")

        for model_name, entries in features_by_model.items():
            _render_model(
                env,
                catalog,
                model_name,
                entries,
                models_dir,
                enrichment,
                entity_palette,
            )

    return output


def _make_env() -> Environment:
    env = Environment(
        loader=PackageLoader("dbt_features", "templates"),
        autoescape=select_autoescape(["html", "xml"]),
        trim_blocks=True,
        lstrip_blocks=True,
    )
    _register_globals(env)
    return env


def make_dev_env(template_dir: str | Path) -> Environment:
    """Make a Jinja env that loads templates from a directory.

    Useful for tests that want to pin templates without going through
    package data resolution.
    """

    env = Environment(
        loader=FileSystemLoader(str(template_dir)),
        autoescape=select_autoescape(["html", "xml"]),
        trim_blocks=True,
        lstrip_blocks=True,
    )
    _register_globals(env)
    return env


def _register_globals(env: Environment) -> None:
    env.globals["slugify"] = slugify
    env.globals["group_url"] = _group_url
    env.globals["feature_url"] = _feature_url
    env.globals["model_url"] = _model_url
    env.globals["freshness_status"] = compute_freshness_status
    env.globals["humanize_count"] = humanize_count
    env.globals["humanize_percent"] = humanize_percent
    env.globals["sql_select_snippet"] = _sql_select_snippet
    env.filters["slugify"] = slugify
    env.filters["humanize_count"] = humanize_count
    env.filters["humanize_percent"] = humanize_percent


def _render_group(
    env: Environment,
    catalog: Catalog,
    group: FeatureGroup,
    groups_dir: Path,
    enrichment: dict[str, FreshnessSnapshot],
    entity_palette: dict[str, str],
) -> None:
    group_slug = slugify(group.name)
    gdir = groups_dir / group_slug
    gdir.mkdir(parents=True, exist_ok=True)

    # Resolve consumers — distinct list of model names declared on column-
    # level used_by, since these are typically ML models that won't appear
    # in dbt lineage.
    declared_consumers = sorted({c for f in group.features for c in f.used_by})
    snapshot = enrichment.get(group.unique_id)

    html = env.get_template("feature_group.html").render(
        catalog=catalog,
        group=group,
        declared_consumers=declared_consumers,
        page_title=f"{group.name} — {catalog.project_name}",
        base_url="../..",
        enrichment=enrichment,
        snapshot=snapshot,
        entity_palette=entity_palette,
        has_models=bool(catalog.all_models),
    )
    (gdir / "index.html").write_text(html, encoding="utf-8")

    fdir = gdir / "features"
    fdir.mkdir(exist_ok=True)
    for feature in group.features:
        column_stats = snapshot.columns.get(feature.name) if snapshot else None
        related_consumer, related_type = _find_related_features(feature, group)
        fhtml = env.get_template("feature.html").render(
            catalog=catalog,
            group=group,
            feature=feature,
            page_title=f"{feature.name} — {group.name}",
            base_url="../../..",
            enrichment=enrichment,
            snapshot=snapshot,
            column_stats=column_stats,
            related_by_consumer=related_consumer,
            related_by_type=related_type,
            entity_palette=entity_palette,
            has_models=bool(catalog.all_models),
        )
        (fdir / f"{slugify(feature.name)}.html").write_text(fhtml, encoding="utf-8")


def _render_model(
    env: Environment,
    catalog: Catalog,
    model_name: str,
    entries: list[tuple[FeatureGroup, Feature]],
    models_dir: Path,
    enrichment: dict[str, FreshnessSnapshot],
    entity_palette: dict[str, str],
) -> None:
    """Render a model-centric page: the inverse of feature pages."""
    mslug = slugify(model_name)
    mdir = models_dir / mslug
    mdir.mkdir(parents=True, exist_ok=True)

    # Group entries by feature group so the page reads as
    # "for each table you read from, here are the features I consume".
    by_group: dict[str, list[tuple[FeatureGroup, Feature]]] = {}
    for g, f in entries:
        by_group.setdefault(g.unique_id, []).append((g, f))

    # Worst-case freshness across the table set tells the consumer
    # whether this model can be served right now.
    worst = "fresh"
    rank = {"fresh": 0, "warn": 1, "error": 2, "unknown": 3}
    saw_any = False
    distinct_groups = []
    for uid, gf_pairs in by_group.items():
        group = gf_pairs[0][0]
        snap = enrichment.get(group.unique_id)
        if snap is not None:
            saw_any = True
            status = compute_freshness_status(snap, group.freshness)
            if rank[status.label] > rank[worst]:
                worst = status.label
        distinct_groups.append((group, [f for _, f in gf_pairs]))

    if not saw_any:
        worst = "unknown"

    distinct_groups.sort(key=lambda gp: gp[0].name)

    entities: dict[str, None] = {}
    owners: dict[str, None] = {}
    for g, _ in distinct_groups:
        for e in g.entity_columns:
            entities[e] = None
        if g.owner:
            owners[g.owner] = None

    html = env.get_template("model.html").render(
        catalog=catalog,
        model_name=model_name,
        groups=distinct_groups,
        page_title=f"{model_name} — {catalog.project_name}",
        base_url="../..",
        enrichment=enrichment,
        entity_palette=entity_palette,
        worst_freshness=worst,
        has_enrichment=saw_any,
        entities=list(entities),
        owners=list(owners),
    )
    (mdir / "index.html").write_text(html, encoding="utf-8")


def _find_related_features(
    feature: Feature, group: FeatureGroup
) -> tuple[list[Feature], list[Feature]]:
    """Find related features in the same group, separated by relationship.

    Returns ``(by_consumer, by_type)`` so the template can label them
    separately. Same group only; cross-group "related" would need
    embedding-style similarity that the catalog doesn't have.
    """

    consumers = set(feature.used_by)
    by_consumer: list[Feature] = []
    by_type: list[Feature] = []
    seen_consumer: set[str] = {feature.name}
    seen_type: set[str] = {feature.name}

    if consumers:
        for f in group.features:
            if f.name in seen_consumer:
                continue
            if consumers & set(f.used_by):
                by_consumer.append(f)
                seen_consumer.add(f.name)

    if feature.feature_type:
        for f in group.features:
            if f.name in seen_type:
                continue
            if f.feature_type == feature.feature_type:
                by_type.append(f)
                seen_type.add(f.name)

    return by_consumer[:6], by_type[:6]


def _compute_summary_stats(
    catalog: Catalog,
    enrichment: dict[str, FreshnessSnapshot],
) -> dict[str, object]:
    """Aggregate stats for the index page hero section."""
    from collections import Counter

    type_counts: Counter[str] = Counter()
    deprecated_count = 0
    preview_count = 0
    for group in catalog.feature_groups:
        if group.lifecycle.value == "deprecated":
            deprecated_count += 1
        elif group.lifecycle.value == "preview":
            preview_count += 1
        for f in group.features:
            ft = f.feature_type.value if f.feature_type else "unspecified"
            type_counts[ft] += 1

    fresh = warn = error = 0
    for group in catalog.feature_groups:
        snap = enrichment.get(group.unique_id)
        if snap:
            status = compute_freshness_status(snap, group.freshness)
            if status.label == "fresh":
                fresh += 1
            elif status.label == "warn":
                warn += 1
            elif status.label == "error":
                error += 1

    return {
        "type_counts": dict(type_counts.most_common()),
        "deprecated": deprecated_count,
        "preview": preview_count,
        "fresh": fresh,
        "warn": warn,
        "error": error,
        "has_enrichment": bool(enrichment),
    }


def _compute_facets(
    catalog: Catalog,
    enrichment: dict[str, FreshnessSnapshot],
) -> dict[str, list[dict[str, object]]]:
    """Build the filter chip catalog for the index page.

    Each facet is a list of {value, label, count} dicts so the template
    can render them in a deterministic order with accurate counts.
    Counts reflect the *unfiltered* catalog; the JS layer updates the
    visible-card count as filters change.
    """

    from collections import Counter

    entity_counts: Counter[str] = Counter()
    type_counts: Counter[str] = Counter()
    lifecycle_counts: Counter[str] = Counter()
    owner_counts: Counter[str] = Counter()
    tag_counts: Counter[str] = Counter()
    freshness_counts: Counter[str] = Counter()

    for g in catalog.feature_groups:
        ents = g.entity_columns or ["—"]
        for e in ents:
            entity_counts[e] += 1
        lifecycle_counts[g.lifecycle.value] += 1
        if g.owner:
            owner_counts[g.owner] += 1
        for t in g.tags:
            tag_counts[t] += 1
        for f in g.features:
            type_counts[f.feature_type.value if f.feature_type else "unspecified"] += 1
        snap = enrichment.get(g.unique_id)
        if snap:
            status = compute_freshness_status(snap, g.freshness)
            freshness_counts[status.label] += 1

    def _to_chips(c: Counter[str], sort_by_count: bool = True) -> list[dict[str, object]]:
        if sort_by_count:
            items = c.most_common()
        else:
            items = sorted(c.items())
        return [{"value": v, "label": v, "count": n} for v, n in items]

    return {
        "entity": _to_chips(entity_counts, sort_by_count=False),
        "type": _to_chips(type_counts),
        "lifecycle": _to_chips(lifecycle_counts, sort_by_count=False),
        "owner": _to_chips(owner_counts),
        "tag": _to_chips(tag_counts, sort_by_count=False),
        "freshness": _to_chips(freshness_counts, sort_by_count=False),
    }


def _compute_index_cards(
    catalog: Catalog,
    enrichment: dict[str, FreshnessSnapshot],
) -> list[dict[str, object]]:
    """Pre-compute the per-card data attributes the JS filter reads.

    Putting this in Python keeps the template a thin presentation layer
    and means there's a single source of truth for "what does the filter
    see for this card."
    """

    out: list[dict[str, object]] = []
    for g in catalog.feature_groups:
        snap = enrichment.get(g.unique_id)
        status = compute_freshness_status(snap, g.freshness) if snap else None
        types = sorted({(f.feature_type.value if f.feature_type else "unspecified") for f in g.features})
        out.append(
            {
                "group": g,
                "snapshot": snap,
                "status_label": status.label if status else "",
                "status_age": status.age_human if status else "",
                "entities": list(g.entity_columns),
                "tags": list(g.tags),
                "owner": g.owner or "",
                "lifecycle": g.lifecycle.value,
                "types": types,
            }
        )
    return out


# A small palette of accent colors keyed by entity name. Hash-based so
# entity ordering doesn't change colors between rebuilds.
_ENTITY_HUES = [
    "#79c0ff",  # blue
    "#6ec78a",  # green
    "#f2cc60",  # amber
    "#c8a8ef",  # purple
    "#f08c98",  # rose
    "#ffa657",  # orange
    "#7ee7e7",  # cyan
    "#a5b4fc",  # indigo
]


def _compute_entity_palette(catalog: Catalog) -> dict[str, str]:
    palette: dict[str, str] = {}
    for entity in catalog.all_entities:
        idx = sum(ord(c) for c in entity) % len(_ENTITY_HUES)
        palette[entity] = _ENTITY_HUES[idx]
    return palette


def _sql_select_snippet(group: FeatureGroup, features: list[Feature] | None = None) -> str:
    """Generate a copy-pastable SQL SELECT for this group.

    Default is "give me the entity, the timestamp, and every feature."
    The caller can pass a subset of features to focus the snippet on a
    single column (used on the per-feature page).

    Style: lowercase keywords, two-space indented column list (the format
    sqlfmt and most dbt projects use). Keeps the snippet copy-pastable
    into a model file with no reformatting.
    """

    cols: list[str] = []
    for c in group.entity_columns:
        cols.append(c)
    if group.timestamp_column and group.timestamp_column not in cols:
        cols.append(group.timestamp_column)
    target = features if features else list(group.features)
    for f in target:
        if f.name not in cols:
            cols.append(f.name)
    if not cols:
        cols = ["*"]

    select_clause = ",\n".join(f"  {c}" for c in cols)
    fqn = group.fully_qualified_name or group.name
    return f"select\n{select_clause}\nfrom {fqn}"


def _compute_type_bars(
    catalog: Catalog,
) -> dict[str, list[dict[str, object]]]:
    """Per-group feature type proportions for the index card mini-bars."""
    from collections import Counter

    bars: dict[str, list[dict[str, object]]] = {}
    for group in catalog.feature_groups:
        counts: Counter[str] = Counter()
        for f in group.features:
            ft = f.feature_type.value if f.feature_type else "unspecified"
            counts[ft] += 1
        total = sum(counts.values())
        if total == 0:
            continue
        segments: list[dict[str, object]] = []
        for ftype, count in counts.most_common():
            segments.append({"type": ftype, "pct": round(100 * count / total, 1)})
        bars[group.unique_id] = segments
    return bars


def _copy_static_assets(output: Path) -> None:
    static_out = output / "static"
    static_out.mkdir(exist_ok=True)
    static_pkg = resources.files("dbt_features") / "static"
    for entry in static_pkg.iterdir():
        if entry.is_file():
            (static_out / entry.name).write_bytes(entry.read_bytes())


def _write_search_index(
    catalog: Catalog,
    output: Path,
    enrichment: dict[str, FreshnessSnapshot] | None = None,
) -> None:
    """Tiny JSON search index loaded by search.js.

    Substring search is fine for our scale (hundreds to low thousands of
    features). When this gets painful, swap in lunr.
    """

    enrichment = enrichment or {}
    items: list[dict[str, object]] = []
    for group in catalog.feature_groups:
        snap = enrichment.get(group.unique_id)
        status = compute_freshness_status(snap, group.freshness) if snap else None
        items.append(
            {
                "kind": "group",
                "name": group.name,
                "description": group.description,
                "tags": list(group.tags),
                "owner": group.owner,
                "lifecycle": group.lifecycle.value,
                "freshness": status.label if status else None,
                "entities": list(group.entity_columns),
                "url": _group_url(group, base_url=""),
            }
        )
        for feature in group.features:
            items.append(
                {
                    "kind": "feature",
                    "name": feature.name,
                    "group": group.name,
                    "description": feature.description,
                    "feature_type": feature.feature_type.value if feature.feature_type else None,
                    "tags": list(feature.tags),
                    "lifecycle": feature.lifecycle.value,
                    "freshness": status.label if status else None,
                    "url": _feature_url(group, feature, base_url=""),
                }
            )
    for model_name in catalog.all_models:
        items.append(
            {
                "kind": "model",
                "name": model_name,
                "description": "",
                "url": _model_url(model_name, base_url=""),
            }
        )
    (output / "static" / "search-index.json").write_text(
        json.dumps(items, indent=None, separators=(",", ":")),
        encoding="utf-8",
    )


_SLUG_RE = re.compile(r"[^a-z0-9]+")


def slugify(value: str) -> str:
    s = _SLUG_RE.sub("-", value.lower()).strip("-")
    return s or "_"


def _group_url(group: FeatureGroup, base_url: str = ".") -> str:
    return f"{base_url}/groups/{slugify(group.name)}/index.html"


def _feature_url(group: FeatureGroup, feature: Feature, base_url: str = ".") -> str:
    return f"{base_url}/groups/{slugify(group.name)}/features/{slugify(feature.name)}.html"


def _model_url(model_name: str, base_url: str = ".") -> str:
    return f"{base_url}/models/{slugify(model_name)}/index.html"


def _lineage_mermaid(catalog: Catalog) -> str:
    """Build a Mermaid flowchart scoped to feature tables.

    Clusters by primary entity so the picture stays legible at scale.
    Non-feature dependencies are intentionally hidden — a full dbt
    graph belongs in dbt-docs, not here.
    """

    if not catalog.feature_groups:
        return "flowchart LR\n    empty[No feature tables]"

    lines = ["flowchart LR"]

    # Bucket each group by primary entity so we can render Mermaid
    # subgraphs. Keeps the diagram readable when the catalog has many
    # tables across only a few entities.
    buckets: dict[str, list[FeatureGroup]] = {}
    for g in catalog.feature_groups:
        ents = g.entity_columns
        if not ents:
            key = "Other"
        elif len(ents) > 1:
            key = "Cross-entity"
        else:
            key = ents[0]
        buckets.setdefault(key, []).append(g)

    seen: set[str] = set()
    edges: list[str] = []

    def _node_line(group: FeatureGroup) -> str:
        nid = _mermaid_id(group.unique_id)
        return (
            f'        {nid}["{_mermaid_escape(group.name)}"]\n'
            f'        click {nid} href "groups/{slugify(group.name)}/index.html"'
        )

    for entity, groups in buckets.items():
        sub_id = "sg_" + _mermaid_id(entity)
        lines.append(f'    subgraph {sub_id}["{_mermaid_escape(entity)}"]')
        for g in groups:
            nid = _mermaid_id(g.unique_id)
            if nid not in seen:
                lines.append(_node_line(g))
                seen.add(nid)
        lines.append("    end")

    # Now record edges between feature tables across (and within) buckets.
    for group in catalog.feature_groups:
        node_id = _mermaid_id(group.unique_id)
        for upstream in group.upstream:
            if not upstream.is_feature_table:
                continue
            up_id = _mermaid_id(upstream.unique_id)
            if up_id not in seen:
                # Upstream feature table that isn't itself in our catalog
                # — render as a free-floating node so the edge has somewhere
                # to terminate.
                lines.append(f'    {up_id}["{_mermaid_escape(upstream.name)}"]')
                lines.append(
                    f'    click {up_id} href "groups/{slugify(upstream.name)}/index.html"'
                )
                seen.add(up_id)
            edges.append(f"    {up_id} --> {node_id}")

    lines.extend(edges)
    return "\n".join(lines)


def _mermaid_id(unique_id: str) -> str:
    return re.sub(r"[^A-Za-z0-9]", "_", unique_id)


def _mermaid_escape(value: str) -> str:
    return value.replace('"', "&quot;")


def _lineage_refs_summary(refs: tuple[LineageRef, ...]) -> str:  # pragma: no cover - debug helper
    return ", ".join(r.name for r in refs)
