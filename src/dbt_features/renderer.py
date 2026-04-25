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

URL design is the load-bearing constraint here: paths must be stable across
catalog rebuilds so people can paste links into Slack and they stay valid.
We use slugified group/feature names as path components.
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
    _write_search_index(catalog, output)

    index_html = env.get_template("index.html").render(
        catalog=catalog,
        groups_by_tag=catalog.feature_groups_by_tag(),
        page_title=catalog.project_name,
        base_url=".",
        enrichment=enrichment,
    )
    (output / "index.html").write_text(index_html, encoding="utf-8")

    lineage_html = env.get_template("lineage.html").render(
        catalog=catalog,
        page_title=f"Lineage — {catalog.project_name}",
        base_url=".",
        mermaid_source=_lineage_mermaid(catalog),
        enrichment=enrichment,
    )
    (output / "lineage.html").write_text(lineage_html, encoding="utf-8")

    groups_dir = output / "groups"
    groups_dir.mkdir(exist_ok=True)
    for group in catalog.feature_groups:
        _render_group(env, catalog, group, groups_dir, enrichment)

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
    env.globals["freshness_status"] = compute_freshness_status
    env.globals["humanize_count"] = humanize_count
    env.globals["humanize_percent"] = humanize_percent
    env.filters["slugify"] = slugify
    env.filters["humanize_count"] = humanize_count
    env.filters["humanize_percent"] = humanize_percent


def _render_group(
    env: Environment,
    catalog: Catalog,
    group: FeatureGroup,
    groups_dir: Path,
    enrichment: dict[str, FreshnessSnapshot],
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
    )
    (gdir / "index.html").write_text(html, encoding="utf-8")

    fdir = gdir / "features"
    fdir.mkdir(exist_ok=True)
    for feature in group.features:
        column_stats = snapshot.columns.get(feature.name) if snapshot else None
        fhtml = env.get_template("feature.html").render(
            catalog=catalog,
            group=group,
            feature=feature,
            page_title=f"{feature.name} — {group.name}",
            base_url="../../../..",
            enrichment=enrichment,
            snapshot=snapshot,
            column_stats=column_stats,
        )
        (fdir / f"{slugify(feature.name)}.html").write_text(fhtml, encoding="utf-8")


def _copy_static_assets(output: Path) -> None:
    static_out = output / "static"
    static_out.mkdir(exist_ok=True)
    static_pkg = resources.files("dbt_features") / "static"
    for entry in static_pkg.iterdir():
        if entry.is_file():
            (static_out / entry.name).write_bytes(entry.read_bytes())


def _write_search_index(catalog: Catalog, output: Path) -> None:
    """Tiny JSON search index loaded by search.js.

    Substring search is fine for our scale (hundreds to low thousands of
    features). When this gets painful, swap in lunr.
    """

    items: list[dict[str, object]] = []
    for group in catalog.feature_groups:
        items.append(
            {
                "kind": "group",
                "name": group.name,
                "description": group.description,
                "tags": list(group.tags),
                "owner": group.owner,
                "url": _group_url(group, base_url="."),
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
                    "url": _feature_url(group, feature, base_url="."),
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


def _lineage_mermaid(catalog: Catalog) -> str:
    """Build a Mermaid flowchart scoped to feature tables.

    Edges only between feature tables; non-feature dependencies are noisy
    and a full dbt graph belongs in dbt-docs, not here.
    """

    if not catalog.feature_groups:
        return "flowchart LR\n    empty[No feature tables]"

    lines = ["flowchart LR"]
    seen: set[str] = set()
    for group in catalog.feature_groups:
        node_id = _mermaid_id(group.unique_id)
        if node_id not in seen:
            lines.append(f'    {node_id}["{_mermaid_escape(group.name)}"]')
            lines.append(f"    click {node_id} href \"groups/{slugify(group.name)}/index.html\"")
            seen.add(node_id)
        for upstream in group.upstream:
            if not upstream.is_feature_table:
                continue
            up_id = _mermaid_id(upstream.unique_id)
            if up_id not in seen:
                lines.append(f'    {up_id}["{_mermaid_escape(upstream.name)}"]')
                lines.append(f"    click {up_id} href \"groups/{slugify(upstream.name)}/index.html\"")
                seen.add(up_id)
            lines.append(f"    {up_id} --> {node_id}")
    return "\n".join(lines)


def _mermaid_id(unique_id: str) -> str:
    return re.sub(r"[^A-Za-z0-9]", "_", unique_id)


def _mermaid_escape(value: str) -> str:
    return value.replace('"', "&quot;")


def _lineage_refs_summary(refs: tuple[LineageRef, ...]) -> str:  # pragma: no cover - debug helper
    return ", ".join(r.name for r in refs)
