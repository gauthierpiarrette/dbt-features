from __future__ import annotations

import json
from pathlib import Path

import pytest

from dbt_features.parser import parse_project
from dbt_features.renderer import render_catalog, slugify


@pytest.fixture()
def built_catalog(tmp_path: Path, project_dir: Path) -> Path:
    cat = parse_project(project_dir)
    out = tmp_path / "site"
    render_catalog(cat, out)
    return out


def test_index_html_written(built_catalog: Path) -> None:
    index = built_catalog / "index.html"
    assert index.exists()
    content = index.read_text()
    assert "customer_features_daily" in content
    assert "jaffle_features" in content


def test_lineage_page_written(built_catalog: Path) -> None:
    lineage = built_catalog / "lineage.html"
    assert lineage.exists()
    content = lineage.read_text()
    assert "flowchart" in content
    assert "customer_features_daily" in content
    assert "customer_features_lifetime" in content


def test_per_group_pages_written(built_catalog: Path) -> None:
    g1 = built_catalog / "groups" / "customer-features-daily" / "index.html"
    g2 = built_catalog / "groups" / "customer-features-lifetime" / "index.html"
    assert g1.exists()
    assert g2.exists()
    content = g1.read_text()
    assert "orders_count_7d" in content
    assert "growth-team@jaffle.com" in content
    assert "feature_date" in content  # appears as timestamp column


def test_per_feature_pages_written(built_catalog: Path) -> None:
    f = built_catalog / "groups" / "customer-features-daily" / "features" / "orders-count-7d.html"
    assert f.exists()
    content = f.read_text()
    assert "orders_count_7d" in content
    assert "churn_model_v2" in content
    assert "ltv_model_v3" in content


def test_search_index_written(built_catalog: Path) -> None:
    idx_path = built_catalog / "static" / "search-index.json"
    assert idx_path.exists()
    items = json.loads(idx_path.read_text())
    assert any(i["kind"] == "group" and i["name"] == "customer_features_daily" for i in items)
    assert any(i["kind"] == "feature" and i["name"] == "orders_count_7d" for i in items)


def test_static_assets_copied(built_catalog: Path) -> None:
    assert (built_catalog / "static" / "style.css").exists()
    assert (built_catalog / "static" / "search.js").exists()
    assert (built_catalog / "static" / "theme.js").exists()


def test_theme_toggle_present_on_pages(built_catalog: Path) -> None:
    """Toggle button must be on every page, with pre-paint init script that
    sets data-theme BEFORE the stylesheet so dark users don't flash light."""

    pages = [
        built_catalog / "index.html",
        built_catalog / "lineage.html",
        built_catalog / "groups" / "customer-features-daily" / "index.html",
        built_catalog / "groups" / "customer-features-daily" / "features" / "orders-count-7d.html",
    ]
    for p in pages:
        content = p.read_text()
        assert 'id="theme-toggle"' in content, f"toggle missing on {p}"
        # Pre-paint init must come before the stylesheet link to avoid FOUC.
        init_idx = content.index("dbt-features-theme")
        css_idx = content.index("style.css")
        assert init_idx < css_idx, f"theme init must run before stylesheet on {p}"
        assert "theme.js" in content


def test_breadcrumb_links_use_relative_paths(built_catalog: Path) -> None:
    f = built_catalog / "groups" / "customer-features-daily" / "features" / "orders-count-7d.html"
    content = f.read_text()
    # Feature pages need to walk back up four levels to reach root
    assert "../../../../index.html" in content


def test_slugify_handles_underscores_and_punctuation() -> None:
    assert slugify("customer_features_daily") == "customer-features-daily"
    assert slugify("Foo Bar 7-day") == "foo-bar-7-day"
    assert slugify("---weird---") == "weird"
    assert slugify("!!!") == "_"


def test_render_is_idempotent(tmp_path: Path, project_dir: Path) -> None:
    cat = parse_project(project_dir)
    out = tmp_path / "site"
    render_catalog(cat, out)
    first = (out / "index.html").read_text()
    render_catalog(cat, out)
    second = (out / "index.html").read_text()
    # Generated_at differs but the rest should be stable. Just check the page renders again without crashing.
    assert "customer_features_daily" in second
    assert first.split("Built ")[0] == second.split("Built ")[0]


def test_empty_catalog_renders(tmp_path: Path) -> None:
    """Edge case: a project with no feature tables should still produce output."""

    manifest = {"metadata": {"project_name": "empty"}, "nodes": {}, "sources": {}}
    target = tmp_path / "proj" / "target"
    target.mkdir(parents=True)
    (target / "manifest.json").write_text(json.dumps(manifest))

    cat = parse_project(tmp_path / "proj")
    out = tmp_path / "site"
    render_catalog(cat, out)
    content = (out / "index.html").read_text()
    assert "No feature tables found" in content
