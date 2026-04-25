from __future__ import annotations

import json
from pathlib import Path

import pytest

FIXTURES = Path(__file__).parent / "fixtures"


@pytest.fixture()
def fixtures_dir() -> Path:
    return FIXTURES


@pytest.fixture()
def manifest_path() -> Path:
    return FIXTURES / "manifest.json"


@pytest.fixture()
def catalog_path() -> Path:
    return FIXTURES / "catalog.json"


@pytest.fixture()
def manifest_dict(manifest_path: Path) -> dict:
    return json.loads(manifest_path.read_text())


@pytest.fixture()
def project_dir(tmp_path: Path, manifest_path: Path, catalog_path: Path) -> Path:
    """Build a fake dbt project with target/ artifacts in a tmp dir."""

    target = tmp_path / "target"
    target.mkdir()
    (target / "manifest.json").write_text(manifest_path.read_text())
    (target / "catalog.json").write_text(catalog_path.read_text())
    return tmp_path
