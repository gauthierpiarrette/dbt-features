"""Bundled sample data for `dbt-features demo`.

Ships as package data so the demo command works offline with zero setup.
Kept separate from tests/fixtures/ so tests don't accidentally couple to
demo content (or vice versa).
"""

from importlib import resources
from pathlib import Path


def demo_manifest_path() -> Path:
    """Path to the bundled demo ``manifest.json``."""

    return Path(str(resources.files(__name__) / "manifest.json"))
