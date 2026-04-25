"""dbt-features — a feature catalog for ML teams whose features live as dbt models."""

from dbt_features.catalog import Catalog, Feature, FeatureGroup
from dbt_features.parser import parse_project
from dbt_features.renderer import render_catalog

__version__ = "0.2.0"

__all__ = [
    "Catalog",
    "Feature",
    "FeatureGroup",
    "__version__",
    "parse_project",
    "render_catalog",
]
