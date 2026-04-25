"""Optional warehouse-enrichment subsystem.

Public surface: ``enrich_catalog``, ``EnrichmentCache``, ``EnrichmentError``,
``FreshnessSnapshot``, ``ColumnStats``. Importing this package does not
import any warehouse driver — drivers are loaded lazily by ``get_adapter``
the first time a connection is opened.
"""

from dbt_features.enrichment.cache import EnrichmentCache
from dbt_features.enrichment.engine import enrich_catalog
from dbt_features.enrichment.exceptions import EnrichmentError
from dbt_features.enrichment.models import (
    ColumnStats,
    FreshnessSnapshot,
    FreshnessStatus,
)

__all__ = [
    "ColumnStats",
    "EnrichmentCache",
    "EnrichmentError",
    "FreshnessSnapshot",
    "FreshnessStatus",
    "enrich_catalog",
]
