"""Warehouse adapter protocol + dispatch.

Each warehouse gets its own adapter module (``duckdb.py``, ``postgres.py``,
etc.). They all implement the same minimal protocol: connect, fetch one
snapshot per feature group, close. The engine doesn't know which warehouse
it's talking to.

We deliberately don't depend on dbt-core's adapter classes. Their API
shifts between minor versions and would force us into a version-pinning
fight users don't want. Native drivers are stable enough across years.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Protocol, runtime_checkable

from dbt_features.enrichment.exceptions import EnrichmentError

if TYPE_CHECKING:
    from dbt_features.catalog import FeatureGroup
    from dbt_features.enrichment.models import FreshnessSnapshot


@runtime_checkable
class WarehouseAdapter(Protocol):
    """The contract every warehouse adapter implements.

    ``fetch_group_stats`` is allowed (encouraged) to capture per-group
    failures on the returned snapshot rather than raising — a missing
    table for one feature group shouldn't tank the whole build. Adapter
    setup errors (bad creds, missing driver) raise ``EnrichmentError``
    immediately so the user sees the problem.
    """

    def fetch_group_stats(self, group: FeatureGroup) -> FreshnessSnapshot: ...
    def close(self) -> None: ...


def get_adapter(profile: dict[str, object]) -> WarehouseAdapter:
    """Construct the right adapter for ``profile['type']``.

    Imports the warehouse driver lazily so users only pay for what they
    use. Each branch raises a clear, actionable error if the matching
    extra hasn't been installed.
    """

    db_type = profile.get("type")
    if db_type == "duckdb":
        from dbt_features.enrichment.adapters.duckdb import DuckDBAdapter

        return DuckDBAdapter(profile)
    if db_type == "postgres":
        from dbt_features.enrichment.adapters.postgres import PostgresAdapter

        return PostgresAdapter(profile)
    if db_type == "redshift":
        from dbt_features.enrichment.adapters.redshift import RedshiftAdapter

        return RedshiftAdapter(profile)
    if db_type == "snowflake":
        from dbt_features.enrichment.adapters.snowflake import SnowflakeAdapter

        return SnowflakeAdapter(profile)
    if db_type == "bigquery":
        from dbt_features.enrichment.adapters.bigquery import BigQueryAdapter

        return BigQueryAdapter(profile)

    raise EnrichmentError(
        f"Unsupported warehouse type: {db_type!r}. "
        "Supported: duckdb, postgres, redshift, snowflake, bigquery."
    )
