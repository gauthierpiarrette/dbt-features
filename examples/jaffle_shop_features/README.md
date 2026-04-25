# Example: jaffle_shop_features

A toy dbt project that demonstrates `feature_catalog` metadata. It models
two feature tables — `customer_features_daily` and `customer_features_lifetime` —
and a non-feature `consumer_dashboard_metrics` that consumes them.

## Run it

This example uses [duckdb](https://duckdb.org/) so you can run it locally with
no warehouse setup.

```bash
pip install dbt-duckdb 'dbt-features[duckdb]'

cd examples/jaffle_shop_features
dbt deps                                   # no deps, but harmless
dbt seed
dbt run
dbt parse                                  # produces target/manifest.json

# Build the catalog without warehouse enrichment
dbt-features build --output ./catalog

# Or with warehouse enrichment — runs read-only queries against the
# DuckDB file dbt just wrote, fills in actual freshness + null % +
# cardinality:
dbt-features build \
    --connection jaffle_features \
    --profiles-dir . \
    --output ./catalog

dbt-features serve --output ./catalog
```

Open <http://127.0.0.1:8080> in a browser.
