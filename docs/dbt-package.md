# Companion dbt package

For compile-time validation - catch metadata typos in `dbt parse` rather
than at catalog build time - install the companion dbt package shipped at
[`dbt_package/`](../dbt_package).

It ships a `validate` macro you can wire into your project's tests, so
malformed `feature_catalog` blocks fail the dbt build instead of being
discovered later when running `dbt-features build`.

See [`dbt_package/README.md`](../dbt_package/README.md) for installation
and configuration.
