# feature_catalog (dbt package)

The companion dbt package for [`dbt-features`][py]. Ships compile-time
validation for the `feature_catalog` metadata schema so typos fail
`dbt parse` rather than silently disappearing or only being caught at catalog
build time.

## Install

In your `packages.yml`:

```yaml
packages:
  - git: "https://github.com/gauthierpiarrette/dbt-features.git"
    subdirectory: "dbt_package"
    revision: "v0.1.0"
```

Then `dbt deps`.

## Usage

```bash
# Run the metadata sanity check. Fails dbt operation if any model declares
# `is_feature_table: true` with malformed metadata.
dbt run-operation feature_catalog__validate
```

Wire it into your CI in front of `dbt parse` so issues are caught early.

[py]: https://github.com/gauthierpiarrette/dbt-features
