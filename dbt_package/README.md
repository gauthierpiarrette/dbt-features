# feature_catalog (dbt package)

The companion dbt package for [`dbt-features`][py]. Ships a small typo
check that runs inside `dbt run-operation`, so misspelled metadata fields
are flagged at `dbt parse` time instead of silently dropping models from
the catalog.

## What this package does (and what it doesn't)

**This package** is intentionally narrow. It scans `feature_catalog` blocks
in your project and flags any field name that isn't recognized — e.g. it
will catch:

```yaml
meta:
  feature_catalog:
    is_featur_table: true   # typo — silently ignored without this check
```

That's the one failure mode the Python tool can't catch: a typo'd
`is_feature_table` means the model never enters the catalog, so there's
nothing for the Python tool to validate.

**The Python tool** (`dbt-features validate`) handles everything else —
enum values, freshness shape, integer ranges, `extra="forbid"` on every
nested field, etc. It uses Pydantic and is the canonical validator. Keeping
the deep rules in one place prevents this dbt package and the Python tool
from drifting apart over time.

If you need full validation, run:

```bash
pip install dbt-features
dbt-features validate
```

## Install

In your `packages.yml`:

```yaml
packages:
  - git: "https://github.com/gauthierpiarrette/dbt-features.git"
    subdirectory: "dbt_package"
    revision: "v0.2.0"
```

Then `dbt deps`.

## Usage

```bash
# Run the typo check. Fails dbt operation if any feature_catalog block
# contains an unknown field name.
dbt run-operation feature_catalog__validate
```

Wire it into your CI in front of `dbt parse` for fast feedback.
For full schema validation, run `dbt-features validate` after `dbt parse`.

[py]: https://github.com/gauthierpiarrette/dbt-features
