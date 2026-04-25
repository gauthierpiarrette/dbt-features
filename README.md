# dbt-features

A **feature catalog** for ML teams whose features live as dbt models.

Get the discovery UX of Tecton or Chalk's feature-catalog page — without
adopting the platform — by layering ML-feature-aware metadata on top of your
existing dbt project.

> Not a data catalog. A feature catalog. The distinction is load-bearing.

## What it does

- Reads your dbt project's `manifest.json` (and optionally `catalog.json`).
- Picks up models you've marked with `meta.feature_catalog.is_feature_table: true`.
- Generates a static HTML site with three views: feature-group index, feature-group
  detail, and individual feature detail — plus client-side search and a
  feature-table-only lineage graph.
- Validates your `feature_catalog` metadata against a versioned, opinionated
  schema. Bad metadata = clear error message.

It is intentionally:

- **Read-only.** Your dbt project is the source of truth.
- **Static.** No backend, no auth, no infra to babysit.
- **Single-purpose.** Not a feature store. Not drift detection. Not a
  general-purpose data catalog.

See [`specs.md`](./specs.md) for the full motivation.

## Install

```bash
pip install dbt-features
```

Requires Python 3.10+. The tool reads dbt artifacts as JSON — it does not
import `dbt-core`, so it won't fight your dbt version.

## See it in 10 seconds

```bash
dbt-features demo
```

Builds a catalog from bundled sample data, serves it on a free port, and
opens your browser. No dbt project required, nothing written to your
codebase — output goes to a temp directory that's cleaned up on exit.

For headless use:

```bash
dbt-features demo --no-open --port 9000
```

## Quickstart

In your dbt project directory:

```bash
# 1. Generate manifest.json (and optionally catalog.json for column types)
dbt parse
dbt docs generate         # optional, for warehouse column types

# 2. Mark a model as a feature table — see "Metadata schema" below
$EDITOR models/features/customer_features_daily.yml

# 3. Validate before building
dbt-features validate

# 4. Build the catalog
dbt-features build --output ./target/feature-catalog

# 5. Preview locally
dbt-features serve --output ./target/feature-catalog
# → http://127.0.0.1:8080
```

For CI: run `dbt-features build` after `dbt parse`, then publish the
output directory to GitHub Pages, S3, Netlify, or anywhere that serves
static files. See [`docs/example-github-action.yml`](./docs/example-github-action.yml)
for a copy-paste GitHub Action that builds the catalog on every merge and
publishes it to GitHub Pages.

## Metadata schema

Mark dbt models as feature tables under `meta.feature_catalog`:

```yaml
version: 2

models:
  - name: customer_features_daily
    description: Daily booking-related features per customer
    meta:
      feature_catalog:
        is_feature_table: true
        entity: customer_id
        grain: [feature_date, customer_id]
        timestamp_column: feature_date
        freshness:
          warn_after: { count: 36, period: hour }
          error_after: { count: 48, period: hour }
        owner: growth-team@company.com
        tags: [customer, daily]

    columns:
      - name: orders_count_7d
        description: Count of orders in the trailing 7 days
        meta:
          feature_catalog:
            is_feature: true
            feature_type: numeric
            null_behavior: zero
            used_by: [churn_model_v2, ltv_model_v3]
```

### Model-level fields (`meta.feature_catalog`)

| Field                | Type            | Required | Notes                                                          |
|----------------------|-----------------|----------|----------------------------------------------------------------|
| `is_feature_table`   | bool            | yes      | Must be `true` to be picked up.                                |
| `entity`             | str \| str[]    | rec.     | Entity column(s) — the "who/what" the features describe.       |
| `grain`              | str[]           | rec.     | Grain columns. Typically entity + timestamp.                   |
| `timestamp_column`   | str             | rec.     | Anchor for time-relative reasoning.                            |
| `freshness`          | object          | no       | `warn_after` / `error_after` (same shape as dbt source freshness). |
| `owner`              | str             | no       | Email or team name.                                            |
| `tags`               | str[]           | no       | For grouping / search.                                         |
| `description`        | str             | no       | Falls back to dbt model description.                           |
| `definition_version` | int             | no       | Bumped when the table's semantic definition changes. Defaults to `1`. |
| `lifecycle`          | enum            | no       | `active` (default), `preview`, `deprecated`.                   |
| `replacement`        | str             | no       | Name of the replacement table — most useful with `lifecycle: deprecated`. |
| `version`            | str             | no       | Metadata schema version. Defaults to `"0.1"`. Reserved for future use. |

### Column-level fields (`columns[].meta.feature_catalog`)

| Field                | Type     | Required | Notes                                                                |
|----------------------|----------|----------|----------------------------------------------------------------------|
| `is_feature`         | bool     | yes      | Must be `true` to be picked up. Lets you keep keys/timestamps clean. |
| `feature_type`       | enum     | rec.     | `numeric`, `categorical`, `boolean`, `embedding`, `timestamp`, `text`, `identifier`. |
| `null_behavior`      | enum     | no       | `zero`, `mean`, `propagate`, `error`, `ignore`. Documentation only.  |
| `used_by`            | str[]    | no       | Models / systems consuming this feature (manual list for v0.1).      |
| `description`        | str      | no       | Falls back to dbt column description.                                |
| `definition_version` | int      | no       | Bumped when the feature's semantic definition changes. Defaults to `1`. |
| `lifecycle`          | enum     | no       | `active` (default), `preview`, `deprecated`.                         |
| `replacement`        | str      | no       | Name of the replacement feature — most useful with `lifecycle: deprecated`. |

The schema is validated by Pydantic. Unknown fields are rejected — typos
become errors instead of silently dropped data.

## What's deliberately out of scope (v0.1)

These are real feature requests; the answer is "no, with reason":

- Online serving / runtime feature retrieval (use a feature store).
- Feature computation (dbt does this).
- Drift / monitoring / alerting (orthogonal tools exist).
- Multi-project federation.
- Authentication / access control (it's a static site — host it however you want).
- A web UI for editing metadata (edit your `schema.yml`).
- A hosted SaaS.

Warehouse-backed freshness checks (querying `MAX(timestamp_column)`) are
planned for v0.2. The `freshness` SLA fields render as configuration today.

## Companion dbt package

For compile-time validation (catch metadata typos in `dbt parse` rather
than at catalog build time), install the companion dbt package — see
[`dbt_package/`](./dbt_package). It ships a YAML schema and a `validate`
macro you can wire into your project's tests.

## Development

```bash
git clone https://github.com/gauthierpiarrette/dbt-features
cd dbt-features
python -m venv .venv && source .venv/bin/activate
pip install -e ".[dev]"
pytest
```

## License

Apache 2.0.
