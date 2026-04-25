# dbt-features

[![tests](https://github.com/gauthierpiarrette/dbt-features/actions/workflows/test.yml/badge.svg)](https://github.com/gauthierpiarrette/dbt-features/actions/workflows/test.yml)
[![demo](https://github.com/gauthierpiarrette/dbt-features/actions/workflows/deploy-demo.yml/badge.svg)](https://gauthierpiarrette.github.io/dbt-features/)
[![python](https://img.shields.io/badge/python-3.10%2B-blue.svg)](https://www.python.org/downloads/)
[![license](https://img.shields.io/badge/license-Apache%202.0-green.svg)](./LICENSE)

A **feature catalog** for ML teams whose features live as dbt models.

[**Live demo →**](https://gauthierpiarrette.github.io/dbt-features/) ·
[Install](#install) ·
[Schema](#metadata-schema) ·
[Warehouse enrichment](#warehouse-enrichment-optional)

> Not a data catalog. A feature catalog. The distinction is load-bearing.

![Feature group page with freshness, row count, null %, and per-feature cardinality](docs/screenshots/02-feature-group.png)

## What you get

- **Feature-aware metadata schema.** Entity, grain, timestamp column,
  freshness SLA, owner, lifecycle, version, ML consumers — first-class
  fields, validated by Pydantic.
- **Static HTML site** with feature-group index, feature-group detail,
  individual feature pages, client-side search, and a feature-table-only
  lineage graph. Hostable on GitHub Pages, S3, Netlify, anywhere.
- **Optional warehouse enrichment** — pass `--connection <profile>` and the
  catalog renders **actual freshness** (green/yellow/red), row counts,
  null %, and per-feature cardinality. DuckDB, Postgres, Redshift,
  Snowflake, BigQuery — read your existing `~/.dbt/profiles.yml`.

It is intentionally:

- **Read-only.** Your dbt project is the source of truth.
- **Static.** No backend, no auth, no infra to babysit.
- **Single-purpose.** Not a feature store. Not drift detection. Not a
  general-purpose data catalog.

## Why not just dbt-docs?

|                                            | dbt-docs                | dbt-features                                      |
|--------------------------------------------|-------------------------|---------------------------------------------------|
| Knows what a feature is (entity, grain, PIT) | No                    | Yes                                               |
| Lineage scope                              | Full graph (noisy)      | Feature-tables only (focused)                     |
| Freshness                                  | dbt source freshness    | Per-feature-table SLA + actual age, with stats    |
| Owner / ML consumers / lifecycle           | Generic `meta` blob     | First-class fields, rendered                      |
| Search                                     | Yes (basic)             | Yes (basic)                                       |
| Setup                                      | `dbt docs generate`     | `dbt parse` + `feature_catalog:` blocks + build   |
| Hosting                                    | Static                  | Static                                            |

dbt-docs is great. **It's just not built for ML feature workflows.** This
tool is the answer to "we use dbt for our feature pipelines and dbt-docs
isn't enough."

## Install

```bash
pip install dbt-features                      # core: static catalog from manifest.json
pip install 'dbt-features[duckdb]'            # + warehouse enrichment for dbt-duckdb
pip install 'dbt-features[postgres]'          # + Postgres
pip install 'dbt-features[redshift]'          # + Redshift (password or IAM)
pip install 'dbt-features[snowflake]'         # + Snowflake (password, key-pair, SSO)
pip install 'dbt-features[bigquery]'          # + BigQuery (ADC, service-account, inline JSON)
```

Requires Python 3.10+. The base install does not depend on `dbt-core` —
it reads dbt artifacts as JSON, so it won't fight your dbt version. The
optional warehouse extras bring in only that warehouse's native driver.

## See it in 10 seconds

```bash
dbt-features demo
```

Builds a catalog from bundled sample data, serves it on a free port, and
opens your browser. No dbt project required, nothing written to your
codebase — output goes to a temp directory that's cleaned up on exit.

![Index page — feature groups grouped by tag, with status dots](docs/screenshots/01-index.png)

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

Mark dbt models as feature tables under `meta.feature_catalog`. Once a
table is opted in, every column on it is a feature unless excluded —
column blocks are pure overrides:

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
      - name: feature_date           # excluded automatically (timestamp_column + grain)
        description: Date the features are computed for
      - name: customer_id            # excluded automatically (entity + grain)
        description: Customer identifier
      - name: orders_count_7d        # auto-included; type inferred as numeric
        description: Count of orders in the trailing 7 days
        meta:
          feature_catalog:
            null_behavior: zero
            used_by: [churn_model_v2, ltv_model_v3]
      - name: is_repeat_customer     # auto-included; type inferred as boolean
        description: Whether the customer ordered more than once
      - name: preferred_category     # VARCHAR — inference can't decide; override
        description: Most-purchased category in the trailing 30 days
        meta:
          feature_catalog:
            feature_type: categorical
```

### How columns are picked up

A column is published to the catalog iff:

1. The model is marked `is_feature_table: true`, **and**
2. The column is **not** in `entity`, `grain`, `timestamp_column`, or
   `exclude_columns`, **and**
3. Its column block does not set `is_feature: false`.

The `feature_type` falls back to inference from the warehouse `data_type`
when not declared (`INT`/`FLOAT`/`DECIMAL` -> `numeric`, `BOOL` ->
`boolean`, `DATE`/`TIMESTAMP` -> `timestamp`, `ARRAY`/`VECTOR` ->
`embedding`). `VARCHAR`/`TEXT` are intentionally left unspecified —
override when you need `categorical` or `text`.

### Model-level fields (`meta.feature_catalog`)

| Field                | Type            | Required | Notes                                                          |
|----------------------|-----------------|----------|----------------------------------------------------------------|
| `is_feature_table`   | bool            | yes      | Must be `true` to be picked up.                                |
| `entity`             | str \| str[]    | rec.     | Entity column(s) — the "who/what" the features describe. Auto-excluded from features. |
| `grain`              | str[]           | rec.     | Grain columns. Typically entity + timestamp. Auto-excluded from features. |
| `timestamp_column`   | str             | rec.     | Anchor for time-relative reasoning. Auto-excluded from features. |
| `exclude_columns`    | str[]           | no       | Additional columns to skip (e.g., `_loaded_at`, `_batch_id`).  |
| `freshness`          | object          | no       | `warn_after` / `error_after` (same shape as dbt source freshness). |
| `owner`              | str             | no       | Email or team name.                                            |
| `tags`               | str[]           | no       | For grouping / search.                                         |
| `description`        | str             | no       | Falls back to dbt model description.                           |
| `definition_version` | int             | no       | Bumped when the table's semantic definition changes. Defaults to `1`. |
| `lifecycle`          | enum            | no       | `active` (default), `preview`, `deprecated`.                   |
| `replacement`        | str             | no       | Name of the replacement table — most useful with `lifecycle: deprecated`. |
| `version`            | str             | no       | Metadata schema version. Defaults to `"0.2"`. Reserved for future use. |

### Column-level fields (`columns[].meta.feature_catalog`)

Column blocks are **overrides**. Their absence is *not* an opt-out —
non-key columns become features automatically.

| Field                | Type     | Required | Notes                                                                |
|----------------------|----------|----------|----------------------------------------------------------------------|
| `is_feature`         | bool     | no       | Defaults to `true`. Set `false` to exclude this single column.       |
| `feature_type`       | enum     | rec.     | `numeric`, `categorical`, `boolean`, `embedding`, `timestamp`, `text`, `identifier`. Inferred from `data_type` when omitted. |
| `null_behavior`      | enum     | no       | `zero`, `mean`, `propagate`, `error`, `ignore`. Documentation only.  |
| `used_by`            | str[]    | no       | Models / systems consuming this feature (manual list for v0.2).      |
| `description`        | str      | no       | Falls back to dbt column description.                                |
| `definition_version` | int      | no       | Bumped when the feature's semantic definition changes. Defaults to `1`. |
| `lifecycle`          | enum     | no       | `active` (default), `preview`, `deprecated`.                         |
| `replacement`        | str      | no       | Name of the replacement feature — most useful with `lifecycle: deprecated`. |

The schema is validated by Pydantic. Unknown fields are rejected — typos
become errors instead of silently dropped data.

### Individual feature page

Every feature column gets its own page with semantic type, null behavior,
warehouse type, declared ML consumers, and (when enriched) actual null
rate and cardinality.

![Feature page — null rate, distinct values, ML consumers, lifecycle](docs/screenshots/03-feature.png)

### Lineage view

A feature-table-only lineage graph (Mermaid, bundled locally — works
offline). The full dbt graph is out of scope here: it's noisy and
already lives in dbt-docs.

![Lineage page — feature-table-only graph](docs/screenshots/04-lineage.png)

## What's deliberately out of scope (v0.1)

These are real feature requests; the answer is "no, with reason":

- Online serving / runtime feature retrieval (use a feature store).
- Feature computation (dbt does this).
- Drift / monitoring / alerting (orthogonal tools exist).
- Multi-project federation.
- Authentication / access control (it's a static site — host it however you want).
- A web UI for editing metadata (edit your `schema.yml`).
- A hosted SaaS.

## Warehouse enrichment (optional)

When you pass `--connection <profile>`, the catalog reads `~/.dbt/profiles.yml`,
runs read-only queries against your warehouse, and renders:

- **Freshness status** — green / yellow / red badge per feature group, based on
  declared `warn_after` / `error_after` thresholds vs. actual `MAX(timestamp_column)`.
- **Row count** per feature group.
- **Null rate** and **distinct values** per feature column.

Without `--connection`, the catalog falls back to declared metadata only — the
warehouse is never contacted.

### Install the extra for your warehouse

```bash
pip install 'dbt-features[duckdb]'      # local dev / dbt-duckdb projects
pip install 'dbt-features[postgres]'    # Postgres
pip install 'dbt-features[redshift]'    # Redshift (password or IAM)
pip install 'dbt-features[snowflake]'   # Snowflake (password, key-pair, SSO/OAuth)
pip install 'dbt-features[bigquery]'    # BigQuery (ADC, service-account, inline JSON)
```

### Run

```bash
dbt parse                                 # populate manifest.json
dbt-features build \
    --connection prod \                   # profile name in profiles.yml
    --target dev \                        # optional: override profile's default target
    --output ./catalog
```

Subsequent rebuilds reuse the cached results (1 h TTL by default):

| Flag                    | Effect                                                       |
|-------------------------|--------------------------------------------------------------|
| `--cache-ttl <seconds>` | Override TTL. `0` forces a refresh on every build.           |
| `--no-cache`            | Skip the cache entirely. Always queries the warehouse.       |
| `--profiles-dir <dir>`  | Override `~/.dbt`. Honors `$DBT_PROFILES_DIR` by default.    |

The cache lives at `<output>/.cache/enrichment.json` and survives `--clean`
rebuilds — running `dbt-features build` without `--connection` will still
reuse the cached enrichment until it expires.

### Profile examples

`profiles.yml` is the same one dbt uses. Examples for each supported warehouse:

<details>
<summary><b>Postgres</b></summary>

```yaml
prod:
  target: dev
  outputs:
    dev:
      type: postgres
      host: warehouse.example.com
      port: 5432
      user: analytics_ro
      password: "{{ env_var('PG_PASSWORD') }}"
      dbname: prod
      schema: analytics
      sslmode: require
```
</details>

<details>
<summary><b>Redshift</b> (password or IAM)</summary>

```yaml
# Password auth
prod:
  target: dev
  outputs:
    dev:
      type: redshift
      host: cluster.abc.us-east-1.redshift.amazonaws.com
      port: 5439
      user: analytics_ro
      password: "{{ env_var('RS_PASSWORD') }}"
      dbname: prod
      schema: analytics

# IAM auth
prod-iam:
  target: dev
  outputs:
    dev:
      type: redshift
      method: iam
      host: cluster.abc.us-east-1.redshift.amazonaws.com
      cluster_id: my-cluster
      region: us-east-1
      iam_profile: default        # AWS named profile
      user: iam-readonly-user
      dbname: prod
      schema: analytics
```
</details>

<details>
<summary><b>Snowflake</b> (password, key-pair, or SSO)</summary>

```yaml
# Password auth
prod:
  target: dev
  outputs:
    dev:
      type: snowflake
      account: myorg-myaccount
      user: ANALYTICS_RO
      password: "{{ env_var('SNOWFLAKE_PASSWORD') }}"
      role: ANALYTICS_RO
      database: PROD
      warehouse: COMPUTE_WH
      schema: ANALYTICS

# Key-pair auth
prod-keypair:
  target: dev
  outputs:
    dev:
      type: snowflake
      account: myorg-myaccount
      user: ANALYTICS_RO
      private_key_path: ~/.ssh/snowflake_rsa_key.p8
      private_key_passphrase: "{{ env_var('SNOWFLAKE_KEY_PASSPHRASE') }}"
      role: ANALYTICS_RO
      database: PROD
      warehouse: COMPUTE_WH
      schema: ANALYTICS

# Browser-based SSO (interactive — useful for local dev, not CI)
prod-sso:
  target: dev
  outputs:
    dev:
      type: snowflake
      account: myorg-myaccount
      user: alice@example.com
      authenticator: externalbrowser
      role: ANALYTICS_RO
      database: PROD
      warehouse: COMPUTE_WH
      schema: ANALYTICS
```
</details>

<details>
<summary><b>BigQuery</b> (ADC, service-account file, or inline JSON)</summary>

```yaml
# Application Default Credentials (gcloud auth, GAE/GKE metadata, etc.)
prod:
  target: dev
  outputs:
    dev:
      type: bigquery
      method: oauth
      project: my-gcp-project
      dataset: analytics

# Service-account keyfile
prod-sa:
  target: dev
  outputs:
    dev:
      type: bigquery
      method: service-account
      project: my-gcp-project
      dataset: analytics
      keyfile: /etc/gcp/sa-key.json

# Inline service-account JSON (CI-friendly; pass via env_var)
prod-sa-inline:
  target: dev
  outputs:
    dev:
      type: bigquery
      method: service-account-json
      project: my-gcp-project
      dataset: analytics
      keyfile_json: "{{ env_var('GCP_SA_JSON') }}"
```
</details>

<details>
<summary><b>DuckDB</b> (local files)</summary>

```yaml
demo:
  target: dev
  outputs:
    dev:
      type: duckdb
      path: ./warehouse.duckdb
      schema: main
```
</details>

### What runs against your warehouse

For each feature group, two read-only queries:

```sql
SELECT MAX("<timestamp_column>"), COUNT(*) FROM "<db>"."<schema>"."<table>";
SELECT
    COUNT(*) - COUNT("<col>") AS "<col>__nulls",
    COUNT(DISTINCT "<col>") AS "<col>__distinct",
    -- ...repeated for each feature column
FROM "<db>"."<schema>"."<table>";
```

The Snowflake adapter additionally tags every query with
`QUERY_TAG=dbt-features-enrichment` so it's easy to spot in
`QUERY_HISTORY`. We never write.

### Troubleshooting

- **"Permission denied"** on a feature group — granted the read role to your
  catalog's tables? Per-table failures land on the snapshot's `error` field
  (rendered inline) rather than aborting the whole build, so you can see
  exactly which tables failed.
- **"Profile 'X' not found"** — check `--profiles-dir` and the spelling
  against `dbt debug --profile X`. We read the same file dbt does.
- **`{{ env_var(...) }}` references** are expanded the same way dbt does.
  Anything fancier (Jinja `var()`, secrets-manager hooks) is left as a
  literal string — the adapter will reject it with a clear error.
- **Warehouse not in the supported list?** Open an issue describing the
  profile shape and your usage; adapters are ~100 lines each and we're
  happy to add more driven by real demand.

## Companion dbt package

For compile-time validation (catch metadata typos in `dbt parse` rather
than at catalog build time), install the companion dbt package — see
[`dbt_package/`](./dbt_package). It ships a `validate` macro you can wire
into your project's tests.

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
