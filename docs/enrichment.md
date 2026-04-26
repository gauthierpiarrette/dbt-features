# Warehouse enrichment

When you pass `--connection <profile>`, the catalog reads `~/.dbt/profiles.yml`,
runs read-only queries against your warehouse, and renders:

- **Freshness status** - green / yellow / red badge per feature group, based on
  declared `warn_after` / `error_after` thresholds vs. actual `MAX(timestamp_column)`.
- **Row count** per feature group.
- **Null rate** and **distinct values** per feature column.

Without `--connection`, the catalog falls back to declared metadata only - the
warehouse is never contacted.

## Install the extra for your warehouse

```bash
pip install 'dbt-features[duckdb]'      # local dev / dbt-duckdb projects
pip install 'dbt-features[postgres]'    # Postgres
pip install 'dbt-features[redshift]'    # Redshift (password or IAM)
pip install 'dbt-features[snowflake]'   # Snowflake (password, key-pair, SSO/OAuth)
pip install 'dbt-features[bigquery]'    # BigQuery (ADC, service-account, inline JSON)
```

## Run

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
rebuilds - running `dbt-features build` without `--connection` will still
reuse the cached enrichment until it expires.

## Profile examples

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

# Browser-based SSO (interactive - useful for local dev, not CI)
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

## What runs against your warehouse

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

## Production setup

Building the catalog against a production warehouse requires two things: a
manifest with production schemas baked in, and column type information for
feature type inference.

### Use a production manifest

The manifest produced by `dbt parse` uses your *local* profile's default
target — often a dev schema like `dbt_alice`. To build the catalog against
production tables, use one of these approaches:

**Option A: point at an existing prod manifest** (recommended for CI):

```bash
# Your CI pipeline already builds a prod manifest during dbt build.
dbt-features build \
    --manifest ./artifacts/manifest.json \
    --connection prod \
    --output ./catalog
```

**Option B: use `--target` to override the schema** (recommended for local):

```bash
dbt parse
dbt-features build \
    --connection my_profile \
    --target prod \
    --output ./catalog
```

The `--target` flag selects a named target from your `profiles.yml`. If your
prod target has `schema: analytics`, enrichment queries will run against
`analytics.*` regardless of what's baked in the manifest.

### Get feature type inference working

dbt-features infers feature types (`numeric`, `boolean`, `timestamp`, etc.)
from warehouse column types. This requires column type data in one of two
places:

1. **`data_type` on columns in the manifest** — dbt populates this if your
   adapter and version support it (dbt 1.6+ with most adapters).
2. **`catalog.json`** — generated by `dbt docs generate`. This contains
   the warehouse-sourced column types for every materialized model.

If your features all show "Type: —" in the catalog, run:

```bash
dbt docs generate     # writes target/catalog.json
dbt-features build \
    --manifest target/manifest.json \
    --catalog target/catalog.json \
    --connection prod
```

If `dbt docs generate` fails on unrelated models (e.g., missing
dependencies in other parts of the project), scope it to your feature
tables:

```bash
dbt docs generate --select tag:is_feature_table   # or use a path selector
dbt docs generate --select models/features/       # only feature models
```

This produces a `catalog.json` with just the models you need, avoiding
compilation errors in unrelated parts of the project.

Explicit `feature_type` in your YAML always takes precedence over inference.
See the [schema docs](schema.md) for supported types.

### Wide tables / timeout tuning

`COUNT(DISTINCT col)` across many columns on large tables can be slow. If
enrichment fails with a timeout error, increase the query timeout:

```bash
dbt-features build --connection prod --timeout 600   # 10 minutes
```

The default is 120 seconds per adapter. You can also set `connect_timeout`
(Redshift/Postgres) or `timeout_seconds` (BigQuery) in your profile.

## Troubleshooting

- **"Permission denied"** on a feature group - granted the read role to your
  catalog's tables? Per-table failures land on the snapshot's `error` field
  (rendered inline) rather than aborting the whole build, so you can see
  exactly which tables failed.
- **"Profile 'X' not found"** - check `--profiles-dir` and the spelling
  against `dbt debug --profile X`. We read the same file dbt does.
- **`{{ env_var(...) }}` references** are expanded the same way dbt does.
  Anything fancier (Jinja `var()`, secrets-manager hooks) is left as a
  literal string - the adapter will reject it with a clear error.
- **Warehouse not in the supported list?** Open an issue describing the
  profile shape and your usage; adapters are ~100 lines each.
