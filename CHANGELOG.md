# Changelog

All notable changes to this project will be documented in this file. The
format follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/) and
this project adheres to [Semantic Versioning](https://semver.org/).

## [Unreleased]

### Added
- **Warehouse enrichment** (`--connection PROFILE` on `build`):
  - DuckDB, Postgres, Redshift (password + IAM), Snowflake (password,
    key-pair, SSO/OAuth), BigQuery (ADC, service-account, inline JSON).
  - Reads `~/.dbt/profiles.yml` (honors `$DBT_PROFILES_DIR`).
  - Renders green/yellow/red freshness status, last-update age, row count,
    null %, and per-feature distinct-value count in the UI.
  - JSON cache at `<output>/.cache/enrichment.json` with configurable TTL.
    Survives `--clean` rebuilds; reused by subsequent `build` calls without
    `--connection`.
  - Per-feature-group failures captured on the snapshot rather than
    aborting the build.
- **Lifecycle + `definition_version`** schema fields with rendered
  badges and an inline deprecation banner pointing at the replacement.
- **Mermaid bundled locally** — lineage view works offline / behind CSP /
  in air-gapped environments.
- **Themed Mermaid** to match the site palette and re-render on the dark/light
  toggle.
- **`demo` command** — one-shot zero-setup catalog rendered into a temp
  directory, with synthesized enrichment so screenshots show every state.
- **Dark mode** — defaults to dark; toggle persists in localStorage.
- **Favicon**.


## [0.1.0] - Initial release

### Added
- `feature_catalog` metadata schema (versioned, validated by Pydantic).
- Parser that reads `manifest.json` and (optionally) `catalog.json` without
  importing `dbt-core`.
- Static HTML site generator with three views: feature-group index,
  feature-group detail, individual feature detail. Plus a feature-table-only
  lineage page rendered with Mermaid.
- Client-side substring search.
- CLI: `build`, `serve`, `validate`, `demo`. The `demo` command builds a
  bundled sample catalog into a temp directory and serves it locally —
  zero setup, nothing written to the user's project.
- Companion dbt package (`feature_catalog`) shipping a
  `feature_catalog__validate` run-operation for compile-time metadata
  validation.

### Warehouse adapters shipped in v0.1
DuckDB, Postgres, Redshift (password + IAM auth), Snowflake (password,
key-pair, external-browser SSO, OAuth pass-through), BigQuery (ADC,
service-account keyfile, inline service-account JSON).

### Not yet implemented
- Auto-derivation of `used_by` from dbt lineage / model registries.
- Multi-project federation.
- Live (non-cached) freshness — currently cached with a configurable TTL.
