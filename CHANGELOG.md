# Changelog

All notable changes to this project will be documented in this file. The
format follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/) and
this project adheres to [Semantic Versioning](https://semver.org/).

## [Unreleased]

## [0.3.0] - 2026-04-26 - UI redesign, ML model pages, faster search

### Added
- **ML model pages.** Each consumer in `used_by` now gets its own page
  listing every feature group and feature it consumes — closes the
  feature-to-model loop in both directions.
- **Cmd-K / `/` search modal** with match highlighting, no-results
  feedback, and a `kind:` filter (`kind:feature`, `kind:group`,
  `kind:model`) for narrowing by entity type.
- **Faceted filter chips** on the homepage and feature-group pages
  (entity, type, owner, lifecycle, freshness), URL-synced so filtered
  views are linkable.
- **Sortable columns** on the feature table.
- **Related features** sections on feature-group and feature-detail
  pages (related-by-consumer and related-by-type).
- **Summary stats bar** and per-entity type-distribution mini-bars on
  the index.
- **Copy-to-clipboard buttons** on SQL, dbt-ref, and feature-name
  snippets.
- **Owner and model indexes** on the catalog object.
- **Deep-link anchors** on feature rows.
- **A11y**: ARIA attributes on interactive controls, print stylesheet.

### Changed
- **Homepage** rewritten around entity sections with faceted filtering
  and URL-synced state.
- **Feature-group page** rewritten with an info grid, SQL / dbt-ref
  snippet tabs, and a dedicated lineage section.
- **Feature-detail page** rewritten with an info grid and
  related-features sections.
- **Lineage page** redesigned: Mermaid entity clusters, search-to-focus,
  theme-aware re-render on toggle.
- **Global header** redesigned with a search trigger and a two-state
  theme toggle; Safari-safe icons throughout.
- **Brand logo** is now the favicon.
- **CSS rewrite** with design tokens, type pills, entity dots, type
  bars, and cleaner section spacing.

### Fixed
- Broken feature-page URLs in some lineage edges.
- Stray HTML in the rendered feature table.
- Overly aggressive `ARRAY` -> `embedding` inference for non-vector
  array columns.
- Stale docstrings and a misleading test name.

## [0.2.0] - Auto-include feature columns

### Changed (breaking)
- **Column inclusion is now automatic.** Marking a model with
  `is_feature_table: true` is the only opt-in needed; every column on the
  model becomes a feature *unless* it appears in `entity`, `grain`,
  `timestamp_column`, or the new `exclude_columns` list, or its column
  block sets `is_feature: false`. The old per-column `is_feature: true`
  flag is gone — column blocks are now pure overrides.

### Migration
1. Remove `is_feature: true` from your column meta blocks — it's now a
   no-op (still accepted, but redundant).
2. Optionally add `exclude_columns: [...]` at the table level for
   internal columns (`_loaded_at`, `_batch_id`, debug scratch, etc.).
3. To exclude a single column without listing it at table level, set
   `is_feature: false` on the column.
4. Existing column overrides (`feature_type`, `used_by`, `null_behavior`,
   `lifecycle`, `replacement`, `definition_version`) keep working
   unchanged.

### Added
- `exclude_columns` field on `FeatureTableMeta` for table-level column
  exclusion.
- `feature_type` is now **inferred** from the warehouse `data_type` when
  not declared. Conservative mapping: `INT*`/`FLOAT*`/`DECIMAL` ->
  `numeric`, `BOOL` -> `boolean`, `DATE`/`TIMESTAMP` -> `timestamp`,
  `ARRAY`/`VECTOR` -> `embedding`. `VARCHAR`/`TEXT` left unspecified
  (override if needed).
- Schema bumped to `0.2`.

## [0.1.x] - Warehouse enrichment

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
