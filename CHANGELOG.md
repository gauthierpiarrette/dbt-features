# Changelog

All notable changes to this project will be documented in this file. The
format follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/) and
this project adheres to [Semantic Versioning](https://semver.org/).

## [Unreleased]

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

### Not yet implemented
- Warehouse-backed freshness checks (planned for v0.2). The freshness SLA
  fields are accepted, validated, and rendered today but not queried.
- Auto-derivation of `used_by` from dbt lineage / model registries.
- Multi-project federation.
