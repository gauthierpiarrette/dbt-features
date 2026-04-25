# Contributing

Thanks for being interested. This is a small, focused project — please read
the [spec](./specs.md) before opening anything substantial. The "out of
scope" section is load-bearing.

## Setup

```bash
python -m venv .venv && source .venv/bin/activate
pip install -e ".[dev]"
pytest
```

## Before sending a PR

- `ruff check src tests` — lint clean.
- `pytest` — all tests pass.
- For schema or rendering changes: include or update a fixture and a test.
- For schema changes: bump `SCHEMA_VERSION` in `src/dbt_features/schema.py`
  and update the migration notes in `CHANGELOG.md`. Both packages
  (Python + dbt) version together.

## What we'll likely accept

- Bug fixes, with a regression test.
- Small UX improvements to the rendered site.
- New `feature_type` / `null_behavior` enum members backed by a real use case.
- Documentation improvements.

## What we'll likely push back on

- Adding new dependencies. The current dependency surface (`click`,
  `jinja2`, `pydantic`) is intentional.
- Features that turn the catalog into a feature store, drift detector, or
  general-purpose data catalog. See [spec § Non-goals](./specs.md).
- Server-side rendering or a long-running backend.
- Re-introducing a CDN dependency for the lineage page (or any other view).
  Mermaid is bundled locally on purpose — see below.

## Regenerating the real-manifest fixture

`tests/fixtures/real/manifest.json` is a real `dbt parse` output captured
from the bundled example project, pruned to the keys our parser reads.
Regenerate when bumping the supported-dbt floor or when introducing new
schema fields:

```bash
pip install dbt-duckdb           # only needed to regenerate the fixture
cd examples/jaffle_shop_features
DBT_PROFILES_DIR=. dbt seed
DBT_PROFILES_DIR=. dbt run
DBT_PROFILES_DIR=. dbt docs generate

# Copy + prune
cp target/manifest.json ../../tests/fixtures/real/
cp target/catalog.json ../../tests/fixtures/real/

python - <<'PY'
import json
m = json.load(open('../../tests/fixtures/real/manifest.json'))
keep = {'metadata': m['metadata'], 'nodes': m['nodes'], 'sources': m.get('sources', {})}
json.dump(keep, open('../../tests/fixtures/real/manifest.json', 'w'), indent=2, sort_keys=True)
PY

rm -rf target dbt_packages logs *.duckdb*
```

## Updating the bundled Mermaid

The lineage view uses [Mermaid](https://github.com/mermaid-js/mermaid),
vendored at `src/dbt_features/static/mermaid.min.js`. We bundle locally
(rather than loading from a CDN) so the catalog works offline, behind
strict CSPs, and in air-gapped environments.

To update to a new Mermaid release:

```bash
MERMAID_VERSION=10.9.3   # pick a stable v10
curl -sSL "https://cdn.jsdelivr.net/npm/mermaid@${MERMAID_VERSION}/dist/mermaid.min.js" \
  -o src/dbt_features/static/mermaid.min.js
```

Then update the version recorded in `THIRD_PARTY_LICENSES.md` and run the
test suite. Major version bumps may need template adjustments — pin
deliberately.
