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
