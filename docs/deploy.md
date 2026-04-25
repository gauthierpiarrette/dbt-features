# Deploying the catalog

The catalog is a directory of static HTML, JS, and CSS. Host it anywhere
that serves static files: GitHub Pages, S3 + CloudFront, Netlify, Vercel,
an internal nginx, etc. There's no backend, no database, no auth layer -
treat the output of `dbt-features build` like a static documentation site.

## GitHub Actions → GitHub Pages

Copy this workflow into your dbt project at
`.github/workflows/feature-catalog.yml`. It builds the catalog on every
push to `main` and publishes it to GitHub Pages. Adjust paths, triggers,
and the warehouse extra to match your project.

```yaml
name: feature-catalog

on:
  push:
    branches: [main]

jobs:
  build:
    runs-on: ubuntu-latest
    permissions:
      contents: read
      pages: write
      id-token: write
    steps:
      - uses: actions/checkout@v4
      - uses: actions/setup-python@v5
        with:
          python-version: "3.11"
      - name: Install dbt + dbt-features
        run: |
          pip install dbt-duckdb dbt-features
      - name: Parse dbt project
        run: dbt parse --profiles-dir .
      - name: (Optional) generate catalog.json for warehouse types
        run: dbt docs generate --profiles-dir .
      - name: Build feature catalog
        run: dbt-features build --output ./catalog-site
      - uses: actions/upload-pages-artifact@v3
        with:
          path: ./catalog-site
      - uses: actions/deploy-pages@v4
```

Enable GitHub Pages for the repo (Settings → Pages → Source: GitHub
Actions) and the catalog will publish to
`https://<org>.github.io/<repo>/` on every push.

## Other targets

- **S3 + CloudFront** - `aws s3 sync ./catalog-site s3://my-bucket/ --delete`
  in CI, then invalidate the CloudFront distribution.
- **Netlify / Vercel** - point at the output directory; no build command
  is needed beyond `dbt-features build`.
- **Internal nginx** - copy `./catalog-site` to the doc root. The site
  uses relative paths, so it works under any subpath.

## Adding warehouse enrichment in CI

If you want freshness, row counts, and null % in the published catalog,
add `--connection <profile>` to the `dbt-features build` step and ensure
your CI environment has access to a read-only warehouse role. Profile
secrets typically come from `env_var(...)` references resolved against
GitHub Actions secrets.

See [enrichment.md](enrichment.md) for the full set of authentication
options per warehouse.
