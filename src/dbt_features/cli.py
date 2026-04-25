"""Command-line interface."""

from __future__ import annotations

import http.server
import signal
import socket
import socketserver
import sys
import tempfile
import threading
import webbrowser
from pathlib import Path

import click

from dbt_features import __version__
from dbt_features.demo import demo_manifest_path
from dbt_features.parser import parse_project
from dbt_features.renderer import render_catalog
from dbt_features.schema import SchemaError


@click.group(context_settings={"help_option_names": ["-h", "--help"]})
@click.version_option(__version__, prog_name="dbt-features")
def main() -> None:
    """A feature catalog for dbt-based ML feature pipelines."""


_PROJECT_DIR_OPT = click.option(
    "--project-dir",
    type=click.Path(exists=True, file_okay=False, path_type=Path),
    default=Path.cwd(),
    show_default="current directory",
    help="Path to the dbt project directory.",
)
_MANIFEST_OPT = click.option(
    "--manifest",
    "manifest_path",
    type=click.Path(dir_okay=False, path_type=Path),
    default=None,
    help="Path to manifest.json. Defaults to <project-dir>/target/manifest.json.",
)
_CATALOG_OPT = click.option(
    "--catalog",
    "catalog_path",
    type=click.Path(dir_okay=False, path_type=Path),
    default=None,
    help="Path to catalog.json. Defaults to <project-dir>/target/catalog.json.",
)


@main.command()
@_PROJECT_DIR_OPT
@_MANIFEST_OPT
@_CATALOG_OPT
@click.option(
    "--output",
    "-o",
    type=click.Path(file_okay=False, path_type=Path),
    default=Path("./target/feature-catalog"),
    show_default=True,
    help="Output directory for the generated site.",
)
@click.option(
    "--clean/--no-clean",
    default=True,
    show_default=True,
    help="Clear the output directory before writing.",
)
@click.option(
    "--connection",
    "connection_profile",
    default=None,
    metavar="PROFILE",
    help="Optional dbt profile name in profiles.yml. When set, the catalog is "
    "enriched with actual freshness, row counts, and per-column stats from the warehouse.",
)
@click.option(
    "--target",
    "connection_target",
    default=None,
    metavar="TARGET",
    help="Override the profile's default target. Only meaningful with --connection.",
)
@click.option(
    "--profiles-dir",
    type=click.Path(file_okay=False, path_type=Path),
    default=None,
    help="Directory containing profiles.yml. Defaults to $DBT_PROFILES_DIR or ~/.dbt.",
)
@click.option(
    "--cache-ttl",
    type=int,
    default=3600,
    show_default=True,
    metavar="SECONDS",
    help="How long to reuse cached enrichment data before re-querying. 0 forces refresh.",
)
@click.option(
    "--no-cache",
    is_flag=True,
    default=False,
    help="Skip the enrichment cache entirely (always queries the warehouse).",
)
@click.option(
    "--demo",
    "use_demo_data",
    is_flag=True,
    default=False,
    help="Build from the bundled sample data with synthesized enrichment. "
    "Useful for hosting a permanent demo on GitHub Pages or generating screenshots.",
)
def build(
    project_dir: Path,
    manifest_path: Path | None,
    catalog_path: Path | None,
    output: Path,
    clean: bool,
    connection_profile: str | None,
    connection_target: str | None,
    profiles_dir: Path | None,
    cache_ttl: int,
    no_cache: bool,
    use_demo_data: bool,
) -> None:
    """Build a static feature catalog site from your dbt artifacts."""

    if use_demo_data:
        # Bundled-demo path: ignore --project-dir/--manifest, use the
        # vendored manifest, and synthesize enrichment so the rendered
        # site shows freshness/null%/cardinality without a warehouse.
        try:
            catalog = parse_project(project_dir, manifest_path=demo_manifest_path())
        except FileNotFoundError as e:  # pragma: no cover - packaging accident
            raise click.ClickException(str(e)) from e
    else:
        try:
            catalog = parse_project(
                project_dir, manifest_path=manifest_path, catalog_path=catalog_path
            )
        except FileNotFoundError as e:
            raise click.ClickException(str(e)) from e
        except SchemaError as e:
            msg = str(e)
            if e.node_id:
                msg = f"[{e.node_id}] {msg}"
            raise click.ClickException(msg) from e

    if clean and output.exists():
        _safe_clear(output)
    output.mkdir(parents=True, exist_ok=True)

    if use_demo_data:
        enrichment = _synthesize_demo_enrichment(catalog)  # type: ignore[assignment]
    else:
        enrichment = _resolve_enrichment(
            catalog,
            output=output,
            profile_name=connection_profile,
            target=connection_target,
            profiles_dir=profiles_dir,
            cache_ttl=cache_ttl,
            use_cache=not no_cache,
        )

    render_catalog(catalog, output, enrichment=enrichment)

    click.echo(
        f"Built catalog: {len(catalog.feature_groups)} feature group(s), "
        f"{catalog.feature_count} feature(s) → {output}/index.html"
    )


def _resolve_enrichment(
    catalog: object,
    *,
    output: Path,
    profile_name: str | None,
    target: str | None,
    profiles_dir: Path | None,
    cache_ttl: int,
    use_cache: bool,
) -> dict[str, object] | None:
    """Decide what enrichment data (if any) the renderer should consume.

    Three paths:
      - ``--connection`` set → query/refresh and write cache.
      - ``--connection`` unset but a fresh cache exists → reuse it. This
        keeps the rendered site stable across a build that forgot to pass
        ``--connection``, instead of silently dropping freshness UI.
      - Otherwise → no enrichment. The renderer falls back gracefully.
    """

    from dbt_features.enrichment import (
        EnrichmentCache,
        EnrichmentError,
        enrich_catalog,
    )

    cache_path = output / ".cache" / "enrichment.json"

    if profile_name is None:
        # Cache-only mode: read whatever's there. Missing/expired/corrupt
        # cache returns None — the renderer falls back to no enrichment.
        if not use_cache:
            return None
        cache = EnrichmentCache(cache_path, ttl_seconds=cache_ttl)
        cached = cache.read()
        if cached is None:
            return None
        click.echo(
            f"Enrichment: reusing cached data for {len(cached)} feature group(s) "
            f"(no --connection passed; pass it to refresh)."
        )
        return cached  # type: ignore[return-value]

    cache = EnrichmentCache(cache_path, ttl_seconds=cache_ttl) if use_cache else None
    if not use_cache and cache_path.exists():
        cache_path.unlink()

    try:
        snapshots = enrich_catalog(
            catalog,  # type: ignore[arg-type]
            profile_name=profile_name,
            target=target,
            profiles_dir=profiles_dir,
            cache=cache,
        )
    except EnrichmentError as e:
        raise click.ClickException(f"Enrichment failed: {e}") from e

    failed = [uid for uid, snap in snapshots.items() if snap.error]
    succeeded = len(snapshots) - len(failed)
    click.echo(
        f"Enrichment: queried {len(snapshots)} feature group(s) "
        f"({succeeded} succeeded, {len(failed)} failed). Cache → {cache_path}"
    )
    if failed:
        for uid in failed:
            click.echo(f"  - {uid}: {snapshots[uid].error}", err=True)
    return snapshots  # type: ignore[return-value]


@main.command()
@_PROJECT_DIR_OPT
@_MANIFEST_OPT
@_CATALOG_OPT
def validate(
    project_dir: Path,
    manifest_path: Path | None,
    catalog_path: Path | None,
) -> None:
    """Validate feature_catalog metadata without building output.

    Useful in CI before a deploy: fail fast on schema errors.
    """

    try:
        catalog = parse_project(project_dir, manifest_path=manifest_path, catalog_path=catalog_path)
    except FileNotFoundError as e:
        raise click.ClickException(str(e)) from e
    except SchemaError as e:
        msg = str(e)
        if e.node_id:
            msg = f"[{e.node_id}] {msg}"
        raise click.ClickException(msg) from e

    if not catalog.feature_groups:
        click.echo(
            "No feature tables found. Mark a model with "
            "`meta.feature_catalog.is_feature_table: true` to start."
        )
        return

    click.echo(
        f"OK: {len(catalog.feature_groups)} feature group(s), "
        f"{catalog.feature_count} feature(s) — schema valid."
    )


@main.command()
@click.option(
    "--output",
    "-o",
    type=click.Path(exists=True, file_okay=False, path_type=Path),
    default=Path("./target/feature-catalog"),
    show_default=True,
    help="Directory containing a built catalog.",
)
@click.option("--port", "-p", default=8080, show_default=True, help="Port to serve on.")
@click.option("--host", default="127.0.0.1", show_default=True, help="Host to bind to.")
def serve(output: Path, port: int, host: str) -> None:
    """Serve a built catalog locally for development.

    Not a deployment target — use a static host (GitHub Pages, S3, Netlify)
    in CI for the real deal.
    """

    if not (output / "index.html").exists():
        raise click.ClickException(
            f"No index.html in {output}. Run `dbt-features build` first."
        )

    handler_cls = _make_handler(output)
    with _ReusableTCPServer((host, port), handler_cls) as httpd:
        click.echo(f"Serving {output} at http://{host}:{port} (Ctrl-C to stop)")
        try:
            httpd.serve_forever()
        except KeyboardInterrupt:
            click.echo("\nStopped.")


@main.command()
@click.option("--port", "-p", default=8080, show_default=True, help="Port to serve on (auto-picks the next free port if busy).")
@click.option("--host", default="127.0.0.1", show_default=True, help="Host to bind to.")
@click.option(
    "--open/--no-open",
    "open_browser",
    default=True,
    show_default=True,
    help="Open a browser tab to the demo.",
)
def demo(port: int, host: str, open_browser: bool) -> None:
    """Build and serve a sample catalog. Zero setup, no dbt install required.

    Uses bundled sample data — three feature groups across two domains
    (customer + store) with cross-table lineage. Output goes to a temp
    directory that's cleaned up on exit. Nothing is written to your project.
    """

    manifest = demo_manifest_path()
    if not manifest.exists():
        raise click.ClickException(  # pragma: no cover - packaging accident
            "Demo data missing from this install — please reinstall dbt-features."
        )

    with tempfile.TemporaryDirectory(prefix="dbt-features-demo-") as tmp:
        site = Path(tmp) / "site"
        catalog = parse_project(Path(tmp), manifest_path=manifest)
        # Synthesize a plausible enrichment cache so the demo shows
        # freshness/null%/cardinality without needing a real warehouse.
        # Values are deterministic per group name, so screenshots are
        # reproducible.
        enrichment = _synthesize_demo_enrichment(catalog)
        render_catalog(catalog, site, enrichment=enrichment)

        actual_port = _pick_free_port(host, port)
        url = f"http://{host}:{actual_port}"

        click.echo(
            f"Demo catalog ready: {len(catalog.feature_groups)} feature group(s), "
            f"{catalog.feature_count} feature(s)."
        )
        click.echo(f"Serving at {url}  (Ctrl-C to stop)")
        click.echo(
            "Sample data only — to catalog your own project, run `dbt-features build --help`."
        )

        if open_browser:
            # Tiny delay so the server is listening when the browser hits it.
            threading.Timer(0.4, lambda: webbrowser.open(url)).start()

        handler_cls = _make_handler(site)
        # Translate SIGTERM into the same KeyboardInterrupt path Ctrl-C uses,
        # so the TemporaryDirectory context manager always cleans up. Without
        # this, `kill <pid>` on the demo process leaves orphan temp dirs.
        with (
            _sigterm_as_keyboard_interrupt(),
            _ReusableTCPServer((host, actual_port), handler_cls) as httpd,
        ):
            try:
                httpd.serve_forever()
            except KeyboardInterrupt:
                click.echo("\nStopped.")
            finally:
                httpd.shutdown()


def _synthesize_demo_enrichment(catalog: object) -> dict[str, object]:
    """Build a plausible-looking enrichment map for the bundled demo.

    Times are computed relative to ``now`` so the demo never shows "last
    updated 3 years ago" if a user installs an old version. Per-group
    ages are picked to land in different freshness states (fresh / warn /
    error) so the UI exercises every code path.
    """

    from datetime import datetime, timedelta, timezone

    from dbt_features.enrichment.models import ColumnStats, FreshnessSnapshot

    now = datetime.now(timezone.utc)

    # Hand-tuned to demonstrate the three states and a "no timestamp_column"
    # case, matching the bundled demo manifest. Falls back to plausible
    # defaults for any group not in this map (forward-compatible if the
    # demo grows).
    age_overrides = {
        "customer_features_daily": timedelta(hours=2),       # fresh
        "customer_features_lifetime": None,                  # no timestamp anchor
        "store_features_hourly": timedelta(hours=3),         # past 2h warn, before 6h error
    }
    row_count_overrides = {
        "customer_features_daily": 12_847,
        "customer_features_lifetime": 8_234,
        "store_features_hourly": 184_532,
    }

    out: dict[str, object] = {}
    for group in catalog.feature_groups:  # type: ignore[attr-defined]
        age = age_overrides.get(group.name, timedelta(hours=4))
        max_ts = (now - age) if age is not None else None
        row_count = row_count_overrides.get(group.name, 1_000)

        columns = {}
        for feature in group.features:
            null_count, distinct_count = _demo_column_stats(feature.name, row_count)
            columns[feature.name] = ColumnStats(null_count=null_count, distinct_count=distinct_count)

        out[group.unique_id] = FreshnessSnapshot(
            queried_at=now,
            max_timestamp=max_ts,
            row_count=row_count,
            columns=columns,
        )
    return out


def _demo_column_stats(name: str, row_count: int) -> tuple[int, int]:
    """Pick believable null/distinct counts for the demo, by column name.

    The point is for the UI to look real, not for the numbers to mean
    anything. Heuristics match feature names in the bundled demo manifest.
    """

    if name.startswith("is_") or name == "is_repeat_customer":
        return 0, 2
    if name == "preferred_category":
        return int(row_count * 0.04), 8
    if "count" in name or name.endswith("_value_usd"):
        return int(row_count * 0.012), max(1, int(row_count * 0.18))
    if name == "demand_score":
        return int(row_count * 0.001), max(1, int(row_count * 0.4))
    if name == "first_order_date":
        return int(row_count * 0.005), max(1, int(row_count * 0.22))
    return int(row_count * 0.01), max(1, int(row_count * 0.3))


class _sigterm_as_keyboard_interrupt:
    """Context manager: while active, SIGTERM raises KeyboardInterrupt.

    Restored on exit. No-op on platforms without ``signal.SIGTERM`` (Windows
    handles signals differently; the default behavior there is fine).
    """

    def __enter__(self) -> _sigterm_as_keyboard_interrupt:
        import contextlib

        self._previous = None
        if hasattr(signal, "SIGTERM"):
            with contextlib.suppress(ValueError, OSError):
                self._previous = signal.signal(signal.SIGTERM, self._handler)
        return self

    def __exit__(self, *exc: object) -> None:
        import contextlib

        if self._previous is not None and hasattr(signal, "SIGTERM"):
            with contextlib.suppress(ValueError, OSError):
                signal.signal(signal.SIGTERM, self._previous)

    @staticmethod
    def _handler(signum: int, frame: object) -> None:
        raise KeyboardInterrupt


def _pick_free_port(host: str, preferred: int, search_window: int = 20) -> int:
    """Return ``preferred`` if free, else the next free port within a small window.

    Bounded search avoids ``demo`` quietly drifting onto an unexpected high
    port. If the user's preferred range is fully occupied we surface that
    rather than guessing.
    """

    for candidate in [preferred, *range(preferred + 1, preferred + 1 + search_window)]:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            try:
                s.bind((host, candidate))
            except OSError:
                continue
            return candidate
    raise click.ClickException(
        f"Couldn't find a free port between {preferred} and {preferred + search_window}. "
        f"Pass --port explicitly."
    )


def _make_handler(directory: Path) -> type[http.server.SimpleHTTPRequestHandler]:
    class Handler(http.server.SimpleHTTPRequestHandler):
        def __init__(self, *args: object, **kwargs: object) -> None:
            super().__init__(*args, directory=str(directory), **kwargs)

        def log_message(self, format: str, *args: object) -> None:  # noqa: A002
            sys.stderr.write(f"{self.address_string()} - {format % args}\n")

    return Handler


class _ReusableTCPServer(socketserver.TCPServer):
    allow_reuse_address = True


def _safe_clear(directory: Path) -> None:
    """Remove children of ``directory`` without nuking the directory itself.

    Refuses to operate above the cwd to avoid disasters from a typo'd
    ``--output``. The check is conservative — if you need to write outside
    cwd, just delete the directory yourself.

    Preserves the ``.cache/`` subdirectory so warehouse enrichment data
    survives a clean rebuild. The cache represents real warehouse facts
    that are independent of the rendered site; wiping it on every build
    would force a re-query even when ``--connection`` isn't passed.
    """

    import os

    cwd = Path.cwd().resolve()
    target = directory.resolve()
    try:
        target.relative_to(cwd)
    except ValueError:
        click.echo(
            f"Refusing to clean {directory}: not under current directory. "
            "Delete manually or use --no-clean.",
            err=True,
        )
        sys.exit(2)

    for entry in directory.iterdir():
        if entry.name == ".cache":
            continue
        if entry.is_dir() and not entry.is_symlink():
            _rmtree(entry)
        else:
            try:
                entry.unlink()
            except OSError:
                os.remove(entry)


def _rmtree(path: Path) -> None:
    import shutil

    shutil.rmtree(path)


if __name__ == "__main__":  # pragma: no cover
    main()
