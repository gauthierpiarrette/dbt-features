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
def build(
    project_dir: Path,
    manifest_path: Path | None,
    catalog_path: Path | None,
    output: Path,
    clean: bool,
) -> None:
    """Build a static feature catalog site from your dbt artifacts."""

    try:
        catalog = parse_project(project_dir, manifest_path=manifest_path, catalog_path=catalog_path)
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

    render_catalog(catalog, output)

    click.echo(
        f"Built catalog: {len(catalog.feature_groups)} feature group(s), "
        f"{catalog.feature_count} feature(s) → {output}/index.html"
    )


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
        render_catalog(catalog, site)

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
