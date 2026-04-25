from __future__ import annotations

import json
from pathlib import Path

from click.testing import CliRunner

from dbt_features.cli import main


def test_build_command(project_dir: Path, tmp_path: Path) -> None:
    runner = CliRunner()
    out = tmp_path / "out"
    result = runner.invoke(
        main, ["build", "--project-dir", str(project_dir), "--output", str(out)]
    )
    assert result.exit_code == 0, result.output
    assert (out / "index.html").exists()
    assert "feature group" in result.output


def test_validate_command_ok(project_dir: Path) -> None:
    runner = CliRunner()
    result = runner.invoke(main, ["validate", "--project-dir", str(project_dir)])
    assert result.exit_code == 0, result.output
    assert "OK" in result.output


def test_validate_command_no_features(tmp_path: Path) -> None:
    target = tmp_path / "target"
    target.mkdir()
    (target / "manifest.json").write_text(
        json.dumps({"metadata": {"project_name": "x"}, "nodes": {}, "sources": {}})
    )
    runner = CliRunner()
    result = runner.invoke(main, ["validate", "--project-dir", str(tmp_path)])
    assert result.exit_code == 0
    assert "No feature tables found" in result.output


def test_validate_command_schema_error(tmp_path: Path) -> None:
    bad = {
        "metadata": {"project_name": "x"},
        "nodes": {
            "model.x.bad": {
                "name": "bad",
                "unique_id": "model.x.bad",
                "resource_type": "model",
                "package_name": "x",
                "path": "bad.sql",
                "original_file_path": "models/bad.sql",
                "schema": "s",
                "database": "d",
                "config": {"materialized": "view"},
                "meta": {"feature_catalog": {"is_feature_table": True, "freshness": {}}},
                "depends_on": {"nodes": []},
                "columns": {}
            }
        },
        "sources": {}
    }
    target = tmp_path / "target"
    target.mkdir()
    (target / "manifest.json").write_text(json.dumps(bad))

    runner = CliRunner()
    result = runner.invoke(main, ["validate", "--project-dir", str(tmp_path)])
    assert result.exit_code != 0
    assert "model.x.bad" in result.output
    assert "freshness" in result.output


def test_build_missing_manifest(tmp_path: Path) -> None:
    runner = CliRunner()
    result = runner.invoke(
        main, ["build", "--project-dir", str(tmp_path), "--output", str(tmp_path / "out")]
    )
    assert result.exit_code != 0
    assert "manifest.json" in result.output


def test_build_with_explicit_paths(
    project_dir: Path, manifest_path: Path, catalog_path: Path, tmp_path: Path
) -> None:
    runner = CliRunner()
    out = tmp_path / "out2"
    result = runner.invoke(
        main,
        [
            "build",
            "--project-dir",
            str(project_dir),
            "--manifest",
            str(manifest_path),
            "--catalog",
            str(catalog_path),
            "--output",
            str(out),
        ],
    )
    assert result.exit_code == 0, result.output
    assert (out / "index.html").exists()


def test_build_clean_overwrites_existing(project_dir: Path, tmp_path: Path) -> None:
    runner = CliRunner()
    out = tmp_path / "out"
    out.mkdir()
    stale = out / "stale.html"
    stale.write_text("stale")

    # CliRunner runs with cwd=tmp_path equivalent? It uses isolated_filesystem if asked.
    # We need cwd to be a parent of out; use isolated_filesystem to avoid clobber refusal.
    with runner.isolated_filesystem(temp_dir=tmp_path):
        local_out = Path("out")
        local_out.mkdir()
        (local_out / "stale.html").write_text("stale")
        result = runner.invoke(
            main,
            ["build", "--project-dir", str(project_dir), "--output", str(local_out)],
        )
        assert result.exit_code == 0, result.output
        assert not (local_out / "stale.html").exists()
        assert (local_out / "index.html").exists()


def test_version_flag() -> None:
    runner = CliRunner()
    result = runner.invoke(main, ["--version"])
    assert result.exit_code == 0
    assert "dbt-features" in result.output


def test_demo_synthesizes_enrichment() -> None:
    """The demo command must produce realistic-looking warehouse stats
    so screenshots show the freshness UI."""

    from dbt_features.cli import _synthesize_demo_enrichment
    from dbt_features.demo import demo_manifest_path
    from dbt_features.parser import parse_project

    cat = parse_project(demo_manifest_path().parent.parent, manifest_path=demo_manifest_path())
    enrichment = _synthesize_demo_enrichment(cat)

    # One snapshot per group
    assert set(enrichment.keys()) == {g.unique_id for g in cat.feature_groups}
    # Each snapshot has plausible content
    for g in cat.feature_groups:
        snap = enrichment[g.unique_id]
        assert snap.row_count > 0
        # All feature columns covered
        for f in g.features:
            assert f.name in snap.columns


def test_demo_data_is_valid() -> None:
    """The bundled demo manifest must always parse cleanly. If this breaks,
    `dbt-features demo` is broken for every user."""

    from dbt_features.demo import demo_manifest_path
    from dbt_features.parser import parse_project
    from dbt_features.schema import Lifecycle

    path = demo_manifest_path()
    assert path.exists()
    cat = parse_project(path.parent.parent, manifest_path=path)
    assert len(cat.feature_groups) >= 2
    assert cat.feature_count >= 5
    # At least one group has freshness, one has multi-domain tags, etc.
    assert any(g.freshness is not None for g in cat.feature_groups)
    assert {"customer", "store"}.issubset(set(cat.all_tags))
    # Demo exercises lifecycle + version fields so the rendered site shows them off.
    assert any(g.lifecycle == Lifecycle.PREVIEW for g in cat.feature_groups)
    all_features = [f for g in cat.feature_groups for f in g.features]
    assert any(f.lifecycle == Lifecycle.DEPRECATED for f in all_features)
    assert any(f.definition_version > 1 for f in all_features)


def test_demo_command_builds_and_serves(monkeypatch: object, tmp_path: Path) -> None:
    """End-to-end: invoke `demo`, intercept the server start, verify build happened."""

    import dbt_features.cli as cli_mod

    started: dict[str, object] = {}
    opened: dict[str, str] = {}

    class FakeServer:
        def __init__(self, addr: tuple[str, int], handler_cls: object) -> None:
            self.addr = addr
            self.handler_cls = handler_cls

        def __enter__(self) -> FakeServer:
            started["addr"] = self.addr
            return self

        def __exit__(self, *exc: object) -> None:
            pass

        def serve_forever(self) -> None:
            # Don't actually serve — pretend Ctrl-C immediately so the cmd returns.
            raise KeyboardInterrupt

        def shutdown(self) -> None:
            started["shutdown"] = True

    def fake_open(url: str) -> bool:
        opened["url"] = url
        return True

    monkeypatch.setattr(cli_mod, "_ReusableTCPServer", FakeServer)
    monkeypatch.setattr(cli_mod.webbrowser, "open", fake_open)
    # Make the browser-open timer fire synchronously so the test sees it.
    monkeypatch.setattr(cli_mod.threading, "Timer", _SyncTimer)

    runner = CliRunner()
    result = runner.invoke(main, ["demo", "--port", "0", "--host", "127.0.0.1"])
    # Port 0 lets the OS pick — _pick_free_port handles that fine.
    assert result.exit_code == 0, result.output
    assert "Demo catalog ready" in result.output
    assert started, "server was never started"
    assert opened.get("url", "").startswith("http://127.0.0.1:")


def test_demo_command_no_open(monkeypatch: object) -> None:
    import dbt_features.cli as cli_mod

    opened: dict[str, str] = {}

    class FakeServer:
        def __init__(self, *_: object, **__: object) -> None: ...
        def __enter__(self) -> FakeServer: return self
        def __exit__(self, *exc: object) -> None: ...
        def serve_forever(self) -> None: raise KeyboardInterrupt
        def shutdown(self) -> None: ...

    def fake_open(url: str) -> bool:
        opened["url"] = url  # pragma: no cover - should not be called
        return True

    monkeypatch.setattr(cli_mod, "_ReusableTCPServer", FakeServer)
    monkeypatch.setattr(cli_mod.webbrowser, "open", fake_open)
    monkeypatch.setattr(cli_mod.threading, "Timer", _SyncTimer)

    runner = CliRunner()
    result = runner.invoke(main, ["demo", "--port", "0", "--no-open"])
    assert result.exit_code == 0, result.output
    assert "url" not in opened


class _SyncTimer:
    """Stand-in for ``threading.Timer`` that fires synchronously when ``start`` is called."""

    def __init__(self, _delay: float, fn: object) -> None:
        self._fn = fn

    def start(self) -> None:
        self._fn()  # type: ignore[operator]
