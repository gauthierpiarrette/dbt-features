"""Read dbt's ``profiles.yml`` to extract warehouse connection config.

We parse profiles.yml ourselves rather than importing dbt-core so the
base install stays lean. Trade-off: we don't render Jinja-templated values
inside profiles (e.g. ``{{ env_var('PG_HOST') }}``) the way dbt does
natively — but we do support the ``env_var`` form because almost every
real profile uses it for credentials.
"""

from __future__ import annotations

import os
import re
from pathlib import Path
from typing import Any

from dbt_features.enrichment.exceptions import EnrichmentError


def default_profiles_dir() -> Path:
    """Return ``$DBT_PROFILES_DIR`` if set, otherwise ``~/.dbt``.

    Matches dbt-core's resolution rules so users don't have to think about
    it.
    """

    env = os.environ.get("DBT_PROFILES_DIR")
    if env:
        return Path(env).expanduser()
    return Path.home() / ".dbt"


def load_profile(
    profile_name: str,
    target: str | None = None,
    profiles_dir: Path | None = None,
) -> dict[str, Any]:
    """Load and resolve one target out of profiles.yml.

    Returns the rendered ``outputs.<target>`` dict (the connection config).
    The ``type`` field selects the warehouse adapter; everything else is
    adapter-specific.
    """

    try:
        import yaml
    except ImportError as e:  # pragma: no cover - guarded at install time
        raise EnrichmentError(
            "PyYAML is required to read profiles.yml. "
            "Install with: pip install dbt-features[duckdb] (or any other warehouse extra)"
        ) from e

    profiles_dir = profiles_dir or default_profiles_dir()
    profiles_path = profiles_dir / "profiles.yml"
    if not profiles_path.exists():
        raise EnrichmentError(
            f"profiles.yml not found at {profiles_path}. "
            f"Set DBT_PROFILES_DIR or pass --profiles-dir."
        )

    try:
        with profiles_path.open("r", encoding="utf-8") as f:
            profiles = yaml.safe_load(f) or {}
    except yaml.YAMLError as e:
        raise EnrichmentError(f"Could not parse {profiles_path}: {e}") from e

    if profile_name not in profiles:
        available = sorted(k for k in profiles if k != "config")
        raise EnrichmentError(
            f"Profile '{profile_name}' not found in {profiles_path}. "
            f"Available: {', '.join(available) or '(none)'}"
        )

    profile = profiles[profile_name]
    outputs = profile.get("outputs") or {}
    chosen_target = target or profile.get("target")
    if not chosen_target:
        raise EnrichmentError(
            f"No target specified and profile '{profile_name}' has no default."
        )
    if chosen_target not in outputs:
        raise EnrichmentError(
            f"Target '{chosen_target}' not found in profile '{profile_name}'. "
            f"Available: {', '.join(sorted(outputs)) or '(none)'}"
        )

    raw = dict(outputs[chosen_target])
    if "type" not in raw:
        raise EnrichmentError(
            f"profile '{profile_name}' target '{chosen_target}' missing required 'type' field."
        )

    return _render_env_vars(raw)


_ENV_VAR_RE = re.compile(
    r"""\{\{\s*env_var\(\s*['"]([A-Z0-9_]+)['"](?:\s*,\s*['"]([^'"]*)['"])?\s*\)\s*\}\}""",
    re.IGNORECASE,
)


def _render_env_vars(value: Any) -> Any:
    """Recursively resolve ``{{ env_var('FOO') }}`` references.

    Mirrors dbt's ``env_var`` Jinja helper for the only Jinja form we
    actually see in practice. Anything fancier (other Jinja, ``var()``,
    secrets manager) is left untouched and the user gets a clear error
    when the adapter rejects it.
    """

    if isinstance(value, str):
        def replace(match: re.Match[str]) -> str:
            name = match.group(1)
            default = match.group(2)
            v = os.environ.get(name)
            if v is None:
                if default is None:
                    raise EnrichmentError(
                        f"profiles.yml references env_var('{name}') but {name} is not set."
                    )
                return default
            return v

        return _ENV_VAR_RE.sub(replace, value)
    if isinstance(value, dict):
        return {k: _render_env_vars(v) for k, v in value.items()}
    if isinstance(value, list):
        return [_render_env_vars(v) for v in value]
    return value
