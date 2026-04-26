"""Snowflake adapter via ``snowflake-connector-python``.

The Snowflake connector implements DBAPI 2.0, so we reuse the shared
``_dbapi.run_group_query`` helper. The complication is auth — Snowflake
supports many modes (password, key-pair, SSO, OAuth, JWT). We ship support
for the two that cover the vast majority of dbt-snowflake users:

- **Password** (``user`` + ``password``)
- **Key-pair** (``user`` + ``private_key_path`` [+ ``private_key_passphrase``])

Other modes (external-browser SSO, OAuth) route through the underlying
driver via ``authenticator``/``token`` passthrough — we accept them but
don't reimplement the auth dance ourselves.
"""

from __future__ import annotations

import contextlib
from datetime import datetime, timezone
from typing import TYPE_CHECKING, Any

from dbt_features.enrichment.adapters._dbapi import run_group_query
from dbt_features.enrichment.exceptions import EnrichmentError
from dbt_features.enrichment.models import FreshnessSnapshot

if TYPE_CHECKING:
    from dbt_features.catalog import FeatureGroup


class SnowflakeAdapter:
    def __init__(self, profile: dict[str, Any]):
        try:
            import snowflake.connector as sf
        except ImportError as e:
            raise EnrichmentError(
                "Snowflake driver not installed. "
                "Install with: pip install dbt-features[snowflake]"
            ) from e

        account = profile.get("account")
        if not account:
            raise EnrichmentError("Snowflake profile missing 'account'.")
        user = profile.get("user")
        if not user:
            raise EnrichmentError("Snowflake profile missing 'user'.")
        database = profile.get("database")
        if not database:
            raise EnrichmentError("Snowflake profile missing 'database'.")

        connect_kwargs: dict[str, Any] = {
            "account": account,
            "user": user,
            "database": database,
            # warehouse + role + schema are recommended but not strictly
            # required (Snowflake will use account defaults if missing).
            "warehouse": profile.get("warehouse"),
            "role": profile.get("role"),
            "schema": profile.get("schema"),
            "login_timeout": int(
                profile.get("_enrichment_timeout")
                or profile.get("login_timeout")
                or 120
            ),
            # Tag every query so DBAs can spot us in QUERY_HISTORY.
            "session_parameters": {
                "QUERY_TAG": profile.get("query_tag", "dbt-features-enrichment"),
            },
        }

        # Auth resolution. Order matters: explicit authenticator > key-pair
        # > password. We never silently combine them (e.g., password + key
        # is ambiguous and probably a misconfiguration).
        if profile.get("authenticator"):
            # SSO / OAuth / external browser — pass through and trust the
            # driver to do the right thing.
            connect_kwargs["authenticator"] = profile["authenticator"]
            if profile.get("token"):
                connect_kwargs["token"] = profile["token"]
            if profile.get("password"):
                # Some authenticator modes (oauth) want a token via password.
                connect_kwargs["password"] = profile["password"]
        elif profile.get("private_key_path") or profile.get("private_key"):
            connect_kwargs.update(_resolve_key_pair_auth(profile))
        elif profile.get("password"):
            connect_kwargs["password"] = profile["password"]
        else:
            raise EnrichmentError(
                "Snowflake profile has no auth configured. "
                "Set 'password', 'private_key_path', or 'authenticator'."
            )

        try:
            self._conn = sf.connect(**{k: v for k, v in connect_kwargs.items() if v is not None})
        except Exception as e:  # noqa: BLE001 - any driver-internal error is fatal at setup
            raise EnrichmentError(f"Could not connect to Snowflake: {e}") from e

        # Snowflake auto-commits by default. Be explicit so we never sit on
        # an open transaction if a future change introduces one.
        with contextlib.suppress(Exception):
            self._conn.autocommit(True)

        self._default_schema = profile.get("schema") or "PUBLIC"
        self._default_database = database

    def fetch_group_stats(self, group: FeatureGroup) -> FreshnessSnapshot:
        queried_at = datetime.now(timezone.utc)
        cursor = None
        try:
            cursor = self._conn.cursor()
            max_ts_raw, row_count, columns = run_group_query(
                cursor,
                group,
                default_schema=self._default_schema,
                default_database=self._default_database,
            )
            return FreshnessSnapshot(
                queried_at=queried_at,
                max_timestamp=_coerce_dt(max_ts_raw),
                row_count=row_count,
                columns=columns,
            )
        except Exception as e:  # noqa: BLE001
            return FreshnessSnapshot(queried_at=queried_at, error=f"{type(e).__name__}: {e}")
        finally:
            if cursor is not None:
                with contextlib.suppress(Exception):
                    cursor.close()

    def close(self) -> None:
        with contextlib.suppress(Exception):
            self._conn.close()


def _resolve_key_pair_auth(profile: dict[str, Any]) -> dict[str, Any]:
    """Translate dbt's key-pair fields into the connector's expected form.

    dbt-snowflake accepts:
      - ``private_key_path`` — path to the .p8 file
      - ``private_key`` — the PEM bytes inline (less common in practice)
      - ``private_key_passphrase`` — optional

    The Snowflake connector wants ``private_key`` as DER bytes. We let
    ``cryptography`` (a transitive dep of snowflake-connector) do the
    parsing so we don't reimplement PKCS8 handling.
    """

    try:
        from cryptography.hazmat.backends import default_backend
        from cryptography.hazmat.primitives import serialization
    except ImportError as e:  # pragma: no cover - cryptography is a transitive dep
        raise EnrichmentError(
            "Key-pair auth requires the `cryptography` package "
            "(usually pulled in by snowflake-connector-python)."
        ) from e

    passphrase = profile.get("private_key_passphrase")
    pem_bytes: bytes
    if profile.get("private_key_path"):
        with open(profile["private_key_path"], "rb") as f:
            pem_bytes = f.read()
    else:
        raw = profile["private_key"]
        pem_bytes = raw.encode("utf-8") if isinstance(raw, str) else raw

    try:
        key = serialization.load_pem_private_key(
            pem_bytes,
            password=passphrase.encode("utf-8") if passphrase else None,
            backend=default_backend(),
        )
    except Exception as e:  # noqa: BLE001
        raise EnrichmentError(f"Could not load Snowflake private key: {e}") from e

    der = key.private_bytes(
        encoding=serialization.Encoding.DER,
        format=serialization.PrivateFormat.PKCS8,
        encryption_algorithm=serialization.NoEncryption(),
    )
    return {"private_key": der}


def _coerce_dt(value: object) -> datetime | None:
    """Snowflake's connector returns timezone-aware ``datetime`` for
    ``timestamp_tz`` / ``timestamp_ltz`` and naive for ``timestamp_ntz``.
    Normalize everything to aware UTC."""

    if value is None:
        return None
    if isinstance(value, datetime):
        return value if value.tzinfo else value.replace(tzinfo=timezone.utc)
    from datetime import date as _date

    if isinstance(value, _date):
        return datetime(value.year, value.month, value.day, tzinfo=timezone.utc)
    return None
