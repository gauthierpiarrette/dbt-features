"""BigQuery adapter via ``google-cloud-bigquery``.

BigQuery is the odd one out in this set: it uses backtick-quoted
identifiers (not double-quoted), it speaks Standard SQL with subtle
differences (e.g. ``COUNT_DISTINCT`` is just ``COUNT(DISTINCT)``, fine),
and its Python client uses Job objects rather than DBAPI cursors. So we
write the queries inline here rather than reusing ``_dbapi.run_group_query``.

Auth modes shipped:
- **Application Default Credentials** (``method: oauth`` / unset). Picks up
  ``GOOGLE_APPLICATION_CREDENTIALS`` or ``gcloud auth application-default``.
- **Service-account key file** (``method: service-account``, ``keyfile``).
- **Inline service-account JSON** (``method: service-account-json``,
  ``keyfile_json``).

Other modes (oauth-secrets, external-oauth) involve token-refresh dances
the Google SDK can do but require additional config; deferred to v0.2.
"""

from __future__ import annotations

import contextlib
from datetime import datetime, timezone
from typing import TYPE_CHECKING, Any

from dbt_features.enrichment.exceptions import EnrichmentError
from dbt_features.enrichment.models import ColumnStats, FreshnessSnapshot

if TYPE_CHECKING:
    from dbt_features.catalog import FeatureGroup


class BigQueryAdapter:
    def __init__(self, profile: dict[str, Any]):
        try:
            from google.cloud import bigquery
        except ImportError as e:
            raise EnrichmentError(
                "BigQuery driver not installed. "
                "Install with: pip install dbt-features[bigquery]"
            ) from e

        project = profile.get("project")
        if not project:
            raise EnrichmentError("BigQuery profile missing 'project'.")

        method = (profile.get("method") or "oauth").lower()
        try:
            if method in ("oauth", "default", ""):
                # Application Default Credentials (gcloud auth, GAE/GKE
                # metadata server, GOOGLE_APPLICATION_CREDENTIALS, etc.)
                self._client = bigquery.Client(project=project)
            elif method == "service-account":
                keyfile = profile.get("keyfile")
                if not keyfile:
                    raise EnrichmentError(
                        "BigQuery method='service-account' requires 'keyfile'."
                    )
                self._client = bigquery.Client.from_service_account_json(
                    keyfile, project=project
                )
            elif method == "service-account-json":
                keyfile_json = profile.get("keyfile_json")
                if not keyfile_json:
                    raise EnrichmentError(
                        "BigQuery method='service-account-json' requires 'keyfile_json'."
                    )
                self._client = self._client_from_json_dict(bigquery, project, keyfile_json)
            else:
                raise EnrichmentError(
                    f"Unsupported BigQuery auth method: {method!r}. "
                    "v0.1 supports: oauth, service-account, service-account-json."
                )
        except EnrichmentError:
            raise
        except Exception as e:  # noqa: BLE001
            raise EnrichmentError(f"Could not initialize BigQuery client: {e}") from e

        # ``dataset`` is dbt-bigquery's name for what most warehouses call
        # "schema". Tolerate both.
        self._default_schema = profile.get("dataset") or profile.get("schema")
        if not self._default_schema:
            raise EnrichmentError("BigQuery profile missing 'dataset' (or 'schema').")
        self._default_project = project
        self._timeout = float(
            profile.get("_enrichment_timeout")
            or profile.get("timeout_seconds")
            or 120
        )

    @staticmethod
    def _client_from_json_dict(bigquery_module: Any, project: str, keyfile_json: Any) -> Any:
        """Build a BigQuery client from an inline service-account JSON dict.

        ``keyfile_json`` may arrive as either a dict (from YAML) or a JSON
        string. Both are reasonable and we don't want to make users guess.
        """

        from google.oauth2 import service_account

        if isinstance(keyfile_json, str):
            import json as _json

            info = _json.loads(keyfile_json)
        else:
            info = dict(keyfile_json)
        creds = service_account.Credentials.from_service_account_info(info)
        return bigquery_module.Client(project=project, credentials=creds)

    def fetch_group_stats(self, group: FeatureGroup) -> FreshnessSnapshot:
        queried_at = datetime.now(timezone.utc)
        try:
            fqn = self._fqn(group)
            ts_col = group.timestamp_column

            if ts_col:
                row = self._fetch_one(
                    f"SELECT MAX(`{_quote_inner(ts_col)}`) AS mx, COUNT(*) AS cnt FROM {fqn}"
                )
                max_ts_raw, row_count = row["mx"], row["cnt"]
            else:
                row = self._fetch_one(f"SELECT COUNT(*) AS cnt FROM {fqn}")
                max_ts_raw, row_count = None, row["cnt"]

            columns: dict[str, ColumnStats] = {}
            feature_cols = [f.name for f in group.features]
            if feature_cols and row_count and row_count > 0:
                clauses: list[str] = []
                for col in feature_cols:
                    safe = _quote_inner(col)
                    clauses.append(
                        f"COUNT(*) - COUNT(`{safe}`) AS `{safe}__nulls`, "
                        f"COUNT(DISTINCT `{safe}`) AS `{safe}__distinct`"
                    )
                stats_row = self._fetch_one(f"SELECT {', '.join(clauses)} FROM {fqn}")
                for col in feature_cols:
                    columns[col] = ColumnStats(
                        null_count=_int_or_none(stats_row.get(f"{col}__nulls")),
                        distinct_count=_int_or_none(stats_row.get(f"{col}__distinct")),
                    )

            return FreshnessSnapshot(
                queried_at=queried_at,
                max_timestamp=_coerce_dt(max_ts_raw),
                row_count=_int_or_none(row_count),
                columns=columns,
            )
        except Exception as e:  # noqa: BLE001
            return FreshnessSnapshot(queried_at=queried_at, error=f"{type(e).__name__}: {e}")

    def close(self) -> None:
        with contextlib.suppress(Exception):
            self._client.close()

    def _fetch_one(self, sql: str) -> dict[str, Any]:
        """Run ``sql`` and return the first row as a name->value dict."""

        job = self._client.query(sql)
        rows = list(job.result(timeout=self._timeout))
        if not rows:
            raise RuntimeError("query returned no rows")
        # ``Row`` has dict-like access; ``.items()`` gives the field name/value.
        return dict(rows[0].items())

    def _fqn(self, group: FeatureGroup) -> str:
        """Build a backtick-quoted ``project.dataset.table`` reference.

        BigQuery requires the fully-qualified form for cross-dataset
        queries, and accepts it for single-dataset queries too — so we
        always emit three parts.
        """

        project = group.database or self._default_project
        dataset = group.schema_name or self._default_schema
        return (
            f"`{_quote_inner(project)}`."
            f"`{_quote_inner(dataset)}`."
            f"`{_quote_inner(group.name)}`"
        )


def _quote_inner(identifier: str) -> str:
    """Escape backticks in a BigQuery identifier.

    BigQuery doesn't support escaping backticks inside backtick-quoted
    identifiers (unlike SQL standard double-quoted ones). The legal way
    to handle them is to reject the name. In practice no real dbt model
    has a backtick in its name, so we strip them as a defensive measure
    rather than raise mid-query.
    """

    return identifier.replace("`", "")


def _int_or_none(value: object) -> int | None:
    if value is None:
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _coerce_dt(value: object) -> datetime | None:
    """BigQuery returns timezone-aware ``datetime`` for ``TIMESTAMP`` and
    naive for ``DATETIME``. Both get normalized to UTC-aware here."""

    if value is None:
        return None
    if isinstance(value, datetime):
        return value if value.tzinfo else value.replace(tzinfo=timezone.utc)
    from datetime import date as _date

    if isinstance(value, _date):
        return datetime(value.year, value.month, value.day, tzinfo=timezone.utc)
    return None
