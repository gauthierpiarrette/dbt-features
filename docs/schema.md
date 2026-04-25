# Metadata schema

Mark dbt models as feature tables under `meta.feature_catalog`. Once a
table is opted in, every column on it is a feature unless excluded -
column blocks are pure overrides.

```yaml
version: 2

models:
  - name: customer_features_daily
    description: Daily booking-related features per customer
    meta:
      feature_catalog:
        is_feature_table: true
        entity: customer_id
        grain: [feature_date, customer_id]
        timestamp_column: feature_date
        freshness:
          warn_after: { count: 36, period: hour }
          error_after: { count: 48, period: hour }
        owner: growth-team@company.com
        tags: [customer, daily]

    columns:
      - name: feature_date           # excluded automatically (timestamp_column + grain)
        description: Date the features are computed for
      - name: customer_id            # excluded automatically (entity + grain)
        description: Customer identifier
      - name: orders_count_7d        # auto-included; type inferred as numeric
        description: Count of orders in the trailing 7 days
        meta:
          feature_catalog:
            null_behavior: zero
            used_by: [churn_model_v2, ltv_model_v3]
      - name: is_repeat_customer     # auto-included; type inferred as boolean
        description: Whether the customer ordered more than once
      - name: preferred_category     # VARCHAR - inference can't decide; override
        description: Most-purchased category in the trailing 30 days
        meta:
          feature_catalog:
            feature_type: categorical
```

## How columns are picked up

A column is published to the catalog iff:

1. The model is marked `is_feature_table: true`, **and**
2. The column is **not** in `entity`, `grain`, `timestamp_column`, or
   `exclude_columns`, **and**
3. Its column block does not set `is_feature: false`.

The `feature_type` falls back to inference from the warehouse `data_type`
when not declared (`INT`/`FLOAT`/`DECIMAL` -> `numeric`, `BOOL` ->
`boolean`, `DATE`/`TIMESTAMP` -> `timestamp`, `ARRAY`/`VECTOR` ->
`embedding`). `VARCHAR`/`TEXT` are intentionally left unspecified -
override when you need `categorical` or `text`.

## Model-level fields (`meta.feature_catalog`)

| Field                | Type            | Required | Notes                                                          |
|----------------------|-----------------|----------|----------------------------------------------------------------|
| `is_feature_table`   | bool            | yes      | Must be `true` to be picked up.                                |
| `entity`             | str \| str[]    | rec.     | Entity column(s) - the "who/what" the features describe. Auto-excluded from features. |
| `grain`              | str[]           | rec.     | Grain columns. Typically entity + timestamp. Auto-excluded from features. |
| `timestamp_column`   | str             | rec.     | Anchor for time-relative reasoning. Auto-excluded from features. |
| `exclude_columns`    | str[]           | no       | Additional columns to skip (e.g., `_loaded_at`, `_batch_id`).  |
| `freshness`          | object          | no       | `warn_after` / `error_after` (same shape as dbt source freshness). |
| `owner`              | str             | no       | Email or team name.                                            |
| `tags`               | str[]           | no       | For grouping / search.                                         |
| `description`        | str             | no       | Falls back to dbt model description.                           |
| `definition_version` | int             | no       | Bumped when the table's semantic definition changes. Defaults to `1`. |
| `lifecycle`          | enum            | no       | `active` (default), `preview`, `deprecated`.                   |
| `replacement`        | str             | no       | Name of the replacement table - most useful with `lifecycle: deprecated`. |
| `version`            | str             | no       | Metadata schema version. Defaults to `"0.2"`. Reserved for future use. |

## Column-level fields (`columns[].meta.feature_catalog`)

Column blocks are **overrides**. Their absence is *not* an opt-out -
non-key columns become features automatically.

| Field                | Type     | Required | Notes                                                                |
|----------------------|----------|----------|----------------------------------------------------------------------|
| `is_feature`         | bool     | no       | Defaults to `true`. Set `false` to exclude this single column.       |
| `feature_type`       | enum     | rec.     | `numeric`, `categorical`, `boolean`, `embedding`, `timestamp`, `text`, `identifier`. Inferred from `data_type` when omitted. |
| `null_behavior`      | enum     | no       | `zero`, `mean`, `propagate`, `error`, `ignore`. Documentation only.  |
| `used_by`            | str[]    | no       | Models / systems consuming this feature (manual list for v0.2).      |
| `description`        | str      | no       | Falls back to dbt column description.                                |
| `definition_version` | int      | no       | Bumped when the feature's semantic definition changes. Defaults to `1`. |
| `lifecycle`          | enum     | no       | `active` (default), `preview`, `deprecated`.                         |
| `replacement`        | str      | no       | Name of the replacement feature - most useful with `lifecycle: deprecated`. |

The schema is validated by Pydantic. Unknown fields are rejected - typos
become errors instead of silently dropped data.
