{# Quick typo check for `feature_catalog` metadata, run at dbt-parse time.

   Scope is intentionally narrow: this macro flags unknown fields (typos
   like `is_featur_table` or `featuer_type`) so users see them inside
   `dbt run-operation` instead of having the model silently dropped from
   the catalog later. That's the one thing the Python tool can't catch — a
   typo'd `is_feature_table` means the node never enters the catalog, so
   there's nothing to validate.

   Everything else (enum values, freshness shape, definition_version
   ranges, etc.) is the Python tool's job. Run `dbt-features validate` for
   full Pydantic-backed validation. Keeping the rules in one place
   prevents drift between this macro and the Python schema. #}

{% macro feature_catalog__validate() %}
    {% set known_table_keys = [
        'is_feature_table', 'version', 'entity', 'grain', 'timestamp_column',
        'exclude_columns', 'freshness', 'owner', 'tags', 'description',
        'definition_version', 'lifecycle', 'replacement'
    ] %}
    {% set known_feature_keys = [
        'is_feature', 'feature_type', 'null_behavior', 'used_by', 'description',
        'definition_version', 'lifecycle', 'replacement'
    ] %}

    {% set errors = [] %}
    {% set table_count = namespace(n=0) %}

    {% for node in graph.nodes.values() %}
        {% if node.resource_type == 'model' %}
            {% set fc = (node.meta or {}).get('feature_catalog')
                       or ((node.config or {}).get('meta') or {}).get('feature_catalog') %}
            {% if fc and fc.get('is_feature_table') %}
                {% set table_count.n = table_count.n + 1 %}
                {% for k in fc.keys() %}
                    {% if k not in known_table_keys %}
                        {% do errors.append(node.unique_id ~ ": unknown field `" ~ k ~ "` in feature_catalog. Valid: " ~ known_table_keys|join(", ")) %}
                    {% endif %}
                {% endfor %}
                {% for col_name, col in (node.columns or {}).items() %}
                    {% set ffc = (col.meta or {}).get('feature_catalog') %}
                    {% if ffc %}
                        {% for k in ffc.keys() %}
                            {% if k not in known_feature_keys %}
                                {% do errors.append(node.unique_id ~ "." ~ col_name ~ ": unknown field `" ~ k ~ "`. Valid: " ~ known_feature_keys|join(", ")) %}
                            {% endif %}
                        {% endfor %}
                    {% endif %}
                {% endfor %}
            {% endif %}
        {% endif %}
    {% endfor %}

    {% if errors %}
        {% do log("feature_catalog: " ~ errors|length ~ " typo(s) in metadata:", info=True) %}
        {% for e in errors %}{% do log("  - " ~ e, info=True) %}{% endfor %}
        {% do log("Run `dbt-features validate` for full schema validation.", info=True) %}
        {% do exceptions.raise_compiler_error("feature_catalog: unknown field(s) in feature_catalog metadata.") %}
    {% else %}
        {% do log("feature_catalog: " ~ table_count.n ~ " feature table(s) — no unknown fields.", info=True) %}
        {% do log("For full validation (enum values, freshness shape, etc.), run `dbt-features validate`.", info=True) %}
    {% endif %}
{% endmacro %}
