{# Validate feature_catalog metadata across all models in the project.

   Runs at compile time via `dbt run-operation feature_catalog__validate`.
   Pure Jinja — no warehouse calls. Catches typos and missing required
   fields before the catalog build step ever runs.

   The Python tool re-validates with Pydantic anyway; this is a fast
   in-dbt smoke test for users who'd rather find issues during dbt parse.
#}

{% macro feature_catalog__validate() %}
    {% set known_table_keys = [
        'is_feature_table', 'version', 'entity', 'grain', 'timestamp_column',
        'freshness', 'owner', 'tags', 'description',
        'definition_version', 'lifecycle', 'replacement'
    ] %}
    {% set known_feature_keys = [
        'is_feature', 'feature_type', 'null_behavior', 'used_by', 'description',
        'definition_version', 'lifecycle', 'replacement'
    ] %}
    {% set known_feature_types = [
        'numeric', 'categorical', 'boolean', 'embedding', 'timestamp', 'text', 'identifier'
    ] %}
    {% set known_null_behaviors = [
        'zero', 'mean', 'propagate', 'error', 'ignore'
    ] %}
    {% set known_lifecycles = ['active', 'preview', 'deprecated'] %}
    {% set known_periods = ['minute', 'hour', 'day'] %}

    {% set errors = [] %}
    {% set table_count = namespace(n=0) %}
    {% set feature_count = namespace(n=0) %}

    {% for node in graph.nodes.values() %}
        {% if node.resource_type == 'model' %}
            {% set fc = (node.meta or {}).get('feature_catalog')
                       or ((node.config or {}).get('meta') or {}).get('feature_catalog') %}
            {% if fc and fc.get('is_feature_table') %}
                {% set table_count.n = table_count.n + 1 %}
                {% for k in fc.keys() %}
                    {% if k not in known_table_keys %}
                        {% do errors.append(node.unique_id ~ ': unknown field `' ~ k ~ '` in feature_catalog') %}
                    {% endif %}
                {% endfor %}
                {% if fc.get('lifecycle') and fc.get('lifecycle') not in known_lifecycles %}
                    {% do errors.append(node.unique_id ~ ': lifecycle must be one of ' ~ known_lifecycles|join(', ')) %}
                {% endif %}
                {% if fc.get('definition_version') is not none and fc.get('definition_version') < 1 %}
                    {% do errors.append(node.unique_id ~ ': definition_version must be >= 1') %}
                {% endif %}
                {% if fc.get('freshness') %}
                    {% set fr = fc.get('freshness') %}
                    {% if not fr.get('warn_after') and not fr.get('error_after') %}
                        {% do errors.append(node.unique_id ~ ': freshness must declare warn_after or error_after') %}
                    {% endif %}
                    {% for thr_key in ['warn_after', 'error_after'] %}
                        {% set thr = fr.get(thr_key) %}
                        {% if thr %}
                            {% if not thr.get('count') or thr.get('count') < 1 %}
                                {% do errors.append(node.unique_id ~ ': freshness.' ~ thr_key ~ '.count must be >= 1') %}
                            {% endif %}
                            {% if thr.get('period') not in known_periods %}
                                {% do errors.append(node.unique_id ~ ': freshness.' ~ thr_key ~ '.period must be one of ' ~ known_periods|join(', ')) %}
                            {% endif %}
                        {% endif %}
                    {% endfor %}
                {% endif %}
                {% for col_name, col in (node.columns or {}).items() %}
                    {% set ffc = (col.meta or {}).get('feature_catalog') %}
                    {% if ffc %}
                        {% if ffc.get('is_feature') %}
                            {% set feature_count.n = feature_count.n + 1 %}
                        {% endif %}
                        {% for k in ffc.keys() %}
                            {% if k not in known_feature_keys %}
                                {% do errors.append(node.unique_id ~ '.' ~ col_name ~ ': unknown field `' ~ k ~ '`') %}
                            {% endif %}
                        {% endfor %}
                        {% if ffc.get('feature_type') and ffc.get('feature_type') not in known_feature_types %}
                            {% do errors.append(node.unique_id ~ '.' ~ col_name ~ ': feature_type must be one of ' ~ known_feature_types|join(', ')) %}
                        {% endif %}
                        {% if ffc.get('null_behavior') and ffc.get('null_behavior') not in known_null_behaviors %}
                            {% do errors.append(node.unique_id ~ '.' ~ col_name ~ ': null_behavior must be one of ' ~ known_null_behaviors|join(', ')) %}
                        {% endif %}
                        {% if ffc.get('lifecycle') and ffc.get('lifecycle') not in known_lifecycles %}
                            {% do errors.append(node.unique_id ~ '.' ~ col_name ~ ': lifecycle must be one of ' ~ known_lifecycles|join(', ')) %}
                        {% endif %}
                        {% if ffc.get('definition_version') is not none and ffc.get('definition_version') < 1 %}
                            {% do errors.append(node.unique_id ~ '.' ~ col_name ~ ': definition_version must be >= 1') %}
                        {% endif %}
                    {% endif %}
                {% endfor %}
            {% endif %}
        {% endif %}
    {% endfor %}

    {% if errors %}
        {% do log('feature_catalog: validation failed with ' ~ errors|length ~ ' error(s):', info=True) %}
        {% for e in errors %}{% do log('  - ' ~ e, info=True) %}{% endfor %}
        {% do exceptions.raise_compiler_error('feature_catalog: invalid metadata. See errors above.') %}
    {% else %}
        {% do log('feature_catalog: ' ~ table_count.n ~ ' feature table(s), ' ~ feature_count.n ~ ' feature(s) — schema valid.', info=True) %}
    {% endif %}
{% endmacro %}
