{% macro handle_schema_evolution(existing_relation, full_refresh_mode, tmp_relation, on_schema_change) %}

{% if existing_relation is not none and not existing_relation.is_view and not full_refresh_mode %}
  {% set tmp_table_sql = sql %}
  {% set on_schema_change = incremental_validate_on_schema_change(config.get('on_schema_change'), default='ignore') %}
  {% do adapter.expand_target_column_types(from_relation=tmp_relation, to_relation=existing_relation) %}
  {% set target_columns = adapter.get_columns_in_relation(existing_relation) %}
  {% set tmp_columns = adapter.get_columns_in_relation(tmp_relation) %}

  {# -- Compare the schemas #}
  {% set target_cols_csv = target_columns | map(attribute='column') | join(', ') %}
  {% set tmp_cols_csv = tmp_columns | map(attribute='column') | join(', ') %}

  {% set missing_cols = [] %}
  {% set changed_cols = [] %}

  {# -- Identify missing columns in the target relation #}
  {% for col in tmp_columns %}
    {% if col.column | lower not in target_columns | map(attribute='column') | map('lower') | list %}
      {% do missing_cols.append(col) %}
    {% endif %}
  {% endfor %}

  {# -- Check for changed column types #}
  {% for col in tmp_columns %}
    {% for target_col in target_columns %}
      {% if (target_col.column|lower) == (col.column|lower) %}
        {% if target_col.dtype != col.dtype %}
          {% do changed_cols.append({'column': col.column, 'from_type': target_col.dtype, 'to_type': col.dtype}) %}
          {% break %}
        {% endif %}
      {% endif %}
    {% endfor %}
  {% endfor %}

  {# -- Apply schema changes based on the on_schema_change setting #}
  {% if missing_cols and on_schema_change != 'ignore' %}
    {% if on_schema_change == 'fail' %}
      {% do exceptions.raise_compiler_error('Schema changes detected: ' ~ missing_cols | map(attribute='column') | join(', ') ~ '. Add columns to target or use "ignore" or "append_new_columns" as on_schema_change config.') %}
    {% elif on_schema_change == 'append_new_columns' %}
      {% do log("Adding new columns: " ~ missing_cols | map(attribute='column') | join(', '), info=True) %}
      {% for col in missing_cols %}
        {% set sql_add_col %}
          {{ doris_add_column(existing_relation, col.column, col.data_type) }}
        {% endset %}
        {% do run_query(sql_add_col) %}
      {% endfor %}
    {% endif %}
  {% endif %}

  {% if changed_cols and on_schema_change != 'ignore' %}
    {% if on_schema_change == 'fail' %}
      {% do exceptions.raise_compiler_error('Schema changes detected: ' ~ changed_cols | map(attribute='column') | join(', ') ~ '. Alter columns to target or use "ignore" or "append_new_columns" as on_schema_change config.') %}
    {% elif on_schema_change == 'append_new_columns' %}
      {% do log("Alter columns: " ~ changed_cols | map(attribute='column') | join(', '), info=True) %}
      {% for col in changed_cols %}
        {% set sql_alter_col %}
          {{ doris_alter_column_type(existing_relation, col.column, col.to_type) }}
        {% endset %}
        {% do run_query(sql_alter_col) %}
      {% endfor %}
    {% endif %}
  {% endif %}
{% endif %}

{% endmacro %}