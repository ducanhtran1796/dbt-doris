{% macro handle_schema_evolution(existing_relation, full_refresh_mode, tmp_relation) %}

{% if existing_relation is not none and not existing_relation.is_view and not full_refresh_mode %}
  {% set tmp_table_sql = sql %}
  {% do adapter.expand_target_column_types(from_relation=tmp_relation, to_relation=existing_relation) %}
  {% set target_columns = adapter.get_columns_in_relation(existing_relation) %}
  {% set tmp_columns = adapter.get_columns_in_relation(tmp_relation) %}

  {# -- Compare the schemas #}
  {% set target_cols_csv = target_columns | map(attribute='name') | join(', ') %}
  {% set tmp_cols_csv = tmp_columns | map(attribute='name') | join(', ') %}
  {% set missing_cols = [] %}
  {% for col in tmp_columns %}
    {% if col.name | lower not in target_columns | map(attribute='name') | map('lower') | list %}
      {% do missing_cols.append(col) %}
    {% endif %}
  {% endfor %}

  {# -- Apply schema changes based on the on_schema_change setting #}
  {% if missing_cols and on_schema_change != 'ignore' %}
    {% if on_schema_change == 'fail' %}
      {% do exceptions.raise_compiler_error('Schema changes detected: ' ~ missing_cols | map(attribute='name') | join(', ') ~ '. Add columns to target or use "ignore" or "append_new_columns" as on_schema_change config.') %}
    {% elif on_schema_change == 'append_new_columns' %}
      {% do log("Adding new columns: " ~ missing_cols | map(attribute='name') | join(', '), info=True) %}
      {% for col in missing_cols %}
        {% set sql_add_col %}
          {{ doris_add_column(existing_relation, col.name, col.data_type) }}
        {% endset %}
        {% do run_query(sql_add_col) %}
      {% endfor %}
    {% endif %}
  {% endif %}
{% endif %}

{% endmacro %}