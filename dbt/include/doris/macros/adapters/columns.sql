{% macro doris__get_columns_in_relation(relation) -%}
    {% call statement('get_columns_in_relation', fetch_result=True) %}
        select column_name              as `column`,
       data_type                as 'dtype',
       character_maximum_length as char_size,
       numeric_precision,
       numeric_scale
from information_schema.columns
where table_schema = '{{ relation.schema }}'
  and table_name = '{{ relation.identifier }}'
    {% endcall %}
    {% set table = load_result('get_columns_in_relation').table %}
    {{ return(sql_convert_columns_in_relation(table)) }}
{%- endmacro %}

{% macro columns_and_constraints(table_type="table") %}
  {# loop through user_provided_columns to create DDL with data types and constraints #}
    {%- set raw_column_constraints = adapter.render_raw_columns_constraints(raw_columns=model['columns']) -%}
    {% for c in raw_column_constraints -%}
      {% if table_type == "table" %}
        {{ c.get_table_column_constraint() }}{{ "," if not loop.last or raw_model_constraints }}
      {% else %}
        {{ c.get_view_column_constraint() }}{{ "," if not loop.last or raw_model_constraints }}
      {% endif %}
    {% endfor %}
{% endmacro %}

{% macro doris__get_table_columns_and_constraints() -%}
  {{ return(columns_and_constraints("table")) }}
{%- endmacro %}


{% macro doris__get_view_columns_comment() -%}
  {{ return(columns_and_constraints("view")) }}
{%- endmacro %}

{% macro doris_alter_column_type(relation, column_name, new_column_type) %}
{% set sql %}
ALTER TABLE {{ relation }} MODIFY COLUMN {{ column_name }} {{ new_column_type }};
{% endset %}
{% do run_query(sql) %}
{% do log("Column '" ~ column_name ~ "' type changed to '" ~ new_column_type ~ "' in " ~ relation, info=true) %}
{% endmacro %}

{% macro doris_add_column(relation, column_name, column_type, column_position='', column_default='', column_comment='') %}
{% set sql %}
ALTER TABLE {{ relation }} ADD COLUMN {{ column_name }} {{ column_type }}
  {% if column_default != '' %}DEFAULT {{ column_default }}{% endif %}
  {% if column_comment != '' %}COMMENT '{{ column_comment }}'{% endif %}
    {% if column_position != '' %}{{ column_position }}{% endif %};
{% endset %}
{% do run_query(sql) %}
{% do log("Column '" ~ column_name ~ "' added to " ~ relation, info=true) %}
{% endmacro %}

{% macro doris_drop_column(relation, column_name) %}
{% set sql %}
ALTER TABLE {{ relation }} DROP COLUMN {{ column_name }};
{% endset %}
{% do run_query(sql) %}
{% do log("Column '" ~ column_name ~ "' dropped from " ~ relation, info=true) %}
{% endmacro %}

{% macro doris_rename_column(relation, old_column_name, new_column_name) %}
{% set sql %}
ALTER TABLE {{ relation }} RENAME COLUMN {{ old_column_name }} {{ new_column_name }};
{% endset %}
{% do run_query(sql) %}
{% do log("Column '" ~ old_column_name ~ "' renamed to '" ~ new_column_name ~ "' in " ~ relation, info=true) %}
{% endmacro %}


{% macro doris__alter_column_type(relation, column_name, new_column_type) %}
{{ doris_alter_column_type(relation, column_name, new_column_type) }}
{% endmacro %}

{% macro doris__add_column(relation, column_name, column_type) %}
{{ doris_add_column(relation, column_name, column_type) }}
{% endmacro %}

{% macro doris__drop_column(relation, column_name) %}
{{ doris_drop_column(relation, column_name) }}
{% endmacro %}

{% macro doris__rename_column(relation, old_column_name, new_column_name) %}
{{ doris_rename_column(relation, old_column_name, new_column_name) }}
{% endmacro %}