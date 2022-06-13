{% macro smart_source(source_name, table_name) %}
  {%- if env_var('DBT_RUN_ENV') == 'DEV' -%}
    (select * from {{source(source_name,table_name) }} where 1 = 0) as __dbt_source_{{ table_name }} 
  {% else %}
    {{source(source_name, table_name)}}
  {% endif %}
{% endmacro %}
