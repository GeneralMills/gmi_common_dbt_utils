{% macro smart_source(source_name, table_name) %}
  {%- if env_var('DBT_RUN_ENV') | upper == 'DEV' and env_var('DBT_USE_ZDEV_DATASET') | upper == 'TRUE' -%}
    (select * from {{source(source_name,table_name) }} where 1 = 0) as __dbt_source_{{ table_name }} 
  {% else %}
    {{source(source_name, table_name)}}
  {% endif %}
{% endmacro %}
