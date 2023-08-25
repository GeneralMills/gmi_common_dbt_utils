{% macro smart_source(source_name, table_name) %}
  {%- set dbt_populate_dev_tables = env_var('DBT_POPULATE_DEV_TABLES','FALSE') -%}
  {%- if env_var('DBT_RUN_ENV') == 'DEV' -%}
    {%- if dbt_populate_dev_tables | upper == 'TRUE'-%}
      {{source(source_name, table_name)}}
    {% else %}
      (select * from {{source(source_name,table_name) }} where 1 = 0) as __dbt_source_{{ table_name }} 
    {% endif %}
  {% else %}
    {{source(source_name, table_name)}}
  {% endif %}
	