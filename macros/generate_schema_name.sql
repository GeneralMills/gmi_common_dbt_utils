{% macro generate_schema_name(custom_schema_name, node) -%}
    {%- set developer_dataset_number = env_var('DBT_DEVELOPER_DATASET_NUMBER') -%}
    {%- set use_zdev = env_var('DBT_USE_ZDEV_DATASET') -%}
    {%- set default_schema = target.schema -%}
    {%- if env_var('DBT_USE_ZDEV_DATASET') | upper  == 'TRUE' and custom_schema_name is not none -%}
        {{ default_schema }}{{ developer_dataset_number }}_{{ custom_schema_name | trim }}
    {%- else -%}
        {{ custom_schema_name | trim }}
    {%- endif -%}
{%- endmacro %}