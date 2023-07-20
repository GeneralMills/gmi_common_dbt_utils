{#-
  This macro overrides the builtin generate_schema_name macro for
  determining schema (dataset) names. This override is necessary to
  make zdev datasets work for testing.

  zdev datasets work by prepending zdevXX_ to your existing dataset name
  during developer testing (when DBT_USE_ZDEV_DATASET is set to 'TRUE'),
  where XX is '00' through '09'. For example, if DBT_DEVELOPER_DATASET_NUMBER
  is set to '02', and the original dataset name is 'processed', the dataset
  name will become 'zdev02_processed' during developer testing.

  Environment variables:
  DBT_USE_ZDEV_DATASET - 'TRUE' to enable zdevXX_ prepending, 'FALSE' to disable.
  DBT_DEVELOPER_DATASET_NUMBER - Set to 00 - 09. Used to determine zdev dataset
                                 number. i.e. 00 = 'zdev00_*'.

  Global variables:
  zdev_disable_project_ids - List of gcp project ids to disable zdev logic on.
-#}
{% macro generate_schema_name(custom_schema_name, node) -%}
    {%- set developer_dataset_number = env_var('DBT_DEVELOPER_DATASET_NUMBER') -%}
    {%- set use_zdev = env_var('DBT_USE_ZDEV_DATASET') -%}
    {%- set default_schema = target.schema -%}
    {%- if node.database in var('zdev_disable_project_ids', []) -%}
        {#- If database (gcp_project_id) is in disable list, do not mess with value -#}
        {{ custom_schema_name | trim }}
    {%- elif use_zdev | upper  == 'TRUE' and custom_schema_name is not none -%}
        {#- If DBT_USE_ZDEV_DATASET is true, prepend zdevXX_ -#}
        {{ default_schema }}{{ developer_dataset_number }}_{{ custom_schema_name | trim }}
    {%- else -%}
        {#- Else just return naked custom value value -#}
        {{ custom_schema_name | trim }}
    {%- endif -%}
{%- endmacro %}