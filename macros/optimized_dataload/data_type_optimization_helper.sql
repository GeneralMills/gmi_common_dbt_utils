{% macro find_proposed_column_for_numbers(column_name, table_fqn) %}
    {%set find_proposed_column_qry%}
        {% set project_id = table_fqn.database %}
        {% set dataset_id = table_fqn.schema %}
        {% set table_id = table_fqn.identifier %}

        SELECT
            '{{column_name}}' as `Column_Name`,
            (CASE
            WHEN {{column_name}}_supports_boolean_fg = 1 
                  AND {{column_name}}_min_value IN (0, 1)
                  AND {{column_name}}_max_value IN (0, 1) THEN 'BOOLEAN'
            WHEN {{column_name}}_supports_int64_fg = {{column_name}}_not_missing_cnt 
                OR
                {{column_name}}_unique_values_cnt = 0
            THEN 'INT64'
            WHEN {{column_name}}_supports_float64_fg = {{column_name}}_not_missing_cnt 
            THEN 'FLOAT64'
            WHEN {{column_name}}_supports_numeric_fg = {{column_name}}_not_missing_cnt 
            THEN 'NUMERIC'
            END
            ) AS proposed_data_type
        FROM (
            SELECT
                '{{column_name}}' AS column_name_to_be_updated,
                sum(CASE
                    WHEN {{column_name}} IS NOT NULL THEN 1
                    ELSE 0 END 
                    ) AS {{column_name}}_not_missing_cnt,
                count(DISTINCT {{column_name}}) AS {{column_name}}_unique_values_cnt,
                min({{column_name}}) AS {{column_name}}_min_value,
                max({{column_name}}) AS {{column_name}}_max_value,
                (CASE
                    WHEN (count(distinct {{column_name}}) = 2) 
                    AND CONTAINS_SUBSTR('{{column_name}}', 'flg') = true THEN 1
                ELSE 0 END
                ) AS {{column_name}}_supports_boolean_fg,
                sum(CASE
                    WHEN safe_cast({{column_name}} AS NUMERIC) = {{column_name}} THEN 1
                    ELSE 0 END
                ) AS {{column_name}}_supports_numeric_fg,
                sum(CASE
                    WHEN safe_cast({{column_name}} AS FLOAT64) = {{column_name}} THEN 1
                    ELSE 0 END
                ) AS {{column_name}}_supports_float64_fg,
                sum(CASE
                    WHEN safe_cast({{column_name}} AS INT64) = {{column_name}} THEN 1
                    ELSE 0 END
                ) AS {{column_name}}_supports_int64_fg
            FROM {{project_id}}.{{dataset_id}}.{{table_id}}
            )
    {% endset %} 
    {% set proposed_columns = run_query(find_proposed_column_qry) %}
    {{ return (proposed_columns)}}
{% endmacro %}

{% macro find_proposed_column_for_boolean(column_name, table_fqn)%}
  {%set find_proposed_column_qry%}
        {% set project_id = table_fqn.database %}
        {% set dataset_id = table_fqn.schema %}
        {% set table_id = table_fqn.identifier %}

        SELECT
            '{{column_name}}' as `Column_Name`,
            (CASE
            WHEN supports_boolean_fg = 1 
                  AND upper(min_value) IN ('', 'X')
                  AND upper(max_value) IN ('', 'X') THEN 'BOOLEAN'
            WHEN supports_boolean_fg = 1
                  AND upper(min_value) IN ('', 'Y')
                  AND upper(max_value) IN ('', 'Y') THEN 'BOOLEAN'
            WHEN supports_boolean_fg = 1
                  AND upper(min_value) IN ('Y', 'N')
                  AND upper(max_value) IN ('Y', 'N') THEN 'BOOLEAN'
            WHEN supports_boolean_fg = 1
                  AND upper(min_value) IN ('0', '1')
                  AND upper(max_value) IN ('0', '1') THEN 'BOOLEAN'
            WHEN supports_boolean_fg = 1
                  AND upper(min_value) IN ('YES', 'NO')
                  AND upper(max_value) IN ('NO', 'YES') THEN 'BOOLEAN' 
            WHEN supports_boolean_fg = 1
                  AND upper(min_value) IN ('TRUE', 'FALES')
                  AND upper(max_value) IN ('FALSE', 'TRUE') THEN 'BOOLEAN'
            ELSE 'STRING'
            END
            ) AS proposed_data_type
            , min_value
            , max_value
        FROM (
            SELECT
                '{{column_name}}' AS column_name_to_be_updated,
                sum(CASE
                    WHEN {{column_name}} IS NOT NULL THEN 1
                    ELSE 0 END 
                    ) AS not_missing_cnt,
                count(DISTINCT {{column_name}}) AS unique_values_cnt,
                min({{column_name}}) AS min_value,
                max({{column_name}}) AS max_value,
                (CASE
                    WHEN count(distinct {{column_name}}) = 2 THEN 1
                ELSE 0 END
                ) AS supports_boolean_fg
            FROM {{project_id}}.{{dataset_id}}.{{table_id}}
            )
    {% endset %} 
    {% set proposed_columns = run_query(find_proposed_column_qry) %}
    {{ return (proposed_columns)}}
{% endmacro %}
