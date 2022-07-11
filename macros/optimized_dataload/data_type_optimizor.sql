{% macro data_type_optimizor_v2(table_fqn, optimize_datatypes) %}
  {% do log(table_fqn, info=true) %}
  {{ log('not executed') }}
  {% set project_id = table_fqn.database %}
  {% set dataset_id = table_fqn.schema %}
  {% set table_id = table_fqn.identifier %}

    {% set get_columns_query %}
      SELECT 
        table_catalog, table_schema, table_name, column_name, data_type
      FROM
        {{project_id}}.{{dataset_id}}.INFORMATION_SCHEMA.COLUMNS
      WHERE
        table_name = "{{table_id}}" AND 
        (--data_type LIKE 'BOOL%' OR 
         data_type LIKE 'FLOAT%' OR
         data_type LIKE 'INT%' OR
         data_type = 'NUMERIC'  ) 
    {% endset %}
      SELECT
    {% if execute %}
      {% set results = run_query(get_columns_query) %}
              {% for cols in results %}
                {%- set col_names =  cols[3] -%}
                 {% set pcols =  find_proposed_column_for_numbers(col_names, table_fqn ) %}
                {{log('here for numeric')}}
                {% for c in pcols%}
                  {% if '{{ c[1] }}' != '{{ cols[4] }}' %}
                    {% if '{{ c[1] }}' == 'BOOL' or '{{ c[1] }}' == 'BOOLEAN'  %}
                      {% if '{{ cols[4] }}' == 'NUMERIC' or '{{ cols[4] }}' == 'FLOAT64' or '{{ cols[4] }}' == 'INT64' %}
                          safe_cast(case when safe_cast(`{{ cols['column_name'] }}` as 'INT64') > 0 then 1
                          when safe_cast(`{{ cols['column_name'] }}` as 'INT64') = 0 then 0 
                          else null end as {{ c[1] }} ) as `{{ cols['column_name'] }}` ,
                      {% else %}
                          safe_cast(case when upper(`{{ cols['column_name'] }}`) in ('YES','X','Y') then 1
                          when upper(`{{ cols['column_name'] }}`)  in ('NO', '', 'N') then 0
                          else null end as {{ c[1] }} ) as `{{ cols['column_name'] }}` ,
                      {% endif %}
                    {% elif '{{ c[1] }}' == 'INT64' %}
                          safe_cast(`{{ cols['column_name'] }}` as INT64) as `{{ cols['column_name']}}` ,
                    {% elif '{{ c[1] }}' == 'FLOAT64' %}
                          safe_cast(`{{ cols['column_name'] }}` as FLOAT64) as `{{ cols['column_name']}}` ,
                    {% else %}
                          safe_cast(`{{ cols['column_name'] }}` as {{ c[1] }} ) as `{{ cols['column_name']}}` ,
                    {% endif %}
                  {% else %}
                      `{{ cols['column_name']}}` ,
                  {% endif %}
                {% endfor %}
              {% endfor %}
    {% endif %}

    {% set get_bool_columns_query %}
      SELECT 
        table_catalog, table_schema, table_name, column_name, data_type
      FROM
        {{project_id}}.{{dataset_id}}.INFORMATION_SCHEMA.COLUMNS
      WHERE
       table_name = "{{table_id}}" AND 
       (data_type = 'STRING' and CONTAINS_SUBSTR(column_name, 'flg') = true) 
    {% endset %}

    {% if execute %}
      {% set results = run_query(get_bool_columns_query) %}
              {% for cols in results %}
                {%- set col_names =  cols[3] -%}
                 {% set pcols =  find_proposed_column_for_boolean(col_names, table_fqn ) %}
                {{log('here for bool')}}
                {% for c in pcols%}
                  {% if '{{ c[1] }}' != '{{ cols[4] }}' %}
                    {% if '{{ c[1] }}' == 'BOOL' or '{{ c[1] }}' == 'BOOLEAN'  %}
                      {% if '{{ cols[4] }}' == 'NUMERIC' or '{{ cols[4] }}' == 'FLOAT64' or '{{ cols[4] }}' == 'INT64' %}
                          safe_cast(case when safe_cast(`{{ cols['column_name'] }}` as 'INT64') > 0 then 1
                          when safe_cast(`{{ cols['column_name'] }}` as 'INT64') = 0 then 0 
                          else null end as {{ c[1] }} ) as `{{ cols['column_name'] }}` ,
                      {% else %}
                          safe_cast(case when upper(`{{ cols['column_name'] }}`) in ('YES','X','Y') then 1
                          when upper(`{{ cols['column_name'] }}`)  in ('NO', '', 'N') then 0
                          else null end as {{ c[1] }} ) as `{{ cols['column_name'] }}` ,
                      {% endif %}
                    {% elif '{{ c[1] }}' == 'INT64' %}
                          safe_cast(`{{ cols['column_name'] }}` as INT64) as `{{ cols['column_name']}}` ,
                    {% elif '{{ c[1] }}' == 'FLOAT64' %}
                          safe_cast(`{{ cols['column_name'] }}` as FLOAT64) as `{{ cols['column_name']}}` ,
                    {% else %}
                          safe_cast(`{{ cols['column_name'] }}` as {{ c[1] }} ) as `{{ cols['column_name']}}` ,
                    {% endif %}
                  {% else %}
                      `{{ cols['column_name']}}` ,
                  {% endif %}
                {% endfor %}
              {% endfor %}
    {% endif  %}

    {% set get_remaining_columns_query %}
      SELECT 
        table_catalog, table_schema, table_name, column_name, data_type
      FROM
        {{project_id}}.{{dataset_id}}.INFORMATION_SCHEMA.COLUMNS
      WHERE
       table_name = "{{table_id}}" AND 
       (data_type LIKE 'DATE%' OR 
        data_type LIKE 'BOOL%' OR 
       (data_type = 'STRING' and CONTAINS_SUBSTR(column_name, 'flg') = false))
       
    {% endset %}

    {% if execute %}
      {% set results = run_query(get_remaining_columns_query) %}
              {%- for cols in results -%}
                {{log('here for bool')}}
                    `{{ cols['column_name']}}` ,
              {%- endfor -%}
    {% endif  %}
    FROM
      {{ table_fqn }}
    {% if  optimize_datatypes == 'true' %}
      WHERE false
    {% endif %}
{% endmacro %}