{% macro optimized_data_loader(table_fqn) %}
  {% do log(table_fqn, info=true) %}
  {{ log('not executed') }}
  {% set project_id = table_fqn.database %}
  {% set dataset_id = table_fqn.schema %}
  {% set table_id = table_fqn.identifier %}
  {% set list1 = project_id.split('-') %}
  {% set source_project =  list1[0] %}
  {% set sproject_id = target.database %}
  {% set sdataset_id = generate_schema_name() + 'input' %}
  {% set stable_id =  'dbt_' +  source_project + '_'  + dataset_id + '_' + table_id %}
    {% set get_columns_query %}
      WITH
        src AS (
                SELECT
                  1 AS id,
                  src.*
                FROM
                  `{{ project_id }}.{{ dataset_id }}..INFORMATION_SCHEMA.COLUMNS` src
                WHERE
                  src.table_name = '{{ table_id}}' ),
        tgt AS (
                SELECT
                  1 AS id,
                  tgt.column_name AS tgt_column_name,
                  tgt.data_type AS tgt_data_type
                FROM
                  `{{ sproject_id }}.{{ sdataset_id }}..INFORMATION_SCHEMA.COLUMNS` tgt
                WHERE
                  tgt.table_name = '{{ stable_id }}' )
          SELECT
            src.table_catalog,
            src.table_schema,
            src.column_name,
            tgt.tgt_data_type proposed_data_type,
            src.data_type original_data_type
          FROM
            src
              INNER JOIN
            tgt
          ON
            src.id = tgt.id
            AND src.column_name = tgt.tgt_column_name
    {% endset %}

    {% if execute %}
      {% set results = run_query(get_columns_query) %}
      {%set cmdresultscount %}    
        SELECT count(1) from ({{ get_columns_query }})
      {% endset %}
      {% set results_count = run_query(cmdresultscount) %}
      
        {% if results_count[0][0] > 1  %} 
          SELECT
          {% for cols in results %}
              {% set p_data_type = cols['proposed_data_type'] %}
              {% set o_data_type = cols['original_data_type'] %}
              {% if  p_data_type  !=  o_data_type  %}
                  {% if p_data_type == 'BOOL' or p_data_type == 'BOOLEAN'  %}
                      {% if o_data_type == 'NUMERIC' or o_data_type == 'FLOAT64' or o_data_type == 'INT64' %}
                          safe_cast(case when safe_cast(`{{ cols['column_name'] }}` as 'INT64') > 0 then 1
                          when safe_cast(`{{ cols['column_name'] }}` as 'INT64') = 0 then 0 
                          else null end as {{ p_data_type }} ) as `{{ cols['column_name'] }}` ,
                      {% else %}
                          safe_cast(case when upper(`{{ cols['column_name'] }}`) in ('YES','X','Y') then 1
                          when upper(`{{ cols['column_name'] }}`)  in ('NO', '', 'N') then 0
                          else null end as {{ p_data_type }} ) as `{{ cols['column_name'] }}` ,
                      {% endif %}
                  {% elif p_data_type == 'INT64' %}
                      safe_cast(`{{ cols['column_name'] }}` as INT64) as `{{ cols['column_name']}}` ,
                  {% elif p_data_type == 'FLOAT64' %}
                      safe_cast(`{{ cols['column_name'] }}` as FLOAT64) as `{{ cols['column_name']}}` ,
                  {% else %}
                      safe_cast(`{{ cols['column_name'] }}` as {{ p_data_type}}) as `{{ cols['column_name']}}` ,
                  {% endif %}
              {% else %}
                  {{ cols['column_name'] }} ,
              {% endif %} 
          {% endfor %}
          FROM
              {{ table_fqn }}
        {% else %}
          {{ data_type_optimizor_v2 (table_fqn, 'false'  ) }}
        {% endif %}
    {% endif %}
{% endmacro %}
