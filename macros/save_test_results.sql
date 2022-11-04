{% macro save_test_results(results) %}

{%- set test_results = [] -%}

{%- for result in results -%}
    {%- if result.node.resource_type == 'test' -%}
        {%- do test_results.append(result) -%}
    {%- endif -%}
{%- endfor -%}

{%- set results_tbl -%}
    `{{ target.database }}.{{ gmi_common_dbt_utils.generate_schema_name('processed') }}.dbt_test_results`
{%- endset -%}

{{ log('Centralizing test data in ' + results_tbl, info = true) if execute }}

create table if not exists {{ results_tbl }} (
    test_id string,
    test_name string,
    project_name string,
    target_db string,
    dbt_run_env string,
    test_severity string,
    test_result string,
    test_models string,
    execution_time_seconds string,
    dbt_cloud_run_id string,
    create_update_ts timestamp
)
cluster by dbt_cloud_run_id
;

-- alter statements allow backward compatibility
alter table {{ results_tbl }}
add column if not exists test_short_name string
;
alter table {{ results_tbl }}
add column if not exists test_type string
;
alter table {{ results_tbl }}
add column if not exists file_test_defined string
;

{% if test_results|length > 0 %}
    insert into {{ results_tbl }} (
        {% for result in test_results %}

            {%- set test_short_name='' -%}
            {%- set test_type='' -%}
            {%- if result.node.test_metadata is defined -%}
                {%- set test_short_name = result.node.test_metadata.name -%}
                {%- set test_type='generic' -%}
            {%- elif result.node.name is defined -%}
                {%- set test_short_name = result.node.name -%}
                {%- set test_type='singular' -%}
            {%- endif -%}

            select
                '{{ result.node.unique_id }}' as test_id,
                '{{ result.node.name }}' as test_name,
                '{{ project_name }}' as project_name,
                '{{ target.database }}' as target_db,
                '{{ env_var("DBT_RUN_ENV") }}' as dbt_run_env,
                '{{ result.node.config.severity }}' as test_severity,
                '{{ result.status }}' as test_result,
                '{% for node_id in result.node.depends_on.nodes -%}
                    {{ gmi_common_dbt_utils.get_full_model_name(node_id) }}
                    {%- if not loop.last -%},{%- endif -%}
                {%- endfor %}' as test_models,
                '{{ result.execution_time }}' as execution_time_seconds,
                '{{ env_var("DBT_CLOUD_RUN_ID", invocation_id) }}' as dbt_cloud_run_id,
                current_timestamp() as create_update_ts.
                '{{ test_short_name }}' as test_short_name,
                '{{ test_type }}' as test_type,
                '{{ result.node.original_file_path }}' as file_test_defined
            {{ 'union all' if not loop.last }}
        {% endfor %}
    );
{% endif %}

{% endmacro %}
