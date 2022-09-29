{%- macro _get_full_model_name(node_id) -%}
    {%- set node_list = [] -%}
    {%- if node_id.split('.')[0] == 'model' -%}
        {%- set node_list = graph.nodes.values() | selectattr("resource_type", "equalto", "model") -%}
    {%- elif node_id.split('.')[0] == 'source' -%}
        {% set node_list = graph.sources.values() -%}
    {%- endif -%}

    {%- for node in node_list -%}
        {%- if node.unique_id == node_id -%}
            `{{ node.database }}.{{ node.schema }}.{{ node.name }}`
        {%- endif -%}
    {%- endfor -%}
{%- endmacro -%}