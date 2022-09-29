{#
 # This overrides the default macro that gathers the metadata necessary for generating dbt docs.
 # The default macro queries the `project.dataset.__TABLES__` metadata table,
 # which requires more elevated permissions than the information schema tables.
 # Our dbt service accounts do not typically have access to query __TABLES__, especially in EDW datasets.
 # This custom implementation uses `project.dataset.INFORMATION_SCHEMA.TABLES` in addition to TABLE_STORAGE instead.

 # ~~~~IMPORTANT~~~~
 # In order to access the table_schema in the process of generating the documenation,
 # you must specify the Big Query region where your data is housed. Add the following two lines to your
 # project variables, substituting the Big Query region of your project.
 # vars:
 # bq_region: 'region-US'

 #
 # Original macro: https://github.com/dbt-labs/dbt-bigquery/blob/main/dbt/include/bigquery/macros/catalog.sql
 # Open issue: https://github.com/dbt-labs/dbt-bigquery/issues/113
-#}

{%- macro bigquery__get_catalog(information_schema, schemas) -%}

  {%- if (schemas | length) == 0 -%}
    {# Hopefully nothing cares about the columns we return when there are no rows #}
    {%- set query  = "select 1 as id limit 0" -%}
  {%- else -%}

  {%- set query -%}
    with tables as (
        select
            table_catalog as table_database,
            table_schema as table_schema,
            table_name as original_table_name,
            concat(table_catalog, '.', table_schema, '.', table_name) as relation_id,
            case table_type
                when "BASE TABLE" then 'table'
                when "VIEW" then 'view'
                else 'external'
            end as table_type,
            REGEXP_CONTAINS(table_name, '^.+[0-9]{8}$') and table_type = "BASE TABLE" as is_date_shard,
            REGEXP_EXTRACT(table_name, '^(.+)[0-9]{8}$') as shard_base_name,
            REGEXP_EXTRACT(table_name, '^.+([0-9]{8})$') as shard_name

        from {{ information_schema.replace(information_schema_view='TABLES') }}
    ),

    table_storage as (
        SELECT
        CONCAT(project_id, '.', TABLE_SCHEMA, '.', table_name) as relation_id,
        TOTAL_ROWS AS row_count,
        TOTAL_LOGICAL_BYTES AS size_bytes
        FROM `{{ var('bq_region') }}.INFORMATION_SCHEMA.TABLE_STORAGE`
    ),

    extracted as (

        select tables.*,
        table_storage.row_count,
        table_storage.size_bytes,
            case
                when is_date_shard then shard_base_name
                else original_table_name
            end as table_name

        from tables LEFT JOIN table_storage ON tables.relation_id = table_storage.relation_id

    ),

    unsharded_tables as (

        select
            table_database,
            table_schema,
            table_name,
            coalesce(table_type, 'external') as table_type,
            is_date_shard,

            struct(
                min(shard_name) as shard_min,
                max(shard_name) as shard_max,
                count(*) as shard_count
            ) as table_shards,

            sum(size_bytes) as size_bytes,
            sum(row_count) as row_count,

            max(relation_id) as relation_id

        from extracted
        group by 1,2,3,4,5

    ),

    info_schema_columns as (

        select
            concat(table_catalog, '.', table_schema, '.', table_name) as relation_id,
            table_catalog as table_database,
            table_schema,
            table_name,

            -- use the "real" column name from the paths query below
            column_name as base_column_name,
            ordinal_position as column_index,

            is_partitioning_column,
            clustering_ordinal_position

        from {{ information_schema.replace(information_schema_view='COLUMNS') }}
        where ordinal_position is not null

    ),

    info_schema_column_paths as (

        select
            concat(table_catalog, '.', table_schema, '.', table_name) as relation_id,
            field_path as column_name,
            data_type as column_type,
            column_name as base_column_name,
            description as column_comment

        from {{ information_schema.replace(information_schema_view='COLUMN_FIELD_PATHS') }}

    ),

    columns as (

        select * except (base_column_name)
        from info_schema_columns
        join info_schema_column_paths using (relation_id, base_column_name)

    ),

    column_stats as (

        select
            table_database,
            table_schema,
            table_name,
            max(relation_id) as relation_id,
            max(case when is_partitioning_column = 'YES' then 1 else 0 end) = 1 as is_partitioned,
            max(case when is_partitioning_column = 'YES' then column_name else null end) as partition_column,
            max(case when clustering_ordinal_position is not null then 1 else 0 end) = 1 as is_clustered,
            array_to_string(
                array_agg(
                    case
                        when clustering_ordinal_position is not null then column_name
                        else null
                    end ignore nulls
                    order by clustering_ordinal_position
                ), ', '
            ) as clustering_columns

        from columns
        group by 1,2,3

    )

    select
        unsharded_tables.table_database,
        unsharded_tables.table_schema,
        case
            when is_date_shard then concat(unsharded_tables.table_name, '*')
            else unsharded_tables.table_name
        end as table_name,
        unsharded_tables.table_type,

        -- coalesce name and type for External tables - these columns are not
        -- present in the COLUMN_FIELD_PATHS resultset
        coalesce(columns.column_name, '<unknown>') as column_name,
        -- invent a row number to account for nested fields -- BQ does
        -- not treat these nested properties as independent fields
        row_number() over (
            partition by relation_id
            order by columns.column_index, columns.column_name
        ) as column_index,
        coalesce(columns.column_type, '<unknown>') as column_type,
        columns.column_comment,

        'Shard count' as `stats__date_shards__label`,
        table_shards.shard_count as `stats__date_shards__value`,
        'The number of date shards in this table' as `stats__date_shards__description`,
        is_date_shard as `stats__date_shards__include`,

        'Shard (min)' as `stats__date_shard_min__label`,
        table_shards.shard_min as `stats__date_shard_min__value`,
        'The first date shard in this table' as `stats__date_shard_min__description`,
        is_date_shard as `stats__date_shard_min__include`,

        'Shard (max)' as `stats__date_shard_max__label`,
        table_shards.shard_max as `stats__date_shard_max__value`,
        'The last date shard in this table' as `stats__date_shard_max__description`,
        is_date_shard as `stats__date_shard_max__include`,

        '# Rows' as `stats__num_rows__label`,
        row_count as `stats__num_rows__value`,
        'Approximate count of rows in this table' as `stats__num_rows__description`,
        (unsharded_tables.table_type = 'table') as `stats__num_rows__include`,

        'Approximate Size' as `stats__num_bytes__label`,
        size_bytes as `stats__num_bytes__value`,
        'Approximate size of table as reported by BigQuery' as `stats__num_bytes__description`,
        (unsharded_tables.table_type = 'table') as `stats__num_bytes__include`,

        'Partitioned By' as `stats__partitioning_type__label`,
        partition_column as `stats__partitioning_type__value`,
        'The partitioning column for this table' as `stats__partitioning_type__description`,
        is_partitioned as `stats__partitioning_type__include`,

        'Clustered By' as `stats__clustering_fields__label`,
        clustering_columns as `stats__clustering_fields__value`,
        'The clustering columns for this table' as `stats__clustering_fields__description`,
        is_clustered as `stats__clustering_fields__include`

    -- join using relation_id (an actual relation, not a shard prefix) to make
    -- sure that column metadata is picked up through the join. This will only
    -- return the column information for the "max" table in a date-sharded table set
    from unsharded_tables
    left join columns using (relation_id)
    left join column_stats using (relation_id)
  {%- endset -%}

  {%- endif -%}

  {{ return(run_query(query)) }}

{%- endmacro %}
