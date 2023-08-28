# gmi_common_dbt_utils

This dbt package contains macros which can reused across General Mills dbt projects

## Installation Instructions

To add this package into your dbt project you need to make an entry in the packages.yml file if its not already present

```yml
  - git: "https://github.com/GeneralMills/gmi_common_dbt_utils.git"
    revision: main # use a branch or a tag name
```

## How to update this repo

To update this repo, you will need write access to the General Mills public repo. Once your changes have been peer-reviewed and approved, you will need to create a new release for the repo.

## Contents

### Macros

- [generate_schema_name](#generate_schema_name) [(source)](./macros/generate_schema_name.sql)
- [smart_source](#smart_source) [(source)](./macros/smart_source.sql)
- [materialized_views](#materialized_views) [(source)](./macros/bigquery)
- [save_test_results](#save_test_results) [(source)](./macros/save_test_results.sql)
- [big_query_catalog_macro](#big-query-catalog-macro) [(source)](./macros/bq_catalog)
- [not_null_constraint](#not_null-constraint) [(source)](./macros/bigquery)

### Usage

#### generate_schema_name

This overwrites the default implementation of generate_schema_name from the core package

```text
{{generate_schema_name('input')}}
```

To use, add a macro to your macros directory with the following contents:

```jinja
{% macro generate_schema_name(custom_schema_name, node) -%}
    {{ gmi_common_dbt_utils.generate_schema_name(custom_schema_name, node) }}
{%- endmacro %}
```

#### smart_source

This macro helps us keep a check on the bigquery costs and at the same time validate sql queries end to end.
You can use the codegen package to have the script generated and then replace `source` with `smart_source`.
[Link](https://github.com/dbt-labs/dbt-codegen#usage-1) on usage of codegen for generating SQL for a base model

> **_NOTE:_**  With gmi_common_dbt_utils >= 0.7.0 to populate the tables in the development environment in standard datasets
please add an environment variable `DBT_POPULATE_DEV_TABLES` and set it to `TRUE` if the environment variable
does not exist it will continue with the default behavior. The `DBT_POPULATE_DEV_TABLES` is not mandatory
it is only required if you want to populate your development environment with actual data using the smart_source macro.
The macro assumes the default value of `DBT_POPULATE_DEV_TABLES` as `FALSE` if it does not exist

Snippets to generate base mode code for reference

Executing using scratchpad/statement tab in dbtCloud IDE

```jinja
{{ codegen.generate_base_model(
    source_name='<source_name>',
    table_name='<table_name>'
) }}
```

Executing the macro as an operation

```
dbt run-operation generate_base_model --args '{"source_name": "<source_name>", "table_name": "<table_name>"}'
```

On the generated sql, replace `source` with `smart_source`

```sql
with source as (
    select * from {{ smart_source('<source_name>', '<table_name>') }}
),

renamed as (
    select 
    .
    .
    .
    from source
),

select * from renamed
```

#### materialized_views

Materialized views are powerful but they can be costly, so please consult with the Analytics team if you are thinking about using them. This feature is also in _beta_ with dbt. To use a materialized view, in the the `dbt_project.yml` file, add a `materialized_views` block in the models section, similar to this:

    output: 
      +materialized: table
      +schema: output
      materialized_views: 
        +materialized: materialized_view
        +schema: output

#### save_test_results

This macro saves dbt data quality test results to a table in the target project's processed dataset: `processed.dbt_test_results`. This is an append-only table that associates each data quality check/result to a particular dbt run. For development runs (zdev), a separate results table will be created in the corresponding zdev processed dataset.

Runs that are associated with a dbt Cloud job will be associated with their corresponding `DBT_CLOUD_RUN_ID`, while runs that were kicked off from the CLI are associated with their `invocation_id` (since they are not given a cloud run id).

To use this macro within a project, include the following in the `dbt_project.yml`:

```yml
# SQL statements to be executed after the completion of a run, build, test, etc.
# Full documentation: https://docs.getdbt.com/reference/project-configs/on-run-start-on-run-end
on-run-end:
  - '{{ gmi_common_dbt_utils.save_test_results(results) }}'
```

#### Big Query Catalog Macro

The `bq_catalog.sql` Macro overrides the default macro that gathers the metadata necessary for generating dbt docs.
The default macro queries the `project.dataset.__TABLES__` metadata table,
which requires more elevated permissions than the information schema tables.
Our dbt service accounts do not typically have access to query `__TABLES__`, especially in EDW datasets.
This custom implementation uses `project.dataset.INFORMATION_SCHEMA.TABLES` in addition to TABLE_STORAGE instead.

> **_NOTE:_**
In order to access the table_schema in the process of generating the documenation,
you must specify the Big Query region where your data is housed. Add the following two lines to your
project variables, substituting the Big Query region of your project.
vars:
bq_region: 'region-US'

#### not_null constraint

The purpose of this macro is to require a column _not_ to be `NULLABLE`. (By default, if the mode is not specified BigQuery defaults to `NULLABLE`.)

This feature is also in _beta_ with dbt. More documentation from dbt is available [here](https://gist.github.com/sungchun12/f7ea081773ae824a83294649530d6e41).

To utilize this, you must add a `config` at the top of your model (`.sql` file), like this:

```jinja
{{
  config(
    materialized = "table_with_constraints"
  )
}}
```

Then in the .yml file, you _must_ specify every column. Each time fields are added or removed after this, the `yml` file must be updated, or the run will fail.

```yml
version: 2

models:
  - name: table_constraints_demo
    config:
      has_constraints: true
    columns:
      - name: id
        data_type: int64
        description: I want to describe this one, but I don't want to list all the columns
        meta:
          constraint: not null
      - name: color
        data_type: string
      - name: date_day
        data_type: date
```
