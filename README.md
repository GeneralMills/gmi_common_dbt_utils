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
   - uses helper macro [generate_schema_name](#generate_schema_name) [(source)](./macros/helpers/generate_schema_name.sql)


### Usage 
#### generate_schema_name

This overwrites the default implementation of generate_schema_name from the core package

```text
{{generate_schema_name('input')}}
```

#### smart_source
This macro helps us keep a check on the bigquery costs and at the same time validate sql queries end to end.
You can use the codegen package to have the script generated and then replace `source` with `smart_source`.
[Link](https://github.com/dbt-labs/dbt-codegen#usage-1) on usage of codegen for generating SQL for a base model

Snippets to generate base mode code for reference

Executing using scratchpad/statement tab in dbtCloud IDE
```
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
Materialized views are powerful but they can be costly, so please consult with the Analytics team if you are thinking about using them. This feature is also in beta with dbt. To use a materialized view, in the the `dbt_project.yml` file, add a `materialized_views` block in the models section, similar to this:

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
  - '{{ save_test_results(results) }}'
```