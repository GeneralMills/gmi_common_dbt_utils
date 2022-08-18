# gmi_common_dbt_utils

This dbt package contains macros which can reused across General Mills dbt projects

## Installation Instructions

To add this package into your dbt project you need to make an entry in the packages.yml file if its not already present 

```yml
  - git: "https://github.com/GeneralMills/gmi_common_dbt_utils.git"
    revision: main # use a branch or a tag name
```

## Contents

### Macros

- [generate_schema_name](#generate_schema_name) [(source)](./macros/generate_schema_name.sql)
- [smart_source](#smart_source) [(source)](./macros/smart_source.sql)
- [optimized_data_loader](#optimized_data_loader) [(source)](./macros/optimized_data_loader.sql)
- [data_type_optimizor_v2](#data_type_optimizor) [(source)](./macros/data_type_optimizor.sql) (to be used as internal macro)
- [find_proposed_column_for_numbers](#find_proposed_column_for_numbers) [(source)](./macros/optimized_dataload/data_type_optimization_helper.sql) (to be used as internal macro)
- [find_proposed_column_for_boolean](#find_proposed_column_for_boolean) [(source)](./macros/optimized_dataload/data_type_optimization_helper.sql) (to be used as internal macro)

### Usage 
#### generate_schema_name

This overwrites the default implementation of generate_schema_name from the core package

```text
{{generate_schema_name('input')}}
```

#### smart_source
This macro helps use keep a check on the bigquery costs and at the same time validate sql queries end to end.
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

#### optimized_data_loader

This macro is helpful to begin the optimized data load in the table it is designed in such a way that it also calls the data_type_optimizer when required initially.

Executing using scrachpad/statement tab in dbtCloud IDE

```sql
select * from ({{ optimized_data_loader (source('<source_name>', '<table_name>')) }})
```

#### data_type_optimizor_v2

This macro is helpful to create the optimized table .

Executing using scrachpad/statement tab in dbtCloud IDE

```sql
    select * from ({{ data_type_optimizor_v2 (source('<source_name>', '<table_name>'), 'false'  ) }})
```