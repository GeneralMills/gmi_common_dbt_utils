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

- generate_schema_name [source](./macros/generate_schema_name.sql)
- smart_source [](./macros/smart_source.sql)
