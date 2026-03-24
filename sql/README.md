# SQL

SQL queries, transformations, and models for Snowflake and Databricks.

## Suggested layout

```
sql/
├── snowflake/    # Snowflake-specific queries and DDL
├── databricks/   # Databricks SQL / Delta Lake queries
└── dbt/          # dbt models and macros
```

## Notes

- Use `.sql` extension for raw queries
- Use `.yml` for dbt schema definitions
