# Sigma Sandpit

> **Proof of concept.** This is a personal sandpit — the projects here are reference implementations shared to demonstrate approaches and give others something to extrapolate from, not finished, supported, or authoritative tools. Take the ideas, adapt the patterns, build your own.

> **Disclaimer:** This repository contains personal scripts and tools written independently by the author. Although the author is employed by Sigma Computing, this work is not created, endorsed, tested, or supported by Sigma Computing in any capacity. These scripts are provided as-is, with no warranty or guarantee of fitness for any purpose. Use at your own risk. For official Sigma Computing documentation, support, and tooling, refer to [Sigma's official documentation](https://help.sigmacomputing.com).

A sandpit for Sigma Computing related projects, focused on Databricks and Snowflake integrations.

## Structure

```
sigma_sandpit/
├── DataSetMigrateHelper_SF/   # Snowflake — dataset dependency graph and workbook migration analysis
├── DataModelGraph_SF/         # Snowflake — data model dependency graph
├── writeback_info_sf/         # Snowflake — writeback (input table) inventory and cleanup toolkit
├── writeback_info_dbx/        # Databricks — writeback (input table) inventory and cleanup toolkit
└── sigma_skills/              # Claude Code skills for Sigma work (proof-of-concept reference implementations)
```

## Projects

### [DataSetMigrateHelper_SF](DataSetMigrateHelper_SF/README.md)
Snowflake-native toolkit for mapping Sigma dataset dependency chains and workbook source relationships to support the Dataset → Data Model migration. Includes Snowflake stored procedures, recursive SQL analysis queries, crossover (fork/merge point) analysis, and Sigma-ready views for graph visualisation and chain filtering.

### [DataModelGraph_SF](DataModelGraph_SF/README.md)
Snowflake-native toolkit for mapping the Sigma data model dependency graph. Discovers how data models source from each other, classifies them as ROOT / INTERNAL / LEAF, and captures the warehouse connection each model sources from. Designed as a complement to DataSetMigrateHelper_SF once datasets have been migrated.

### [writeback_info_sf](writeback_info_sf/README.md)
Snowflake port of the writeback_info_dbx toolkit. Inventories and monitors Sigma writeback (input table) activity using Snowflake-native connectors and WAL watermark-based change detection.

### [writeback_info_dbx](writeback_info_dbx/README.md)
Databricks toolkit for inventorying and monitoring Sigma writeback (input table) activity. Maps every active WAL table to its Sigma workbook or data model, enriches records with Delta metadata and Sigma API ownership data, and populates `SIGDS_WORKBOOK_MAP` for reporting and cleanup planning.

### [sigma_skills](sigma_skills/)
Reusable [Claude Code](https://claude.com/claude-code) skills for Sigma Computing work, shared as proof-of-concept reference implementations to extrapolate from. Currently includes [sigma-model-sql-rls-audit](sigma_skills/sigma-model-sql-rls-audit/README.md) — audits the strength of row-level security implemented in Sigma data-model Custom SQL by checking how well each block is scoped by a user attribute.

## Platforms

- [Sigma Computing](https://sigmacomputing.com)
- [Databricks](https://databricks.com)
- [Snowflake](https://snowflake.com)
