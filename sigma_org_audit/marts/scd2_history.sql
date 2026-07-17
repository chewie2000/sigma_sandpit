-- ==============================================================================
-- scd2_history.sql
--
-- Type-2 (SCD2) history for sigma_org_audit marts.
--
-- A single generic procedure builds and maintains an SCD2 history table for any
-- stage view. It introspects the source view's columns, hashes the tracked
-- columns, closes changed current rows, and inserts new versions -- so every
-- object accrues a full version history with VALID_FROM / VALID_TO / IS_CURRENT.
--
-- Because the raw layer is append-only, an SCD2 table is disposable: drop it and
-- replay sigma_scd2_apply across snapshots in SNAPSHOT_TS order to rebuild it.
--
-- The source view must expose a stable key column and a SNAPSHOT_TS column
-- (every STG_* view does). SNAPSHOT_TS / SNAPSHOT_ID and any VARIANT payload
-- columns are excluded from the change hash; everything else is tracked.
--
-- Multi-org: because the proc introspects the source view's columns, ORG_ID is
-- carried into the history table and tracked in the change hash automatically --
-- no parameter needed. Sigma object IDs are globally unique, so the business key
-- alone is a safe SCD2 key across orgs; ORG_ID rides along as a tracked column.
--
-- Parameters
--   SOURCE_VIEW   -- stage view name, e.g. 'STG_WORKBOOKS'
--   TARGET_TABLE  -- history table name, e.g. 'SCD2_WORKBOOKS'
--   KEY_COLUMN    -- business key, e.g. 'WORKBOOK_ID'
--
-- Example
--   CALL sigma_scd2_apply('STG_WORKBOOKS',   'SCD2_WORKBOOKS',   'WORKBOOK_ID');
--   CALL sigma_scd2_apply('STG_DATAMODELS',  'SCD2_DATAMODELS',  'DATA_MODEL_ID');
--   CALL sigma_scd2_apply('STG_DATASETS',    'SCD2_DATASETS',    'DATASET_ID');
--   CALL sigma_scd2_apply('STG_CONNECTIONS', 'SCD2_CONNECTIONS', 'CONNECTION_ID');
--   CALL sigma_scd2_apply('STG_WRITEBACK_TABLES', 'SCD2_WRITEBACK_TABLES', 'SIGDS_TABLE');
-- ==============================================================================

CREATE OR REPLACE PROCEDURE sigma_scd2_apply(
    SOURCE_VIEW  STRING,
    TARGET_TABLE STRING,
    KEY_COLUMN   STRING
)
RETURNS STRING
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'main'
AS
$$
def main(session, source_view, target_table, key_column):
    # Columns to keep out of the tracked-change hash.
    EXCLUDE = {"SNAPSHOT_TS", "SNAPSHOT_ID", "PAYLOAD", "DETAIL_PAYLOAD"}

    fields = session.sql(f"SELECT * FROM {source_view} LIMIT 0").schema.fields
    all_cols  = [f.name for f in fields]
    data_cols = [c for c in all_cols if c not in ("SNAPSHOT_TS", "SNAPSHOT_ID")]
    hash_cols = [c for c in all_cols if c not in EXCLUDE]

    # Hash expression over the tracked columns (VARIANT-safe via TO_JSON).
    hash_expr = "MD5(" + " || '|' || ".join(
        f"COALESCE(TO_VARCHAR({c}), '')" for c in hash_cols
    ) + ")"

    # Explicit insert column list (order-independent of the physical table).
    insert_cols = data_cols + [
        "SNAPSHOT_TS", "SNAPSHOT_ID",
        "SCD_VALID_FROM", "SCD_VALID_TO", "SCD_IS_CURRENT", "SCD_ROW_HASH",
    ]
    insert_col_list = ", ".join(insert_cols)
    select_data     = ", ".join(f"src.{c}" for c in data_cols)

    # 1) Build the history table from the source view's shape on first run.
    session.sql(f"""
        CREATE TABLE IF NOT EXISTS {target_table} AS
        SELECT *,
               CAST(NULL AS TIMESTAMP_NTZ) AS SCD_VALID_FROM,
               CAST(NULL AS TIMESTAMP_NTZ) AS SCD_VALID_TO,
               CAST(NULL AS BOOLEAN)       AS SCD_IS_CURRENT,
               CAST(NULL AS STRING)        AS SCD_ROW_HASH
        FROM {source_view}
        WHERE 1 = 0
    """).collect()

    # 2) Snapshot the source with its row hash into a transient staging table.
    #    TRANSIENT (not TEMPORARY): owner's-rights stored procedures cannot create
    #    temporary tables. Transient keeps it cheap (no fail-safe) and reusable
    #    across runs via CREATE OR REPLACE.
    stg = f"_SCD2_STG_{target_table}"
    session.sql(f"""
        CREATE OR REPLACE TRANSIENT TABLE {stg} AS
        SELECT *, {hash_expr} AS SCD_ROW_HASH
        FROM {source_view}
    """).collect()

    # 3) Close current rows whose hash changed (or whose key vanished is left open;
    #    we only close on observed change to keep last-known-state queryable).
    closed = session.sql(f"""
        UPDATE {target_table} t
        SET    SCD_VALID_TO = src.SNAPSHOT_TS,
               SCD_IS_CURRENT = FALSE
        FROM   {stg} src
        WHERE  t.{key_column} = src.{key_column}
          AND  t.SCD_IS_CURRENT = TRUE
          AND  t.SCD_ROW_HASH <> src.SCD_ROW_HASH
    """).collect()

    # 4) Insert new versions: new keys, or changed keys that now have no open row.
    inserted = session.sql(f"""
        INSERT INTO {target_table} ({insert_col_list})
        SELECT {select_data},
               src.SNAPSHOT_TS,
               src.SNAPSHOT_ID,
               src.SNAPSHOT_TS,
               CAST(NULL AS TIMESTAMP_NTZ),
               TRUE,
               src.SCD_ROW_HASH
        FROM {stg} src
        WHERE NOT EXISTS (
            SELECT 1 FROM {target_table} t
            WHERE t.{key_column} = src.{key_column}
              AND t.SCD_IS_CURRENT = TRUE
        )
    """).collect()

    n_closed   = closed[0][0]   if closed   else 0
    n_inserted = inserted[0][0] if inserted else 0
    return f"{target_table}: closed {n_closed}, inserted {n_inserted} version(s)"
$$;
