
-- =============================================================================
-- FILE: 01_object_ddl_log_loader.sql
-- PURPOSE: ACCOUNT_USAGE catalog-driven incremental DDL log loader.
--
-- EXECUTION MODEL:
--   Runs ONCE across all databases — not once per database.
--   A single inline watermark subquery (joined per block) handles per-DB
--   lookback so every object type block executes exactly one query regardless
--   of how many databases are in the array.
--
--   Per-database behaviour is determined by INCREMENTAL_STATE:
--     NEW DB  (no watermark row) → FULL capture, 10-year lookback.
--     EXISTING DB (watermark row) → INCREMENTAL, lookback from watermark
--                                   minus LATENCY_BUFFER_MINUTES.
--   Watermarks for ALL databases are written in a single MERGE at the end.
--
-- WATERMARK SUBQUERY PATTERN (repeated per block, INCREMENTAL_STATE is tiny):
--   FROM TABLE(FLATTEN(INPUT => :P_TARGET_DATABASES)) f
--   LEFT JOIN INCREMENTAL_STATE s ON s.DATABASE_NAME = f.VALUE::STRING
--   → FULL  : s.LAST_RUN_AT IS NULL → :v_full_lookback  (10 years ago)
--   → INCR  : s.LAST_RUN_AT IS NOT NULL → LEAST(s.LAST_RUN_AT, :v_buffer_ts)
--
-- LOOP types : TABLE / DYNAMIC_TABLE / ICEBERG_TABLE, STAGE, SEMANTIC_VIEW
-- FLAT types : VIEW, PROCEDURE, FUNCTION, TASK, PIPE,
--              SEMANTIC_TABLE, SEMANTIC_FACT, SEMANTIC_DIMENSION, SEMANTIC_METRIC
-- COMMENTED  : STREAM, SEQUENCE — column names unverified.
--              Run DESC VIEW SNOWFLAKE.ACCOUNT_USAGE.STREAMS / ...SEQUENCES first.
-- =============================================================================
CREATE DATABASE ADMIN_DB; 
USE DATABASE ADMIN_DB;

CREATE SCHEMA ADMIN_DB.VERSION_CONTROL;
USE SCHEMA VERSION_CONTROL;

-- -----------------------------------------------------------------------
-- Repository table — append-only log; current state + full history.
-- -----------------------------------------------------------------------
CREATE OR REPLACE TABLE  ADMIN_DB.VERSION_CONTROL.OBJECT_DDL_LOG (
    LOG_ID          STRING        DEFAULT UUID_STRING(),
    RUN_ID          STRING,
    DATABASE_NAME   STRING,
    SCHEMA_NAME     STRING,
    OBJECT_NAME     STRING,
    OBJECT_TYPE     STRING,
    OWNER_NAME      STRING,
    CREATED_AT      TIMESTAMP_LTZ,
    LAST_ALTERED_AT TIMESTAMP_LTZ,
    LAST_DDL_AT     TIMESTAMP_LTZ,
    LAST_DDL_BY     STRING,
    DELETED_AT      TIMESTAMP_LTZ,
    DEFINITION      STRING,
    CAPTURED_AT     TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

-- -----------------------------------------------------------------------
-- Error log — persists failures in case the CALL result wasn't captured.
-- -----------------------------------------------------------------------
CREATE OR REPLACE TABLE  ADMIN_DB.VERSION_CONTROL.PROCEDURE_ERROR_LOG (
    ERROR_ID    STRING        DEFAULT UUID_STRING(),
    RUN_ID      STRING,
    BLOCK_NAME  STRING,
    SQLCODE     NUMBER,
    SQLERRM     STRING,
    LOGGED_AT   TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);


-- =============================================================================
-- LOADER PROCEDURE
-- =============================================================================
CREATE OR REPLACE PROCEDURE ADMIN_DB.VERSION_CONTROL.LOAD_OBJECT_DDL_LOG(
    P_TARGET_DATABASES ARRAY,
    P_EXCLUDE_SCHEMAS  ARRAY,
    P_DRY_RUN          BOOLEAN
)
RETURNS OBJECT
LANGUAGE SQL
EXECUTE AS CALLER
AS
DECLARE
    v_run_id          STRING  DEFAULT UUID_STRING();
    v_rows_loaded     INT     DEFAULT 0;
    v_errors          INT     DEFAULT 0;
    v_error_list      STRING  DEFAULT '';
    v_err_code        NUMBER;
    v_err_msg         STRING;
    v_db_run_summary  STRING  DEFAULT '';

    -- Pre-computed timestamps used in every block's watermark subquery.
    -- Computed once here so DATEADD isn't repeated inside each inline query.
    v_buffer_ts       TIMESTAMP_NTZ; -- NOW minus latency buffer (for INCREMENTAL)
    v_full_lookback   TIMESTAMP_NTZ; -- 10 years ago (for FULL / new databases)

    -- Loop variables shared across all GET_DDL loop blocks
    v_loop_type    STRING;
    v_loop_cat     STRING;
    v_loop_sch     STRING;
    v_loop_nm      STRING;
    v_loop_owner   STRING;
    v_loop_created TIMESTAMP_LTZ;
    v_loop_altered TIMESTAMP_LTZ;
    v_loop_ddl_at  TIMESTAMP_LTZ;
    v_loop_ddl_by  STRING;
    v_loop_deleted TIMESTAMP_LTZ;
    v_loop_ddl     STRING;

    LATENCY_BUFFER_MINUTES INT DEFAULT 180;
BEGIN

    -- Pre-compute timestamps once
    v_buffer_ts    := DATEADD('minute', -:LATENCY_BUFFER_MINUTES, CURRENT_TIMESTAMP());
    v_full_lookback := DATEADD('year', -10, CURRENT_TIMESTAMP());

    -- ---------------------------------------------------------------
    -- Pre-run summary: classify each database as FULL or INCREMENTAL.
    -- Visible immediately in the CALL return — no extra query needed.
    -- ---------------------------------------------------------------
    LET summary_rs RESULTSET := (
        SELECT
            f.VALUE::STRING AS DB_NAME,
            CASE WHEN s.LAST_RUN_AT IS NULL THEN 'FULL' ELSE 'INCREMENTAL' END AS RUN_TYPE
        FROM TABLE(FLATTEN(INPUT => :P_TARGET_DATABASES)) f
        LEFT JOIN ADMIN_DB.VERSION_CONTROL.INCREMENTAL_STATE s
               ON s.DATABASE_NAME = f.VALUE::STRING
        ORDER BY DB_NAME
    );
    FOR sum_row IN summary_rs DO
        v_db_run_summary := v_db_run_summary
            || IFF(:v_db_run_summary = '', '', ' | ')
            || sum_row.DB_NAME || '=' || sum_row.RUN_TYPE;
    END FOR;

    IF (NOT :P_DRY_RUN) THEN

        -- =================================================================
        -- TABLE / DYNAMIC TABLE / ICEBERG TABLE  [GET_DDL loop]
        -- =================================================================
        BEGIN
            LET tbl_rs RESULTSET := (
                SELECT
                    CASE WHEN t.IS_ICEBERG = 'YES' THEN 'ICEBERG_TABLE'
                         WHEN t.IS_DYNAMIC  = 'YES' THEN 'DYNAMIC_TABLE'
                         ELSE 'TABLE' END  AS OBJ_TYPE,
                    t.TABLE_CATALOG  AS CAT,
                    t.TABLE_SCHEMA   AS SCH,
                    t.TABLE_NAME     AS NM,
                    t.TABLE_OWNER    AS OWN,
                    t.CREATED, t.LAST_ALTERED, t.LAST_DDL, t.LAST_DDL_BY, t.DELETED
                FROM SNOWFLAKE.ACCOUNT_USAGE.TABLES t
                JOIN (
                    SELECT f.VALUE::STRING AS DB_NAME,
                           CASE WHEN s.LAST_RUN_AT IS NULL THEN :v_full_lookback
                                ELSE LEAST(s.LAST_RUN_AT, :v_buffer_ts) END AS LOOKBACK_START
                    FROM TABLE(FLATTEN(INPUT => :P_TARGET_DATABASES)) f
                    LEFT JOIN ADMIN_DB.VERSION_CONTROL.INCREMENTAL_STATE s
                           ON s.DATABASE_NAME = f.VALUE::STRING
                ) dbs ON t.TABLE_CATALOG = dbs.DB_NAME
                WHERE t.LAST_DDL > dbs.LOOKBACK_START
                  AND NOT ARRAY_CONTAINS(t.TABLE_SCHEMA::VARIANT, :P_EXCLUDE_SCHEMAS)
                -- GET_DDL always returns current live state, not a point-in-time
                -- snapshot, so intermediate changes since the watermark are useless.
                -- Take the latest row per object only.
                QUALIFY ROW_NUMBER() OVER (
                    PARTITION BY t.TABLE_CATALOG, t.TABLE_SCHEMA, t.TABLE_NAME
                    ORDER BY t.LAST_DDL DESC
                ) = 1
            );
            FOR tbl_row IN tbl_rs DO
                v_loop_type    := tbl_row.OBJ_TYPE;
                v_loop_cat     := tbl_row.CAT;
                v_loop_sch     := tbl_row.SCH;
                v_loop_nm      := tbl_row.NM;
                v_loop_owner   := tbl_row.OWN;
                v_loop_created := tbl_row.CREATED;
                v_loop_altered := tbl_row.LAST_ALTERED;
                v_loop_ddl_at  := tbl_row.LAST_DDL;
                v_loop_ddl_by  := tbl_row.LAST_DDL_BY;
                v_loop_deleted := tbl_row.DELETED;
                v_loop_ddl     := NULL;

                IF (v_loop_deleted IS NULL) THEN
                    BEGIN
                        LET v_tbl_sql STRING := 'SELECT GET_DDL(''' || v_loop_type || ''', ''' || v_loop_cat || '.' || v_loop_sch || '.' || v_loop_nm || ''') AS MY_DDL';
                        LET v_tbl_rs RESULTSET := (EXECUTE IMMEDIATE :v_tbl_sql);
                        FOR ddl_row IN v_tbl_rs DO v_loop_ddl := ddl_row.MY_DDL; END FOR;
                    EXCEPTION WHEN OTHER THEN
                        v_errors := v_errors + 1;
                        v_error_list := v_error_list || ' | TABLE GET_DDL (' || v_loop_cat || '.' || v_loop_sch || '.' || v_loop_nm || '): ' || SQLERRM;
                    END;
                END IF;

                INSERT INTO ADMIN_DB.VERSION_CONTROL.OBJECT_DDL_LOG
                    (RUN_ID, DATABASE_NAME, SCHEMA_NAME, OBJECT_NAME, OBJECT_TYPE,
                     OWNER_NAME, CREATED_AT, LAST_ALTERED_AT, LAST_DDL_AT, LAST_DDL_BY, DELETED_AT, DEFINITION)
                VALUES (:v_run_id, :v_loop_cat, :v_loop_sch, :v_loop_nm, :v_loop_type,
                        :v_loop_owner, :v_loop_created, :v_loop_altered, :v_loop_ddl_at, :v_loop_ddl_by, :v_loop_deleted, :v_loop_ddl);
                v_rows_loaded := v_rows_loaded + 1;
            END FOR;
        EXCEPTION WHEN OTHER THEN
            v_errors := v_errors + 1;
            v_error_list := v_error_list || ' | TABLE Block: ' || SQLERRM;
            v_err_code := SQLCODE; v_err_msg := SQLERRM;
            INSERT INTO ADMIN_DB.VERSION_CONTROL.PROCEDURE_ERROR_LOG (RUN_ID, BLOCK_NAME, SQLCODE, SQLERRM)
            VALUES (:v_run_id, 'TABLE', :v_err_code, :v_err_msg);
        END;

        -- =================================================================
        -- VIEW  [flat INSERT...SELECT]
        -- =================================================================
        BEGIN
            INSERT INTO ADMIN_DB.VERSION_CONTROL.OBJECT_DDL_LOG
                (RUN_ID, DATABASE_NAME, SCHEMA_NAME, OBJECT_NAME, OBJECT_TYPE,
                 OWNER_NAME, CREATED_AT, LAST_ALTERED_AT, LAST_DDL_AT, LAST_DDL_BY, DELETED_AT, DEFINITION)
            SELECT :v_run_id, v.TABLE_CATALOG, v.TABLE_SCHEMA, v.TABLE_NAME, 'VIEW',
                   v.TABLE_OWNER, v.CREATED, v.LAST_ALTERED, v.LAST_DDL, v.LAST_DDL_BY, v.DELETED, v.VIEW_DEFINITION
            FROM SNOWFLAKE.ACCOUNT_USAGE.VIEWS v
            JOIN (
                SELECT f.VALUE::STRING AS DB_NAME,
                       CASE WHEN s.LAST_RUN_AT IS NULL THEN :v_full_lookback
                            ELSE LEAST(s.LAST_RUN_AT, :v_buffer_ts) END AS LOOKBACK_START
                FROM TABLE(FLATTEN(INPUT => :P_TARGET_DATABASES)) f
                LEFT JOIN ADMIN_DB.VERSION_CONTROL.INCREMENTAL_STATE s
                       ON s.DATABASE_NAME = f.VALUE::STRING
            ) dbs ON v.TABLE_CATALOG = dbs.DB_NAME
            WHERE v.LAST_DDL > dbs.LOOKBACK_START
              AND NOT ARRAY_CONTAINS(v.TABLE_SCHEMA::VARIANT, :P_EXCLUDE_SCHEMAS);
            v_rows_loaded := v_rows_loaded + SQLROWCOUNT;
        EXCEPTION WHEN OTHER THEN
            v_errors := v_errors + 1;
            v_error_list := v_error_list || ' | VIEW Block: ' || SQLERRM;
            v_err_code := SQLCODE; v_err_msg := SQLERRM;
            INSERT INTO ADMIN_DB.VERSION_CONTROL.PROCEDURE_ERROR_LOG (RUN_ID, BLOCK_NAME, SQLCODE, SQLERRM)
            VALUES (:v_run_id, 'VIEW', :v_err_code, :v_err_msg);
        END;

        -- =================================================================
        -- PROCEDURE  [flat INSERT...SELECT — no LAST_DDL, uses LAST_ALTERED]
        -- =================================================================
        BEGIN
            INSERT INTO ADMIN_DB.VERSION_CONTROL.OBJECT_DDL_LOG
                (RUN_ID, DATABASE_NAME, SCHEMA_NAME, OBJECT_NAME, OBJECT_TYPE,
                 OWNER_NAME, CREATED_AT, LAST_ALTERED_AT, LAST_DDL_AT, LAST_DDL_BY, DELETED_AT, DEFINITION)
            SELECT :v_run_id, p.PROCEDURE_CATALOG, p.PROCEDURE_SCHEMA, p.PROCEDURE_NAME, 'PROCEDURE',
                   p.PROCEDURE_OWNER, p.CREATED, p.LAST_ALTERED, p.LAST_ALTERED, NULL, p.DELETED, p.PROCEDURE_DEFINITION
            FROM SNOWFLAKE.ACCOUNT_USAGE.PROCEDURES p
            JOIN (
                SELECT f.VALUE::STRING AS DB_NAME,
                       CASE WHEN s.LAST_RUN_AT IS NULL THEN :v_full_lookback
                            ELSE LEAST(s.LAST_RUN_AT, :v_buffer_ts) END AS LOOKBACK_START
                FROM TABLE(FLATTEN(INPUT => :P_TARGET_DATABASES)) f
                LEFT JOIN ADMIN_DB.VERSION_CONTROL.INCREMENTAL_STATE s
                       ON s.DATABASE_NAME = f.VALUE::STRING
            ) dbs ON p.PROCEDURE_CATALOG = dbs.DB_NAME
            WHERE p.LAST_ALTERED > dbs.LOOKBACK_START
              AND NOT ARRAY_CONTAINS(p.PROCEDURE_SCHEMA::VARIANT, :P_EXCLUDE_SCHEMAS);
            v_rows_loaded := v_rows_loaded + SQLROWCOUNT;
        EXCEPTION WHEN OTHER THEN
            v_errors := v_errors + 1;
            v_error_list := v_error_list || ' | PROCEDURE Block: ' || SQLERRM;
            v_err_code := SQLCODE; v_err_msg := SQLERRM;
            INSERT INTO ADMIN_DB.VERSION_CONTROL.PROCEDURE_ERROR_LOG (RUN_ID, BLOCK_NAME, SQLCODE, SQLERRM)
            VALUES (:v_run_id, 'PROCEDURE', :v_err_code, :v_err_msg);
        END;

        -- =================================================================
        -- FUNCTION  [flat INSERT...SELECT — VERIFY column names]
        -- =================================================================
        BEGIN
            INSERT INTO ADMIN_DB.VERSION_CONTROL.OBJECT_DDL_LOG
                (RUN_ID, DATABASE_NAME, SCHEMA_NAME, OBJECT_NAME, OBJECT_TYPE,
                 OWNER_NAME, CREATED_AT, LAST_ALTERED_AT, LAST_DDL_AT, LAST_DDL_BY, DELETED_AT, DEFINITION)
            SELECT :v_run_id, fn.FUNCTION_CATALOG, fn.FUNCTION_SCHEMA, fn.FUNCTION_NAME, 'FUNCTION',
                   fn.FUNCTION_OWNER, fn.CREATED, fn.LAST_ALTERED, fn.LAST_ALTERED, NULL, fn.DELETED, fn.FUNCTION_DEFINITION
            FROM SNOWFLAKE.ACCOUNT_USAGE.FUNCTIONS fn
            JOIN (
                SELECT f.VALUE::STRING AS DB_NAME,
                       CASE WHEN s.LAST_RUN_AT IS NULL THEN :v_full_lookback
                            ELSE LEAST(s.LAST_RUN_AT, :v_buffer_ts) END AS LOOKBACK_START
                FROM TABLE(FLATTEN(INPUT => :P_TARGET_DATABASES)) f
                LEFT JOIN ADMIN_DB.VERSION_CONTROL.INCREMENTAL_STATE s
                       ON s.DATABASE_NAME = f.VALUE::STRING
            ) dbs ON fn.FUNCTION_CATALOG = dbs.DB_NAME
            WHERE fn.LAST_ALTERED > dbs.LOOKBACK_START
              AND NOT ARRAY_CONTAINS(fn.FUNCTION_SCHEMA::VARIANT, :P_EXCLUDE_SCHEMAS);
            v_rows_loaded := v_rows_loaded + SQLROWCOUNT;
        EXCEPTION WHEN OTHER THEN
            v_errors := v_errors + 1;
            v_error_list := v_error_list || ' | FUNCTION Block: ' || SQLERRM;
            v_err_code := SQLCODE; v_err_msg := SQLERRM;
            INSERT INTO ADMIN_DB.VERSION_CONTROL.PROCEDURE_ERROR_LOG (RUN_ID, BLOCK_NAME, SQLCODE, SQLERRM)
            VALUES (:v_run_id, 'FUNCTION', :v_err_code, :v_err_msg);
        END;

        -- =================================================================
        -- TASK  [flat INSERT...SELECT]
        -- =================================================================
        BEGIN
            INSERT INTO ADMIN_DB.VERSION_CONTROL.OBJECT_DDL_LOG
                (RUN_ID, DATABASE_NAME, SCHEMA_NAME, OBJECT_NAME, OBJECT_TYPE,
                 OWNER_NAME, CREATED_AT, LAST_ALTERED_AT, LAST_DDL_AT, LAST_DDL_BY, DELETED_AT, DEFINITION)
            SELECT :v_run_id, tk.TASK_DATABASE, tk.TASK_SCHEMA, tk.TASK_NAME, 'TASK',
                   tk.TASK_OWNER, tk.CREATED, tk.LAST_ALTERED, tk.LAST_ALTERED, NULL, tk.DELETED, tk.DEFINITION
            FROM SNOWFLAKE.ACCOUNT_USAGE.TASKS tk
            JOIN (
                SELECT f.VALUE::STRING AS DB_NAME,
                       CASE WHEN s.LAST_RUN_AT IS NULL THEN :v_full_lookback
                            ELSE LEAST(s.LAST_RUN_AT, :v_buffer_ts) END AS LOOKBACK_START
                FROM TABLE(FLATTEN(INPUT => :P_TARGET_DATABASES)) f
                LEFT JOIN ADMIN_DB.VERSION_CONTROL.INCREMENTAL_STATE s
                       ON s.DATABASE_NAME = f.VALUE::STRING
            ) dbs ON tk.TASK_DATABASE = dbs.DB_NAME
            WHERE tk.LAST_ALTERED > dbs.LOOKBACK_START
              AND NOT ARRAY_CONTAINS(tk.TASK_SCHEMA::VARIANT, :P_EXCLUDE_SCHEMAS);
            v_rows_loaded := v_rows_loaded + SQLROWCOUNT;
        EXCEPTION WHEN OTHER THEN
            v_errors := v_errors + 1;
            v_error_list := v_error_list || ' | TASK Block: ' || SQLERRM;
            v_err_code := SQLCODE; v_err_msg := SQLERRM;
            INSERT INTO ADMIN_DB.VERSION_CONTROL.PROCEDURE_ERROR_LOG (RUN_ID, BLOCK_NAME, SQLCODE, SQLERRM)
            VALUES (:v_run_id, 'TASK', :v_err_code, :v_err_msg);
        END;

        -- =================================================================
        -- PIPE  [flat INSERT...SELECT]
        -- =================================================================
        BEGIN
            INSERT INTO ADMIN_DB.VERSION_CONTROL.OBJECT_DDL_LOG
                (RUN_ID, DATABASE_NAME, SCHEMA_NAME, OBJECT_NAME, OBJECT_TYPE,
                 OWNER_NAME, CREATED_AT, LAST_ALTERED_AT, LAST_DDL_AT, LAST_DDL_BY, DELETED_AT, DEFINITION)
            SELECT :v_run_id, pi.PIPE_CATALOG, pi.PIPE_SCHEMA, pi.PIPE_NAME, 'PIPE',
                   pi.PIPE_OWNER, pi.CREATED, pi.LAST_ALTERED, pi.LAST_ALTERED, NULL, pi.DELETED, pi.DEFINITION
            FROM SNOWFLAKE.ACCOUNT_USAGE.PIPES pi
            JOIN (
                SELECT f.VALUE::STRING AS DB_NAME,
                       CASE WHEN s.LAST_RUN_AT IS NULL THEN :v_full_lookback
                            ELSE LEAST(s.LAST_RUN_AT, :v_buffer_ts) END AS LOOKBACK_START
                FROM TABLE(FLATTEN(INPUT => :P_TARGET_DATABASES)) f
                LEFT JOIN ADMIN_DB.VERSION_CONTROL.INCREMENTAL_STATE s
                       ON s.DATABASE_NAME = f.VALUE::STRING
            ) dbs ON pi.PIPE_CATALOG = dbs.DB_NAME
            WHERE pi.LAST_ALTERED > dbs.LOOKBACK_START
              AND NOT ARRAY_CONTAINS(pi.PIPE_SCHEMA::VARIANT, :P_EXCLUDE_SCHEMAS);
            v_rows_loaded := v_rows_loaded + SQLROWCOUNT;
        EXCEPTION WHEN OTHER THEN
            v_errors := v_errors + 1;
            v_error_list := v_error_list || ' | PIPE Block: ' || SQLERRM;
            v_err_code := SQLCODE; v_err_msg := SQLERRM;
            INSERT INTO ADMIN_DB.VERSION_CONTROL.PROCEDURE_ERROR_LOG (RUN_ID, BLOCK_NAME, SQLCODE, SQLERRM)
            VALUES (:v_run_id, 'PIPE', :v_err_code, :v_err_msg);
        END;

        -- =================================================================
        -- STAGE  [GET_DDL loop]
        -- =================================================================
        -- BEGIN
        --     LET stg_rs RESULTSET := (
        --         SELECT st.STAGE_CATALOG AS CAT, st.STAGE_SCHEMA AS SCH, st.STAGE_NAME AS NM,
        --                st.STAGE_OWNER AS OWN, st.CREATED, st.LAST_ALTERED, st.DELETED
        --         FROM SNOWFLAKE.ACCOUNT_USAGE.STAGES st
        --         JOIN (
        --             SELECT f.VALUE::STRING AS DB_NAME,
        --                    CASE WHEN s.LAST_RUN_AT IS NULL THEN :v_full_lookback
        --                         ELSE LEAST(s.LAST_RUN_AT, :v_buffer_ts) END AS LOOKBACK_START
        --             FROM TABLE(FLATTEN(INPUT => :P_TARGET_DATABASES)) f
        --             LEFT JOIN ADMIN_DB.VERSION_CONTROL.INCREMENTAL_STATE s
        --                    ON s.DATABASE_NAME = f.VALUE::STRING
        --         ) dbs ON st.STAGE_CATALOG = dbs.DB_NAME
        --         WHERE st.LAST_ALTERED > dbs.LOOKBACK_START
        --           AND NOT ARRAY_CONTAINS(st.STAGE_SCHEMA::VARIANT, :P_EXCLUDE_SCHEMAS)
        --         -- GET_DDL always returns current live state — take latest row per object only.
        --         QUALIFY ROW_NUMBER() OVER (
        --             PARTITION BY st.STAGE_CATALOG, st.STAGE_SCHEMA, st.STAGE_NAME
        --             ORDER BY st.LAST_ALTERED DESC
        --         ) = 1
        --     );
        --     FOR stg_row IN stg_rs DO
        --         v_loop_cat := stg_row.CAT; v_loop_sch := stg_row.SCH; v_loop_nm := stg_row.NM;
        --         v_loop_owner := stg_row.OWN; v_loop_created := stg_row.CREATED;
        --         v_loop_altered := stg_row.LAST_ALTERED; v_loop_deleted := stg_row.DELETED;
        --         v_loop_ddl := NULL;

        --         IF (v_loop_deleted IS NULL) THEN
        --             BEGIN
        --                 LET v_stg_sql STRING := 'SELECT GET_DDL(''STAGE'', ''' || v_loop_cat || '.' || v_loop_sch || '.' || v_loop_nm || ''') AS MY_DDL';
        --                 LET v_stg_rs RESULTSET := (EXECUTE IMMEDIATE :v_stg_sql);
        --                 FOR ddl_row IN v_stg_rs DO v_loop_ddl := ddl_row.MY_DDL; END FOR;
        --             EXCEPTION WHEN OTHER THEN
        --                 v_errors := v_errors + 1;
        --                 v_error_list := v_error_list || ' | STAGE GET_DDL (' || v_loop_cat || '.' || v_loop_sch || '.' || v_loop_nm || '): ' || SQLERRM;
        --             END;
        --         END IF;

        --         INSERT INTO ADMIN_DB.VERSION_CONTROL.OBJECT_DDL_LOG
        --             (RUN_ID, DATABASE_NAME, SCHEMA_NAME, OBJECT_NAME, OBJECT_TYPE,
        --              OWNER_NAME, CREATED_AT, LAST_ALTERED_AT, LAST_DDL_AT, LAST_DDL_BY, DELETED_AT, DEFINITION)
        --         VALUES (:v_run_id, :v_loop_cat, :v_loop_sch, :v_loop_nm, 'STAGE',
        --                 :v_loop_owner, :v_loop_created, :v_loop_altered, :v_loop_altered, NULL, :v_loop_deleted, :v_loop_ddl);
        --         v_rows_loaded := v_rows_loaded + 1;
        --     END FOR;
        -- EXCEPTION WHEN OTHER THEN
        --     v_errors := v_errors + 1;
        --     v_error_list := v_error_list || ' | STAGE Block: ' || SQLERRM;
        --     v_err_code := SQLCODE; v_err_msg := SQLERRM;
        --     INSERT INTO ADMIN_DB.VERSION_CONTROL.PROCEDURE_ERROR_LOG (RUN_ID, BLOCK_NAME, SQLCODE, SQLERRM)
        --     VALUES (:v_run_id, 'STAGE', :v_err_code, :v_err_msg);
        -- END;

        -- =================================================================
        -- STREAM — commented out: column names unverified.
        -- Run DESC VIEW SNOWFLAKE.ACCOUNT_USAGE.STREAMS to confirm,
        -- then replicate the STAGE loop pattern above.
        -- =================================================================
        -- BEGIN
        --     LET str_rs RESULTSET := (
        --         SELECT st.STREAM_CATALOG AS CAT, st.STREAM_SCHEMA AS SCH, st.STREAM_NAME AS NM,
        --                st.STREAM_OWNER AS OWN, st.CREATED, st.LAST_ALTERED, st.DELETED
        --         FROM SNOWFLAKE.ACCOUNT_USAGE.STREAMS st
        --         JOIN (
        --             SELECT f.VALUE::STRING AS DB_NAME,
        --                    CASE WHEN s.LAST_RUN_AT IS NULL THEN :v_full_lookback
        --                         ELSE LEAST(s.LAST_RUN_AT, :v_buffer_ts) END AS LOOKBACK_START
        --             FROM TABLE(FLATTEN(INPUT => :P_TARGET_DATABASES)) f
        --             LEFT JOIN ADMIN_DB.VERSION_CONTROL.INCREMENTAL_STATE s
        --                    ON s.DATABASE_NAME = f.VALUE::STRING
        --         ) dbs ON st.STREAM_CATALOG = dbs.DB_NAME
        --         WHERE st.LAST_ALTERED > dbs.LOOKBACK_START
        --           AND NOT ARRAY_CONTAINS(st.STREAM_SCHEMA::VARIANT, :P_EXCLUDE_SCHEMAS)
        --         -- GET_DDL always returns current live state — take latest row per object only.
        --         QUALIFY ROW_NUMBER() OVER (
        --             PARTITION BY st.STREAM_CATALOG, st.STREAM_SCHEMA, st.STREAM_NAME
        --             ORDER BY st.LAST_ALTERED DESC
        --         ) = 1
        --     );
        --     FOR str_row IN str_rs DO ... END FOR;
        -- EXCEPTION WHEN OTHER THEN ...
        -- END;

        -- =================================================================
        -- SEQUENCE — commented out: column names unverified.
        -- Run DESC VIEW SNOWFLAKE.ACCOUNT_USAGE.SEQUENCES to confirm,
        -- then replicate the STAGE loop pattern above.
        -- =================================================================
        -- BEGIN
        --     LET seq_rs RESULTSET := (
        --         SELECT sq.SEQUENCE_DATABASE_NAME AS CAT, sq.SEQUENCE_SCHEMA_NAME AS SCH,
        --                sq.SEQUENCE_NAME AS NM, sq.OWNER AS OWN, sq.CREATED, sq.LAST_ALTERED, sq.DELETED
        --         FROM SNOWFLAKE.ACCOUNT_USAGE.SEQUENCES sq
        --         JOIN (
        --             SELECT f.VALUE::STRING AS DB_NAME,
        --                    CASE WHEN s.LAST_RUN_AT IS NULL THEN :v_full_lookback
        --                         ELSE LEAST(s.LAST_RUN_AT, :v_buffer_ts) END AS LOOKBACK_START
        --             FROM TABLE(FLATTEN(INPUT => :P_TARGET_DATABASES)) f
        --             LEFT JOIN ADMIN_DB.VERSION_CONTROL.INCREMENTAL_STATE s
        --                    ON s.DATABASE_NAME = f.VALUE::STRING
        --         ) dbs ON sq.SEQUENCE_DATABASE_NAME = dbs.DB_NAME
        --         WHERE sq.LAST_ALTERED > dbs.LOOKBACK_START
        --           AND NOT ARRAY_CONTAINS(sq.SEQUENCE_SCHEMA_NAME::VARIANT, :P_EXCLUDE_SCHEMAS)
        --     );
        --     FOR seq_row IN seq_rs DO ... END FOR;
        -- EXCEPTION WHEN OTHER THEN ...
        -- END;

        -- =================================================================
        -- SEMANTIC_VIEW  [GET_DDL loop]
        -- =================================================================
        BEGIN
            LET sv_rs RESULTSET := (
                SELECT sv.SEMANTIC_VIEW_DATABASE_NAME AS CAT, sv.SEMANTIC_VIEW_SCHEMA_NAME AS SCH,
                       sv.SEMANTIC_VIEW_NAME AS NM, sv.OWNER AS OWN, sv.CREATED, sv.LAST_ALTERED, sv.DELETED
                FROM SNOWFLAKE.ACCOUNT_USAGE.SEMANTIC_VIEWS sv
                JOIN (
                    SELECT f.VALUE::STRING AS DB_NAME,
                           CASE WHEN s.LAST_RUN_AT IS NULL THEN :v_full_lookback
                                ELSE LEAST(s.LAST_RUN_AT, :v_buffer_ts) END AS LOOKBACK_START
                    FROM TABLE(FLATTEN(INPUT => :P_TARGET_DATABASES)) f
                    LEFT JOIN ADMIN_DB.VERSION_CONTROL.INCREMENTAL_STATE s
                           ON s.DATABASE_NAME = f.VALUE::STRING
                ) dbs ON sv.SEMANTIC_VIEW_DATABASE_NAME = dbs.DB_NAME
                WHERE sv.LAST_ALTERED > dbs.LOOKBACK_START
                  AND NOT ARRAY_CONTAINS(sv.SEMANTIC_VIEW_SCHEMA_NAME::VARIANT, :P_EXCLUDE_SCHEMAS)
                -- GET_DDL always returns current live state — take latest row per object only.
                QUALIFY ROW_NUMBER() OVER (
                    PARTITION BY sv.SEMANTIC_VIEW_DATABASE_NAME, sv.SEMANTIC_VIEW_SCHEMA_NAME, sv.SEMANTIC_VIEW_NAME
                    ORDER BY sv.LAST_ALTERED DESC
                ) = 1
            );
            FOR sv_row IN sv_rs DO
                v_loop_cat := sv_row.CAT; v_loop_sch := sv_row.SCH; v_loop_nm := sv_row.NM;
                v_loop_owner := sv_row.OWN; v_loop_created := sv_row.CREATED;
                v_loop_altered := sv_row.LAST_ALTERED; v_loop_deleted := sv_row.DELETED;
                v_loop_ddl := NULL;

                IF (v_loop_deleted IS NULL) THEN
                    BEGIN
                        LET v_sv_sql STRING := 'SELECT GET_DDL(''SEMANTIC_VIEW'', ''' || v_loop_cat || '.' || v_loop_sch || '.' || v_loop_nm || ''') AS MY_DDL';
                        LET v_sv_rs RESULTSET := (EXECUTE IMMEDIATE :v_sv_sql);
                        FOR ddl_row IN v_sv_rs DO v_loop_ddl := ddl_row.MY_DDL; END FOR;
                    EXCEPTION WHEN OTHER THEN
                        v_errors := v_errors + 1;
                        v_error_list := v_error_list || ' | SEMANTIC_VIEW GET_DDL (' || v_loop_cat || '.' || v_loop_sch || '.' || v_loop_nm || '): ' || SQLERRM;
                    END;
                END IF;

                INSERT INTO ADMIN_DB.VERSION_CONTROL.OBJECT_DDL_LOG
                    (RUN_ID, DATABASE_NAME, SCHEMA_NAME, OBJECT_NAME, OBJECT_TYPE,
                     OWNER_NAME, CREATED_AT, LAST_ALTERED_AT, LAST_DDL_AT, LAST_DDL_BY, DELETED_AT, DEFINITION)
                VALUES (:v_run_id, :v_loop_cat, :v_loop_sch, :v_loop_nm, 'SEMANTIC_VIEW',
                        :v_loop_owner, :v_loop_created, :v_loop_altered, :v_loop_altered, NULL, :v_loop_deleted, :v_loop_ddl);
                v_rows_loaded := v_rows_loaded + 1;
            END FOR;
        EXCEPTION WHEN OTHER THEN
            v_errors := v_errors + 1;
            v_error_list := v_error_list || ' | SEMANTIC_VIEW Block: ' || SQLERRM;
            v_err_code := SQLCODE; v_err_msg := SQLERRM;
            INSERT INTO ADMIN_DB.VERSION_CONTROL.PROCEDURE_ERROR_LOG (RUN_ID, BLOCK_NAME, SQLCODE, SQLERRM)
            VALUES (:v_run_id, 'SEMANTIC_VIEW', :v_err_code, :v_err_msg);
        END;

        -- =================================================================
        -- SEMANTIC sub-objects  [flat INSERT...SELECT, metadata only]
        -- =================================================================
        BEGIN
            INSERT INTO ADMIN_DB.VERSION_CONTROL.OBJECT_DDL_LOG
                (RUN_ID, DATABASE_NAME, SCHEMA_NAME, OBJECT_NAME, OBJECT_TYPE,
                 OWNER_NAME, CREATED_AT, LAST_ALTERED_AT, LAST_DDL_AT, LAST_DDL_BY, DELETED_AT, DEFINITION)
            SELECT :v_run_id, st.semantic_view_database_name, st.semantic_view_schema_name,
                   st.SEMANTIC_TABLE_NAME, 'SEMANTIC_TABLE', '' AS OWNER, st.CREATED, st.LAST_ALTERED, st.LAST_ALTERED, NULL, st.DELETED, NULL
            FROM SNOWFLAKE.ACCOUNT_USAGE.SEMANTIC_TABLES st
            JOIN (
                SELECT f.VALUE::STRING AS DB_NAME,
                       CASE WHEN s.LAST_RUN_AT IS NULL THEN :v_full_lookback
                            ELSE LEAST(s.LAST_RUN_AT, :v_buffer_ts) END AS LOOKBACK_START
                FROM TABLE(FLATTEN(INPUT => :P_TARGET_DATABASES)) f
                LEFT JOIN ADMIN_DB.VERSION_CONTROL.INCREMENTAL_STATE s ON s.DATABASE_NAME = f.VALUE::STRING
            ) dbs ON st.semantic_view_database_name = dbs.DB_NAME
            WHERE st.LAST_ALTERED > dbs.LOOKBACK_START
              AND NOT ARRAY_CONTAINS(st.semantic_view_schema_name::VARIANT, :P_EXCLUDE_SCHEMAS);
            v_rows_loaded := v_rows_loaded + SQLROWCOUNT;
        EXCEPTION WHEN OTHER THEN
            v_errors := v_errors + 1; v_error_list := v_error_list || ' | SEMANTIC_TABLE Block: ' || SQLERRM;
            v_err_code := SQLCODE; v_err_msg := SQLERRM;
            INSERT INTO ADMIN_DB.VERSION_CONTROL.PROCEDURE_ERROR_LOG (RUN_ID, BLOCK_NAME, SQLCODE, SQLERRM)
            VALUES (:v_run_id, 'SEMANTIC_TABLE', :v_err_code, :v_err_msg);
        END;

        BEGIN
            INSERT INTO ADMIN_DB.VERSION_CONTROL.OBJECT_DDL_LOG
                (RUN_ID, DATABASE_NAME, SCHEMA_NAME, OBJECT_NAME, OBJECT_TYPE,
                 OWNER_NAME, CREATED_AT, LAST_ALTERED_AT, LAST_DDL_AT, LAST_DDL_BY, DELETED_AT, DEFINITION)
            SELECT :v_run_id, sf.semantic_view_database_name, sf.semantic_view_schema_name,
                   sf.SEMANTIC_FACT_NAME, 'SEMANTIC_FACT', '' AS OWNER, sf.CREATED, sf.LAST_ALTERED, sf.LAST_ALTERED, NULL, sf.DELETED, NULL
            FROM SNOWFLAKE.ACCOUNT_USAGE.SEMANTIC_FACTS sf
            JOIN (
                SELECT f.VALUE::STRING AS DB_NAME,
                       CASE WHEN s.LAST_RUN_AT IS NULL THEN :v_full_lookback
                            ELSE LEAST(s.LAST_RUN_AT, :v_buffer_ts) END AS LOOKBACK_START
                FROM TABLE(FLATTEN(INPUT => :P_TARGET_DATABASES)) f
                LEFT JOIN ADMIN_DB.VERSION_CONTROL.INCREMENTAL_STATE s ON s.DATABASE_NAME = f.VALUE::STRING
            ) dbs ON sf.semantic_view_database_name = dbs.DB_NAME
            WHERE sf.LAST_ALTERED > dbs.LOOKBACK_START
              AND NOT ARRAY_CONTAINS(sf.semantic_view_schema_name::VARIANT, :P_EXCLUDE_SCHEMAS);
            v_rows_loaded := v_rows_loaded + SQLROWCOUNT;
        EXCEPTION WHEN OTHER THEN
            v_errors := v_errors + 1; v_error_list := v_error_list || ' | SEMANTIC_FACT Block: ' || SQLERRM;
            v_err_code := SQLCODE; v_err_msg := SQLERRM;
            INSERT INTO ADMIN_DB.VERSION_CONTROL.PROCEDURE_ERROR_LOG (RUN_ID, BLOCK_NAME, SQLCODE, SQLERRM)
            VALUES (:v_run_id, 'SEMANTIC_FACT', :v_err_code, :v_err_msg);
        END;

        BEGIN
            INSERT INTO ADMIN_DB.VERSION_CONTROL.OBJECT_DDL_LOG
                (RUN_ID, DATABASE_NAME, SCHEMA_NAME, OBJECT_NAME, OBJECT_TYPE,
                 OWNER_NAME, CREATED_AT, LAST_ALTERED_AT, LAST_DDL_AT, LAST_DDL_BY, DELETED_AT, DEFINITION)
            SELECT :v_run_id, sd.semantic_view_database_name, sd.semantic_view_schema_name,
                   sd.SEMANTIC_DIMENSION_NAME, 'SEMANTIC_DIMENSION', '' AS OWNER, sd.CREATED, sd.LAST_ALTERED, sd.LAST_ALTERED, NULL, sd.DELETED, NULL
            FROM SNOWFLAKE.ACCOUNT_USAGE.SEMANTIC_DIMENSIONS sd
            JOIN (
                SELECT f.VALUE::STRING AS DB_NAME,
                       CASE WHEN s.LAST_RUN_AT IS NULL THEN :v_full_lookback
                            ELSE LEAST(s.LAST_RUN_AT, :v_buffer_ts) END AS LOOKBACK_START
                FROM TABLE(FLATTEN(INPUT => :P_TARGET_DATABASES)) f
                LEFT JOIN ADMIN_DB.VERSION_CONTROL.INCREMENTAL_STATE s ON s.DATABASE_NAME = f.VALUE::STRING
            ) dbs ON sd.semantic_view_database_name = dbs.DB_NAME
            WHERE sd.LAST_ALTERED > dbs.LOOKBACK_START
              AND NOT ARRAY_CONTAINS(sd.semantic_view_schema_name::VARIANT, :P_EXCLUDE_SCHEMAS);
            v_rows_loaded := v_rows_loaded + SQLROWCOUNT;
        EXCEPTION WHEN OTHER THEN
            v_errors := v_errors + 1; v_error_list := v_error_list || ' | SEMANTIC_DIMENSION Block: ' || SQLERRM;
            v_err_code := SQLCODE; v_err_msg := SQLERRM;
            INSERT INTO ADMIN_DB.VERSION_CONTROL.PROCEDURE_ERROR_LOG (RUN_ID, BLOCK_NAME, SQLCODE, SQLERRM)
            VALUES (:v_run_id, 'SEMANTIC_DIMENSION', :v_err_code, :v_err_msg);
        END;

        BEGIN
            INSERT INTO ADMIN_DB.VERSION_CONTROL.OBJECT_DDL_LOG
                (RUN_ID, DATABASE_NAME, SCHEMA_NAME, OBJECT_NAME, OBJECT_TYPE,
                 OWNER_NAME, CREATED_AT, LAST_ALTERED_AT, LAST_DDL_AT, LAST_DDL_BY, DELETED_AT, DEFINITION)
            SELECT :v_run_id, sm.semantic_view_database_name, sm.semantic_view_schema_name,
                   sm.SEMANTIC_METRIC_NAME, 'SEMANTIC_METRIC', '' AS OWNER, sm.CREATED, sm.LAST_ALTERED, sm.LAST_ALTERED, NULL, sm.DELETED, NULL
            FROM SNOWFLAKE.ACCOUNT_USAGE.SEMANTIC_METRICS sm
            JOIN (
                SELECT f.VALUE::STRING AS DB_NAME,
                       CASE WHEN s.LAST_RUN_AT IS NULL THEN :v_full_lookback
                            ELSE LEAST(s.LAST_RUN_AT, :v_buffer_ts) END AS LOOKBACK_START
                FROM TABLE(FLATTEN(INPUT => :P_TARGET_DATABASES)) f
                LEFT JOIN ADMIN_DB.VERSION_CONTROL.INCREMENTAL_STATE s ON s.DATABASE_NAME = f.VALUE::STRING
            ) dbs ON sm.semantic_view_database_name = dbs.DB_NAME
            WHERE sm.LAST_ALTERED > dbs.LOOKBACK_START
              AND NOT ARRAY_CONTAINS(sm.semantic_view_schema_name::VARIANT, :P_EXCLUDE_SCHEMAS);
            v_rows_loaded := v_rows_loaded + SQLROWCOUNT;
        EXCEPTION WHEN OTHER THEN
            v_errors := v_errors + 1; v_error_list := v_error_list || ' | SEMANTIC_METRIC Block: ' || SQLERRM;
            v_err_code := SQLCODE; v_err_msg := SQLERRM;
            INSERT INTO ADMIN_DB.VERSION_CONTROL.PROCEDURE_ERROR_LOG (RUN_ID, BLOCK_NAME, SQLCODE, SQLERRM)
            VALUES (:v_run_id, 'SEMANTIC_METRIC', :v_err_code, :v_err_msg);
        END;

        -- ---------------------------------------------------------------
        -- Advance watermarks for ALL databases in one statement.
        -- New databases get their first-ever watermark row inserted here.
        -- ---------------------------------------------------------------
        MERGE INTO ADMIN_DB.VERSION_CONTROL.INCREMENTAL_STATE AS tgt
        USING (
            SELECT f.VALUE::STRING AS DATABASE_NAME
            FROM TABLE(FLATTEN(INPUT => :P_TARGET_DATABASES)) f
        ) AS src
        ON tgt.DATABASE_NAME = src.DATABASE_NAME
        WHEN MATCHED    THEN UPDATE SET LAST_RUN_AT = CURRENT_TIMESTAMP(), LAST_RUN_ID = :v_run_id, UPDATED_AT = CURRENT_TIMESTAMP()
        WHEN NOT MATCHED THEN INSERT (DATABASE_NAME, LAST_RUN_AT, LAST_RUN_ID)
                              VALUES (src.DATABASE_NAME, CURRENT_TIMESTAMP(), :v_run_id);

    END IF; -- NOT P_DRY_RUN

    RETURN OBJECT_CONSTRUCT(
        'run_id',         :v_run_id,
        'rows_loaded',    :v_rows_loaded,
        'errors',         :v_errors,
        'db_run_summary', :v_db_run_summary,
        'error_details',  :v_error_list
    );
END;
;


-- =============================================================================
-- Convenience view — latest snapshot per object (current state)
-- =============================================================================
CREATE OR REPLACE VIEW ADMIN_DB.VERSION_CONTROL.V_OBJECT_CURRENT_STATE AS
SELECT *
FROM ADMIN_DB.VERSION_CONTROL.OBJECT_DDL_LOG
QUALIFY ROW_NUMBER() OVER (
    PARTITION BY DATABASE_NAME, SCHEMA_NAME, OBJECT_NAME, OBJECT_TYPE
    ORDER BY LAST_DDL_AT DESC, CAPTURED_AT DESC
) = 1;





-- =============================================================================
-- UTILITY: Force a full rescan of one database
-- Use after: a bulk migration, DB clone, or if you suspect the watermark
-- is stale and changes may have been missed.
-- =============================================================================
CREATE OR REPLACE PROCEDURE ADMIN_DB.VERSION_CONTROL.FORCE_FULL_RESCAN(
    P_DB_NAME STRING
)
RETURNS OBJECT
LANGUAGE SQL
EXECUTE AS CALLER
AS
$$
BEGIN
    DELETE FROM ADMIN_DB.VERSION_CONTROL.INCREMENTAL_STATE
    WHERE  DATABASE_NAME = :P_DB_NAME;

    RETURN OBJECT_CONSTRUCT(
        'message',  'Watermark cleared for ' || :P_DB_NAME
                    || '. Next CAPTURE_DDL_SNAPSHOT call will run a full bootstrap scan.',
        'database', :P_DB_NAME
    );
END;
$$;


-- =============================================================================
-- Ad-hoc usage
-- =============================================================================
-- First run — TEST_DB and BDS both new → both FULL:
-- CALL ADMIN_DB.VERSION_CONTROL.LOAD_OBJECT_DDL_LOG(
--     ARRAY_CONSTRUCT('TEST_DB', 'BDS'),
--     ARRAY_CONSTRUCT('INFORMATION_SCHEMA', 'PUBLIC'),
--     FALSE
-- );
--
-- Add new database — PROD_DB new (FULL), TEST_DB and BDS existing (INCREMENTAL):
-- CALL ADMIN_DB.VERSION_CONTROL.LOAD_OBJECT_DDL_LOG(
--     ARRAY_CONSTRUCT('TEST_DB', 'BDS', 'PROD_DB'),
--     ARRAY_CONSTRUCT('INFORMATION_SCHEMA', 'PUBLIC'),
--     FALSE
-- );
