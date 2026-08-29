-- =============================================================================
-- FILE: 03_scheduling_tasks.sql
-- PURPOSE: Snowflake Tasks to automate DDL capture + GitHub sync on a schedule
-- =============================================================================

USE DATABASE ADMIN_DB;
USE SCHEMA VERSION_CONTROL;

-- -------------------------
-- Warehouse for task execution
-- -------------------------
-- Use an XS warehouse; DDL capture is lightweight.
/* NO NEW WAREHOUSE
    CREATE WAREHOUSE IF NOT EXISTS VC_TASK_WH
        WAREHOUSE_SIZE = 'X-SMALL'
        AUTO_SUSPEND   = 60
        AUTO_RESUME    = TRUE
        COMMENT        = 'Dedicated warehouse for version control tasks';
*/

-- =============================================================================
-- TASK 1: Capture DDL Snapshots (every 15 minutes)
-- =============================================================================
CREATE OR REPLACE TASK ADMIN_DB.VERSION_CONTROL.TASK_CAPTURE_DDL
    WAREHOUSE   = WH_QUERY
    SCHEDULE    = 'USING CRON 0 0 * * * America/Chicago'
    COMMENT     = 'Captures DDL snapshots for all tracked objects across target databases'
AS
CALL ADMIN_DB.VERSION_CONTROL.LOAD_OBJECT_DDL_LOG(
    ARRAY_CONSTRUCT('ADMIN_DB','ADMIN_DB_STAGING','ANALYST', 'COMMON', 'BDS', 'IDS', 'ODS', 'ODS_STAGING', 'LEGACY_DB','MODELS'   ),  -- <-- Update with your databases
    ARRAY_CONSTRUCT('INFORMATION_SCHEMA', 'PUBLIC' ),  -- <-- Schemas to exclude
    FALSE  -- dry_run = FALSE (commit changes)
);
ALTER TASK ADMIN_DB.VERSION_CONTROL.TASK_CAPTURE_DDL RESUME;

-- =============================================================================
-- TASK 2: Sync to GitHub (runs after TASK_CAPTURE_DDL, only if changes exist)
-- =============================================================================
/*--PENDING
        CREATE OR REPLACE TASK ADMIN_DB.VERSION_CONTROL.TASK_SYNC_GITHUB
            WAREHOUSE   = VC_TASK_WH
            AFTER       ADMIN_DB.VERSION_CONTROL.TASK_CAPTURE_DDL
            -- Conditional: only push to GitHub if the last run detected changes
            WHEN SYSTEM$STREAM_HAS_DATA('ADMIN_DB.VERSION_CONTROL.CHANGE_LOG_STREAM')
            COMMENT     = 'Pushes DDL changes to GitHub after each capture run'
        AS
        CALL ADMIN_DB.VERSION_CONTROL.SYNC_TO_GITHUB(NULL, 200);
        
        
        -- Stream to trigger conditional GitHub sync only when new changes exist
        CREATE OR REPLACE STREAM ADMIN_DB.VERSION_CONTROL.CHANGE_LOG_STREAM
            ON TABLE ADMIN_DB.VERSION_CONTROL.DDL_CHANGE_LOG
            COMMENT = 'Detects new rows in DDL_CHANGE_LOG to conditionally trigger GitHub sync';
*/

-- =============================================================================
-- TASK 3: Weekly Snapshot Export to Stage (full backup, every Sunday at 02:00)
-- =============================================================================
CREATE OR REPLACE TASK ADMIN_DB.VERSION_CONTROL.TASK_WEEKLY_EXPORT
    WAREHOUSE = VC_TASK_WH
    SCHEDULE  = 'USING CRON 0 2 * * 0 UTC'
    COMMENT   = 'Full DDL export of current object state to internal stage for cold backup'
AS
COPY INTO @ADMIN_DB.VERSION_CONTROL.VC_BACKUP_STAGE/snapshots/
FROM (
    SELECT
        DATABASE_NAME,
        SCHEMA_NAME,
        OBJECT_TYPE,
        OBJECT_NAME,
        OWNER_NAME,
        LAST_DDL_AT,
        LAST_DDL_BY,
        DEFINITION,
        CAPTURED_AT
    FROM ADMIN_DB.VERSION_CONTROL.V_OBJECT_CURRENT_STATE
    WHERE DELETED_AT IS NULL   -- live objects only
)
FILE_FORMAT = (TYPE = 'JSON')
OVERWRITE   = FALSE;


-- Internal stage for backup exports
CREATE STAGE IF NOT EXISTS ADMIN_DB.VERSION_CONTROL.VC_BACKUP_STAGE
    COMMENT = 'Internal stage for DDL backup exports';


-- =============================================================================
-- Enable / Disable helpers
-- =============================================================================

-- Resume all VC tasks
    -- ALTER TASK ADMIN_DB.VERSION_CONTROL.TASK_SYNC_GITHUB  RESUME;
    -- ALTER TASK ADMIN_DB.VERSION_CONTROL.TASK_CAPTURE_DDL   RESUME;
    -- ALTER TASK ADMIN_DB.VERSION_CONTROL.TASK_WEEKLY_EXPORT RESUME;

-- Suspend all VC tasks (e.g. during maintenance)
-- ALTER TASK ADMIN_DB.VERSION_CONTROL.TASK_SYNC_GITHUB  SUSPEND;
-- ALTER TASK ADMIN_DB.VERSION_CONTROL.TASK_CAPTURE_DDL   SUSPEND;
-- ALTER TASK ADMIN_DB.VERSION_CONTROL.TASK_WEEKLY_EXPORT SUSPEND;
