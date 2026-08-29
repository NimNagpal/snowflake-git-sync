-- =============================================================================
-- FILE: 04_utility_views_and_helpers.sql (v2 - Pure Snowflake SQL Scripting)
-- PURPOSE: History views, rollback, and version tagging — zero Python
-- =============================================================================

USE DATABASE ADMIN_DB;
USE SCHEMA VERSION_CONTROL;

-- -------------------------
-- VIEW: Latest change per object (git log --oneline)
-- -------------------------
CREATE OR REPLACE VIEW ADMIN_DB.VERSION_CONTROL.VW_RECENT_CHANGES AS
SELECT
    CHANGE_ID,
    RUN_ID,
    DATABASE_NAME,
    SCHEMA_NAME,
    OBJECT_NAME,
    OBJECT_TYPE,
    CHANGE_TYPE,
    COMMIT_MESSAGE,
    CHANGED_AT,
    CAPTURED_BY,
    VERSION_TAG,
    SOURCE_QUERY_ID,
    SOURCE_USER_NAME,
    LENGTH(NEW_DDL_TEXT) - LENGTH(OLD_DDL_TEXT) AS CHARS_DELTA,
    ROW_NUMBER() OVER (
        PARTITION BY DATABASE_NAME, SCHEMA_NAME, OBJECT_NAME, OBJECT_TYPE
        ORDER BY CHANGED_AT DESC
    ) AS CHANGE_RANK
FROM ADMIN_DB.VERSION_CONTROL.DDL_CHANGE_LOG;


-- -------------------------
-- VIEW: Full object history (git log <file>)
-- -------------------------
CREATE OR REPLACE VIEW ADMIN_DB.VERSION_CONTROL.VW_OBJECT_HISTORY AS
SELECT
    DATABASE_NAME,
    SCHEMA_NAME,
    OBJECT_NAME,
    OBJECT_TYPE,
    CHANGE_ID,
    CHANGE_TYPE,
    COMMIT_MESSAGE,
    CHANGED_AT,
    OLD_DDL_HASH,
    NEW_DDL_HASH,
    OLD_DDL_TEXT,
    NEW_DDL_TEXT,
    VERSION_TAG,
    SOURCE_QUERY_ID,
    SOURCE_USER_NAME
FROM ADMIN_DB.VERSION_CONTROL.DDL_CHANGE_LOG
ORDER BY DATABASE_NAME, SCHEMA_NAME, OBJECT_NAME, CHANGED_AT DESC;


-- -------------------------
-- VIEW: Changes in last 24 hours (git diff --stat)
-- -------------------------
CREATE OR REPLACE VIEW ADMIN_DB.VERSION_CONTROL.VW_CHANGES_LAST_24H AS
SELECT
    DATABASE_NAME,
    SCHEMA_NAME,
    OBJECT_TYPE,
    OBJECT_NAME,
    CHANGE_TYPE,
    COMMIT_MESSAGE,
    SOURCE_QUERY_ID,
    SOURCE_USER_NAME,
    CHANGED_AT
FROM ADMIN_DB.VERSION_CONTROL.DDL_CHANGE_LOG
WHERE CHANGED_AT >= DATEADD('hour', -24, CURRENT_TIMESTAMP())
ORDER BY CHANGED_AT DESC;


-- -------------------------
-- VIEW: Run summary (git log --merges)
-- -------------------------
CREATE OR REPLACE VIEW ADMIN_DB.VERSION_CONTROL.VW_RUN_SUMMARY AS
SELECT
    r.RUN_ID,
    r.STARTED_AT,
    r.FINISHED_AT,
    DATEDIFF('second', r.STARTED_AT, r.FINISHED_AT) AS DURATION_SECS,
    r.STATUS,
    r.OBJECTS_SCANNED,
    r.CHANGES_DETECTED,
    r.ERROR_COUNT,
    r.TRIGGERED_BY,
    COUNT(cl.CHANGE_ID)                  AS LOG_ENTRIES,
    COUNT_IF(cl.CHANGE_TYPE = 'CREATED') AS CREATED_COUNT,
    COUNT_IF(cl.CHANGE_TYPE = 'MODIFIED') AS MODIFIED_COUNT,
    COUNT_IF(cl.CHANGE_TYPE = 'DROPPED') AS DROPPED_COUNT
FROM ADMIN_DB.VERSION_CONTROL.CAPTURE_RUN_LOG r
LEFT JOIN ADMIN_DB.VERSION_CONTROL.DDL_CHANGE_LOG cl ON cl.RUN_ID = r.RUN_ID
GROUP BY ALL
ORDER BY r.STARTED_AT DESC;


-- =============================================================================
-- PROCEDURE: Rollback an object to a previous version (git checkout <commit>)
-- Pure SQL Scripting — no Python
-- =============================================================================
CREATE OR REPLACE PROCEDURE ADMIN_DB.VERSION_CONTROL.SP_UTIL_ROLLBACK_OBJECT(
    DATABASE_NAME_PARAM STRING,
    SCHEMA_NAME_PARAM   STRING,
    OBJECT_NAME_PARAM   STRING,
    OBJECT_TYPE_PARAM   STRING,
    TARGET_CHANGE_ID    STRING
)
RETURNS OBJECT
LANGUAGE SQL
EXECUTE AS CALLER
AS
$$
DECLARE
    target_ddl      STRING;
    target_ts       TIMESTAMP_NTZ;
    orig_commit     STRING;
    rollback_msg    STRING;
BEGIN
    -- Fetch the DDL at the target change
    SELECT NEW_DDL_TEXT, CHANGED_AT, COMMIT_MESSAGE
    INTO   :target_ddl, :target_ts, :orig_commit
    FROM   ADMIN_DB.VERSION_CONTROL.DDL_CHANGE_LOG
    WHERE  CHANGE_ID      = :TARGET_CHANGE_ID
      AND  DATABASE_NAME  = :DATABASE_NAME_PARAM
      AND  SCHEMA_NAME    = :SCHEMA_NAME_PARAM
      AND  OBJECT_NAME    = :OBJECT_NAME_PARAM
      AND  OBJECT_TYPE    = :OBJECT_TYPE_PARAM;

    IF (target_ddl IS NULL) THEN
        RETURN OBJECT_CONSTRUCT(
            'success', FALSE,
            'error',   'CHANGE_ID not found or object mismatch'
        );
    END IF;

    -- Re-execute the historical DDL
    -- GET_DDL output is always CREATE OR REPLACE compatible in Snowflake
    EXECUTE IMMEDIATE :target_ddl;

    rollback_msg := 'revert: ''' || :OBJECT_NAME_PARAM
                 || ''' rolled back to state from ' || :target_ts::STRING
                 || ' (was: ' || COALESCE(:orig_commit, 'unknown') || ')';

    -- Update the snapshot table to the rolled-back DDL
    UPDATE ADMIN_DB.VERSION_CONTROL.OBJECT_SNAPSHOTS
    SET DDL_TEXT    = :target_ddl,
        DDL_HASH    = SHA2(:target_ddl, 256),
        CAPTURED_AT = CURRENT_TIMESTAMP(),
        IS_ACTIVE   = TRUE
    WHERE DATABASE_NAME = :DATABASE_NAME_PARAM
      AND SCHEMA_NAME   = :SCHEMA_NAME_PARAM
      AND OBJECT_NAME   = :OBJECT_NAME_PARAM
      AND OBJECT_TYPE   = :OBJECT_TYPE_PARAM;

    -- Log the rollback as a new change entry (preserves audit trail)
    INSERT INTO ADMIN_DB.VERSION_CONTROL.DDL_CHANGE_LOG
        (DATABASE_NAME, SCHEMA_NAME, OBJECT_NAME, OBJECT_TYPE,
         CHANGE_TYPE, NEW_DDL_TEXT, NEW_DDL_HASH, COMMIT_MESSAGE)
    VALUES
        (:DATABASE_NAME_PARAM, :SCHEMA_NAME_PARAM, :OBJECT_NAME_PARAM, :OBJECT_TYPE_PARAM,
         'ROLLBACK', :target_ddl, SHA2(:target_ddl, 256), :rollback_msg);

    RETURN OBJECT_CONSTRUCT(
        'success',         TRUE,
        'rolled_back_to',  :target_ts::STRING,
        'original_commit', :orig_commit,
        'rollback_message',:rollback_msg
    );

EXCEPTION
    WHEN OTHER THEN
        RETURN OBJECT_CONSTRUCT(
            'success', FALSE,
            'error',   SQLERRM,
            'sqlcode', SQLCODE
        );
END;
$$;


-- =============================================================================
-- PROCEDURE: Tag a version (git tag v1.2)
-- Pure SQL Scripting — no Python
-- =============================================================================
/*NOT REQUIRED AT THE MOMENT
        CREATE OR REPLACE PROCEDURE ADMIN_DB.VERSION_CONTROL.SP_TAG_VERSION(
            TAG_NAME            STRING,
            TAG_DESCRIPTION     STRING,
            DATABASE_NAME_PARAM STRING,   -- NULL = all databases
            SCHEMA_NAME_PARAM   STRING    -- NULL = all schemas
        )
        RETURNS OBJECT
        LANGUAGE SQL
        EXECUTE AS CALLER
        AS
        $$
        DECLARE
            rows_tagged INT DEFAULT 0;
        BEGIN
            UPDATE ADMIN_DB.VERSION_CONTROL.DDL_CHANGE_LOG
            SET VERSION_TAG    = :TAG_NAME,
                COMMIT_MESSAGE = COMMIT_MESSAGE
                               || ' [tag: ' || :TAG_NAME || ']'
                               || CASE WHEN :TAG_DESCRIPTION IS NOT NULL
                                       THEN ' ' || :TAG_DESCRIPTION
                                       ELSE '' END
            WHERE VERSION_TAG IS NULL
              AND (DATABASE_NAME = :DATABASE_NAME_PARAM OR :DATABASE_NAME_PARAM IS NULL)
              AND (SCHEMA_NAME   = :SCHEMA_NAME_PARAM   OR :SCHEMA_NAME_PARAM   IS NULL)
              AND CHANGED_AT >= DATEADD('minute', -15, CURRENT_TIMESTAMP());
        
            rows_tagged := SQLROWCOUNT;
        
            RETURN OBJECT_CONSTRUCT(
                'tag',         :TAG_NAME,
                'description', :TAG_DESCRIPTION,
                'rows_tagged', :rows_tagged
            );
        END;
        $$;
*/

-- =============================================================================
-- PROCEDURE: Compare two versions of an object (git diff <hash1> <hash2>)
-- Returns old and new DDL side by side for manual inspection
-- =============================================================================
CREATE OR REPLACE PROCEDURE ADMIN_DB.VERSION_CONTROL.SP_UTIL_DIFF_VERSIONS(
    DATABASE_NAME_PARAM STRING,
    SCHEMA_NAME_PARAM   STRING,
    OBJECT_NAME_PARAM   STRING,
    OBJECT_TYPE_PARAM   STRING,
    FROM_CHANGE_ID      STRING,
    TO_CHANGE_ID        STRING   -- NULL = current snapshot
)
RETURNS TABLE (
    FIELD       STRING,
    FROM_VALUE  STRING,
    TO_VALUE    STRING
)
LANGUAGE SQL
EXECUTE AS CALLER
AS
$$
DECLARE
    from_ddl    STRING;
    to_ddl      STRING;
    from_ts     TIMESTAMP_NTZ;
    to_ts       TIMESTAMP_NTZ;
    from_hash   STRING;
    to_hash     STRING;
BEGIN
    SELECT NEW_DDL_TEXT, CHANGED_AT, NEW_DDL_HASH
    INTO   :from_ddl, :from_ts, :from_hash
    FROM   ADMIN_DB.VERSION_CONTROL.DDL_CHANGE_LOG
    WHERE  CHANGE_ID = :FROM_CHANGE_ID;

    IF (:TO_CHANGE_ID IS NULL) THEN
        -- Compare to current live snapshot
        SELECT DDL_TEXT, CAPTURED_AT, DDL_HASH
        INTO   :to_ddl, :to_ts, :to_hash
        FROM   ADMIN_DB.VERSION_CONTROL.OBJECT_SNAPSHOTS
        WHERE  DATABASE_NAME = :DATABASE_NAME_PARAM
          AND  SCHEMA_NAME   = :SCHEMA_NAME_PARAM
          AND  OBJECT_NAME   = :OBJECT_NAME_PARAM
          AND  OBJECT_TYPE   = :OBJECT_TYPE_PARAM;
    ELSE
        SELECT NEW_DDL_TEXT, CHANGED_AT, NEW_DDL_HASH
        INTO   :to_ddl, :to_ts, :to_hash
        FROM   ADMIN_DB.VERSION_CONTROL.DDL_CHANGE_LOG
        WHERE  CHANGE_ID = :TO_CHANGE_ID;
    END IF;

    RETURN TABLE (
        SELECT * FROM (VALUES
            ('timestamp',  (:from_ts)::STRING,  (:to_ts)::STRING),
            ('hash',       :from_hash,         :to_hash),
            ('ddl_length', LENGTH(:from_ddl)::STRING, LENGTH(:to_ddl)::STRING),
            ('changed',    CASE WHEN :from_hash = :to_hash THEN 'NO' ELSE 'YES' END, NULL),
            ('from_ddl',   :from_ddl,          NULL),
            ('to_ddl',     NULL,                :to_ddl)
        ) AS t(FIELD, FROM_VALUE, TO_VALUE)
    );
END;
$$;
