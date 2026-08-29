-- =============================================================================
-- FILE: 05_script_registry_and_manual_ops.sql (v2 - Pure Snowflake SQL Scripting)
-- PURPOSE: Script registry, manual commits, alerting — zero Python
-- =============================================================================

USE DATABASE ADMIN_DB;
USE SCHEMA VERSION_CONTROL;


-- =============================================================================
-- PROCEDURE: Register or update a script in the registry
-- =============================================================================
CREATE OR REPLACE PROCEDURE ADMIN_DB.VERSION_CONTROL.REGISTER_SCRIPT(
    SCRIPT_NAME_PARAM    STRING,
    SCRIPT_PATH_PARAM    STRING,
    SCRIPT_CONTENT_PARAM STRING,
    LANGUAGE_PARAM       STRING
)
RETURNS OBJECT
LANGUAGE SQL
EXECUTE AS CALLER
AS
$$
DECLARE
    new_hash    STRING;
    existing    INT  DEFAULT 0;
    change_type STRING;
    old_hash    STRING;
BEGIN
    new_hash := SHA2(:SCRIPT_CONTENT_PARAM, 256);

    SELECT COUNT(*), MAX(CONTENT_HASH)
    INTO   :existing, :old_hash
    FROM   ADMIN_DB.VERSION_CONTROL.SCRIPT_REGISTRY
    WHERE  SCRIPT_NAME = :SCRIPT_NAME_PARAM;

    IF (existing = 0) THEN
        change_type := 'CREATED';
        INSERT INTO ADMIN_DB.VERSION_CONTROL.SCRIPT_REGISTRY
            (SCRIPT_NAME, SCRIPT_PATH, SCRIPT_CONTENT, CONTENT_HASH, LANGUAGE)
        VALUES
            (:SCRIPT_NAME_PARAM, :SCRIPT_PATH_PARAM,
             :SCRIPT_CONTENT_PARAM, :new_hash, :LANGUAGE_PARAM);

    ELSEIF (:old_hash != :new_hash) THEN
        change_type := 'MODIFIED';
        UPDATE ADMIN_DB.VERSION_CONTROL.SCRIPT_REGISTRY
        SET SCRIPT_CONTENT  = :SCRIPT_CONTENT_PARAM,
            CONTENT_HASH    = :new_hash,
            SCRIPT_PATH     = :SCRIPT_PATH_PARAM,
            LAST_UPDATED_AT = CURRENT_TIMESTAMP(),
            LAST_UPDATED_BY = CURRENT_USER()
        WHERE SCRIPT_NAME   = :SCRIPT_NAME_PARAM;

    ELSE
        -- No change
        RETURN OBJECT_CONSTRUCT(
            'script',      :SCRIPT_NAME_PARAM,
            'change_type', 'NO_CHANGE',
            'hash',        :new_hash
        );
    END IF;

    -- Log into the shared change history
    INSERT INTO ADMIN_DB.VERSION_CONTROL.DDL_CHANGE_LOG
        (DATABASE_NAME, SCHEMA_NAME, OBJECT_NAME, OBJECT_TYPE,
         CHANGE_TYPE, NEW_DDL_TEXT, NEW_DDL_HASH, COMMIT_MESSAGE)
    VALUES
        ('SCRIPTS', 'REGISTRY', :SCRIPT_NAME_PARAM, :LANGUAGE_PARAM,
         :change_type, :SCRIPT_CONTENT_PARAM, :new_hash,
         'chore: script ''' || :SCRIPT_NAME_PARAM || ''' ' || LOWER(:change_type));

    RETURN OBJECT_CONSTRUCT(
        'script',      :SCRIPT_NAME_PARAM,
        'change_type', :change_type,
        'hash',        :new_hash
    );
END;
$$;


-- =============================================================================
-- PROCEDURE: Manual commit for a specific object (git commit -m "...")
-- =============================================================================
/* NOT REQUIRED AT THE MOMENT
        CREATE OR REPLACE PROCEDURE ADMIN_DB.VERSION_CONTROL.MANUAL_COMMIT(
            DATABASE_NAME_PARAM STRING,
            SCHEMA_NAME_PARAM   STRING,
            OBJECT_NAME_PARAM   STRING,
            OBJECT_TYPE_PARAM   STRING,
            COMMIT_MSG_PARAM    STRING
        )
        RETURNS OBJECT
        LANGUAGE SQL
        EXECUTE AS CALLER
        AS
        $$
        DECLARE
            current_ddl     STRING;
            current_hash    STRING;
            full_name       STRING;
        BEGIN
            full_name := :DATABASE_NAME_PARAM || '.' || :SCHEMA_NAME_PARAM || '.' || :OBJECT_NAME_PARAM;
        
            SELECT GET_DDL(:OBJECT_TYPE_PARAM, :full_name)
            INTO   :current_ddl;
        
            current_hash := SHA2(:current_ddl, 256);
        
            MERGE INTO ADMIN_DB.VERSION_CONTROL.OBJECT_SNAPSHOTS AS tgt
            USING (
                SELECT
                    :DATABASE_NAME_PARAM AS DATABASE_NAME,
                    :SCHEMA_NAME_PARAM   AS SCHEMA_NAME,
                    :OBJECT_NAME_PARAM   AS OBJECT_NAME,
                    :OBJECT_TYPE_PARAM   AS OBJECT_TYPE,
                    :current_ddl         AS DDL_TEXT,
                    :current_hash        AS DDL_HASH
            ) AS src
            ON  tgt.DATABASE_NAME = src.DATABASE_NAME
            AND tgt.SCHEMA_NAME   = src.SCHEMA_NAME
            AND tgt.OBJECT_NAME   = src.OBJECT_NAME
            AND tgt.OBJECT_TYPE   = src.OBJECT_TYPE
            WHEN MATCHED THEN UPDATE SET
                DDL_TEXT    = src.DDL_TEXT,
                DDL_HASH    = src.DDL_HASH,
                CAPTURED_AT = CURRENT_TIMESTAMP(),
                IS_ACTIVE   = TRUE
            WHEN NOT MATCHED THEN INSERT
                (DATABASE_NAME, SCHEMA_NAME, OBJECT_NAME, OBJECT_TYPE, DDL_TEXT, DDL_HASH)
            VALUES
                (src.DATABASE_NAME, src.SCHEMA_NAME, src.OBJECT_NAME,
                 src.OBJECT_TYPE, src.DDL_TEXT, src.DDL_HASH);
        
            INSERT INTO ADMIN_DB.VERSION_CONTROL.DDL_CHANGE_LOG
                (DATABASE_NAME, SCHEMA_NAME, OBJECT_NAME, OBJECT_TYPE,
                 CHANGE_TYPE, NEW_DDL_TEXT, NEW_DDL_HASH, COMMIT_MESSAGE)
            VALUES
                (:DATABASE_NAME_PARAM, :SCHEMA_NAME_PARAM, :OBJECT_NAME_PARAM, :OBJECT_TYPE_PARAM,
                 'MANUAL_COMMIT', :current_ddl, :current_hash, :COMMIT_MSG_PARAM);
        
            RETURN OBJECT_CONSTRUCT(
                'object',     :OBJECT_NAME_PARAM,
                'hash',       :current_hash,
                'commit_msg', :COMMIT_MSG_PARAM
            );
        
        EXCEPTION
            WHEN OTHER THEN
                RETURN OBJECT_CONSTRUCT(
                    'success', FALSE,
                    'error',   SQLERRM
                );
        END;
        $$;
*/


-- =============================================================================
-- ALERT: Notify on unexpected off-hours DDL changes
-- Pure SQL — no Python needed
-- =============================================================================
/* NOT REQUIRED AT THE MOMENT
    CREATE OR REPLACE ALERT ADMIN_DB.VERSION_CONTROL.ALERT_UNEXPECTED_DDL_CHANGE
        WAREHOUSE = VC_TASK_WH
        SCHEDULE  = '15 MINUTE'
        IF (
            EXISTS (
                SELECT 1
                FROM ADMIN_DB.VERSION_CONTROL.DDL_CHANGE_LOG
                WHERE CHANGED_AT  >= DATEADD('minute', -15, CURRENT_TIMESTAMP())
                  AND CHANGE_TYPE IN ('MODIFIED', 'DROPPED')
                  AND (HOUR(CHANGED_AT) < 7 OR HOUR(CHANGED_AT) > 20)
            )
        )
        THEN
            CALL SYSTEM$SEND_EMAIL(
                'vc_notifications',
                'data-team@yourcompany.com',
                'Snowflake Schema Change Alert',
                (
                    SELECT LISTAGG(
                        CHANGE_TYPE || ': '
                        || DATABASE_NAME || '.' || SCHEMA_NAME || '.' || OBJECT_NAME
                        || ' (' || OBJECT_TYPE || ')',
                        CHR(10)
                    )
                    FROM ADMIN_DB.VERSION_CONTROL.DDL_CHANGE_LOG
                    WHERE CHANGED_AT  >= DATEADD('minute', -15, CURRENT_TIMESTAMP())
                      AND CHANGE_TYPE IN ('MODIFIED', 'DROPPED')
                )
            );
    
    ALTER ALERT ADMIN_DB.VERSION_CONTROL.ALERT_UNEXPECTED_DDL_CHANGE RESUME;
*/

-- =============================================================================
-- Useful ad-hoc queries
-- =============================================================================

-- View all changes in the last 7 days
-- SELECT * FROM ADMIN_DB.VERSION_CONTROL.V_RECENT_CHANGES
-- WHERE CHANGED_AT >= DATEADD('day', -7, CURRENT_TIMESTAMP())
-- ORDER BY CHANGED_AT DESC;

-- Full history of one object
-- SELECT * FROM ADMIN_DB.VERSION_CONTROL.V_OBJECT_HISTORY
-- WHERE DATABASE_NAME = 'PROD_DB'
--   AND SCHEMA_NAME   = 'PUBLIC'
--   AND OBJECT_NAME   = 'MY_TABLE';

-- Diff two versions of an object
-- CALL ADMIN_DB.VERSION_CONTROL.DIFF_VERSIONS(
--     'PROD_DB', 'PUBLIC', 'MY_TABLE', 'TABLE',
--     '<from_change_id>', '<to_change_id_or_null_for_current>'
-- );

-- Rollback an object
-- CALL ADMIN_DB.VERSION_CONTROL.ROLLBACK_OBJECT(
--     'PROD_DB', 'PUBLIC', 'MY_TABLE', 'TABLE', '<change_id>'
-- );

-- Manually commit a specific object
-- CALL ADMIN_DB.VERSION_CONTROL.MANUAL_COMMIT(
--     'PROD_DB', 'PUBLIC', 'MY_TABLE', 'TABLE',
--     'fix: added NOT NULL constraint to CUSTOMER_ID'
-- );

-- Tag the current state as a release
-- CALL ADMIN_DB.VERSION_CONTROL.TAG_VERSION(
--     'v2.0-release', 'Post-migration schema freeze', 'PROD_DB', NULL
-- );

-- Dry-run to preview what would be captured
-- CALL ADMIN_DB.VERSION_CONTROL.CAPTURE_DDL_SNAPSHOT(
--     ARRAY_CONSTRUCT('PROD_DB'),
--     ARRAY_CONSTRUCT('INFORMATION_SCHEMA'),
--     TRUE
-- );
