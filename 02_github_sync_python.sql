-- =============================================================================
-- FILE: 02_github_sync_python.sql
-- PURPOSE: Python stored procedure that reads V_PENDING_GITHUB_SYNC and
--          commits each pending object's DDL directly to GitHub via the
--          Contents API. Runs inside Snowflake — no external scheduler needed.
--
-- DEPENDENCIES:
--   - OBJECT_DDL_LOG and V_PENDING_GITHUB_SYNC from 06_object_ddl_log_loader.sql
--   - VC_TASK_WH warehouse from 03_scheduling_tasks.sql
--
-- GITHUB API STRATEGY (per object):
--   1. GET  /repos/{owner}/{repo}/contents/{file_path}?ref={branch}
--          → 200: file exists, capture current SHA for the update
--          → 404: new file, no SHA needed
--   2a. PUT  (live object)  — create or update the .sql file
--   2b. DELETE (dropped object) — remove the file from GitHub
--   3. MARK_SYNCED_TO_GITHUB(log_id, new_sha) — writes to GITHUB_SYNC_LOG
--
-- COMMIT MESSAGE FORMAT:
--   snowflake: create|update|drop <object_type> <db>.<schema>.<name> [by <user>]
--   e.g.  snowflake: update table PROD_DB.SALES.ORDERS [by JOHN_DOE]
-- =============================================================================

USE DATABASE ADMIN_DB;
USE SCHEMA VERSION_CONTROL;


-- =============================================================================
-- SYNC TRACKING TABLE  (keep as-is — mechanism-agnostic)
-- =============================================================================
CREATE TABLE IF NOT EXISTS ADMIN_DB.VERSION_CONTROL.GITHUB_SYNC_LOG (
    SYNC_ID    STRING        DEFAULT UUID_STRING(),
    LOG_ID     STRING        NOT NULL,  -- references OBJECT_DDL_LOG.LOG_ID
    GITHUB_SHA STRING,                  -- blob SHA from GitHub Contents API response;
                                        -- required by GitHub for subsequent file updates
    SYNCED_AT  TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);


-- =============================================================================
-- VW_PENDING_GITHUB_SYNC  (keep as-is — mechanism-agnostic)
-- One row per object that has not yet been pushed to GitHub.
-- Shows the latest version of each object only — sync reflects current state.
-- IS_DELETED = TRUE means the object was dropped; procedure will DELETE
-- the file from GitHub rather than PUT.
-- =============================================================================
CREATE OR REPLACE VIEW ADMIN_DB.VERSION_CONTROL.VW_PENDING_GITHUB_SYNC AS
SELECT top 5
    l.LOG_ID,
    l.DATABASE_NAME,
    l.SCHEMA_NAME,
    l.OBJECT_NAME,
    l.OBJECT_TYPE,
    l.LAST_DDL_AT,
    l.LAST_DDL_BY,
    (l.DELETED_AT IS NOT NULL) AS IS_DELETED,

    -- Folder: lowercase object type + 's', underscores for spaces
    -- TABLE → tables | DYNAMIC TABLE → dynamic_tables | SEMANTIC_VIEW → semantic_views
    LOWER(REPLACE(l.OBJECT_TYPE, ' ', '_')) || 's' AS FOLDER_NAME,

    -- Repo-relative file path used directly in the GitHub Contents API URI
    'EDH/' || l.DATABASE_NAME || '/'
        || l.SCHEMA_NAME || '/'
        || LOWER(REPLACE(l.OBJECT_TYPE, ' ', '_')) || 's/'
        || l.OBJECT_NAME || '.sql' AS FILE_PATH,

    -- File content to write
    CASE
        WHEN l.DELETED_AT IS NOT NULL THEN
            '-- OBJECT DROPPED'                                          || CHR(10) ||
            '-- ' || l.DATABASE_NAME || '.' || l.SCHEMA_NAME || '.' || l.OBJECT_NAME
                   || ' (' || l.OBJECT_TYPE || ')'                      || CHR(10) ||
            '-- Dropped at:   ' || l.DELETED_AT::STRING                 || CHR(10) ||
            '-- Last DDL by:  ' || COALESCE(l.LAST_DDL_BY, 'unknown')   || CHR(10) ||
            '-- Last known DDL archived below.'                          || CHR(10) ||
            CHR(10) ||
            COALESCE(l.DEFINITION, '-- Definition not available at drop time.')
        ELSE
            COALESCE(l.DEFINITION, '-- Definition was NULL at capture time (GET_DDL may have failed).')
    END AS FILE_CONTENT

FROM (
    SELECT *
    FROM ADMIN_DB.VERSION_CONTROL.OBJECT_DDL_LOG
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY DATABASE_NAME, SCHEMA_NAME, OBJECT_NAME, OBJECT_TYPE
        ORDER BY LAST_DDL_AT DESC, CAPTURED_AT DESC
    ) = 1
) l
LEFT JOIN ADMIN_DB.VERSION_CONTROL.GITHUB_SYNC_LOG gs ON gs.LOG_ID = l.LOG_ID
WHERE gs.LOG_ID IS NULL and l.DELETED_AT IS  NULL
ORDER BY l.LAST_DDL_AT ASC;  -- oldest first → chronological commits in GitHub


-- =============================================================================
-- HELPER: MARK_SYNCED_TO_GITHUB
-- Called by SYNC_TO_GITHUB after each successful push.
-- Can also be called manually to recover from a partial failure.
-- =============================================================================
CREATE OR REPLACE PROCEDURE ADMIN_DB.VERSION_CONTROL.MARK_SYNCED_TO_GITHUB(
    P_LOG_ID     STRING,
    P_GITHUB_SHA STRING   -- blob SHA returned by GitHub; pass '' if unknown
)
RETURNS OBJECT
LANGUAGE SQL
EXECUTE AS CALLER
AS
BEGIN
    INSERT INTO ADMIN_DB.VERSION_CONTROL.GITHUB_SYNC_LOG (LOG_ID, GITHUB_SHA)
    VALUES (:P_LOG_ID, NULLIF(:P_GITHUB_SHA, ''));

    RETURN OBJECT_CONSTRUCT(
        'log_id',    :P_LOG_ID,
        'github_sha', :P_GITHUB_SHA,
        'synced_at', CURRENT_TIMESTAMP()::STRING
    );
END;


-- =============================================================================
-- EXTERNAL ACCESS PLUMBING  (run once as ACCOUNTADMIN)
-- =============================================================================
USE ROLE ACCOUNTADMIN;

-- Egress rule: only allow outbound to api.github.com
CREATE OR REPLACE NETWORK RULE ADMIN_DB.VERSION_CONTROL.GITHUB_NETWORK_RULE
    MODE       = EGRESS
    TYPE       = HOST_PORT
    VALUE_LIST = ('api.github.com:443')
    COMMENT    = 'Allows outbound HTTPS to GitHub Contents API only';

-- Store the GitHub PAT as a Snowflake Secret — never hardcode it in code
-- Replace <YOUR_GITHUB_PAT> with a fine-grained PAT scoped to:
--   Contents: Read and Write   (to create/update/delete .sql files)
--   Metadata: Read             (to verify the repo exists)
CREATE OR REPLACE SECRET ADMIN_DB.VERSION_CONTROL.GITHUB_PAT
    TYPE          = GENERIC_STRING
    SECRET_STRING = '<YOUR_GITHUB_PAT>'
    COMMENT       = 'GitHub fine-grained PAT for DDL sync — rotate every 90 days';

-- External Access Integration wiring the two together
CREATE OR REPLACE EXTERNAL ACCESS INTEGRATION GITHUB_DDL_SYNC_INTEGRATION
    ALLOWED_NETWORK_RULES          = (ADMIN_DB.VERSION_CONTROL.GITHUB_NETWORK_RULE)
    ALLOWED_AUTHENTICATION_SECRETS = (ADMIN_DB.VERSION_CONTROL.GITHUB_PAT)
    ENABLED                        = TRUE
    COMMENT                        = 'Allows SYNC_TO_GITHUB to call api.github.com';


-- =============================================================================
-- PYTHON STORED PROCEDURE: SYNC_TO_GITHUB
-- Reads V_PENDING_GITHUB_SYNC and pushes each row to GitHub.
-- Parameters:
--   P_REPO_OWNER  — GitHub organisation or username (e.g. 'juice-plus')
--   P_REPO_NAME   — repository name (e.g. 'snowflake-ddl')
--   P_BRANCH      — target branch (default 'main')
-- =============================================================================
USE ROLE SYSADMIN;
USE DATABASE ADMIN_DB;
USE SCHEMA VERSION_CONTROL;

CREATE OR REPLACE PROCEDURE ADMIN_DB.VERSION_CONTROL.SYNC_TO_GITHUB(
    P_REPO_OWNER  STRING,
    P_REPO_NAME   STRING,
    P_BRANCH      STRING DEFAULT 'main'
)
RETURNS OBJECT
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
HANDLER         = 'sync_handler'
EXTERNAL_ACCESS_INTEGRATIONS = (JP_ENTERPRISE_DATA_GITHUB_PAT_2)
SECRETS         = ('github_pat' = ADMIN_DB.INTEGRATION.JP_GITHUB_PAT_2)
PACKAGES        = ('snowflake-snowpark-python', 'requests')
EXECUTE AS CALLER
AS

import _snowflake
import requests
import base64

def sync_handler(session, p_repo_owner: str, p_repo_name: str, p_branch: str = 'main') -> dict:

    # -----------------------------------------------------------------------
    # Retrieve GitHub PAT from Snowflake Secret — never touches source code
    # -----------------------------------------------------------------------
    pat      = _snowflake.get_generic_secret_string('github_pat')
    base_url = f"https://api.github.com/repos/{p_repo_owner}/{p_repo_name}/contents"
    headers  = {
        'Authorization': f'token {pat}',
        'Accept':        'application/vnd.github.v3+json',
        'X-GitHub-Api-Version': '2022-11-28'
    }

    # -----------------------------------------------------------------------
    # Fetch pending rows
    # -----------------------------------------------------------------------
    pending = session.sql(
        "SELECT LOG_ID, DATABASE_NAME, SCHEMA_NAME, OBJECT_NAME, OBJECT_TYPE, "
        "       LAST_DDL_BY, IS_DELETED, FILE_PATH, FILE_CONTENT "
        "FROM ADMIN_DB.VERSION_CONTROL.VW_PENDING_GITHUB_SYNC"
    ).collect()

    synced_count = 0
    skip_count   = 0
    error_count  = 0
    error_list   = []

    for row in pending:
        log_id     = row['LOG_ID']
        file_path  = row['FILE_PATH']
        content    = row['FILE_CONTENT'] or ''
        is_deleted = row['IS_DELETED']
        obj_type   = row['OBJECT_TYPE'].lower()
        db_name    = row['DATABASE_NAME']
        schema     = row['SCHEMA_NAME']
        obj_name   = row['OBJECT_NAME']
        ddl_by     = row['LAST_DDL_BY'] or 'unknown'

        api_url = f"{base_url}/{file_path}"

        try:
            # -----------------------------------------------------------
            # Step 1: GET current file from GitHub → find existing SHA
            # Always GET rather than relying on stored SHA: handles the
            # case where someone edited the file directly in GitHub.
            # -----------------------------------------------------------
            error_list.append(f"DEBUG api_url: {api_url}")
            
            get_resp = requests.get(
                api_url,
                headers=headers,
                params={'ref': p_branch},
                timeout=30
            )

            current_sha  = None
            file_exists  = False

            if get_resp.status_code == 200:
                file_exists = True
                current_sha = get_resp.json().get('sha')
            elif get_resp.status_code == 404:
                file_exists = False
            elif get_resp.status_code == 403:
                error_list.append(f"GET {file_path}: 403 Forbidden — check PAT scope")
                error_count += 1
                continue
            else:
                error_list.append(f"GET {file_path}: HTTP {get_resp.status_code}")
                error_count += 1
                continue

            # -----------------------------------------------------------
            # Step 2a: DROPPED object → DELETE file from GitHub
            # -----------------------------------------------------------
            if is_deleted:
                if file_exists and current_sha:
                    commit_msg = f"snowflake: drop {obj_type} {db_name}.{schema}.{obj_name} [by {ddl_by}]"
                    del_payload = {
                        'message': commit_msg,
                        'sha':     current_sha,
                        'branch':  p_branch
                    }
                    resp = requests.delete(api_url, headers=headers, json=del_payload, timeout=30)
                    if resp.status_code not in (200, 204):
                        error_list.append(f"DELETE {file_path}: HTTP {resp.status_code}")
                        error_count += 1
                        continue
                    new_sha = None
                else:
                    # Already absent from GitHub — just mark synced, nothing to do
                    skip_count += 1
                    new_sha = None

            # -----------------------------------------------------------
            # Step 2b: LIVE object → PUT (create or update) file
            # -----------------------------------------------------------
            else:
                encoded    = base64.b64encode(content.encode('utf-8')).decode('utf-8')
                action     = 'update' if file_exists else 'create'
                commit_msg = f"snowflake: {action} {obj_type} {db_name}.{schema}.{obj_name} [by {ddl_by}]"

                put_payload = {
                    'message': commit_msg,
                    'content': encoded,
                    'branch':  p_branch
                }
                if current_sha:
                    put_payload['sha'] = current_sha   # required for updates

                resp = requests.put(api_url, headers=headers, json=put_payload, timeout=30)

                if resp.status_code not in (200, 201):
                    error_list.append(
                        f"PUT {file_path}: HTTP {resp.status_code} — {resp.text[:200]}"
                    )
                    error_count += 1
                    continue

                # Content SHA returned for subsequent updates
                new_sha = resp.json().get('content', {}).get('sha', '')

            # -----------------------------------------------------------
            # Step 3: Mark row as synced in Snowflake
            # -----------------------------------------------------------
            safe_id  = log_id.replace("'", "''")
            safe_sha = (new_sha or '').replace("'", "''")
            session.sql(
                f"CALL ADMIN_DB.VERSION_CONTROL.MARK_SYNCED_TO_GITHUB('{safe_id}', '{safe_sha}')"
            ).collect()
            synced_count += 1

        except requests.exceptions.Timeout:
            error_list.append(f"{file_path}: request timed out after 30s")
            error_count += 1
        except Exception as e:
            error_list.append(f"{file_path}: {str(e)[:200]}")
            error_count += 1

    return {
        'total_pending': len(pending),
        'synced':        synced_count,
        'skipped':       skip_count,
        'errors':        error_count,
        'error_details': ' | '.join(error_list) if error_list else ''
    }
;


-- =============================================================================
-- TASK: TASK_SYNC_GITHUB
-- Chained after TASK_CAPTURE_DDL — runs only when capture succeeds.
-- Captures first, then immediately pushes any new changes to GitHub.
-- =============================================================================
CREATE OR REPLACE TASK ADMIN_DB.VERSION_CONTROL.TASK_SYNC_GITHUB
    WAREHOUSE = WH_QUERY
    AFTER     ADMIN_DB.VERSION_CONTROL.TASK_CAPTURE_DDL  -- DAG: runs after capture
    COMMENT   = 'Pushes pending DDL changes to GitHub after each capture run'
AS
CALL ADMIN_DB.VERSION_CONTROL.SYNC_TO_GITHUB(
    'juice-plus',          -- <-- update: GitHub org or username
    'jp-enterprise-data-snowflake-dbt',       -- <-- update: repository name
    'main'                 -- <-- update: target branch
);

ALTER TASK ADMIN_DB.VERSION_CONTROL.TASK_SYNC_GITHUB RESUME;


-- =============================================================================
-- GRANTS  (run as ACCOUNTADMIN)
-- =============================================================================
USE ROLE ACCOUNTADMIN;

-- Grant the External Access Integration to the role that owns the procedure
GRANT USAGE ON INTEGRATION GITHUB_DDL_SYNC_INTEGRATION TO ROLE SYSADMIN;

-- If you want a separate role to call sync manually (e.g. for ad-hoc runs)
-- GRANT USAGE ON PROCEDURE ADMIN_DB.VERSION_CONTROL.SYNC_TO_GITHUB(STRING, STRING, STRING)
--     TO ROLE <your_role>;

-- Secret stays tightly held — only the procedure's EXECUTE AS CALLER context needs it
-- (no direct GRANT on the secret needed for the procedure to use it via the integration)


-- =============================================================================
-- AD-HOC USAGE
-- =============================================================================
-- Run immediately without waiting for the task schedule:
-- CALL ADMIN_DB.VERSION_CONTROL.SYNC_TO_GITHUB('juice-plus', 'snowflake-ddl', 'main');

-- Check what's pending before running:
-- SELECT COUNT(*), MIN(LAST_DDL_AT) AS OLDEST_PENDING
-- FROM ADMIN_DB.VERSION_CONTROL.V_PENDING_GITHUB_SYNC;

-- Inspect recent sync history:
-- SELECT l.DATABASE_NAME, l.SCHEMA_NAME, l.OBJECT_NAME, l.OBJECT_TYPE,
--        gs.GITHUB_SHA, gs.SYNCED_AT
-- FROM ADMIN_DB.VERSION_CONTROL.GITHUB_SYNC_LOG gs
-- JOIN ADMIN_DB.VERSION_CONTROL.OBJECT_DDL_LOG  l ON l.LOG_ID = gs.LOG_ID
-- ORDER BY gs.SYNCED_AT DESC
-- LIMIT 50;

-- How far behind is the sync? (should be near-zero after each task run)
-- SELECT DATEDIFF('minute', MIN(LAST_DDL_AT), CURRENT_TIMESTAMP()) AS MINUTES_BEHIND
-- FROM ADMIN_DB.VERSION_CONTROL.V_PENDING_GITHUB_SYNC;

-- Manually mark a row synced (e.g. after recovering from a GitHub outage):
-- CALL ADMIN_DB.VERSION_CONTROL.MARK_SYNCED_TO_GITHUB('<log_id>', '<github_sha>');

-- Rotate the PAT (do this every 90 days or after any suspected compromise):
-- ALTER SECRET ADMIN_DB.VERSION_CONTROL.GITHUB_PAT SET SECRET_STRING = '<NEW_PAT>';
