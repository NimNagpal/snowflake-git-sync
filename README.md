# snowflake-git-sync
Snowflake-native Git versioning for DDL changes. Daily commits across tables, views, procedures, tasks, and more — with Cortex AI commit summaries, incremental watermarking, and a Python stored procedure that runs entirely inside Snowflake.


# Snowflake Version Control System (v5 — Simplified Incremental)
### Database: ADMIN_DB  |  Schema: VERSION_CONTROL

---

## How It Works

**First run** — full bootstrap. No watermark exists so every schema and every
object type is scanned. Every object's DDL is captured and stored in
`OBJECT_SNAPSHOTS`. `SOURCE_QUERY_ID` and `SOURCE_USER_NAME` are NULL on these
records (there is no prior DDL query to reference; we are establishing baseline).

**Every run after** — incremental. `QUERY_HISTORY` is queried for DDL statements
since the last watermark. Each distinct `(SCHEMA_NAME, OBJECT_TYPE)` combo that
had activity is processed. Everything else is untouched.

```
INCREMENTAL RUN
    │
    ▼
ACCOUNT_USAGE.QUERY_HISTORY
  WHERE START_TIME > last watermark
    AND QUERY_TYPE IN (CREATE_*, ALTER_*, DROP_*)
  GROUP BY SCHEMA_NAME, OBJECT_TYPE
  → returns: SCHEMA_NAME, OBJECT_TYPE, LATEST_QUERY_ID, LATEST_USER_NAME
    │
    │  (only active schema/type combos proceed)
    ▼
SP_CAPTURE_SCHEMA_OBJECTS(schema, type, query_id, user_name)
  SHOW objects in schema
  → for each object: GET_DDL → SHA-256 hash → compare to OBJECT_SNAPSHOTS
      changed?  → write to DDL_CHANGE_LOG with SOURCE_QUERY_ID + SOURCE_USER_NAME
      same?     → skip
      missing?  → DROPPED, write to DDL_CHANGE_LOG
    │
    ▼
Update watermark in INCREMENTAL_STATE
```

---

## Tracking Who Changed What

Every change record in `DDL_CHANGE_LOG` carries:

| Column | What it tells you |
|---|---|
| `SOURCE_QUERY_ID` | The Snowflake Query ID of the DDL statement that caused the change |
| `SOURCE_USER_NAME` | The user who ran that DDL statement |
| `CHANGED_AT` | When the capture procedure detected the change |
| `COMMIT_MESSAGE` | Auto-generated description (feat / refactor / chore) |

For bootstrap records (first run), `SOURCE_QUERY_ID` and `SOURCE_USER_NAME` are NULL — this is expected and correct since we are snapshotting existing state, not responding to a specific DDL event.

---

## Procedures

| Procedure | Purpose |
|---|---|
| `CAPTURE_DDL_SNAPSHOT` | Main entry point. Called by the Task. |
| `SP_CAPTURE_SCHEMA_OBJECTS` | Worker — SHOW + GET_DDL + hash + write |
| `FORCE_FULL_RESCAN` | Clears watermark so next run does a full bootstrap |
| `ROLLBACK_OBJECT` | Reverts an object to a previous DDL version |
| `DIFF_VERSIONS` | Side-by-side DDL comparison between two change IDs |
| `TAG_VERSION` | Applies a release tag to recent change log entries |
| `MANUAL_COMMIT` | Ad-hoc snapshot of one object with a custom message |
| `REGISTER_SCRIPT` | Track external scripts (Python, SQL, ADF pipelines) |

---

## Files

| File | Changed? |
|---|---|
| `01_ddl_capture_procedure.sql` | ✅ Full rewrite (simplified incremental) |
| `02_github_sync_procedure.sql` | — unchanged |
| `03_scheduling_tasks.sql` | — unchanged |
| `04_utility_views_and_helpers.sql` | ✅ SOURCE_QUERY_ID/USER_NAME added to all views |
| `05_script_registry_and_manual_ops.sql` | — unchanged |

---

## Deployment

```sql
-- 1. Run 01 first — creates all tables and both procedures
-- File: 01_ddl_capture_procedure.sql

-- 2–5. Run remaining files in order
```

---

## Common Queries

```sql
-- See everything that changed in the last 24 hours, with who did it
SELECT
    CHANGED_AT,
    OBJECT_TYPE,
    DATABASE_NAME || '.' || SCHEMA_NAME || '.' || OBJECT_NAME AS FULL_OBJECT_NAME,
    CHANGE_TYPE,
    SOURCE_USER_NAME,
    SOURCE_QUERY_ID,
    COMMIT_MESSAGE
FROM ADMIN_DB.VERSION_CONTROL.V_CHANGES_LAST_24H
ORDER BY CHANGED_AT DESC;

-- Full history of one object
SELECT
    CHANGED_AT, CHANGE_TYPE, SOURCE_USER_NAME, SOURCE_QUERY_ID, COMMIT_MESSAGE
FROM ADMIN_DB.VERSION_CONTROL.V_OBJECT_HISTORY
WHERE DATABASE_NAME = 'PROD_DB'
  AND OBJECT_NAME   = 'MY_TABLE';

-- Look up the exact SQL that caused a change (join back to QUERY_HISTORY)
SELECT
    cl.OBJECT_NAME, cl.CHANGE_TYPE, cl.SOURCE_USER_NAME,
    qh.QUERY_TEXT,  qh.START_TIME
FROM  ADMIN_DB.VERSION_CONTROL.DDL_CHANGE_LOG cl
JOIN  SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY  qh
   ON qh.QUERY_ID = cl.SOURCE_QUERY_ID
WHERE cl.CHANGED_AT >= DATEADD('day', -7, CURRENT_TIMESTAMP())
ORDER BY cl.CHANGED_AT DESC;

-- Dry run — preview what incremental would find without writing
CALL ADMIN_DB.VERSION_CONTROL.CAPTURE_DDL_SNAPSHOT(
    ARRAY_CONSTRUCT('PROD_DB'),
    ARRAY_CONSTRUCT('INFORMATION_SCHEMA'),
    TRUE
);

-- Force full rescan (after a bulk migration or DB clone)
CALL ADMIN_DB.VERSION_CONTROL.FORCE_FULL_RESCAN('PROD_DB');

-- Check current watermarks
SELECT * FROM ADMIN_DB.VERSION_CONTROL.INCREMENTAL_STATE;

-- Run history
SELECT RUN_ID, RUN_TYPE, STARTED_AT, FINISHED_AT,
       DATEDIFF('second', STARTED_AT, FINISHED_AT) AS DURATION_SECS,
       OBJECTS_SCANNED, CHANGES_DETECTED, STATUS
FROM ADMIN_DB.VERSION_CONTROL.CAPTURE_RUN_LOG
ORDER BY STARTED_AT DESC LIMIT 20;
```

---

## ACCOUNT_USAGE Latency Note

`SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY` has a ~2–3 minute built-in latency.
On a 15-minute schedule a change made 1 minute before the run may be deferred
one cycle. This is acceptable. If needed, `FORCE_FULL_RESCAN` can be called to
catch anything missed.
