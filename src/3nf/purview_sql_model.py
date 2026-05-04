"""
purview_sql_model.py
====================
SQL data-modeling layer for 5-table Purview schema.

Responsibilities
────────────────
• All CREATE TABLE DDLs (IF NOT EXISTS, safe to re-run)
• MERGE / INSERT SQL generation for every table
• Row validation before SQL is emitted (mandatory fields, null-guard)
• write_sql_files()  → creates organised part-files per table + runner scripts
• push_to_azure_sql() → executes files via sqlcmd with live progress logging

5 Tables
────────
1. asset_registry          PK = asset_guid         (UNIQUEIDENTIFIER)
2. purview_collections     PK = collection_name     (NVARCHAR)
3. purview_entities        PK = guid                (UNIQUEIDENTIFIER)
4. purview_glossary_terms  PK = guid                (UNIQUEIDENTIFIER)
5. purview_search_assets   PK = asset_id            (UNIQUEIDENTIFIER)

NULL policy
───────────
Only non-null fields are written.  Mandatory PKs are validated in Python
before any SQL is generated.  Rows missing mandatory fields are skipped
with a WARN log — nothing broken ever reaches the DB.

Duplicate policy
────────────────
Every table uses MERGE on its PK:
  WHEN MATCHED     → UPDATE only non-null fields that actually changed
  WHEN NOT MATCHED → INSERT new row
Re-running any SQL file multiple times is completely safe.

Column-enrichment filter
────────────────────────
asset_registry only captures assets whose columns carry enrichment
(classification applied, sensitivity label applied, or business tag present).
The timeframe filter (LAST_RUN) is driven exclusively by classification
timestamps — NOT by asset created / updated timestamps.
"""

import os, re, uuid, time, shutil, subprocess, datetime
from pathlib import Path

# ═══════════════════════════════════════════════════════════════════════
#  SQL ESCAPE HELPERS
# ═══════════════════════════════════════════════════════════════════════

def esc(val):
    """Return N'...' SQL literal or 'NULL' sentinel (never written)."""
    if val is None or val == "" or (isinstance(val, list) and not val):
        return "NULL"
    s = str(val).strip()
    return "NULL" if not s else "N'" + s.replace("'", "''") + "'"

def esc_bit(val):
    if val is None: return "NULL"
    return "1" if val else "0"

def esc_int(val):
    if val is None: return "NULL"
    try: return str(int(val))
    except (ValueError, TypeError): return "NULL"

def esc_decimal(val, precision=4):
    if val is None: return "NULL"
    try: return f"{float(val):.{precision}f}"
    except (ValueError, TypeError): return "NULL"

def csv_null(lst):
    c = [str(x) for x in lst if x and str(x).strip()]
    return ", ".join(c) if c else None

def has_value(val):
    if val is None or val == "": return False
    if isinstance(val, list): return len(val) > 0
    return True

# ═══════════════════════════════════════════════════════════════════════
#  SMART MERGE  (null-suppressing, duplicate-safe)
#
#  Builds a MERGE statement that ONLY writes non-NULL field values.
#  Optional lifecycle_update lets callers inject extra SET expressions
#  on MATCHED (e.g., "fetched_at = GETUTCDATE()").
# ═══════════════════════════════════════════════════════════════════════

def _smart_merge(S, table, pk_col, pk_val, fields,
                 lifecycle_update=None):
    """
    Build MERGE on pk_col that:
      - On MATCH + business data changed : UPDATE all non-null fields
      - On MATCH + no business change    : no-op  (OUTPUT emits nothing)
      - On NO MATCH                      : INSERT all non-null fields

    Volatile housekeeping columns (scan_run_id, fetched_at, scan_timestamp)
    are EXCLUDED from the change-detection predicate because they differ on
    every run by design.  They are still written on INSERT and on UPDATE
    (when real data actually changed), but they never trigger an update
    by themselves — so re-running the same data produces zero updates.
    """
    # Columns excluded from change-detection.
    # Two categories:
    #
    # 1. Housekeeping — differ every run by design:
    #      scan_run_id, fetched_at, scan_timestamp
    #
    # 2. DATETIME2 audit fields — cause false positives because SQL Server
    #    CAST(DATETIME2 AS NVARCHAR) produces a different format than the
    #    incoming string (e.g. "2024-01-15T10:23:45Z" stored becomes
    #    "2024-01-15 10:23:45.0000000" on readback), so the comparison
    #    always fires even when the underlying value is identical:
    #      created_at, last_modified_at          (purview_collections)
    #      asset_created_at, asset_last_updated_at,
    #      asset_created_by, asset_last_updated_by (asset_registry)
    #
    # All of these are still written normally on INSERT and on real UPDATE —
    # they just never trigger the change-detection predicate themselves.
    _VOLATILE = {
        # ── run housekeeping ──────────────────────────────────────────
        "scan_run_id", "fetched_at", "scan_timestamp",
        # ── DATETIME2 columns (format mismatch on CAST roundtrip) ─────
        "created_at", "last_modified_at",           # purview_collections
        "asset_created_at", "asset_last_updated_at",# asset_registry
        # ── audit "by" fields that can shift between API calls ────────
        "asset_created_by", "asset_last_updated_by",# asset_registry
        "last_modified_by", "last_modified_by_type",# purview_collections
    }

    pk_esc = esc(pk_val)
    if pk_esc == "NULL":
        return None                         # PK itself is null — skip

    non_null_pairs = []
    for col, val in fields.items():
        if col == pk_col:
            continue
        if isinstance(val, bool):
            sql_val = esc_bit(val)
        elif isinstance(val, int) and not isinstance(val, bool):
            sql_val = esc_int(val)
        elif isinstance(val, float):
            sql_val = esc_decimal(val)
        else:
            sql_val = esc(val)
        if sql_val != "NULL":
            non_null_pairs.append((col, sql_val))

    if not non_null_pairs:
        return None                         # nothing to write beyond PK

    insert_cols = [pk_col] + [c for c, _ in non_null_pairs]
    insert_vals = [pk_esc] + [v for _, v in non_null_pairs]
    update_sets = [f"T.[{c}] = {v}" for c, v in non_null_pairs]
    if lifecycle_update:
        update_sets.append(lifecycle_update)

    # Change-detection: only compare stable business columns.
    # Volatile housekeeping cols (scan_run_id, fetched_at, scan_timestamp)
    # are excluded — they always differ and must never trigger a false update.
    stable_pairs = [(c, v) for c, v in non_null_pairs if c not in _VOLATILE]

    if not stable_pairs:
        # All columns are volatile (edge case) — treat as always-unchanged.
        # Use a condition that is never true so WHEN MATCHED never fires.
        change_checks = "1 = 0"
    else:
        change_checks = " OR ".join(
            f"COALESCE(CAST(T.[{c}] AS NVARCHAR(MAX)), '') "
            f"<> COALESCE(CAST({v} AS NVARCHAR(MAX)), '')"
            for c, v in stable_pairs
        )

    return (
        f"MERGE [{S}].[{table}] AS T\n"
        f"USING (SELECT {pk_esc} AS [{pk_col}]) AS S "
        f"ON T.[{pk_col}] = S.[{pk_col}]\n"
        f"WHEN MATCHED AND ({change_checks}) THEN UPDATE SET\n"
        f"    {', '.join(update_sets)}\n"
        f"WHEN NOT MATCHED THEN INSERT\n"
        f"    ({', '.join(f'[{c}]' for c in insert_cols)})\n"
        f"    VALUES ({', '.join(insert_vals)})\n"
        f"OUTPUT $action INTO #merge_out(action_type);\n"
    )

# ═══════════════════════════════════════════════════════════════════════
#  DDL  — Schema + 5 Tables
#  All guarded with IF OBJECT_ID … IS NULL so re-runs are safe.
# ═══════════════════════════════════════════════════════════════════════

def _schema_ddl(S):
    return (
        f"IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = N'{S}')\n"
        f"    EXEC('CREATE SCHEMA [{S}]');\nGO\n\n"
    )


def _asset_registry_ddl(S, tbl):
    return f"""\
-- ── Table: [{S}].[{tbl}] ─────────────────────────────────────────────
-- One row per enriched leaf asset.
-- Enrichment filter: assets that have ≥1 column with classification,
--   sensitivity label, or business tag applied.
-- Timeframe filter : driven by classification timestamps only (LAST_RUN).
IF OBJECT_ID(N'[{S}].[{tbl}]', N'U') IS NULL
BEGIN
CREATE TABLE [{S}].[{tbl}] (
    asset_guid                  UNIQUEIDENTIFIER    NOT NULL,
    asset_name                  NVARCHAR(255)       NULL,
    asset_qualified_name        NVARCHAR(MAX)       NULL,
    asset_entity_type           NVARCHAR(100)       NULL,
    asset_object_type           NVARCHAR(50)        NULL,
    datasource_type             NVARCHAR(100)       NULL,
    datasource_instance         NVARCHAR(500)       NULL,
    schema_path                 NVARCHAR(500)       NULL,
    collection_id               NVARCHAR(50)        NULL,
    collection_name             NVARCHAR(255)       NULL,
    collection_hierarchy_path   NVARCHAR(1000)      NULL,
    total_columns               INT                 NULL,
    total_classified_columns    INT                 NULL,
    has_classified_columns      BIT                 NULL,
    classification_types_found  NVARCHAR(1000)      NULL,
    asset_created_at            DATETIME2           NULL,
    asset_created_by            NVARCHAR(255)       NULL,
    asset_last_updated_at       DATETIME2           NULL,
    asset_last_updated_by       NVARCHAR(255)       NULL,
    scan_run_id                 UNIQUEIDENTIFIER    NULL,
    scan_timestamp              DATETIME2           NULL,
    scan_status                 NVARCHAR(100)       NULL,
    CONSTRAINT PK_{tbl} PRIMARY KEY (asset_guid)
);
PRINT '{tbl} created.';
END
ELSE
    PRINT '{tbl} already exists — skipping DDL.';
GO

"""


def _collections_ddl(S, tbl):
    return f"""\
-- ── Table: [{S}].[{tbl}] ─────────────────────────────────────────────
-- Always fetched in full (no timeframe filter).
IF OBJECT_ID(N'[{S}].[{tbl}]', N'U') IS NULL
BEGIN
CREATE TABLE [{S}].[{tbl}] (
    collection_name                 NVARCHAR(100)   NOT NULL,
    friendly_name                   NVARCHAR(255)   NULL,
    description                     NVARCHAR(MAX)   NULL,
    parent_collection_name          NVARCHAR(100)   NULL,
    created_by                      NVARCHAR(100)   NULL,
    created_by_type                 NVARCHAR(50)    NULL,
    created_at                      DATETIME2       NULL,
    last_modified_by                NVARCHAR(100)   NULL,
    last_modified_by_type           NVARCHAR(50)    NULL,
    last_modified_at                DATETIME2       NULL,
    collection_provisioning_state   NVARCHAR(50)    NULL,
    purview_account                 NVARCHAR(100)   NULL,
    api_endpoint                    NVARCHAR(MAX)   NULL,
    fetched_at                      DATETIME2       NULL,
    scan_run_id                     UNIQUEIDENTIFIER NULL,
    CONSTRAINT PK_{tbl} PRIMARY KEY (collection_name)
);
PRINT '{tbl} created.';
END
ELSE
    PRINT '{tbl} already exists — skipping DDL.';
GO

"""


def _entities_ddl(S, tbl):
    return f"""\
-- ── Table: [{S}].[{tbl}] ─────────────────────────────────────────────
IF OBJECT_ID(N'[{S}].[{tbl}]', N'U') IS NULL
BEGIN
CREATE TABLE [{S}].[{tbl}] (
    guid                    UNIQUEIDENTIFIER    NOT NULL,
    entity_type_name        NVARCHAR(100)       NULL,
    entity_name             NVARCHAR(255)       NULL,
    qualified_name          NVARCHAR(MAX)       NULL,
    owner_name              NVARCHAR(255)       NULL,
    modified_time           BIGINT              NULL,
    total_size_bytes        BIGINT              NULL,
    partition_count         INT                 NULL,
    schema_count            INT                 NULL,
    last_modified_ts        NVARCHAR(20)        NULL,
    is_incomplete           BIT                 NULL,
    provenance_type         INT                 NULL,
    status                  NVARCHAR(50)        NULL,
    created_by              NVARCHAR(255)       NULL,
    updated_by              NVARCHAR(255)       NULL,
    create_time_epoch       BIGINT              NULL,
    update_time_epoch       BIGINT              NULL,
    version_no              INT                 NULL,
    is_indexed              BIT                 NULL,
    source_name             NVARCHAR(100)       NULL,
    scan_resource_id        NVARCHAR(500)       NULL,
    collection_id           NVARCHAR(100)       NULL,
    domain_id               NVARCHAR(100)       NULL,
    display_text            NVARCHAR(255)       NULL,
    proxy_flag              BIT                 NULL,
    fetched_at              DATETIME2           NULL,
    scan_run_id             UNIQUEIDENTIFIER    NULL,
    CONSTRAINT PK_{tbl} PRIMARY KEY (guid)
);
PRINT '{tbl} created.';
END
ELSE
    PRINT '{tbl} already exists — skipping DDL.';
GO

"""


def _glossary_ddl(S, tbl):
    return f"""\
-- ── Table: [{S}].[{tbl}] ─────────────────────────────────────────────
-- Always fetched in full (no timeframe filter).
IF OBJECT_ID(N'[{S}].[{tbl}]', N'U') IS NULL
BEGIN
CREATE TABLE [{S}].[{tbl}] (
    guid                    UNIQUEIDENTIFIER    NOT NULL,
    qualified_name          NVARCHAR(500)       NULL,
    term_name               NVARCHAR(255)       NULL,
    long_description        NVARCHAR(MAX)       NULL,
    last_modified_ts        NVARCHAR(20)        NULL,
    created_by              NVARCHAR(100)       NULL,
    updated_by              NVARCHAR(100)       NULL,
    create_time_epoch       BIGINT              NULL,
    update_time_epoch       BIGINT              NULL,
    domain_id               NVARCHAR(100)       NULL,
    abbreviation            NVARCHAR(500)       NULL,
    status                  NVARCHAR(50)        NULL,
    glossary_guid           UNIQUEIDENTIFIER    NULL,
    relation_guid           UNIQUEIDENTIFIER    NULL,
    synonym_display_text    NVARCHAR(255)       NULL,
    fetched_at              DATETIME2           NULL,
    scan_run_id             UNIQUEIDENTIFIER    NULL,
    CONSTRAINT PK_{tbl} PRIMARY KEY (guid)
);
PRINT '{tbl} created.';
END
ELSE
    PRINT '{tbl} already exists — skipping DDL.';
GO

"""


def _search_assets_ddl(S, tbl):
    return f"""\
-- ── Table: [{S}].[{tbl}] ─────────────────────────────────────────────
-- Raw search catalog from Purview search API.
IF OBJECT_ID(N'[{S}].[{tbl}]', N'U') IS NULL
BEGIN
CREATE TABLE [{S}].[{tbl}] (
    asset_id            UNIQUEIDENTIFIER    NOT NULL,
    asset_name          NVARCHAR(255)       NULL,
    display_text        NVARCHAR(255)       NULL,
    qualified_name      NVARCHAR(MAX)       NULL,
    entity_type         NVARCHAR(100)       NULL,
    object_type         NVARCHAR(100)       NULL,
    description         NVARCHAR(MAX)       NULL,
    collection_id       NVARCHAR(100)       NULL,
    domain_id           NVARCHAR(100)       NULL,
    create_by           NVARCHAR(255)       NULL,
    update_by           NVARCHAR(255)       NULL,
    create_time_epoch   BIGINT              NULL,
    update_time_epoch   BIGINT              NULL,
    is_indexed          BIT                 NULL,
    asset_type          NVARCHAR(255)       NULL,
    search_score        DECIMAL(10,4)       NULL,
    fetched_at          DATETIME2           NULL,
    scan_run_id         UNIQUEIDENTIFIER    NULL,
    CONSTRAINT PK_{tbl} PRIMARY KEY (asset_id)
);
PRINT '{tbl} created.';
END
ELSE
    PRINT '{tbl} already exists — skipping DDL.';
GO

"""


def all_ddls(S, table_names):
    """Return combined DDL string for all 5 tables (schema first)."""
    T = table_names
    return (
        _schema_ddl(S)
        + _search_assets_ddl(S, T["search_assets"])
        + _collections_ddl(S, T["collections"])
        + _entities_ddl(S, T["entities"])
        + _glossary_ddl(S, T["glossary_terms"])
        + _asset_registry_ddl(S, T["asset_registry"])
    )

# ═══════════════════════════════════════════════════════════════════════
#  ROW VALIDATION  — Python-side null-guard per table
# ═══════════════════════════════════════════════════════════════════════

# Mandatory fields per table key (PK must be present and non-empty)
_MANDATORY = {
    "search_assets":  ["asset_id"],
    "collections":    ["collection_name"],
    "entities":       ["guid"],
    "glossary_terms": ["guid"],
    "asset_registry": ["asset_guid"],
}


def validate_row(table_key, row, idx):
    """
    Returns (True, "") if valid, (False, reason) if mandatory field missing.
    """
    for field in _MANDATORY.get(table_key, []):
        v = row.get(field, "")
        if not v or not str(v).strip():
            return False, (f"[{table_key}] row #{idx}: "
                           f"'{field}' is empty/null — row skipped")
    return True, ""

# ═══════════════════════════════════════════════════════════════════════
#  PER-TABLE SQL GENERATORS
#  Each function takes a validated, non-null-stripped row dict and
#  returns a complete MERGE SQL block (or "" if nothing to write).
# ═══════════════════════════════════════════════════════════════════════

def _row_comment(table_key, row):
    """Short SQL comment header identifying the row."""
    if table_key == "asset_registry":
        return (f"-- Asset : {row.get('asset_name','?')}  "
                f"| {row.get('datasource_type','?')}  "
                f"| cls: {row.get('classification_types_found','(none)')}\n")
    if table_key == "search_assets":
        return (f"-- SearchAsset : {row.get('asset_name','?')}  "
                f"| type: {row.get('entity_type','?')}\n")
    if table_key == "collections":
        return f"-- Collection : {row.get('collection_name','?')}\n"
    if table_key == "entities":
        return (f"-- Entity : {row.get('entity_name','?')}  "
                f"| type: {row.get('entity_type_name','?')}\n")
    if table_key == "glossary_terms":
        return f"-- GlossaryTerm : {row.get('term_name','?')}\n"
    return ""


def _pk_for(table_key):
    """Return (pk_col, val_key) for the given table."""
    return {
        "asset_registry": ("asset_guid",     "asset_guid"),
        "collections":    ("collection_name", "collection_name"),
        "entities":       ("guid",            "guid"),
        "glossary_terms": ("guid",            "guid"),
        "search_assets":  ("asset_id",        "asset_id"),
    }[table_key]


def row_to_sql(S, table_name, table_key, row):
    """
    Build the MERGE SQL for one validated row.
    Returns empty string if MERGE produces nothing (only PK with no data).

    NOTE: lifecycle_update is intentionally NOT used here.
    All 5 tables carry 'fetched_at' as a regular data column whose value
    comes from the row dict itself (set to scan_ts by the caller).
    Passing lifecycle_update="T.[fetched_at] = GETUTCDATE()" would cause
    SQL Server error 264 — "column name specified more than once in SET
    clause" — because _smart_merge already emits fetched_at from the row.
    """
    pk_col, pk_key = _pk_for(table_key)
    pk_val = row.get(pk_key)
    merge  = _smart_merge(S, table_name, pk_col, pk_val, row)
    if not merge:
        return ""
    return _row_comment(table_key, row) + merge + "\n"

# ═══════════════════════════════════════════════════════════════════════
#  WRITE SQL FILES
#  Each table → its own subfolder + numbered part-files.
#  Part-1 of every table always contains the DDL.
#  A master run_all.bat + run_all.sh execute everything in dependency order:
#    collections → entities → glossary_terms → search_assets → asset_registry
# ═══════════════════════════════════════════════════════════════════════

# Execution order for runner scripts (dependencies first)
_TABLE_EXEC_ORDER = [
    "collections",
    "entities",
    "glossary_terms",
    "search_assets",
    "asset_registry",
]

# Human-readable labels for on-screen reporting
_TABLE_LABELS = {
    "asset_registry":  "asset_registry        (one row per enriched asset)",
    "collections":     "purview_collections   (full load every run)",
    "entities":        "purview_entities      (fetched entities)",
    "glossary_terms":  "purview_glossary_terms (full load every run)",
    "search_assets":   "purview_search_assets  (raw search catalog)",
}


def write_sql_files(out_dir, S, ts, scan_run_id, scan_ts,
                    row_sets, table_names, max_rows_per_file=500):
    """
    Parameters
    ──────────
    out_dir          : Path or str — root output folder
    S                : schema name (e.g. 'compliance')
    ts               : human timestamp string for file headers
    scan_run_id      : UUID string for this run
    scan_ts          : ISO timestamp string for this run
    row_sets         : dict { table_key → [row_dict, …] }
    table_names      : dict { table_key → actual_table_name_in_db }
    max_rows_per_file: how many MERGE rows per SQL file part

    Returns
    ───────
    (all_files, ordered_files, row_counts)
    all_files      : list of all created Path objects
    ordered_files  : list in execution order for push
    row_counts     : dict { table_key → int rows written }
    """
    out_dir = Path(out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    # Pre-build full DDL block (used in part-1 of each table)
    ddl_block = all_ddls(S, table_names)

    all_files     = []
    ordered_files = []   # filled in exec-order at end
    row_counts    = {}
    table_files   = {}   # table_key → [Path, …]

    for table_key in _TABLE_EXEC_ORDER:
        rows      = row_sets.get(table_key, [])
        tbl_name  = table_names.get(table_key, table_key)
        tbl_dir   = out_dir / tbl_name
        tbl_dir.mkdir(parents=True, exist_ok=True)

        # ── Validate rows and build SQL statements ─────────────────────
        stmts        = []
        skipped_null = 0
        skipped_noop = 0

        for idx, row in enumerate(rows, 1):
            ok, reason = validate_row(table_key, row, idx)
            if not ok:
                _sql_log(reason, "WARN")
                skipped_null += 1
                continue
            # Strip null values from the row before passing to SQL generator
            clean_row = {k: v for k, v in row.items() if has_value(v)}
            sql = row_to_sql(S, tbl_name, table_key, clean_row)
            if sql:
                stmts.append(sql)
            else:
                skipped_noop += 1

        if skipped_null:
            _sql_log(f"  [{tbl_name}] Skipped (null mandatory): {skipped_null}", "WARN")
        if skipped_noop:
            _sql_log(f"  [{tbl_name}] Skipped (no data beyond PK): {skipped_noop}", "INFO")

        row_counts[table_key] = len(stmts)

        # ── Chunk into part-files ──────────────────────────────────────
        chunks      = [stmts[i:i+max_rows_per_file]
                       for i in range(0, max(1, len(stmts)), max_rows_per_file)]
        if not chunks: chunks = [[]]
        total_parts = len(chunks)
        part_paths  = []

        for part_idx, chunk in enumerate(chunks, 1):
            fname = f"{tbl_name}_part{part_idx}.sql"
            p     = tbl_dir / fname

            if part_idx == 1:
                # Part-1 carries the full DDL block so the table always
                # exists before any MERGE references it.
                hdr = (
                    f"-- ============================================================\n"
                    f"-- TABLE  : [{S}].[{tbl_name}]\n"
                    f"-- PART   : {part_idx} of {total_parts}\n"
                    f"-- ROWS   : {len(stmts)} total\n"
                    f"-- RUN ID : {scan_run_id}\n"
                    f"-- TIME   : {ts}\n"
                    f"-- NULL policy  : only non-null values written — PK validated in Python\n"
                    f"-- DUP  policy  : MERGE on PK — re-run safe, no duplicates possible\n"
                    f"-- ENRICH filter: classification applied | label applied | tag present\n"
                    f"-- ============================================================\n\n"
                    f"SET NOCOUNT ON;\nGO\n\n"
                    f"{ddl_block}"
                )
            else:
                hdr = (
                    f"-- ============================================================\n"
                    f"-- TABLE  : [{S}].[{tbl_name}]   Part {part_idx} of {total_parts}\n"
                    f"-- RUN ID : {scan_run_id}   TIME: {ts}\n"
                    f"-- Data rows only — DDL is in part 1.\n"
                    f"-- ============================================================\n\n"
                    f"SET NOCOUNT ON;\nGO\n\n"
                )

            # Build body: temp table + all MERGEs + MERGE_SUMMARY PRINT
            if chunk:
                merge_body = "".join(chunk)
                body = (
                    f"-- Temp table captures MERGE OUTPUT ($action) for each row\n"
                    f"IF OBJECT_ID('tempdb..#merge_out') IS NOT NULL "
                    f"DROP TABLE #merge_out;\n"
                    f"CREATE TABLE #merge_out (action_type NVARCHAR(10));\n"
                    f"GO\n\n"
                    f"{merge_body}"
                    f"\nGO\n"
                    f"-- Summarise inserts vs updates for this batch\n"
                    f"DECLARE @ins INT, @upd INT;\n"
                    f"SELECT @ins = COUNT(*) FROM #merge_out "
                    f"WHERE action_type = 'INSERT';\n"
                    f"SELECT @upd = COUNT(*) FROM #merge_out "
                    f"WHERE action_type = 'UPDATE';\n"
                    f"PRINT 'MERGE_SUMMARY|{tbl_name}"
                    f"|part={part_idx}/{total_parts}"
                    f"|batch={len(chunk)}"
                    f"|inserted=' + CAST(@ins AS NVARCHAR)"
                    f" + '|updated=' + CAST(@upd AS NVARCHAR)"
                    f" + '|unchanged=' + CAST(({len(chunk)} - @ins - @upd) AS NVARCHAR);\n"
                    f"DROP TABLE #merge_out;\n"
                    f"GO\n"
                )
            else:
                body = (
                    f"\nGO\n"
                    f"PRINT 'MERGE_SUMMARY|{tbl_name}"
                    f"|part={part_idx}/{total_parts}"
                    f"|batch=0|inserted=0|updated=0';\n"
                    f"GO\n"
                )

            p.write_text(hdr + body, encoding="utf-8")
            part_paths.append(p)
            all_files.append(p)

        table_files[table_key] = part_paths
        _sql_log(f"  {tbl_name}: {len(stmts)} rows → "
                 f"{total_parts} part-file(s) in {tbl_dir.name}/", "OK")

    # ── Build ordered list for runner scripts ──────────────────────────
    for table_key in _TABLE_EXEC_ORDER:
        raw_paths = table_files.get(table_key, [])
        # Sort by part number
        raw_paths = sorted(
            raw_paths,
            key=lambda p: int(re.search(r'_part(\d+)\.sql$', p.name).group(1))
                          if re.search(r'_part(\d+)\.sql$', p.name) else 0)
        ordered_files.extend(raw_paths)

    # ── Runner scripts ─────────────────────────────────────────────────
    bat = [
        "@echo off",
        f"REM Purview SQL loader — schema [{S}]  (5-table mode)",
        "REM Load order: collections → entities → glossary → search_assets → asset_registry",
        "REM env vars needed: SQL_SERVER  SQL_DATABASE  SQL_USER  SQL_PASSWORD",
        f"REM Total files: {len(ordered_files)}",
        "",
    ]
    sh = [
        "#!/bin/bash",
        f"# Purview SQL loader — schema [{S}]  (5-table mode)",
        "# Load order: collections → entities → glossary → search_assets → asset_registry",
        "# export SQL_SERVER=... SQL_DATABASE=... SQL_USER=... SQL_PASSWORD=...",
        f"# Total files: {len(ordered_files)}",
        "",
    ]

    for pf in ordered_files:
        rel = pf.relative_to(out_dir)
        bat += [
            f'echo Loading {rel}...',
            f'sqlcmd -S %SQL_SERVER% -d %SQL_DATABASE% -U %SQL_USER% '
            f'-P %SQL_PASSWORD% -b -I -i "{rel}"',
            f'if %ERRORLEVEL% NEQ 0 (echo FAILED: {rel} & exit /b 1)',
        ]
        sh += [
            f'echo "Loading {rel}..."',
            f'sqlcmd -S "$SQL_SERVER" -d "$SQL_DATABASE" -U "$SQL_USER" '
            f'-P "$SQL_PASSWORD" -b -I -i "{rel}"',
            f'if [ $? -ne 0 ]; then echo "FAILED: {rel}"; exit 1; fi',
        ]

    bat.append("echo All files loaded successfully.")
    sh.append("echo 'All files loaded successfully.'")

    bat_p = out_dir / "run_all.bat"
    sh_p  = out_dir / "run_all.sh"
    bat_p.write_text("\n".join(bat), encoding="utf-8")
    sh_p.write_text("\n".join(sh),  encoding="utf-8")
    try: sh_p.chmod(0o755)
    except Exception: pass

    _sql_log(f"  Runner scripts: run_all.bat + run_all.sh "
             f"({len(ordered_files)} file(s))", "OK")

    return all_files, ordered_files, row_counts

# ═══════════════════════════════════════════════════════════════════════
#  PUSH TO AZURE SQL
#  Executes each SQL file via sqlcmd with detailed on-screen progress.
#  Shows: file header, SQL command preview, live progress bar,
#         sqlcmd PRINT messages, per-file timing and success/failure.
# ═══════════════════════════════════════════════════════════════════════

def _extract_sql_commands(sql_text):
    """
    Scan a SQL file and extract the key MERGE / INSERT / DDL statements
    for the on-screen command-preview table.
    Returns list of (type_label, description) tuples.
    """
    commands = []
    lines    = sql_text.splitlines()
    pending  = []
    for line in lines:
        s = line.strip()
        if s.startswith("--"):
            pending.append(s.lstrip("- ").strip())
        elif s.upper().startswith("MERGE "):
            desc = " | ".join(pending) if pending else s[:80]
            commands.append(("MERGE", desc))
            pending = []
        elif s.upper().startswith("CREATE TABLE"):
            commands.append(("DDL", s[:80]))
            pending = []
        elif s.upper().startswith("IF NOT EXISTS") and "SCHEMA" in s.upper():
            commands.append(("DDL", "CREATE SCHEMA (if absent)"))
            pending = []
        else:
            if s and not s.startswith("GO") and not s.startswith("SET "):
                pending = []
    return commands


def push_to_azure_sql(sql_files, row_counts, table_names,
                      sql_server, sql_database, sql_user, sql_password,
                      schema_name):
    """
    Execute each SQL file via sqlcmd with full on-screen logging.
    Parses MERGE_SUMMARY PRINT lines from SQL Server to report
    exact inserted / updated (duplicate) counts per table.

    Returns True on complete success, False if any file fails.
    """
    sqlcmd = shutil.which("sqlcmd")
    if not sqlcmd:
        _sql_log("sqlcmd not found in PATH. Run run_all.bat/run_all.sh manually.", "WARN")
        return False
    if not all([sql_server, sql_database, sql_user, sql_password]):
        _sql_log("SQL_SERVER/SQL_DATABASE/SQL_USER/SQL_PASSWORD not fully set.", "WARN")
        return False

    _sql_section(f"PUSH TO DB — {sql_server}/{sql_database}")
    _sql_log(f"  Schema      : {schema_name}")
    _sql_log(f"  Total files : {len(sql_files)}")
    for key in _TABLE_EXEC_ORDER:
        tbl = table_names.get(key, key)
        cnt = row_counts.get(key, 0)
        _sql_log(f"  [{tbl:<35}] {cnt:>6} rows")

    total_rows  = sum(row_counts.values())
    rows_loaded = 0
    t_start     = time.time()

    # Accumulate per-table insert/update/unchanged counts from DB PRINT output
    db_inserted  = {k: 0 for k in _TABLE_EXEC_ORDER}
    db_updated   = {k: 0 for k in _TABLE_EXEC_ORDER}
    db_unchanged = {k: 0 for k in _TABLE_EXEC_ORDER}

    def _parse_summary_line(line, table_names):
        """Parse: MERGE_SUMMARY|<tbl>|part=N/M|batch=B|inserted=I|updated=U|unchanged=C"""
        if not line.startswith("MERGE_SUMMARY|"):
            return None
        parts = dict(seg.split("=", 1) for seg in line.split("|")[1:]
                     if "=" in seg)
        tbl_name_raw = line.split("|")[1]
        for key, tname in table_names.items():
            if tname == tbl_name_raw:
                return (key,
                        int(parts.get("inserted",  0)),
                        int(parts.get("updated",   0)),
                        int(parts.get("unchanged", 0)))
        return None

    for file_idx, sql_file in enumerate(sql_files, 1):
        fname      = sql_file.name
        table_key  = _table_key_from_fname(fname, table_names)
        tbl_name   = table_names.get(table_key, "unknown")

        part_match = re.search(r'_part(\d+)\.sql$', fname)
        part_num   = int(part_match.group(1)) if part_match else 1

        total_parts = _count_parts(sql_file.parent, tbl_name)
        rows_here   = _rows_in_part(part_num, row_counts.get(table_key, 0), 500)

        print(f"\n{'─'*70}")
        print(f"  LOADING  : [{schema_name}].[{tbl_name}]  part {part_num}/{total_parts}")
        print(f"  ROWS     : ~{rows_here} rows in this batch")
        print(f"  LOADED   : {rows_loaded}/{total_rows} total rows pushed so far")
        print(f"  FILE     : {fname}")
        print(f"{'─'*70}")

        try:
            commands = _extract_sql_commands(sql_file.read_text(encoding="utf-8"))
            if commands:
                print(f"\n  SQL COMMANDS IN THIS FILE ({len(commands)} statements):")
                print(f"  {'TYPE':<12} {'DESCRIPTION'}")
                print(f"  {'─'*12} {'─'*55}")
                for cmd_type, desc in commands[:30]:
                    print(f"  {cmd_type:<12} {desc[:55]}")
                if len(commands) > 30:
                    print(f"  ... and {len(commands)-30} more statements")
                print()
        except Exception as e:
            _sql_log(f"  Could not read file for preview: {e}", "WARN")

        _sql_log(f"  Sending to DB: {fname} ...", "INFO")
        push_t = time.time()
        result = subprocess.run(
            [sqlcmd, "-S", sql_server, "-d", sql_database,
             "-U", sql_user, "-P", sql_password, "-i", str(sql_file),
             "-b", "-I"],
            capture_output=True, text=True)
        elapsed = time.time() - push_t

        if result.returncode == 0:
            rows_loaded += rows_here
            _sql_log(f"  OK  [{fname}]  ({elapsed:.1f}s)", "OK")

            stdout_lines = [ln.strip() for ln in result.stdout.splitlines() if ln.strip()]
            summary_found = False
            if stdout_lines:
                print(f"  ── DB Confirmation Messages ─────────────")
                for line in stdout_lines:
                    parsed = _parse_summary_line(line, table_names)
                    if parsed:
                        key, ins, upd, unch = parsed
                        db_inserted[key]  += ins
                        db_updated[key]   += upd
                        db_unchanged[key] += unch
                        _sql_log(
                            f"    DB › {tbl_name} part {part_num}/{total_parts} — "
                            f"batch={rows_here}  "
                            f"inserted={ins}  "
                            f"updated={upd}  "
                            f"unchanged(skipped)={unch}"
                            + (f"  ✓ all new" if upd == 0 and unch == 0 else
                               f"  ✓ no changes, skipped" if ins == 0 and upd == 0 else
                               f"  ⚠ {upd} changed and updated, {unch} identical and skipped"
                               if upd > 0 and unch > 0 else
                               f"  ⚠ {upd} row(s) changed — updated in DB" if upd > 0 else
                               f"  — {unch} row(s) identical — no DB write needed"),
                            "INFO")
                    else:
                        _sql_log(f"    DB › {line}", "INFO")

            if total_rows > 0:
                pct        = int(100 * rows_loaded / total_rows)
                bar_filled = int(pct / 5)
                bar        = "█" * bar_filled + "░" * (20 - bar_filled)
                print(f"  ── Batch Summary ────────────────────────")
                print(f"  Rows in batch  : {rows_here}")
                print(f"  Total loaded   : {rows_loaded}/{total_rows}  ({pct}%)")
                print(f"  Progress       : [{bar}] {pct}%")
        else:
            _sql_log(f"  FAILED [{fname}]  (exit {result.returncode}, {elapsed:.1f}s)", "ERROR")
            print(f"\n  ── Error Output ──────────────────────────")
            for line in (result.stderr or result.stdout or "").splitlines()[:20]:
                if line.strip():
                    _sql_log(f"    ERR › {line.strip()}", "ERROR")
            _sql_log("  Stopping push — remaining files NOT loaded.", "ERROR")
            return False

    total_push = _fmt_dur(time.time() - t_start)
    print(f"\n{'─'*70}")
    _sql_log(f"All {len(sql_files)} file(s) pushed in {total_push}.", "OK")

    # ── Final per-table insert / update / duplicate summary ───────────
    print(f"\n{'='*70}")
    print(f"  DB LOAD SUMMARY  —  [{schema_name}]  (insert / update / no-change detail)")
    print(f"{'='*70}")
    print(f"  {'TABLE':<35} {'SENT':>6}  {'INSERTED':>8}  {'UPDATED':>7}  {'UNCHANGED':>9}  NOTE")
    print(f"  {'─'*35} {'─'*6}  {'─'*8}  {'─'*7}  {'─'*9}  {'─'*35}")
    for key in _TABLE_EXEC_ORDER:
        tbl   = table_names.get(key, key)
        sent  = row_counts.get(key, 0)
        ins   = db_inserted.get(key, 0)
        upd   = db_updated.get(key, 0)
        unch  = db_unchanged.get(key, 0)
        if sent == 0:
            note = "no rows this run"
        elif ins == sent:
            note = "✓ all new — no duplicates"
        elif unch == sent:
            note = "✓ identical to DB — nothing written (no change)"
        elif ins == 0 and upd == 0:
            note = "✓ all identical — fully skipped"
        elif upd > 0 and ins == 0:
            note = f"⚠ {upd} row(s) changed and updated, {unch} identical and skipped"
        else:
            note = f"⚠ {ins} new, {upd} changed+updated, {unch} identical+skipped"
        print(f"  {tbl:<35} {sent:>6}  {ins:>8}  {upd:>7}  {unch:>9}  {note}")
    print(f"{'='*70}")
    return True

# ─── Internal helpers used only within purview_sql_model ──────────────

_print_lock_model = __import__("threading").Lock()


def _sql_log(msg, level="INFO"):
    ts   = datetime.datetime.now(datetime.timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
    icon = {"INFO": "[INFO]", "OK": "[OK]  ", "WARN": "[WARN]", "ERROR": "[ERR] "}.get(level, "     ")
    with _print_lock_model:
        print(f"[{ts}] {icon} {msg}")


def _sql_section(title):
    with _print_lock_model:
        print(f"\n{'='*70}\n  {title}\n{'='*70}")


def _fmt_dur(s):
    s = int(s); h, r = divmod(s, 3600); m, s = divmod(r, 60)
    return f"{h}h {m}m {s}s" if h else (f"{m}m {s}s" if m else f"{s}s")


def _table_key_from_fname(fname, table_names):
    """Reverse-lookup: find table_key whose table_name prefix matches fname."""
    for key, tbl in table_names.items():
        if fname.startswith(tbl):
            return key
    return "unknown"


def _count_parts(tbl_dir, tbl_name):
    """Count how many part-*.sql files exist for a table folder."""
    try:
        return max(
            (int(re.search(r'_part(\d+)\.sql$', p.name).group(1))
             for p in Path(tbl_dir).glob(f"{tbl_name}_part*.sql")
             if re.search(r'_part(\d+)\.sql$', p.name)),
            default=1)
    except Exception:
        return 1


def _rows_in_part(part_num, total_rows, max_per_file):
    """Estimate row count in a specific part file."""
    lo = (part_num - 1) * max_per_file
    return min(max_per_file, max(0, total_rows - lo))