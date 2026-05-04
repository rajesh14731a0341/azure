-- ================================================================
-- TABLE  : [compliance].[change_audit_log]
-- PURPOSE: DDL only — data is written inline by unified_catalog files
-- RULES  : APPEND-ONLY. Never UPDATE or DELETE rows here.
--           One row per detected change. PK = log_entry_id (UUID).
-- change_type values:
--   COLUMN_NEW               column seen for the first time
--   COLUMN_CLASSIFIED_ADDED  new classification applied
--   COLUMN_CLASSIFIED_REMOVED classification was removed
--   COLUMN_LABELED_ADDED     sensitivity label applied
--   COLUMN_LABELED_REMOVED   sensitivity label removed
--   COLUMN_TAGGED_CHANGED    business tag changed
-- RUN ID : 6078de68-5558-4591-9434-04e901979c59
-- TIME   : 2026-04-21 05:18:45
-- ================================================================

SET NOCOUNT ON;
GO

IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = N'compliance')
    EXEC('CREATE SCHEMA [compliance]');
GO

IF OBJECT_ID(N'[compliance].[change_audit_log]', N'U') IS NULL
BEGIN
CREATE TABLE [compliance].[change_audit_log] (
    log_entry_id                NVARCHAR(36)    NOT NULL,
    scan_run_id                 NVARCHAR(36)    NULL,
    scan_timestamp              NVARCHAR(30)    NULL,
    change_type                 NVARCHAR(60)    NOT NULL,
    -- ── Column + Asset context ───────────────────────────────────
    column_guid                 NVARCHAR(64)    NULL,
    column_name                 NVARCHAR(500)   NULL,
    asset_guid                  NVARCHAR(64)    NULL,
    asset_name                  NVARCHAR(500)   NULL,
    asset_qualified_name        NVARCHAR(2000)  NULL,
    datasource_type             NVARCHAR(200)   NULL,
    collection_id               NVARCHAR(64)    NULL,
    collection_name             NVARCHAR(500)   NULL,
    collection_hierarchy_path   NVARCHAR(2000)  NULL,
    -- ── What changed ─────────────────────────────────────────────
    changed_field               NVARCHAR(200)   NULL,
    old_value                   NVARCHAR(2000)  NULL,
    new_value                   NVARCHAR(2000)  NULL,
    -- ── Who / when (from Purview) ─────────────────────────────────
    changed_at_in_purview       NVARCHAR(30)    NULL,
    changed_by_in_purview       NVARCHAR(500)   NULL,
    -- ── When this script detected the change ─────────────────────
    detected_at                 DATETIME2       NOT NULL DEFAULT GETUTCDATE(),
    CONSTRAINT PK_change_audit_log PRIMARY KEY (log_entry_id)
);
PRINT 'change_audit_log created.';
END
ELSE
    PRINT 'change_audit_log already exists — skipping DDL.';
GO

PRINT 'change_audit_log DDL complete.';
GO
