-- ============================================================
-- TABLE  : [compliance].[purview_glossary_terms]
-- PART   : 1 of 1
-- ROWS   : 4 total
-- RUN ID : 21310311-e808-4393-abd0-27cce12414b3
-- TIME   : 2026-04-21 00:38:05
-- NULL policy  : only non-null values written — PK validated in Python
-- DUP  policy  : MERGE on PK — re-run safe, no duplicates possible
-- ENRICH filter: classification applied | label applied | tag present
-- ============================================================

SET NOCOUNT ON;
GO

IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = N'compliance')
    EXEC('CREATE SCHEMA [compliance]');
GO

-- ── Table: [compliance].[purview_search_assets] ─────────────────────────────────────────────
-- Raw search catalog from Purview search API.
IF OBJECT_ID(N'[compliance].[purview_search_assets]', N'U') IS NULL
BEGIN
CREATE TABLE [compliance].[purview_search_assets] (
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
    CONSTRAINT PK_purview_search_assets PRIMARY KEY (asset_id)
);
PRINT 'purview_search_assets created.';
END
ELSE
    PRINT 'purview_search_assets already exists — skipping DDL.';
GO

-- ── Table: [compliance].[purview_collections] ─────────────────────────────────────────────
-- Always fetched in full (no timeframe filter).
IF OBJECT_ID(N'[compliance].[purview_collections]', N'U') IS NULL
BEGIN
CREATE TABLE [compliance].[purview_collections] (
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
    CONSTRAINT PK_purview_collections PRIMARY KEY (collection_name)
);
PRINT 'purview_collections created.';
END
ELSE
    PRINT 'purview_collections already exists — skipping DDL.';
GO

-- ── Table: [compliance].[purview_entities] ─────────────────────────────────────────────
IF OBJECT_ID(N'[compliance].[purview_entities]', N'U') IS NULL
BEGIN
CREATE TABLE [compliance].[purview_entities] (
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
    CONSTRAINT PK_purview_entities PRIMARY KEY (guid)
);
PRINT 'purview_entities created.';
END
ELSE
    PRINT 'purview_entities already exists — skipping DDL.';
GO

-- ── Table: [compliance].[purview_glossary_terms] ─────────────────────────────────────────────
-- Always fetched in full (no timeframe filter).
IF OBJECT_ID(N'[compliance].[purview_glossary_terms]', N'U') IS NULL
BEGIN
CREATE TABLE [compliance].[purview_glossary_terms] (
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
    CONSTRAINT PK_purview_glossary_terms PRIMARY KEY (guid)
);
PRINT 'purview_glossary_terms created.';
END
ELSE
    PRINT 'purview_glossary_terms already exists — skipping DDL.';
GO

-- ── Table: [compliance].[asset_registry] ─────────────────────────────────────────────
-- One row per enriched leaf asset.
-- Enrichment filter: assets that have ≥1 column with classification,
--   sensitivity label, or business tag applied.
-- Timeframe filter : driven by classification timestamps only (LAST_RUN).
IF OBJECT_ID(N'[compliance].[asset_registry]', N'U') IS NULL
BEGIN
CREATE TABLE [compliance].[asset_registry] (
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
    CONSTRAINT PK_asset_registry PRIMARY KEY (asset_guid)
);
PRINT 'asset_registry created.';
END
ELSE
    PRINT 'asset_registry already exists — skipping DDL.';
GO

-- Temp table captures MERGE OUTPUT ($action) for each row
IF OBJECT_ID('tempdb..#merge_out') IS NOT NULL DROP TABLE #merge_out;
CREATE TABLE #merge_out (action_type NVARCHAR(10));
GO

-- GlossaryTerm : CDE copy
MERGE [compliance].[purview_glossary_terms] AS T
USING (SELECT N'1b008b3f-e1a1-428a-857c-0354634d31b6' AS [guid]) AS S ON T.[guid] = S.[guid]
WHEN MATCHED AND (COALESCE(CAST(T.[qualified_name] AS NVARCHAR(MAX)), '') <> COALESCE(CAST(N'CDE copy@Glossary_Test' AS NVARCHAR(MAX)), '') OR COALESCE(CAST(T.[term_name] AS NVARCHAR(MAX)), '') <> COALESCE(CAST(N'CDE copy' AS NVARCHAR(MAX)), '') OR COALESCE(CAST(T.[last_modified_ts] AS NVARCHAR(MAX)), '') <> COALESCE(CAST(N'1' AS NVARCHAR(MAX)), '') OR COALESCE(CAST(T.[created_by] AS NVARCHAR(MAX)), '') <> COALESCE(CAST(N'907552d6-5afe-4479-81ec-ea1fd2eeeead' AS NVARCHAR(MAX)), '') OR COALESCE(CAST(T.[updated_by] AS NVARCHAR(MAX)), '') <> COALESCE(CAST(N'907552d6-5afe-4479-81ec-ea1fd2eeeead' AS NVARCHAR(MAX)), '') OR COALESCE(CAST(T.[create_time_epoch] AS NVARCHAR(MAX)), '') <> COALESCE(CAST(1773401858479 AS NVARCHAR(MAX)), '') OR COALESCE(CAST(T.[update_time_epoch] AS NVARCHAR(MAX)), '') <> COALESCE(CAST(1773401858479 AS NVARCHAR(MAX)), '') OR COALESCE(CAST(T.[domain_id] AS NVARCHAR(MAX)), '') <> COALESCE(CAST(N'finastrapurview' AS NVARCHAR(MAX)), '') OR COALESCE(CAST(T.[abbreviation] AS NVARCHAR(MAX)), '') <> COALESCE(CAST(N'Critical Data Elements' AS NVARCHAR(MAX)), '') OR COALESCE(CAST(T.[status] AS NVARCHAR(MAX)), '') <> COALESCE(CAST(N'Draft' AS NVARCHAR(MAX)), '') OR COALESCE(CAST(T.[glossary_guid] AS NVARCHAR(MAX)), '') <> COALESCE(CAST(N'0bbc2a44-2ab4-4229-91cc-2b242af1cd3a' AS NVARCHAR(MAX)), '') OR COALESCE(CAST(T.[synonym_display_text] AS NVARCHAR(MAX)), '') <> COALESCE(CAST(N'CDE_CDE - SSN' AS NVARCHAR(MAX)), '')) THEN UPDATE SET
    T.[qualified_name] = N'CDE copy@Glossary_Test', T.[term_name] = N'CDE copy', T.[last_modified_ts] = N'1', T.[created_by] = N'907552d6-5afe-4479-81ec-ea1fd2eeeead', T.[updated_by] = N'907552d6-5afe-4479-81ec-ea1fd2eeeead', T.[create_time_epoch] = 1773401858479, T.[update_time_epoch] = 1773401858479, T.[domain_id] = N'finastrapurview', T.[abbreviation] = N'Critical Data Elements', T.[status] = N'Draft', T.[glossary_guid] = N'0bbc2a44-2ab4-4229-91cc-2b242af1cd3a', T.[synonym_display_text] = N'CDE_CDE - SSN', T.[fetched_at] = N'2026-04-21 00:38:05', T.[scan_run_id] = N'21310311-e808-4393-abd0-27cce12414b3'
WHEN NOT MATCHED THEN INSERT
    ([guid], [qualified_name], [term_name], [last_modified_ts], [created_by], [updated_by], [create_time_epoch], [update_time_epoch], [domain_id], [abbreviation], [status], [glossary_guid], [synonym_display_text], [fetched_at], [scan_run_id])
    VALUES (N'1b008b3f-e1a1-428a-857c-0354634d31b6', N'CDE copy@Glossary_Test', N'CDE copy', N'1', N'907552d6-5afe-4479-81ec-ea1fd2eeeead', N'907552d6-5afe-4479-81ec-ea1fd2eeeead', 1773401858479, 1773401858479, N'finastrapurview', N'Critical Data Elements', N'Draft', N'0bbc2a44-2ab4-4229-91cc-2b242af1cd3a', N'CDE_CDE - SSN', N'2026-04-21 00:38:05', N'21310311-e808-4393-abd0-27cce12414b3')
OUTPUT $action INTO #merge_out(action_type);

-- GlossaryTerm : CDE
MERGE [compliance].[purview_glossary_terms] AS T
USING (SELECT N'ab4827f7-3bb1-4469-9135-894fe18181cf' AS [guid]) AS S ON T.[guid] = S.[guid]
WHEN MATCHED AND (COALESCE(CAST(T.[qualified_name] AS NVARCHAR(MAX)), '') <> COALESCE(CAST(N'CDE@Glossary_Test' AS NVARCHAR(MAX)), '') OR COALESCE(CAST(T.[term_name] AS NVARCHAR(MAX)), '') <> COALESCE(CAST(N'CDE' AS NVARCHAR(MAX)), '') OR COALESCE(CAST(T.[last_modified_ts] AS NVARCHAR(MAX)), '') <> COALESCE(CAST(N'1' AS NVARCHAR(MAX)), '') OR COALESCE(CAST(T.[created_by] AS NVARCHAR(MAX)), '') <> COALESCE(CAST(N'907552d6-5afe-4479-81ec-ea1fd2eeeead' AS NVARCHAR(MAX)), '') OR COALESCE(CAST(T.[updated_by] AS NVARCHAR(MAX)), '') <> COALESCE(CAST(N'907552d6-5afe-4479-81ec-ea1fd2eeeead' AS NVARCHAR(MAX)), '') OR COALESCE(CAST(T.[create_time_epoch] AS NVARCHAR(MAX)), '') <> COALESCE(CAST(1771572834719 AS NVARCHAR(MAX)), '') OR COALESCE(CAST(T.[update_time_epoch] AS NVARCHAR(MAX)), '') <> COALESCE(CAST(1771572834719 AS NVARCHAR(MAX)), '') OR COALESCE(CAST(T.[domain_id] AS NVARCHAR(MAX)), '') <> COALESCE(CAST(N'finastrapurview' AS NVARCHAR(MAX)), '') OR COALESCE(CAST(T.[abbreviation] AS NVARCHAR(MAX)), '') <> COALESCE(CAST(N'Critical Data Elements' AS NVARCHAR(MAX)), '') OR COALESCE(CAST(T.[status] AS NVARCHAR(MAX)), '') <> COALESCE(CAST(N'Draft' AS NVARCHAR(MAX)), '') OR COALESCE(CAST(T.[glossary_guid] AS NVARCHAR(MAX)), '') <> COALESCE(CAST(N'0bbc2a44-2ab4-4229-91cc-2b242af1cd3a' AS NVARCHAR(MAX)), '') OR COALESCE(CAST(T.[synonym_display_text] AS NVARCHAR(MAX)), '') <> COALESCE(CAST(N'CDE_CDE - SSN' AS NVARCHAR(MAX)), '')) THEN UPDATE SET
    T.[qualified_name] = N'CDE@Glossary_Test', T.[term_name] = N'CDE', T.[last_modified_ts] = N'1', T.[created_by] = N'907552d6-5afe-4479-81ec-ea1fd2eeeead', T.[updated_by] = N'907552d6-5afe-4479-81ec-ea1fd2eeeead', T.[create_time_epoch] = 1771572834719, T.[update_time_epoch] = 1771572834719, T.[domain_id] = N'finastrapurview', T.[abbreviation] = N'Critical Data Elements', T.[status] = N'Draft', T.[glossary_guid] = N'0bbc2a44-2ab4-4229-91cc-2b242af1cd3a', T.[synonym_display_text] = N'CDE_CDE - SSN', T.[fetched_at] = N'2026-04-21 00:38:05', T.[scan_run_id] = N'21310311-e808-4393-abd0-27cce12414b3'
WHEN NOT MATCHED THEN INSERT
    ([guid], [qualified_name], [term_name], [last_modified_ts], [created_by], [updated_by], [create_time_epoch], [update_time_epoch], [domain_id], [abbreviation], [status], [glossary_guid], [synonym_display_text], [fetched_at], [scan_run_id])
    VALUES (N'ab4827f7-3bb1-4469-9135-894fe18181cf', N'CDE@Glossary_Test', N'CDE', N'1', N'907552d6-5afe-4479-81ec-ea1fd2eeeead', N'907552d6-5afe-4479-81ec-ea1fd2eeeead', 1771572834719, 1771572834719, N'finastrapurview', N'Critical Data Elements', N'Draft', N'0bbc2a44-2ab4-4229-91cc-2b242af1cd3a', N'CDE_CDE - SSN', N'2026-04-21 00:38:05', N'21310311-e808-4393-abd0-27cce12414b3')
OUTPUT $action INTO #merge_out(action_type);

-- GlossaryTerm : CDE_CDE - SSN
MERGE [compliance].[purview_glossary_terms] AS T
USING (SELECT N'6842bb5c-88b0-4d8c-9ff9-029db061e811' AS [guid]) AS S ON T.[guid] = S.[guid]
WHEN MATCHED AND (COALESCE(CAST(T.[qualified_name] AS NVARCHAR(MAX)), '') <> COALESCE(CAST(N'CDE_CDE - SSN@Glossary_Test' AS NVARCHAR(MAX)), '') OR COALESCE(CAST(T.[term_name] AS NVARCHAR(MAX)), '') <> COALESCE(CAST(N'CDE_CDE - SSN' AS NVARCHAR(MAX)), '') OR COALESCE(CAST(T.[long_description] AS NVARCHAR(MAX)), '') <> COALESCE(CAST(N'<div>Creating this term for testing purpose.</div>' AS NVARCHAR(MAX)), '') OR COALESCE(CAST(T.[last_modified_ts] AS NVARCHAR(MAX)), '') <> COALESCE(CAST(N'2' AS NVARCHAR(MAX)), '') OR COALESCE(CAST(T.[created_by] AS NVARCHAR(MAX)), '') <> COALESCE(CAST(N'907552d6-5afe-4479-81ec-ea1fd2eeeead' AS NVARCHAR(MAX)), '') OR COALESCE(CAST(T.[updated_by] AS NVARCHAR(MAX)), '') <> COALESCE(CAST(N'907552d6-5afe-4479-81ec-ea1fd2eeeead' AS NVARCHAR(MAX)), '') OR COALESCE(CAST(T.[create_time_epoch] AS NVARCHAR(MAX)), '') <> COALESCE(CAST(1773398438621 AS NVARCHAR(MAX)), '') OR COALESCE(CAST(T.[update_time_epoch] AS NVARCHAR(MAX)), '') <> COALESCE(CAST(1773400112427 AS NVARCHAR(MAX)), '') OR COALESCE(CAST(T.[domain_id] AS NVARCHAR(MAX)), '') <> COALESCE(CAST(N'finastrapurview' AS NVARCHAR(MAX)), '') OR COALESCE(CAST(T.[abbreviation] AS NVARCHAR(MAX)), '') <> COALESCE(CAST(N'Critical Data Element, Social Security Number' AS NVARCHAR(MAX)), '') OR COALESCE(CAST(T.[status] AS NVARCHAR(MAX)), '') <> COALESCE(CAST(N'Draft' AS NVARCHAR(MAX)), '') OR COALESCE(CAST(T.[glossary_guid] AS NVARCHAR(MAX)), '') <> COALESCE(CAST(N'0bbc2a44-2ab4-4229-91cc-2b242af1cd3a' AS NVARCHAR(MAX)), '') OR COALESCE(CAST(T.[synonym_display_text] AS NVARCHAR(MAX)), '') <> COALESCE(CAST(N'CDE, CDE copy' AS NVARCHAR(MAX)), '')) THEN UPDATE SET
    T.[qualified_name] = N'CDE_CDE - SSN@Glossary_Test', T.[term_name] = N'CDE_CDE - SSN', T.[long_description] = N'<div>Creating this term for testing purpose.</div>', T.[last_modified_ts] = N'2', T.[created_by] = N'907552d6-5afe-4479-81ec-ea1fd2eeeead', T.[updated_by] = N'907552d6-5afe-4479-81ec-ea1fd2eeeead', T.[create_time_epoch] = 1773398438621, T.[update_time_epoch] = 1773400112427, T.[domain_id] = N'finastrapurview', T.[abbreviation] = N'Critical Data Element, Social Security Number', T.[status] = N'Draft', T.[glossary_guid] = N'0bbc2a44-2ab4-4229-91cc-2b242af1cd3a', T.[synonym_display_text] = N'CDE, CDE copy', T.[fetched_at] = N'2026-04-21 00:38:05', T.[scan_run_id] = N'21310311-e808-4393-abd0-27cce12414b3'
WHEN NOT MATCHED THEN INSERT
    ([guid], [qualified_name], [term_name], [long_description], [last_modified_ts], [created_by], [updated_by], [create_time_epoch], [update_time_epoch], [domain_id], [abbreviation], [status], [glossary_guid], [synonym_display_text], [fetched_at], [scan_run_id])
    VALUES (N'6842bb5c-88b0-4d8c-9ff9-029db061e811', N'CDE_CDE - SSN@Glossary_Test', N'CDE_CDE - SSN', N'<div>Creating this term for testing purpose.</div>', N'2', N'907552d6-5afe-4479-81ec-ea1fd2eeeead', N'907552d6-5afe-4479-81ec-ea1fd2eeeead', 1773398438621, 1773400112427, N'finastrapurview', N'Critical Data Element, Social Security Number', N'Draft', N'0bbc2a44-2ab4-4229-91cc-2b242af1cd3a', N'CDE, CDE copy', N'2026-04-21 00:38:05', N'21310311-e808-4393-abd0-27cce12414b3')
OUTPUT $action INTO #merge_out(action_type);

-- GlossaryTerm : Account number-test
MERGE [compliance].[purview_glossary_terms] AS T
USING (SELECT N'1656cda7-b334-4090-9832-1118a4ff2392' AS [guid]) AS S ON T.[guid] = S.[guid]
WHEN MATCHED AND (COALESCE(CAST(T.[qualified_name] AS NVARCHAR(MAX)), '') <> COALESCE(CAST(N'Account number-test@workflow_glossary' AS NVARCHAR(MAX)), '') OR COALESCE(CAST(T.[term_name] AS NVARCHAR(MAX)), '') <> COALESCE(CAST(N'Account number-test' AS NVARCHAR(MAX)), '') OR COALESCE(CAST(T.[last_modified_ts] AS NVARCHAR(MAX)), '') <> COALESCE(CAST(N'1' AS NVARCHAR(MAX)), '') OR COALESCE(CAST(T.[created_by] AS NVARCHAR(MAX)), '') <> COALESCE(CAST(N'c19d1d80-8edc-43ef-be10-808ee8a42782' AS NVARCHAR(MAX)), '') OR COALESCE(CAST(T.[updated_by] AS NVARCHAR(MAX)), '') <> COALESCE(CAST(N'c19d1d80-8edc-43ef-be10-808ee8a42782' AS NVARCHAR(MAX)), '') OR COALESCE(CAST(T.[create_time_epoch] AS NVARCHAR(MAX)), '') <> COALESCE(CAST(1772426604337 AS NVARCHAR(MAX)), '') OR COALESCE(CAST(T.[update_time_epoch] AS NVARCHAR(MAX)), '') <> COALESCE(CAST(1772426604337 AS NVARCHAR(MAX)), '') OR COALESCE(CAST(T.[domain_id] AS NVARCHAR(MAX)), '') <> COALESCE(CAST(N'syhcvr' AS NVARCHAR(MAX)), '') OR COALESCE(CAST(T.[status] AS NVARCHAR(MAX)), '') <> COALESCE(CAST(N'Draft' AS NVARCHAR(MAX)), '') OR COALESCE(CAST(T.[glossary_guid] AS NVARCHAR(MAX)), '') <> COALESCE(CAST(N'e4120468-9a1f-44cb-af02-0e181e34d039' AS NVARCHAR(MAX)), '')) THEN UPDATE SET
    T.[qualified_name] = N'Account number-test@workflow_glossary', T.[term_name] = N'Account number-test', T.[last_modified_ts] = N'1', T.[created_by] = N'c19d1d80-8edc-43ef-be10-808ee8a42782', T.[updated_by] = N'c19d1d80-8edc-43ef-be10-808ee8a42782', T.[create_time_epoch] = 1772426604337, T.[update_time_epoch] = 1772426604337, T.[domain_id] = N'syhcvr', T.[status] = N'Draft', T.[glossary_guid] = N'e4120468-9a1f-44cb-af02-0e181e34d039', T.[fetched_at] = N'2026-04-21 00:38:05', T.[scan_run_id] = N'21310311-e808-4393-abd0-27cce12414b3'
WHEN NOT MATCHED THEN INSERT
    ([guid], [qualified_name], [term_name], [last_modified_ts], [created_by], [updated_by], [create_time_epoch], [update_time_epoch], [domain_id], [status], [glossary_guid], [fetched_at], [scan_run_id])
    VALUES (N'1656cda7-b334-4090-9832-1118a4ff2392', N'Account number-test@workflow_glossary', N'Account number-test', N'1', N'c19d1d80-8edc-43ef-be10-808ee8a42782', N'c19d1d80-8edc-43ef-be10-808ee8a42782', 1772426604337, 1772426604337, N'syhcvr', N'Draft', N'e4120468-9a1f-44cb-af02-0e181e34d039', N'2026-04-21 00:38:05', N'21310311-e808-4393-abd0-27cce12414b3')
OUTPUT $action INTO #merge_out(action_type);


GO
-- Summarise inserts vs updates for this batch
DECLARE @ins INT, @upd INT;
SELECT @ins = COUNT(*) FROM #merge_out WHERE action_type = 'INSERT';
SELECT @upd = COUNT(*) FROM #merge_out WHERE action_type = 'UPDATE';
PRINT 'MERGE_SUMMARY|purview_glossary_terms|part=1/1|batch=4|inserted=' + CAST(@ins AS NVARCHAR) + '|updated=' + CAST(@upd AS NVARCHAR) + '|unchanged=' + CAST((4 - @ins - @upd) AS NVARCHAR);
DROP TABLE #merge_out;
GO
