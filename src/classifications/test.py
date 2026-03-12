import os
import sys
import json
import requests
import pandas as pd
from collections import defaultdict

# ------------------------------------------------
# CONFIG
# ------------------------------------------------

CLIENT_ID       = "2c98dd46-5ec9-4198-b265-058e18078125"
CLIENT_SECRET   = "Ijh8Q~s8DdTg~PNkeiFR.Al~rjAPMBb5eovbqcbY"
TENANT_ID       = "5f9bacc0-ffe8-41f7-8d25-215d55cb0f96"
PURVIEW_ACCOUNT = "finastrapurview"

SEARCH_API       = f"https://{PURVIEW_ACCOUNT}.purview.azure.com/datamap/api/search/query?api-version=2023-09-01"
ENTITY_API       = f"https://{PURVIEW_ACCOUNT}.purview.azure.com/datamap/api/atlas/v2/entity/guid"
COLLECTIONS_API  = f"https://{PURVIEW_ACCOUNT}.purview.azure.com/account/collections?api-version=2019-11-01-preview"
DATASOURCES_API  = f"https://{PURVIEW_ACCOUNT}.purview.azure.com/scan/datasources?api-version=2022-07-01-preview"

EXCEL_FILE       = "purview_columns_to_classify.xlsx"
RAW_CATALOG_FILE = "purview_raw_catalog.json"
CATALOG_SNAPSHOT = "catalog_snapshot.json"
CDE_FILE         = "rajesh_test.xlsx"

# ------------------------------------------------
# FILTER CONFIG
# ------------------------------------------------

# FILTER_COLLECTIONS
#   Purview collection paths using / as separator.
#   Use full path to avoid ambiguity when same name exists in multiple places.
#   Find the path from Data Map -> Collections breadcrumb.
#
#   "POC_Finastra"                        — top-level only
#   "POC_Finastra/Lending-POS"            — specific child
#   "POC_Finastra/Lending-POS/Lending-US" — deeply nested
#   None                                  — all collections
#
FILTER_COLLECTIONS = ["POC_Finastra/Lending-POS","POC_Finastra/US Region"]

# FILTER_DATA_SOURCES
#   Registered source names exactly as shown in Data Map.
#   The script resolves each name to its endpoint via the scan API,
#   then matches assets whose qualifiedName starts with that endpoint.
#
#   ["finastra-onprem-oracle"]                         — one source
#   ["finastra-onprem-oracle", "Fusion_Loan_IQ"]       — multiple
#   None                                               — all sources
#
FILTER_DATA_SOURCES = ["finastra-onprem-oracle", "Fusion_Loan_IQ"]

# ------------------------------------------------
# CONSTANTS
# ------------------------------------------------

TABLE_TYPES = {
    "mssql_table",
    "azure_sql_table",
    "oracle_table",
    "postgresql_table",
    "azure_blob_path",
    "azure_blob_resource_set",
    "azure_cosmosdb_sqlapi_collection",
}

COSMOS_SYSTEM_FIELDS = {
    "_rid", "_self", "_etag", "_attachments", "_ts",
    "_lsn", "_metadata", "_docs", "_sprocs", "_triggers",
    "_udfs", "_conflicts"
}

ENTITY_TYPE_TO_TAB = {
    "mssql_table":                      "SQL Server",
    "azure_sql_table":                  "Azure SQL Database",
    "oracle_table":                     "Oracle",
    "postgresql_table":                 "PostgreSQL",
    "azure_blob_path":                  "Azure Blob Storage",
    "azure_blob_resource_set":          "Azure Blob Storage",
    "azure_cosmosdb_sqlapi_collection": "Azure Cosmos DB",
}

TAB_ORDER = ["SQL Server", "Azure SQL Database", "Oracle",
             "PostgreSQL", "Azure Blob Storage", "Azure Cosmos DB"]

# ------------------------------------------------
# RUN MODES
# ------------------------------------------------
#   python purview_classifier.py
#     -> Scan, generate Excel + JSON files
#
#   python purview_classifier.py purview_columns_to_classify.xlsx
#     -> Manual: apply classifications filled in Excel
#
#   python purview_classifier.py rajesh_test.xlsx purview_columns_to_classify.xlsx
#     -> CDE: auto-match rules and apply classifications
# ------------------------------------------------

# ------------------------------------------------
# AUTH
# ------------------------------------------------

def get_token():
    url = f"https://login.microsoftonline.com/{TENANT_ID}/oauth2/token"
    r   = requests.post(url, data={
        "grant_type":    "client_credentials",
        "client_id":     CLIENT_ID,
        "client_secret": CLIENT_SECRET,
        "resource":      "https://purview.azure.net"
    })
    r.raise_for_status()
    return r.json()["access_token"]

token   = get_token()
headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}

# ------------------------------------------------
# COLLECTION FILTER  —  name/path -> collectionId
# ------------------------------------------------

def fetch_all_collections():
    r = requests.get(COLLECTIONS_API, headers=headers)
    if r.status_code != 200:
        print(f"  [WARN] Collections API returned {r.status_code} — collection filter disabled.")
        return []
    results = []
    for col in r.json().get("value", []):
        col_id   = col.get("name", "")
        friendly = col.get("friendlyName") or col.get("name", "")
        parent   = col.get("parentCollection", {}).get("referenceName", "")
        if col_id:
            results.append({"id": col_id, "friendlyName": friendly, "parentId": parent})
    return results


def build_path_map(collections):
    """{ collectionId -> "Parent/Child/Leaf" } built by walking parentId chain."""
    by_id = {c["id"]: c for c in collections}

    def get_path(cid, visited=None):
        if visited is None:
            visited = set()
        if cid in visited or cid not in by_id:
            return ""
        visited.add(cid)
        col    = by_id[cid]
        parent = col["parentId"]
        name   = col["friendlyName"]
        if parent and parent in by_id and parent != cid:
            pp = get_path(parent, visited)
            return f"{pp}/{name}" if pp else name
        return name

    return {c["id"]: get_path(c["id"]) for c in collections}


def resolve_collection_ids(filter_paths):
    """Resolve list of path strings to collectionIds. Prints full hierarchy."""
    collections = fetch_all_collections()
    if not collections:
        return []

    path_map = build_path_map(collections)

    print("\n  Purview collection hierarchy:")
    for cid, path in sorted(path_map.items(), key=lambda x: x[1]):
        print(f"    {path}  [{cid}]")

    resolved = []
    for wanted in filter_paths:
        wanted = wanted.strip()
        is_full_path = "/" in wanted
        matches = []

        for cid, path in path_map.items():
            if is_full_path:
                if path.lower() == wanted.lower():
                    matches.append((cid, path))
            else:
                leaf = path.split("/")[-1].strip()
                if leaf.lower() == wanted.lower():
                    matches.append((cid, path))

        if not matches:
            print(f"\n  [WARN] '{wanted}' not found — check hierarchy above.")
        elif len(matches) > 1 and not is_full_path:
            print(f"\n  [WARN] '{wanted}' is ambiguous — {len(matches)} matches:")
            for cid, path in matches:
                print(f"         FILTER_COLLECTIONS = [\"{path}\"]")
        else:
            cid, path = matches[0]
            resolved.append(cid)
            print(f"\n  Resolved: '{wanted}' -> '{path}'  [{cid}]")

    return resolved

# ------------------------------------------------
# DATA SOURCE FILTER  —  source name -> endpoint prefix
# ------------------------------------------------

_DS_ENDPOINT_CACHE = {}   # { source_name_lower -> endpoint_lower }


def resolve_datasource_endpoints(all_assets):
    """
    Dynamically resolves each FILTER_DATA_SOURCES name to its qualifiedName
    prefix by cross-referencing two sources:

      1. /scan/datasources API  -> gives bare host/IP per registered source name
                                   e.g. "20.51.200.183"
      2. all_assets (catalog)   -> gives real qualifiedNames which already contain
                                   the correct protocol + host
                                   e.g. "oracle://20.51.200.183/..."
                                        "mssql://20.51.200.183/..."

    By looking up the bare host inside the actual qualifiedNames from the catalog,
    we pick up the protocol automatically — no hardcoding needed.
    Works for any source type Purview supports now or in the future.
    """
    global _DS_ENDPOINT_CACHE
    if not FILTER_DATA_SOURCES or _DS_ENDPOINT_CACHE:
        return

    # Build lookup: bare_host_lower -> set of "protocol://host" prefixes seen in catalog
    host_to_prefixes = defaultdict(set)
    for asset in all_assets:
        qn = asset.get("qualifiedName", "")
        if "://" in qn:
            parts = qn.split("/")
            if len(parts) >= 3:
                prefix = "/".join(parts[:3]).lower()   # "oracle://20.51.200.183"
                host   = parts[2].lower()              # "20.51.200.183"
                host_to_prefixes[host].add(prefix)

    r = requests.get(DATASOURCES_API, headers=headers)
    if r.status_code != 200:
        print(f"  [WARN] Scan datasources API returned {r.status_code}.")
        print(f"         Data source filter will use name-in-qualifiedName fallback.")
        return

    wanted_lower = {ds.strip().lower(): ds.strip() for ds in FILTER_DATA_SOURCES}

    print("\n  Resolving data source endpoints:")
    for ds in r.json().get("value", []):
        ds_name = ds.get("name", "").strip()
        if ds_name.lower() not in wanted_lower:
            continue

        kind  = ds.get("kind", "")
        props = ds.get("properties", {})

        # Get raw value from scan API (may be bare IP/hostname or full URL)
        raw = (
            props.get("endpoint")        or
            props.get("serverEndpoint")  or
            props.get("host")            or
            props.get("serviceUrl")      or
            props.get("location")        or
            ""
        ).strip().rstrip("/")

        original_name = wanted_lower[ds_name.lower()]

        if not raw:
            _DS_ENDPOINT_CACHE[ds_name.lower()] = ""
            print(f"    [{original_name}]  ->  (no host in API — using name fallback)  [{kind}]")
            continue

        if "://" in raw:
            # Already a full URL — use as-is
            endpoint = raw.lower()
        else:
            # Bare host/IP — find the correct protocol from actual catalog qualifiedNames
            raw_lower = raw.lower()
            prefixes  = host_to_prefixes.get(raw_lower, set())
            if prefixes:
                # If multiple protocols found for same host, pick the one whose
                # protocol matches the source kind (e.g. oracle:// for Oracle kind)
                kind_lower = kind.lower()
                matched = next(
                    (p for p in prefixes if kind_lower[:4] in p),
                    sorted(prefixes)[0]   # fallback: alphabetical first
                )
                endpoint = matched
            else:
                # Host not yet seen in catalog — store bare host, startswith will still
                # partially match if qualifiedName contains it
                endpoint = raw_lower

        _DS_ENDPOINT_CACHE[ds_name.lower()] = endpoint
        print(f"    [{original_name}]  ->  {endpoint}  [{kind}]")

    # Warn about names not found in the scan API at all
    found = set(_DS_ENDPOINT_CACHE.keys())
    for ds in FILTER_DATA_SOURCES:
        if ds.strip().lower() not in found:
            print(f"    [{ds}]  ->  NOT FOUND in scan/datasources — check source name spelling")


def asset_matches_datasource(asset):
    """
    True if this asset's qualifiedName starts with the resolved endpoint
    of one of the FILTER_DATA_SOURCES.

    Fallback: if endpoint resolution failed for a source, checks whether
    the source name appears as a substring in the qualifiedName.
    """
    if not FILTER_DATA_SOURCES:
        return True

    qn = asset.get("qualifiedName", "").lower().rstrip("/")

    for wanted in FILTER_DATA_SOURCES:
        key      = wanted.strip().lower()
        endpoint = _DS_ENDPOINT_CACHE.get(key, "")

        if endpoint:
            if qn.startswith(endpoint):
                return True
        else:
            if key in qn:
                return True

    return False

# ------------------------------------------------
# FETCH CATALOG
# ------------------------------------------------

def fetch_catalog():
    print("\n" + "="*60)
    print("  STEP 1 — Fetching assets from Purview catalog")
    print("="*60)

    collection_ids = []
    if FILTER_COLLECTIONS:
        print(f"\n  Resolving collection paths: {FILTER_COLLECTIONS}")
        collection_ids = resolve_collection_ids(FILTER_COLLECTIONS)
        if not collection_ids:
            print("  [WARN] No collection IDs resolved — fetching all assets.")

    all_assets         = []
    continuation_token = None
    page               = 1

    while True:
        body = {"keywords": "*", "limit": 100}

        if collection_ids:
            body["filter"] = (
                {"collectionId": collection_ids[0]}
                if len(collection_ids) == 1
                else {"or": [{"collectionId": cid} for cid in collection_ids]}
            )

        if continuation_token:
            body["continuationToken"] = continuation_token

        r = requests.post(SEARCH_API, headers=headers, json=body)
        r.raise_for_status()
        data   = r.json()
        assets = data.get("value", [])
        all_assets.extend(assets)

        print(f"  Page {page:>2}: {len(assets):>4} assets  |  Running total: {len(all_assets)}")

        continuation_token = data.get("continuationToken")
        if not continuation_token or not assets:
            break
        page += 1

    print(f"\n  Total assets discovered -> {len(all_assets)}")
    with open(RAW_CATALOG_FILE, "w") as f:
        json.dump(all_assets, f, indent=2)
    print(f"  Raw catalog saved      -> {RAW_CATALOG_FILE}")

    return all_assets

# ------------------------------------------------
# FETCH ENTITY
# ------------------------------------------------

def fetch_entity(guid, mini=False):
    url = f"{ENTITY_API}/{guid}?minExtInfo={'true' if mini else 'false'}"
    r   = requests.get(url, headers=headers)
    return r.json() if r.status_code == 200 else None

# ------------------------------------------------
# EXTRACT COLUMNS
# ------------------------------------------------

def extract_columns(entity_json):
    """
    Returns list of column dicts with typeName, guid, attributes, classifications.

    SQL / Oracle / PostgreSQL  -> relationshipAttributes.columns / table_columns
                                   + referredEntities
    Blob / CSV                 -> relationshipAttributes.attachedSchema
                                   -> fetch schema entity -> referredEntities
    Cosmos DB                  -> relationshipAttributes.tabular_schema
                                   -> fetch tabular_schema (minExtInfo=true)
                                   -> referredEntities contains column objects
    """
    columns       = {}
    entity        = entity_json.get("entity", {})
    entity_type   = entity.get("typeName", "").lower()
    relationships = entity.get("relationshipAttributes", {})
    referred      = entity_json.get("referredEntities", {})

    # 1. SQL / Oracle / PostgreSQL
    for key in ["columns", "table_columns"]:
        for ref in relationships.get(key, []):
            g = ref.get("guid")
            if g in referred:
                columns[g] = referred[g]

    # 2. Blob / CSV
    for schema in relationships.get("attachedSchema", []):
        sguid = schema.get("guid")
        if not sguid:
            continue
        sj = fetch_entity(sguid)
        if not sj:
            continue
        for g, obj in sj.get("referredEntities", {}).items():
            tn = obj.get("typeName", "").lower()
            if "column" in tn or "field" in tn or "attribute" in tn:
                columns[g] = obj

    # 3. Cosmos DB
    if "cosmosdb" in entity_type:
        ts_ref  = relationships.get("tabular_schema", {})
        ts_guid = ts_ref.get("guid") if isinstance(ts_ref, dict) else None
        if ts_guid:
            ts_json = fetch_entity(ts_guid, mini=True)
            if ts_json:
                ts_referred = ts_json.get("referredEntities", {})
                ts_rels     = ts_json.get("entity", {}).get("relationshipAttributes", {})
                for g, obj in ts_referred.items():
                    if "column" in obj.get("typeName", "").lower():
                        columns[g] = obj
                for ref in ts_rels.get("columns", []):
                    g = ref.get("guid")
                    if g and g in ts_referred and g not in columns:
                        columns[g] = ts_referred[g]

    return list(columns.values())

# ------------------------------------------------
# SCAN MODE
# ------------------------------------------------

def scan_catalog():
    print("\n  ACTIVE FILTERS")
    print(f"  Collections  : {FILTER_COLLECTIONS  if FILTER_COLLECTIONS  else 'ALL'}")
    print(f"  Data sources : {FILTER_DATA_SOURCES if FILTER_DATA_SOURCES else 'ALL'}")

    all_assets   = fetch_catalog()
    table_assets = [a for a in all_assets if a.get("entityType") in TABLE_TYPES]

    # Resolve data source endpoints then filter
    resolve_datasource_endpoints(all_assets)
    if FILTER_DATA_SOURCES:
        before       = len(table_assets)
        table_assets = [a for a in table_assets if asset_matches_datasource(a)]
        print(f"\n  Data source filter: {before} -> {len(table_assets)} assets kept")
        if not table_assets:
            print(f"  [WARN] No assets matched. Check source names match exactly what is in Purview.")
            return

    print(f"\n  Table-level assets to process -> {len(table_assets)}")

    source_groups = defaultdict(list)
    for a in table_assets:
        tab = ENTITY_TYPE_TO_TAB.get(a.get("entityType", ""), "Unknown")
        source_groups[tab].append(a)

    print("\n" + "="*60)
    print("  STEP 2 — Fetching entity details & columns")
    print("="*60)

    rows_by_tab      = defaultdict(list)
    snapshot_sources = {}

    for tab_name, assets in source_groups.items():
        print(f"\n{'='*60}")
        print(f"  DATA SOURCE : {tab_name}  |  Assets: {len(assets)}")
        print(f"{'='*60}")

        source_snapshot      = []
        total_classified     = 0
        total_unclassified   = 0

        for asset in assets:
            guid           = asset.get("id")
            qualified_name = asset.get("qualifiedName", "")
            entity_type    = asset.get("entityType", "")
            collection_id  = asset.get("collectionId", "")

            if not guid:
                continue

            entity_json = fetch_entity(guid)
            if not entity_json:
                continue

            columns = extract_columns(entity_json)
            if not columns:
                continue

            classified_cols   = []
            unclassified_cols = []

            asset_snapshot = {
                "qualifiedName": qualified_name,
                "entityType":    entity_type,
                "collectionId":  collection_id,
                "tab":           tab_name,
                "columns":       []
            }

            for col in columns:
                attr     = col.get("attributes", {})
                col_name = attr.get("name")
                if not col_name or col_name in COSMOS_SYSTEM_FIELDS:
                    continue

                class_list = [c.get("typeName") for c in col.get("classifications", [])]

                asset_snapshot["columns"].append({
                    "name":            col_name,
                    "guid":            col.get("guid"),
                    "classifications": class_list
                })

                if class_list:
                    classified_cols.append(col_name)
                    total_classified += 1
                else:
                    unclassified_cols.append(col_name)
                    total_unclassified += 1
                    rows_by_tab[tab_name].append({
                        "DataSource":        tab_name,
                        "EntityType":        entity_type,
                        "FullQualifiedName": qualified_name,
                        "Column":            col_name,
                        "ColumnGUID":        col.get("guid"),
                        "Classification":    "",
                        "Status":            ""
                    })

            source_snapshot.append(asset_snapshot)

            total = len(classified_cols) + len(unclassified_cols)
            print(f"\n  ASSET        : {qualified_name}")
            print(f"  COLLECTION   : {collection_id}  |  TYPE: {entity_type}")
            print(f"  COLS         : {total} total | {len(classified_cols)} classified | {len(unclassified_cols)} unclassified")
            if classified_cols:
                print(f"  [+] Classified   : {', '.join(classified_cols)}")
            if unclassified_cols:
                print(f"  [-] Unclassified : {', '.join(unclassified_cols)}")

        print(f"\n  -- {tab_name} SUMMARY --")
        print(f"     Assets    : {len(source_snapshot)}")
        print(f"     Classified cols   : {total_classified}")
        print(f"     Unclassified cols : {total_unclassified}")
        print(f"{'='*60}\n")

        snapshot_sources[tab_name] = source_snapshot

    with open(CATALOG_SNAPSHOT, "w") as f:
        json.dump(snapshot_sources, f, indent=2)

    total_rows = 0
    with pd.ExcelWriter(EXCEL_FILE, engine="openpyxl") as writer:
        for tab in TAB_ORDER:
            rows = rows_by_tab.get(tab, [])
            df   = pd.DataFrame(rows)
            if not df.empty:
                df = df.drop_duplicates(subset=["FullQualifiedName", "Column"])
            df.to_excel(writer, sheet_name=tab, index=False)
            total_rows += len(df)
            print(f"  Tab [{tab}] -> {len(df)} unclassified columns")

    print(f"\n{'='*60}")
    print(f"  Snapshot -> {CATALOG_SNAPSHOT}")
    print(f"  Raw      -> {RAW_CATALOG_FILE}")
    print(f"  Excel    -> {EXCEL_FILE}  ({total_rows} unclassified columns)")
    print("="*60)

# ------------------------------------------------
# MANUAL MODE
# ------------------------------------------------

def apply_classifications(catalog_file):
    print("\n" + "="*60)
    print("  MANUAL CLASSIFICATION MODE")
    print(f"  File: {catalog_file}")
    print("="*60)

    updated_dfs = {}
    for sheet in pd.ExcelFile(catalog_file).sheet_names:
        df = pd.read_excel(catalog_file, sheet_name=sheet,
                           dtype={"Classification": str, "Status": str})
        if df.empty:
            updated_dfs[sheet] = df
            continue

        statuses = {}
        updated = skipped = failed = 0

        print(f"\n{'='*60}")
        print(f"  TAB: {sheet}  |  Rows: {len(df)}")
        print(f"{'='*60}")

        for idx, row in df.iterrows():
            guid           = row.get("ColumnGUID", "")
            column         = row.get("Column", "")
            asset          = row.get("FullQualifiedName", "")
            classification = row.get("Classification", "")

            if pd.isna(classification) or str(classification).strip() == "":
                statuses[idx] = "Skipped"
                skipped += 1
                continue

            r = requests.post(
                f"{ENTITY_API}/{guid}/classifications",
                headers=headers,
                json=[{"typeName": str(classification).strip()}]
            )

            print(f"\n  ASSET : {asset}")
            print(f"  COL   : {column}  |  CLASS: {classification}")

            if r.status_code in [200, 204]:
                print("  STATUS: UPDATED [OK]")
                statuses[idx] = "Updated"
                updated += 1
            else:
                print(f"  STATUS: FAILED  -> {r.text}")
                statuses[idx] = "Failed"
                failed += 1

        for idx, status in statuses.items():
            df.at[idx, "Status"] = status
        updated_dfs[sheet] = df

        print(f"\n  Updated: {updated}  Skipped: {skipped}  Failed: {failed}")

    with pd.ExcelWriter(catalog_file, engine="openpyxl") as writer:
        for sheet, df in updated_dfs.items():
            df.to_excel(writer, sheet_name=sheet, index=False)
    print(f"\n  Excel updated -> {catalog_file}")

# ------------------------------------------------
# CDE MODE
# ------------------------------------------------

def apply_cde_classifications(cde_file, catalog_file):
    print("\n" + "="*60)
    print("  CDE AUTO-MATCH & CLASSIFY MODE")
    print(f"  CDE: {cde_file}  |  Catalog: {catalog_file}")
    print("="*60)

    cde_lookup = {}
    for tab in pd.ExcelFile(cde_file).sheet_names:
        df = pd.read_excel(cde_file, sheet_name=tab)
        df.columns = [c.strip().lower() for c in df.columns]
        if "column_name" not in df.columns or "classification" not in df.columns:
            print(f"  [WARN] Tab '{tab}' missing column_name/classification — skipped")
            continue
        mapping = {}
        for _, row in df.iterrows():
            col = row.get("column_name", "")
            cls = row.get("classification", "")
            if not pd.isna(col) and not pd.isna(cls):
                mapping[str(col).strip().lower()] = str(cls).strip()
        cde_lookup[tab.lower()] = mapping
        print(f"  CDE [{tab}] -> {len(mapping)} rules")

    if not cde_lookup:
        print("  [ERROR] No CDE rules found.")
        return

    updated_dfs   = {}
    grand_matched = grand_updated = grand_failed = grand_no_match = 0

    for sheet in pd.ExcelFile(catalog_file).sheet_names:
        df = pd.read_excel(catalog_file, sheet_name=sheet,
                           dtype={"Classification": str, "Status": str})
        if df.empty:
            updated_dfs[sheet] = df
            continue

        print(f"\n{'='*60}")
        print(f"  TAB: {sheet}  |  Rows: {len(df)}")
        print(f"{'='*60}")

        # Match catalog tab to CDE tab
        sl = sheet.lower()
        cde_tab = sl if sl in cde_lookup else next(
            (k for k in cde_lookup if k in sl or sl in k), None
        )

        if not cde_tab:
            print(f"  [SKIP] No CDE tab matched '{sheet}'")
            updated_dfs[sheet] = df
            grand_no_match += len(df)
            continue

        rules    = cde_lookup[cde_tab]
        statuses = {}
        matched = updated = failed = skipped = 0

        print(f"  CDE tab: [{cde_tab}]  ({len(rules)} rules)")
        print(f"{'─'*60}")

        for idx, row in df.iterrows():
            guid            = row.get("ColumnGUID", "")
            column          = row.get("Column", "")
            asset           = row.get("FullQualifiedName", "")
            existing_status = row.get("Status", "")

            if str(existing_status).strip() in ["Updated", "CDE-Applied"]:
                statuses[idx] = existing_status
                skipped += 1
                continue

            col_lower = str(column).strip().lower()
            if col_lower not in rules:
                statuses[idx] = str(existing_status) if not pd.isna(existing_status) else ""
                continue

            classification = rules[col_lower]
            matched += 1

            r = requests.post(
                f"{ENTITY_API}/{guid}/classifications",
                headers=headers,
                json=[{"typeName": classification}]
            )

            print(f"\n  ASSET  : {asset}")
            print(f"  COL    : {column}  |  CLASS: {classification}")

            if r.status_code in [200, 204]:
                print("  STATUS : CDE-Applied [OK]")
                statuses[idx]               = "CDE-Applied"
                df.at[idx, "Classification"] = classification
                updated += 1
            else:
                print(f"  STATUS : FAILED -> {r.text}")
                statuses[idx] = "Failed"
                failed += 1

        for idx, status in statuses.items():
            df.at[idx, "Status"] = status
        updated_dfs[sheet] = df
        grand_matched += matched
        grand_updated += updated
        grand_failed  += failed

        print(f"\n  Matched: {matched}  Applied: {updated}  Failed: {failed}  Skipped: {skipped}")

    with pd.ExcelWriter(catalog_file, engine="openpyxl") as writer:
        for sheet, df in updated_dfs.items():
            df.to_excel(writer, sheet_name=sheet, index=False)

    print(f"\n{'='*60}")
    print(f"  GRAND SUMMARY — Matched: {grand_matched}  Applied: {grand_updated}  Failed: {grand_failed}  No match: {grand_no_match}")
    print(f"  Excel updated -> {catalog_file}")
    print("="*60)

# ------------------------------------------------
# MAIN
# ------------------------------------------------

def main():
    args = sys.argv[1:]
    if len(args) == 0:
        scan_catalog()
    elif len(args) == 1:
        if not os.path.exists(args[0]):
            print(f"  [ERROR] File not found: {args[0]}")
            return
        apply_classifications(args[0])
    elif len(args) == 2:
        cde_file, catalog_file = args
        for f in [cde_file, catalog_file]:
            if not os.path.exists(f):
                print(f"  [ERROR] File not found: {f}")
                return
        apply_cde_classifications(cde_file, catalog_file)
    else:
        print(f"\n  Usage:")
        print(f"    python purview_classifier.py")
        print(f"    python purview_classifier.py {EXCEL_FILE}")
        print(f"    python purview_classifier.py {CDE_FILE} {EXCEL_FILE}")

if __name__ == "__main__":
    main()