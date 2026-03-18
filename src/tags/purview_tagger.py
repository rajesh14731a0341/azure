import os
import sys
import json
import time
import requests
import pandas as pd
import configparser
from collections import defaultdict

# ------------------------------------------------
# CONFIG — same credentials as purview_classifier.py
# ------------------------------------------------

CLIENT_ID       = "2c98dd46-5ec9-4198-b265-058e18078125"
CLIENT_SECRET   = os.environ.get("PURVIEW_CLIENT_SECRET", "")
TENANT_ID       = "5f9bacc0-ffe8-41f7-8d25-215d55cb0f96"
PURVIEW_ACCOUNT = "finastrapurview"

# ------------------------------------------------
# API ENDPOINTS
# ------------------------------------------------
# Tags in Purview are called "Labels" in the Atlas API
# They are free-text strings applied to any entity (table, column, asset)
#
# Classification API (your existing script):
#   POST /entity/guid/{guid}/classifications  → apply
#   DELETE /entity/guid/{guid}/classification/{typeName} → delete
#
# Labels/Tags API (this script):
#   POST /entity/guid/{guid}/labels           → SET labels (replaces all)
#   PUT  /entity/guid/{guid}/labels           → ADD labels (appends)
#   DELETE /entity/guid/{guid}/labels         → DELETE specific labels
#
# Key difference:
#   Classifications = predefined typeNames (MICROSOFT.PERSONAL.EMAIL etc)
#   Labels/Tags     = free text strings ("PII", "Finance", "Sensitive" etc)
# ------------------------------------------------

SEARCH_API       = f"https://{PURVIEW_ACCOUNT}.purview.azure.com/datamap/api/search/query?api-version=2023-09-01"
ENTITY_API       = f"https://{PURVIEW_ACCOUNT}.purview.azure.com/datamap/api/atlas/v2/entity/guid"
COLLECTIONS_API  = f"https://{PURVIEW_ACCOUNT}.purview.azure.com/account/collections?api-version=2019-11-01-preview"
DATASOURCES_API  = f"https://{PURVIEW_ACCOUNT}.purview.azure.com/scan/datasources?api-version=2022-07-01-preview"
BULK_ENTITY_API  = f"https://{PURVIEW_ACCOUNT}.purview.azure.com/datamap/api/atlas/v2/entity/bulk"
BATCH_SIZE       = 100

# ------------------------------------------------
# LOAD CONFIG FROM config.ini
# ------------------------------------------------

_cfg = configparser.ConfigParser()
_cfg.read(os.path.join(os.path.dirname(__file__), "tag_config.ini"))

if not _cfg.has_section("FILES"):
    print("  [ERROR] tag_config.ini not found or missing [FILES] section.")
    print("          Make sure tag_config.ini is in the same folder as purview_tagger.py")
    sys.exit(1)

TAG_EXCEL_FILE  = _cfg["FILES"]["TAG_EXCEL_FILE"]
TAG_ADD_FILE    = _cfg["FILES"]["TAG_ADD_FILE"]
TAG_DELETE_FILE = _cfg["FILES"]["TAG_DELETE_FILE"]

_col = _cfg["FILTERS"]["FILTER_COLLECTIONS"].strip()
_ds  = _cfg["FILTERS"]["FILTER_DATA_SOURCES"].strip()

FILTER_COLLECTIONS  = None if _col == "None" else [x.strip() for x in _col.split(",") if x.strip()]
FILTER_DATA_SOURCES = None if _ds  == "None" else [x.strip() for x in _ds.split(",")  if x.strip()]

# ------------------------------------------------
# RETRY & PERFORMANCE CONFIG
# ------------------------------------------------

SEARCH_PAGE_SIZE   = 1000
MAX_RETRIES        = 5
RETRY_BACKOFF      = 2
RETRY_STATUS_CODES = {429, 500, 502, 503, 504}
TOKEN_REFRESH_MINUTES = 50

# ------------------------------------------------
# HIERARCHY DETECTION — same as purview_classifier.py
# ------------------------------------------------

STRUCTURAL_OBJECT_TYPES = {
    "Process", "Column", "Schema", "Database", "Server",
    "Account", "Namespace", "Subscription", "Queue",
    "ResourceGroup", "Tenant", "Cluster", "Workspace",
}

STRUCTURAL_KINDS = {
    "schema", "server", "db", "database", "instance",
    "account", "container", "folder", "namespace", "service",
    "warehouse", "cluster", "catalog", "pipeline", "workspace",
    "location", "subscription", "resourcegroup", "tenant",
    "filesystem", "directory",
}

COSMOS_SYSTEM_FIELDS = {
    "_rid", "_self", "_etag", "_attachments", "_ts",
    "_lsn", "_metadata", "_docs", "_sprocs", "_triggers",
    "_udfs", "_conflicts"
}

_PREFIX_TO_TAB = {
    "mssql":          "SQL Server",
    "azure_sql":      "Azure SQL Database",
    "oracle":         "Oracle",
    "postgresql":     "PostgreSQL",
    "mysql":          "MySQL",
    "db2":            "DB2",
    "sap_hana":       "SAP HANA",
    "sap_ecc":        "SAP ECC",
    "snowflake":      "Snowflake",
    "databricks":     "Databricks",
    "azure_blob":     "Azure Blob Storage",
    "azure_adls":     "Azure Data Lake",
    "azure_datalake": "Azure Data Lake",
    "azure_synapse":  "Azure Synapse",
    "azure_cosmosdb": "Azure Cosmos DB",
    "azure_cosmos":   "Azure Cosmos DB",
    "teradata":       "Teradata",
    "amazon_rds":     "Amazon RDS",
    "amazon_s3":      "Amazon S3",
    "hive":           "Hive",
    "hdfs":           "HDFS",
}


def entity_type_to_tab(entity_type):
    et = entity_type.lower().strip()
    for prefix in sorted(_PREFIX_TO_TAB, key=len, reverse=True):
        if et.startswith(prefix):
            return _PREFIX_TO_TAB[prefix]
    parts = et.rsplit("_", 1)
    base  = parts[0] if len(parts) > 1 else et
    return base.replace("_", " ").title()


def is_column_bearing_asset(asset):
    obj_type    = asset.get("objectType", "")
    entity_type = asset.get("entityType", "").lower().strip()
    if obj_type in STRUCTURAL_OBJECT_TYPES:
        return False
    if not entity_type:
        return False
    last_segment = entity_type.rsplit("_", 1)[-1]
    if last_segment in STRUCTURAL_KINDS:
        return False
    return True

# ------------------------------------------------
# AUTH
# ------------------------------------------------

_token_value   = None
_token_fetched = 0.0


def get_token():
    url = f"https://login.microsoftonline.com/{TENANT_ID}/oauth2/token"
    r   = requests.post(url, data={
        "grant_type":    "client_credentials",
        "client_id":     CLIENT_ID,
        "client_secret": CLIENT_SECRET,
        "resource":      "https://purview.azure.net"
    }, timeout=30)
    r.raise_for_status()
    return r.json()["access_token"]


def get_headers():
    global _token_value, _token_fetched
    age = (time.time() - _token_fetched) / 60
    if _token_value is None or age >= TOKEN_REFRESH_MINUTES:
        print(f"  [AUTH] {'Refreshing' if _token_value else 'Fetching'} token...")
        _token_value   = get_token()
        _token_fetched = time.time()
    return {"Authorization": f"Bearer {_token_value}", "Content-Type": "application/json"}

# ------------------------------------------------
# HTTP WRAPPER
# ------------------------------------------------

def api_request(method, url, *, json_body=None, label=""):
    delay = RETRY_BACKOFF
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            hdrs = get_headers()
            if method == "GET":
                r = requests.get(url, headers=hdrs, timeout=60)
            elif method == "DELETE":
                r = requests.delete(url, headers=hdrs, json=json_body, timeout=60)
            elif method == "PUT":
                r = requests.put(url, headers=hdrs, json=json_body, timeout=60)
            else:
                r = requests.post(url, headers=hdrs, json=json_body, timeout=60)

            if r.status_code == 401:
                print(f"  [AUTH] 401 on {label or url} — refreshing token (attempt {attempt})")
                global _token_value
                _token_value = None
                continue

            if r.status_code in RETRY_STATUS_CODES:
                wait = delay * attempt
                print(f"  [RETRY] HTTP {r.status_code} on {label or url} — waiting {wait}s")
                time.sleep(wait)
                continue

            return r

        except (requests.exceptions.ConnectionError,
                requests.exceptions.Timeout,
                requests.exceptions.ChunkedEncodingError) as exc:
            wait = delay * attempt
            print(f"  [RETRY] Network error: {exc} — waiting {wait}s")
            time.sleep(wait)

    print(f"  [FAIL] Gave up after {MAX_RETRIES} attempts: {label or url}")
    return None

# ------------------------------------------------
# COLLECTION FILTER
# ------------------------------------------------

def fetch_all_collections():
    r = api_request("GET", COLLECTIONS_API, label="Collections API")
    if r is None or r.status_code != 200:
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
    collections = fetch_all_collections()
    if not collections:
        return []
    path_map = build_path_map(collections)
    print("\n  Purview collection hierarchy:")
    for cid, path in sorted(path_map.items(), key=lambda x: x[1]):
        print(f"    {path}  [{cid}]")
    resolved = []
    for wanted in filter_paths:
        wanted       = wanted.strip()
        is_full_path = "/" in wanted
        matches      = []
        for cid, path in path_map.items():
            if is_full_path:
                if path.lower() == wanted.lower():
                    matches.append((cid, path))
            else:
                leaf = path.split("/")[-1].strip()
                if leaf.lower() == wanted.lower():
                    matches.append((cid, path))
        if not matches:
            print(f"\n  [WARN] '{wanted}' not found.")
        elif len(matches) > 1 and not is_full_path:
            print(f"\n  [WARN] '{wanted}' ambiguous — {len(matches)} matches")
        else:
            cid, path = matches[0]
            resolved.append(cid)
            print(f"\n  Resolved: '{wanted}' -> '{path}'  [{cid}]")
    return resolved

# ------------------------------------------------
# DATA SOURCE FILTER
# ------------------------------------------------

_DS_ENDPOINT_CACHE = {}


def resolve_datasource_endpoints(all_assets):
    global _DS_ENDPOINT_CACHE
    if not FILTER_DATA_SOURCES or _DS_ENDPOINT_CACHE:
        return
    host_to_prefixes = defaultdict(set)
    for asset in all_assets:
        qn = asset.get("qualifiedName", "")
        if "://" in qn:
            parts = qn.split("/")
            if len(parts) >= 3:
                host_to_prefixes[parts[2].lower()].add("/".join(parts[:3]).lower())
    r = api_request("GET", DATASOURCES_API, label="Scan datasources API")
    if r is None or r.status_code != 200:
        return
    wanted_lower = {ds.strip().lower(): ds.strip() for ds in FILTER_DATA_SOURCES}
    print("\n  Resolving data source endpoints:")
    for ds in r.json().get("value", []):
        ds_name = ds.get("name", "").strip()
        if ds_name.lower() not in wanted_lower:
            continue
        kind  = ds.get("kind", "")
        props = ds.get("properties", {})
        raw   = (props.get("endpoint") or props.get("serverEndpoint") or
                 props.get("host") or props.get("serviceUrl") or "").strip().rstrip("/")
        if not raw:
            _DS_ENDPOINT_CACHE[ds_name.lower()] = ""
            continue
        if "://" in raw:
            endpoint = raw.lower()
        else:
            raw_lower = raw.lower().replace(",", ":")
            prefixes  = host_to_prefixes.get(raw_lower, set())
            if prefixes:
                kind_lower = kind.lower()
                endpoint   = next((p for p in prefixes if kind_lower[:4] in p), sorted(prefixes)[0])
            else:
                endpoint = raw_lower
        _DS_ENDPOINT_CACHE[ds_name.lower()] = endpoint
        print(f"    [{wanted_lower[ds_name.lower()]}]  ->  {endpoint}  [{kind}]")


def asset_matches_datasource(asset):
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
    print(f"  Page size: {SEARCH_PAGE_SIZE} assets per request")
    print("="*60)

    collection_ids = []
    if FILTER_COLLECTIONS:
        print(f"\n  Resolving collection paths: {FILTER_COLLECTIONS}")
        collection_ids = resolve_collection_ids(FILTER_COLLECTIONS)

    all_assets         = []
    continuation_token = None
    page               = 1
    consecutive_fails  = 0

    while True:
        body = {"keywords": "*", "limit": SEARCH_PAGE_SIZE}
        if collection_ids:
            body["filter"] = ({"collectionId": collection_ids[0]}
                if len(collection_ids) == 1
                else {"or": [{"collectionId": cid} for cid in collection_ids]})
        if continuation_token:
            body["continuationToken"] = continuation_token

        r = api_request("POST", SEARCH_API, json_body=body, label=f"Search page {page}")
        if r is None:
            consecutive_fails += 1
            if consecutive_fails >= 3:
                break
            break
        if r.status_code != 200:
            break

        consecutive_fails = 0
        data   = r.json()
        assets = data.get("value", [])
        all_assets.extend(assets)
        print(f"  Page {page:>3}: {len(assets):>5} assets  |  Running total: {len(all_assets)}")

        continuation_token = data.get("continuationToken")
        if not continuation_token or not assets:
            break
        page += 1

    print(f"\n  Total assets discovered -> {len(all_assets)}")
    return all_assets

# ------------------------------------------------
# FETCH ENTITY DETAILS
# ------------------------------------------------

def fetch_entity(guid, mini=False):
    url = f"{ENTITY_API}/{guid}?minExtInfo={'true' if mini else 'false'}"
    r   = api_request("GET", url, label=f"Entity {guid[:8]}...")
    if r is None or r.status_code != 200:
        return None
    try:
        return r.json()
    except Exception:
        return None


def fetch_entities_bulk(guids):
    results = {}
    for i in range(0, len(guids), BATCH_SIZE):
        batch  = guids[i:i + BATCH_SIZE]
        params = "&".join(f"guid={g}" for g in batch)
        url    = f"{BULK_ENTITY_API}?{params}&minExtInfo=true&ignoreRelationships=false"
        r      = api_request("GET", url, label=f"Bulk fetch {len(batch)} entities")
        if r is not None and r.status_code == 200:
            try:
                data     = r.json()
                entities = data.get("entities", [])
                referred = data.get("referredEntities", {})
                for ent in entities:
                    g = ent.get("guid")
                    if g:
                        results[g] = {"entity": ent, "referredEntities": referred}
                continue
            except Exception:
                pass
        for g in batch:
            single = fetch_entity(g)
            if single:
                results[g] = single
    return results


def extract_columns(entity_json):
    columns       = {}
    entity        = entity_json.get("entity", {})
    entity_type   = entity.get("typeName", "").lower()
    relationships = entity.get("relationshipAttributes", {})
    referred      = entity_json.get("referredEntities", {})

    def _inject_guid(g, obj):
        if obj.get("guid") != g:
            obj = dict(obj)
            obj["guid"] = g
        return obj

    for key in ["columns", "table_columns"]:
        for ref in relationships.get(key, []):
            g = ref.get("guid")
            if g in referred:
                columns[g] = _inject_guid(g, referred[g])

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
                columns[g] = _inject_guid(g, obj)

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
                        columns[g] = _inject_guid(g, obj)
                for ref in ts_rels.get("columns", []):
                    g = ref.get("guid")
                    if g and g in ts_referred and g not in columns:
                        columns[g] = _inject_guid(g, ts_referred[g])

    return list(columns.values())

# ------------------------------------------------
# SCAN MODE — scan all assets and export tags to Excel
# ------------------------------------------------

def scan_tags():
    """
    Scans all assets/columns and exports existing tags to Excel.
    Two sheets per datasource:
      - "<Source> - Tagged"    → columns that already have tags
      - "<Source> - Untagged"  → columns with no tags (ready for tagging)
    """
    print("\n  ACTIVE FILTERS")
    print(f"  Collections  : {FILTER_COLLECTIONS  if FILTER_COLLECTIONS  else 'ALL'}")
    print(f"  Data sources : {FILTER_DATA_SOURCES if FILTER_DATA_SOURCES else 'ALL'}")

    all_assets   = fetch_catalog()
    table_assets = [a for a in all_assets if is_column_bearing_asset(a)]

    resolve_datasource_endpoints(all_assets)
    if FILTER_DATA_SOURCES:
        before       = len(table_assets)
        table_assets = [a for a in table_assets if asset_matches_datasource(a)]
        print(f"\n  Data source filter: {before} -> {len(table_assets)} assets kept")
        if not table_assets:
            print(f"  [WARN] No assets matched.")
            return

    print(f"\n  Column-bearing assets to process -> {len(table_assets)}")

    def get_instance_key(asset):
        tab = entity_type_to_tab(asset.get("entityType", "unknown"))
        qn  = asset.get("qualifiedName", "")
        if "://" in qn:
            parts = qn.split("/")
            host  = "/".join(parts[:3])
        else:
            host = qn.split("/")[0] if "/" in qn else qn[:50]
        return (tab, host)

    instance_groups = defaultdict(list)
    for a in table_assets:
        instance_groups[get_instance_key(a)].append(a)

    print("\n" + "="*60)
    print(f"  STEP 2 — Fetching entity details & tags  (batch size: {BATCH_SIZE})")
    print("="*60)

    tagged_by_tab   = defaultdict(list)
    untagged_by_tab = defaultdict(list)

    all_guids     = [a.get("id") for a in table_assets if a.get("id")]
    guid_to_asset = {a.get("id"): a for a in table_assets if a.get("id")}

    print(f"\n  Bulk fetching ALL {len(all_guids)} entities...")
    entity_map = fetch_entities_bulk(all_guids)
    print(f"  Total fetched: {len(entity_map)} entities\n")

    for (tab_name, instance_host), instance_assets in sorted(instance_groups.items()):
        instance_guids = [a.get("id") for a in instance_assets if a.get("id")]

        print(f"\n{'='*60}")
        print(f"  DATA SOURCE  : {tab_name}")
        print(f"  INSTANCE     : {instance_host}")
        print(f"  Assets       : {len(instance_assets)}")
        print(f"{'='*60}")

        total_tagged   = 0
        total_untagged = 0

        for guid in instance_guids:
            entity_json = entity_map.get(guid)
            if not entity_json:
                print(f"  [WARN] No entity data for GUID {guid[:8]}... — skipping")
                continue

            asset          = guid_to_asset.get(guid, {})
            qualified_name = asset.get("qualifiedName", "")
            entity_type    = asset.get("entityType", "")
            collection_id  = asset.get("collectionId", "")

            columns = extract_columns(entity_json)
            if not columns:
                single = fetch_entity(guid, mini=True)
                if single:
                    columns = extract_columns(single)
            if not columns:
                print(f"\n  ASSET        : {qualified_name}")
                print(f"  COLLECTION   : {collection_id}  |  TYPE: {entity_type}")
                print(f"  COLS         : 0 — no column metadata in Purview")
                continue

            tagged_cols   = []
            untagged_cols = []

            # Fetch full details for each column to get labels
            # labels[] is NOT included in bulk/referredEntities — needs individual fetch
            col_guids   = [c.get("guid") for c in columns if c.get("guid")]
            col_details = {}
            if col_guids:
                col_entity_map = fetch_entities_bulk(col_guids)
                for cg, cj in col_entity_map.items():
                    ent = cj.get("entity", {})
                    col_details[cg] = ent.get("labels", [])

            for col in columns:
                attr     = col.get("attributes", {})
                col_name = attr.get("name")
                if not col_name or col_name in COSMOS_SYSTEM_FIELDS:
                    continue

                # Labels are on the column entity itself — fetched individually above
                col_guid = col.get("guid", "")
                tag_list = col_details.get(col_guid, []) or col.get("labels", [])

                row = {
                    "DataSource":        tab_name,
                    "EntityType":        entity_type,
                    "FullQualifiedName": qualified_name,
                    "Column":            col_name,
                    "ColumnGUID":        col.get("guid") or "",
                    "Tags":              ", ".join(tag_list) if tag_list else "",
                    "Status":            ""
                }

                if tag_list:
                    tagged_cols.append(col_name)
                    total_tagged += 1
                    tagged_by_tab[tab_name].append(row)
                else:
                    untagged_cols.append(col_name)
                    total_untagged += 1
                    untagged_by_tab[tab_name].append(row)

            total = len(tagged_cols) + len(untagged_cols)
            lines = [
                f"\n  ASSET        : {qualified_name}",
                f"  COLLECTION   : {collection_id}  |  TYPE: {entity_type}",
                f"  COLS         : {total} total | {len(tagged_cols)} tagged | {len(untagged_cols)} untagged",
            ]
            if tagged_cols:
                lines.append(f"  [+] Tagged   : {', '.join(tagged_cols)}")
            if untagged_cols:
                lines.append(f"  [-] Untagged : {', '.join(untagged_cols)}")
            print("\n".join(lines))

        print(f"\n  -- {tab_name} | {instance_host} SUMMARY --")
        print(f"     Tagged cols   : {total_tagged}")
        print(f"     Untagged cols : {total_untagged}")
        print(f"{'='*60}\n")

    # Write Excel
    PREFERRED_TAB_ORDER = [
        "SQL Server", "Azure SQL Database", "Oracle", "PostgreSQL",
        "MySQL", "DB2", "Snowflake", "Databricks", "Teradata",
        "SAP HANA", "SAP ECC", "Azure Blob Storage", "Azure Data Lake",
        "Azure Synapse", "Azure Cosmos DB", "Amazon RDS", "Amazon S3",
    ]
    all_tabs  = set(tagged_by_tab.keys()) | set(untagged_by_tab.keys())
    tab_order = [t for t in PREFERRED_TAB_ORDER if t in all_tabs]
    tab_order += sorted(all_tabs - set(tab_order))

    def sheet_name(tab, suffix):
        return f"{tab} - {suffix}"[:31]

    total_tagged_rows   = 0
    total_untagged_rows = 0

    with pd.ExcelWriter(TAG_EXCEL_FILE, engine="openpyxl") as writer:
        for tab in tab_order:
            u_rows = untagged_by_tab.get(tab, [])
            df_u   = pd.DataFrame(u_rows)
            if not df_u.empty:
                df_u = df_u.drop_duplicates(subset=["FullQualifiedName", "Column"])
            df_u.to_excel(writer, sheet_name=sheet_name(tab, "Untagged"), index=False)
            total_untagged_rows += len(df_u)

            t_rows = tagged_by_tab.get(tab, [])
            df_t   = pd.DataFrame(t_rows)
            if not df_t.empty:
                df_t = df_t.drop_duplicates(subset=["FullQualifiedName", "Column"])
            df_t.to_excel(writer, sheet_name=sheet_name(tab, "Tagged"), index=False)
            total_tagged_rows += len(df_t)

            print(f"  [{tab}]  Untagged: {len(df_u)}  |  Tagged: {len(df_t)}")

    print(f"\n{'='*60}")
    print(f"  Excel    -> {TAG_EXCEL_FILE}")
    print(f"             Untagged cols : {total_untagged_rows}  (fill Tags column then run APPLY)")
    print(f"             Tagged cols   : {total_tagged_rows}   (use for DELETE)")
    print("="*60)

# ------------------------------------------------
# APPLY TAGS MODE — reads tag_add.xlsx, applies tags to matching columns
# ------------------------------------------------
# tag_add.xlsx format (same as cde_add.xlsx):
#   Sheet name = data source tab name (e.g. "Oracle")
#   Columns    : column_name | tags
#   Example row: EMAIL_ADDRESS | PII, Sensitive
#
# Tags are comma-separated free text strings.
# Rules: up to 50 tags per entity, max 50 chars each, letters/numbers/-/_
# ------------------------------------------------

def apply_tags(tag_file, catalog_file):
    print("\n" + "="*60)
    print("  TAG APPLY MODE")
    print(f"  Rules   : {tag_file}")
    print(f"  Catalog : {catalog_file}")
    print("="*60)

    # Build lookup: { tab_lower -> { col_lower -> [tag1, tag2, ...] } }
    tag_lookup = {}
    for tab in pd.ExcelFile(tag_file).sheet_names:
        df = pd.read_excel(tag_file, sheet_name=tab)
        df.columns = [c.strip().lower() for c in df.columns]
        if "column_name" not in df.columns or "tags" not in df.columns:
            print(f"  [WARN] Tab '{tab}' missing column_name/tags columns — skipped")
            print(f"         Required columns: column_name | tags")
            continue
        mapping = {}
        for _, row in df.iterrows():
            col  = row.get("column_name", "")
            tags = row.get("tags", "")
            if not pd.isna(col) and not pd.isna(tags) and str(col).strip() and str(tags).strip():
                tag_list = [t.strip() for t in str(tags).split(",") if t.strip()]
                if tag_list:
                    mapping[str(col).strip().lower()] = tag_list
        tag_lookup[tab.lower()] = mapping
        print(f"  Tag rules [{tab}] -> {len(mapping)} column rules")

    if not tag_lookup:
        print("  [ERROR] No tag rules found.")
        return

    updated_dfs   = {}
    grand_matched = grand_applied = grand_failed = grand_no_match = 0

    all_sheets = pd.ExcelFile(catalog_file).sheet_names

    for sheet in all_sheets:
        df = pd.read_excel(catalog_file, sheet_name=sheet,
                           dtype={"Tags": str, "Status": str})

        # APPLY only on Untagged sheets
        if "- tagged" in sheet.lower():
            updated_dfs[sheet] = df
            continue
        if df.empty:
            updated_dfs[sheet] = df
            continue

        print(f"\n{'='*60}")
        print(f"  TAB: {sheet}  |  Rows: {len(df)}")
        print(f"{'='*60}")

        sl      = sheet.lower().replace(" - untagged", "").strip()
        tag_tab = sl if sl in tag_lookup else next(
            (k for k in tag_lookup if k in sl or sl in k), None
        )

        if not tag_tab:
            print(f"  [SKIP] No tag rules matched '{sheet}'")
            updated_dfs[sheet] = df
            grand_no_match += len(df)
            continue

        rules    = tag_lookup[tag_tab]
        statuses = {}
        matched = applied = failed = skipped = 0

        print(f"  Tag rules tab: [{tag_tab}]  ({len(rules)} rules)")
        print(f"{'─'*60}")

        for idx, row in df.iterrows():
            guid            = row.get("ColumnGUID", "")
            column          = row.get("Column", "")
            asset           = row.get("FullQualifiedName", "")
            existing_status = str(row.get("Status", "")).strip()

            if existing_status in ["Tag-Applied"]:
                statuses[idx] = existing_status
                skipped += 1
                continue

            col_lower = str(column).strip().lower()
            if col_lower not in rules:
                statuses[idx] = existing_status if existing_status else ""
                continue

            tag_list = rules[col_lower]
            matched += 1

            if not str(guid).strip() or str(guid).strip().lower() in ["", "nan", "none"]:
                print(f"\n  [SKIP] {column} — ColumnGUID empty (re-run scan)")
                statuses[idx] = "No GUID"
                continue

            print(f"\n  ASSET  : {asset}")
            print(f"  COL    : {column}  |  TAGS: {tag_list}")

            # ── TAG API ──────────────────────────────────────────────
            # PUT /entity/guid/{guid}/labels  → ADDS tags (keeps existing)
            # POST /entity/guid/{guid}/labels → SETS tags (replaces all)
            #
            # We use PUT here so existing tags on the column are preserved
            # and the new tags are ADDED on top.
            # ─────────────────────────────────────────────────────────
            url = f"{ENTITY_API}/{guid}/labels"
            r   = api_request("PUT", url, json_body=tag_list, label=f"Tag {column}")

            if r is not None and r.status_code in [200, 204]:
                print(f"  STATUS : Tag-Applied [OK]  {tag_list}")
                statuses[idx]       = "Tag-Applied"
                df.at[idx, "Tags"]  = ", ".join(tag_list)
                applied += 1
            else:
                err = r.text if r is not None else "No response"
                print(f"  STATUS : FAILED -> {err}")
                statuses[idx] = "Failed"
                failed += 1

        for idx, status in statuses.items():
            df.at[idx, "Status"] = status
        updated_dfs[sheet] = df
        grand_matched += matched
        grand_applied += applied
        grand_failed  += failed

        print(f"\n  Matched: {matched}  Applied: {applied}  Failed: {failed}  Skipped: {skipped}")

    with pd.ExcelWriter(catalog_file, engine="openpyxl") as writer:
        for sheet, df in updated_dfs.items():
            df.to_excel(writer, sheet_name=sheet, index=False)

    print(f"\n{'='*60}")
    print(f"  GRAND SUMMARY — Matched: {grand_matched}  Applied: {grand_applied}  Failed: {grand_failed}  No match: {grand_no_match}")
    print(f"  Excel updated -> {catalog_file}")
    print("="*60)

# ------------------------------------------------
# DELETE TAGS MODE — removes specific tags from columns
# ------------------------------------------------
# tag_delete.xlsx format (same as tag_add.xlsx):
#   Sheet name = data source tab name
#   Columns    : column_name | tags
#   Example row: EMAIL_ADDRESS | PII, Sensitive
# ------------------------------------------------

def delete_tags(tag_file, catalog_file):
    print("\n" + "="*60)
    print("  TAG DELETE MODE")
    print(f"  Rules   : {tag_file}")
    print(f"  Catalog : {catalog_file}")
    print("="*60)

    # Build delete lookup: { tab_lower -> { col_lower -> [tags_to_delete] } }
    delete_lookup = {}
    for tab in pd.ExcelFile(tag_file).sheet_names:
        df = pd.read_excel(tag_file, sheet_name=tab)
        df.columns = [c.strip().lower() for c in df.columns]
        if "column_name" not in df.columns or "tags" not in df.columns:
            print(f"  [WARN] Tab '{tab}' missing column_name/tags — skipped")
            continue
        mapping = {}
        for _, row in df.iterrows():
            col  = row.get("column_name", "")
            tags = row.get("tags", "")
            if not pd.isna(col) and not pd.isna(tags) and str(col).strip() and str(tags).strip():
                tag_list = [t.strip() for t in str(tags).split(",") if t.strip()]
                if tag_list:
                    mapping[str(col).strip().lower()] = tag_list
        delete_lookup[tab.lower()] = mapping
        print(f"  Delete rules [{tab}] -> {len(mapping)} column rules")

    if not delete_lookup:
        print("  [ERROR] No delete rules found.")
        return

    all_sheets  = pd.ExcelFile(catalog_file).sheet_names
    updated_dfs = {}
    grand_matched = grand_deleted = grand_failed = 0

    for sheet in all_sheets:
        df = pd.read_excel(catalog_file, sheet_name=sheet,
                           dtype={"Tags": str, "Status": str})

        # DELETE only on Tagged sheets
        if "- tagged" not in sheet.lower():
            updated_dfs[sheet] = df
            continue
        if df.empty:
            updated_dfs[sheet] = df
            continue

        print(f"\n{'='*60}")
        print(f"  TAB: {sheet}  |  Rows: {len(df)}")
        print(f"{'='*60}")

        sl      = sheet.lower().replace(" - tagged", "").strip()
        del_tab = sl if sl in delete_lookup else next(
            (k for k in delete_lookup if k in sl or sl in k), None
        )

        if not del_tab:
            print(f"  [SKIP] No delete rules matched '{sheet}'")
            updated_dfs[sheet] = df
            continue

        rules    = delete_lookup[del_tab]
        statuses = {}
        matched = deleted = failed = skipped = 0

        print(f"  Delete rules tab: [{del_tab}]  ({len(rules)} rules)")
        print(f"{'─'*60}")

        for idx, row in df.iterrows():
            guid            = row.get("ColumnGUID", "")
            column          = row.get("Column", "")
            asset           = row.get("FullQualifiedName", "")
            existing_status = str(row.get("Status", "")).strip()
            existing_tags   = str(row.get("Tags", "")).strip()

            if existing_status == "Tag-Deleted":
                statuses[idx] = existing_status
                skipped += 1
                continue

            col_lower = str(column).strip().lower()
            if col_lower not in rules:
                statuses[idx] = existing_status
                continue

            tags_to_delete = rules[col_lower]
            matched += 1
            grand_matched += 1

            if not str(guid).strip() or str(guid).strip().lower() in ["", "nan", "none"]:
                print(f"\n  [SKIP] {column} — ColumnGUID empty (re-run scan)")
                statuses[idx] = "No GUID"
                continue

            print(f"\n  ASSET  : {asset}")
            print(f"  COL    : {column}  |  DELETE TAGS: {tags_to_delete}")

            # ── DELETE TAG API ────────────────────────────────────────
            # DELETE /entity/guid/{guid}/labels
            # Body: ["tag1", "tag2"]   → removes only these specific tags
            # Existing tags NOT in the list are preserved
            # ─────────────────────────────────────────────────────────
            url = f"{ENTITY_API}/{guid}/labels"
            r   = api_request("DELETE", url, json_body=tags_to_delete,
                              label=f"Delete tags {column}")

            if r is not None and r.status_code in [200, 204]:
                print(f"  STATUS : Tag-Deleted [OK]  {tags_to_delete}")
                statuses[idx]      = "Tag-Deleted"
                # Remove deleted tags from the Tags column in Excel
                remaining = [t for t in existing_tags.split(", ")
                             if t and t not in tags_to_delete]
                df.at[idx, "Tags"] = ", ".join(remaining)
                deleted += 1
                grand_deleted += 1
            else:
                err = r.text if r is not None else "No response"
                print(f"  STATUS : FAILED -> {err}")
                statuses[idx] = "Failed"
                failed += 1
                grand_failed += 1

        for idx, status in statuses.items():
            df.at[idx, "Status"] = status
        updated_dfs[sheet] = df

        print(f"\n  Matched: {matched}  Deleted: {deleted}  Failed: {failed}  Skipped: {skipped}")

    with pd.ExcelWriter(catalog_file, engine="openpyxl") as writer:
        for sheet, df in updated_dfs.items():
            df.to_excel(writer, sheet_name=sheet, index=False)

    print(f"\n{'='*60}")
    print(f"  GRAND SUMMARY — Matched: {grand_matched}  Deleted: {grand_deleted}  Failed: {grand_failed}")
    print(f"  Excel updated -> {catalog_file}")
    print("="*60)

# ------------------------------------------------
# MAIN
# ------------------------------------------------
#
# RUN MODES:
#   python purview_tagger.py
#     -> Scan all assets, export tags to Excel (purview_tags.xlsx)
#
#   python purview_tagger.py tag_add.xlsx purview_tags.xlsx
#     -> Apply tags from tag_add.xlsx to matching columns
#
#   python purview_tagger.py tag_delete.xlsx purview_tags.xlsx
#     -> Delete tags from tag_delete.xlsx from matching columns
#
#   python purview_tagger.py --cleanup-only
#     -> Clear tag input files (tag_add.xlsx, tag_delete.xlsx)
#
# tag_add.xlsx / tag_delete.xlsx FORMAT:
#   Sheet name : data source name (e.g. "Oracle", "SQL Server")
#   Columns    : column_name | tags
#   Example    : EMAIL_ADDRESS | PII, Sensitive, GDPR
# ------------------------------------------------

def main():
    args = sys.argv[1:]

    # No args — scan and export tags
    if len(args) == 0:
        scan_tags()
        return

    # 2 args — apply or delete, auto-detected by filename
    if len(args) == 2:
        rules_file, catalog_file = args

        if not os.path.exists(rules_file):
            print(f"  [ERROR] Rules file not found: {rules_file}")
            return
        if not os.path.exists(catalog_file):
            print(f"  [ERROR] Catalog file not found: {catalog_file}")
            print(f"          Run 'python purview_tagger.py' first to generate it.")
            return

        rules_basename  = os.path.basename(rules_file).lower()
        delete_basename = os.path.basename(TAG_DELETE_FILE).lower()
        add_basename    = os.path.basename(TAG_ADD_FILE).lower()

        if rules_basename == delete_basename:
            print(f"  [MODE] DELETE TAGS — matched TAG_DELETE_FILE: {TAG_DELETE_FILE}")
            delete_tags(rules_file, catalog_file)
        elif rules_basename == add_basename:
            print(f"  [MODE] APPLY TAGS — matched TAG_ADD_FILE: {TAG_ADD_FILE}")
            apply_tags(rules_file, catalog_file)
        else:
            print(f"  [WARN] '{rules_file}' does not match TAG_ADD_FILE or TAG_DELETE_FILE.")
            print(f"         TAG_ADD_FILE    = '{TAG_ADD_FILE}'")
            print(f"         TAG_DELETE_FILE = '{TAG_DELETE_FILE}'")
            print(f"  Defaulting to APPLY TAGS mode.")
            apply_tags(rules_file, catalog_file)
        return

    print(f"\n  Usage:")
    print(f"    python purview_tagger.py")
    print(f"      -> Scan: export all column tags to {TAG_EXCEL_FILE}")
    print(f"")
    print(f"    python purview_tagger.py {TAG_ADD_FILE} {TAG_EXCEL_FILE}")
    print(f"      -> Apply: add tags to columns based on rules")
    print(f"")
    print(f"    python purview_tagger.py {TAG_DELETE_FILE} {TAG_EXCEL_FILE}")
    print(f"      -> Delete: remove tags from columns based on rules")


if __name__ == "__main__":
    main()