import os
import sys
import time
import requests
import pandas as pd
import configparser
from collections import defaultdict

# ------------------------------------------------
# CONFIG — credentials
# ------------------------------------------------

CLIENT_ID       = "2c98dd46-5ec9-4198-b265-058e18078125"
CLIENT_SECRET   = "Ijh8Q~s8DdTg~PNkeiFR.Al~rjAPMBb5eovbqcbY"
TENANT_ID       = "5f9bacc0-ffe8-41f7-8d25-215d55cb0f96"
PURVIEW_ACCOUNT = "finastrapurview"

SEARCH_API      = f"https://{PURVIEW_ACCOUNT}.purview.azure.com/datamap/api/search/query?api-version=2023-09-01"
ENTITY_API      = f"https://{PURVIEW_ACCOUNT}.purview.azure.com/datamap/api/atlas/v2/entity/guid"
COLLECTIONS_API = f"https://{PURVIEW_ACCOUNT}.purview.azure.com/account/collections?api-version=2019-11-01-preview"
DATASOURCES_API = f"https://{PURVIEW_ACCOUNT}.purview.azure.com/scan/datasources?api-version=2022-07-01-preview"
BULK_ENTITY_API = f"https://{PURVIEW_ACCOUNT}.purview.azure.com/datamap/api/atlas/v2/entity/bulk"
BATCH_SIZE      = 100

# ------------------------------------------------
# LOAD CONFIG FROM tag_config.ini
# ------------------------------------------------

_cfg = configparser.ConfigParser()
_cfg.read(os.path.join(os.path.dirname(__file__), "tag_config.ini"))

if not _cfg.has_section("FILES"):
    print("  [ERROR] tag_config.ini not found or missing [FILES] section.")
    sys.exit(1)

TAG_EXCEL_FILE  = _cfg["FILES"]["TAG_EXCEL_FILE"]    # purview_tags.xlsx — scan output (runtime generated)
TAG_PARAMS_FILE = _cfg["FILES"]["TAG_PARAMS_FILE"]   # tag_params.xlsx   — parameter file (user fills in)

_col = _cfg["FILTERS"]["FILTER_COLLECTIONS"].strip()
_ds  = _cfg["FILTERS"]["FILTER_DATA_SOURCES"].strip()

FILTER_COLLECTIONS  = None if _col == "None" else [x.strip() for x in _col.split(",") if x.strip()]
FILTER_DATA_SOURCES = None if _ds  == "None" else [x.strip() for x in _ds.split(",")  if x.strip()]

# ------------------------------------------------
# RETRY CONFIG
# ------------------------------------------------

SEARCH_PAGE_SIZE      = 1000
MAX_RETRIES           = 5
RETRY_BACKOFF         = 2
RETRY_STATUS_CODES    = {429, 500, 502, 503, 504}
TOKEN_REFRESH_MINUTES = 50

# ------------------------------------------------
# HIERARCHY DETECTION
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
    if entity_type.rsplit("_", 1)[-1] in STRUCTURAL_KINDS:
        return False
    return True

# ------------------------------------------------
# AUTH
# ------------------------------------------------

_token_value   = None
_token_fetched = 0.0


def get_token():
    r = requests.post(
        f"https://login.microsoftonline.com/{TENANT_ID}/oauth2/token",
        data={"grant_type": "client_credentials", "client_id": CLIENT_ID,
              "client_secret": CLIENT_SECRET, "resource": "https://purview.azure.net"},
        timeout=30
    )
    r.raise_for_status()
    return r.json()["access_token"]


def get_headers():
    global _token_value, _token_fetched
    if _token_value is None or (time.time() - _token_fetched) / 60 >= TOKEN_REFRESH_MINUTES:
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
                global _token_value
                _token_value = None
                continue
            if r.status_code in RETRY_STATUS_CODES:
                time.sleep(delay * attempt)
                continue
            return r

        except (requests.exceptions.ConnectionError,
                requests.exceptions.Timeout,
                requests.exceptions.ChunkedEncodingError) as exc:
            print(f"  [RETRY] {exc} — waiting {delay * attempt}s")
            time.sleep(delay * attempt)

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
            endpoint  = next((p for p in prefixes if kind.lower()[:4] in p),
                             sorted(prefixes)[0]) if prefixes else raw_lower
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

    all_assets, continuation_token, page, consecutive_fails = [], None, 1, 0

    while True:
        body = {"keywords": "*", "limit": SEARCH_PAGE_SIZE}
        if collection_ids:
            body["filter"] = ({"collectionId": collection_ids[0]} if len(collection_ids) == 1
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
# HELPER: extract table name from qualifiedName
# oracle://host/SCHEMA/TABLE → TABLE
# ------------------------------------------------

def extract_table_name(fqn):
    fqn = str(fqn).strip()
    if "/" in fqn:
        return fqn.split("/")[-1].lower()
    if "." in fqn:
        return fqn.split(".")[-1].lower()
    return fqn.lower()

# ------------------------------------------------
# HELPER: check if params file has any data
# ------------------------------------------------

def params_file_has_changes(params_file):
    """
    Returns True if tag_params.xlsx has at least one row with
    add_tags or delete_tags filled in.
    Returns False if all sheets are empty — normal scan-only run.
    """
    if not os.path.exists(params_file):
        return False
    try:
        for sheet in pd.ExcelFile(params_file).sheet_names:
            df = pd.read_excel(params_file, sheet_name=sheet,
                               dtype={"add_tags": str, "delete_tags": str})
            if df.empty:
                continue
            for _, row in df.iterrows():
                add_t = str(row.get("add_tags", "")).strip()
                del_t = str(row.get("delete_tags", "")).strip()
                if add_t and add_t.lower() not in ("nan", "none", ""):
                    return True
                if del_t and del_t.lower() not in ("nan", "none", ""):
                    return True
    except Exception as e:
        print(f"  [WARN] Could not read params file: {e}")
    return False

# ------------------------------------------------
# SCAN MODE
# Scans Purview, builds purview_tags.xlsx
# Also merges existing tag_params.xlsx rows in if file exists
# so user can see current tags + their param columns side by side
# ------------------------------------------------

def scan_tags():
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
        host = "/".join(qn.split("/")[:3]) if "://" in qn else (qn.split("/")[0] if "/" in qn else qn[:50])
        return (tab, host)

    instance_groups = defaultdict(list)
    for a in table_assets:
        instance_groups[get_instance_key(a)].append(a)

    print("\n" + "="*60)
    print(f"  STEP 2 — Fetching entity details & tags  (batch: {BATCH_SIZE})")
    print("="*60)

    rows_by_tab = defaultdict(list)

    all_guids     = [a.get("id") for a in table_assets if a.get("id")]
    guid_to_asset = {a.get("id"): a for a in table_assets if a.get("id")}

    print(f"\n  Bulk fetching ALL {len(all_guids)} entities...")
    entity_map = fetch_entities_bulk(all_guids)
    print(f"  Total fetched: {len(entity_map)} entities\n")

    for (tab_name, instance_host), instance_assets in sorted(instance_groups.items()):
        print(f"\n{'='*60}")
        print(f"  DATA SOURCE : {tab_name}  |  INSTANCE: {instance_host}")
        print(f"{'='*60}")

        for guid in [a.get("id") for a in instance_assets if a.get("id")]:
            entity_json = entity_map.get(guid)
            if not entity_json:
                continue

            asset          = guid_to_asset.get(guid, {})
            qualified_name = asset.get("qualifiedName", "")
            entity_type    = asset.get("entityType", "")
            collection_id  = asset.get("collectionId", "")
            table_name     = extract_table_name(qualified_name)

            columns = extract_columns(entity_json)
            if not columns:
                single = fetch_entity(guid, mini=True)
                if single:
                    columns = extract_columns(single)
            if not columns:
                print(f"\n  ASSET: {qualified_name}  COLS: 0 — no metadata")
                continue

            # Fetch labels per column
            col_guids      = [c.get("guid") for c in columns if c.get("guid")]
            col_details    = {}
            if col_guids:
                col_entity_map = fetch_entities_bulk(col_guids)
                for cg, cj in col_entity_map.items():
                    col_details[cg] = cj.get("entity", {}).get("labels", [])

            tagged_cols   = []
            untagged_cols = []

            for col in columns:
                attr     = col.get("attributes", {})
                col_name = attr.get("name")
                if not col_name or col_name in COSMOS_SYSTEM_FIELDS:
                    continue

                col_guid = col.get("guid", "")
                tag_list = col_details.get(col_guid, []) or col.get("labels", [])

                # ── OUTPUT ROW — the runtime Excel has these columns ──
                # DataSource | EntityType | FullQualifiedName | TableName
                # Column | ColumnGUID | CurrentTags | add_tags | delete_tags | Status
                # ──────────────────────────────────────────────────────
                row = {
                    "DataSource":        tab_name,
                    "EntityType":        entity_type,
                    "FullQualifiedName": qualified_name,
                    "TableName":         table_name,
                    "Column":            col_name,
                    "ColumnGUID":        col_guid or "",
                    "CurrentTags":       ", ".join(tag_list) if tag_list else "",
                    "add_tags":          "",   # user fills this in params file
                    "delete_tags":       "",   # user fills this in params file
                    "Status":            ""
                }
                rows_by_tab[tab_name].append(row)

                if tag_list:
                    tagged_cols.append(col_name)
                else:
                    untagged_cols.append(col_name)

            total = len(tagged_cols) + len(untagged_cols)
            lines = [f"\n  ASSET: {qualified_name}",
                     f"  COLS : {total} total | {len(tagged_cols)} tagged | {len(untagged_cols)} untagged"]
            if tagged_cols:
                lines.append(f"  [+] Tagged   : {', '.join(tagged_cols)}")
            if untagged_cols:
                lines.append(f"  [-] Untagged : {', '.join(untagged_cols)}")
            print("\n".join(lines))

    # ── Write purview_tags.xlsx (runtime output) ──────────────────
    PREFERRED_TAB_ORDER = [
        "SQL Server", "Azure SQL Database", "Oracle", "PostgreSQL",
        "MySQL", "DB2", "Snowflake", "Databricks", "Teradata",
        "SAP HANA", "SAP ECC", "Azure Blob Storage", "Azure Data Lake",
        "Azure Synapse", "Azure Cosmos DB", "Amazon RDS", "Amazon S3",
    ]
    all_tabs  = set(rows_by_tab.keys())
    tab_order = [t for t in PREFERRED_TAB_ORDER if t in all_tabs]
    tab_order += sorted(all_tabs - set(tab_order))

    COLS = ["DataSource", "EntityType", "FullQualifiedName", "TableName",
            "Column", "ColumnGUID", "CurrentTags", "add_tags", "delete_tags", "Status"]

    total_rows = 0
    with pd.ExcelWriter(TAG_EXCEL_FILE, engine="openpyxl") as writer:
        for tab in tab_order:
            rows  = rows_by_tab.get(tab, [])
            df    = pd.DataFrame(rows, columns=COLS)
            if not df.empty:
                df = df.drop_duplicates(subset=["FullQualifiedName", "Column"])
            df.to_excel(writer, sheet_name=tab[:31], index=False)
            total_rows += len(df)
            tagged_count   = df["CurrentTags"].apply(lambda x: bool(str(x).strip()) and str(x).strip().lower() not in ("nan","none","")).sum() if not df.empty else 0
            untagged_count = len(df) - tagged_count
            print(f"  [{tab}]  Total: {len(df)}  |  Tagged: {tagged_count}  |  Untagged: {untagged_count}")

    print(f"\n{'='*60}")
    print(f"  Runtime Excel  -> {TAG_EXCEL_FILE}  ({total_rows} rows)")
    print(f"  Download from artifacts, fill add_tags / delete_tags columns")
    print(f"  Save as {TAG_PARAMS_FILE} and commit to trigger apply/delete")
    print("="*60)

# ------------------------------------------------
# APPLY & DELETE — reads tag_params.xlsx
# Processes both add_tags and delete_tags in one pass
# Matches on TableName + Column (exact table) or just Column (all tables)
# ------------------------------------------------

def process_params(params_file, catalog_file):
    print("\n" + "="*60)
    print("  TAG APPLY & DELETE MODE")
    print(f"  Params  : {params_file}")
    print(f"  Catalog : {catalog_file}")
    print("="*60)

    # Build lookup from params file
    # Key = (fqn_lower, col_lower) — exact match using FullQualifiedName + Column
    # GUID used directly from params file for API calls
    params_lookup = {}

    for tab in pd.ExcelFile(params_file).sheet_names:
        df = pd.read_excel(params_file, sheet_name=tab,
                           dtype={"add_tags": str, "delete_tags": str,
                                  "FullQualifiedName": str, "ColumnGUID": str})
        df.columns = [c.strip().lower() for c in df.columns]

        required = {"fullqualifiedname", "column", "columnguid"}
        if not required.issubset(set(df.columns)):
            print(f"  [WARN] Tab '{tab}' missing required columns (FullQualifiedName/Column/ColumnGUID) — skipped")
            continue

        mapping = {}
        for _, row in df.iterrows():
            fqn   = str(row.get("fullqualifiedname", "")).strip()
            col   = str(row.get("column", "")).strip()
            guid  = str(row.get("columnguid", "")).strip()
            add_t = str(row.get("add_tags", "")).strip()
            del_t = str(row.get("delete_tags", "")).strip()

            if not fqn or fqn.lower() in ("nan","none","") or not col or col.lower() in ("nan","none",""):
                continue

            add_list = [t.strip() for t in add_t.split(",") if t.strip() and add_t.lower() not in ("nan","none","")]
            del_list = [t.strip() for t in del_t.split(",") if t.strip() and del_t.lower() not in ("nan","none","")]

            if not add_list and not del_list:
                continue

            mapping[(fqn.lower(), col.lower())] = {"guid": guid, "add": add_list, "delete": del_list}

        params_lookup[tab.lower()] = mapping

        add_count = sum(1 for v in mapping.values() if v["add"])
        del_count = sum(1 for v in mapping.values() if v["delete"])
        print(f"  Params [{tab}] -> {len(mapping)} rules  ({add_count} add, {del_count} delete)")
        for (fqn, col), ops in mapping.items():
            print(f"    fqn={fqn} | col={col} | add={ops['add']} | delete={ops['delete']}")

    if not params_lookup:
        print("  [ERROR] No rules found in params file.")
        return

    # Process catalog Excel
    all_sheets  = pd.ExcelFile(catalog_file).sheet_names
    updated_dfs = {}
    grand_add_ok = grand_del_ok = grand_failed = grand_no_match = 0

    for sheet in all_sheets:
        df = pd.read_excel(catalog_file, sheet_name=sheet,
                           dtype={"add_tags": str, "delete_tags": str, "Status": str})
        if df.empty:
            updated_dfs[sheet] = df
            continue

        print(f"\n{'='*60}")
        print(f"  TAB: {sheet}  |  Rows: {len(df)}")
        print(f"{'='*60}")

        sl         = sheet.lower().strip()
        params_tab = sl if sl in params_lookup else next(
            (k for k in params_lookup if k in sl or sl in k), None
        )

        if not params_tab:
            print(f"  [SKIP] No params matched '{sheet}'")
            updated_dfs[sheet] = df
            grand_no_match += len(df)
            continue

        rules    = params_lookup[params_tab]
        statuses = {}
        add_ok   = del_ok = failed = skipped = 0

        for idx, row in df.iterrows():
            column          = str(row.get("Column", "")).strip()
            fqn             = str(row.get("FullQualifiedName", "")).strip()
            existing_status = str(row.get("Status", "")).strip()

            if existing_status in ["Tag-Applied", "Tag-Deleted", "Tag-Updated"]:
                statuses[idx] = existing_status
                skipped += 1
                continue

            # Match on exact FullQualifiedName + Column
            key = (fqn.lower(), column.lower())
            if key not in rules:
                statuses[idx] = existing_status if existing_status else ""
                continue

            ops  = rules[key]
            # Use GUID from params file directly — no lookup needed
            guid = ops["guid"]

            if not guid or guid.lower() in ("", "nan", "none"):
                print(f"\n  [SKIP] {column} in {fqn} — ColumnGUID empty")
                statuses[idx] = "No GUID"
                continue

            print(f"\n  ASSET  : {fqn}")
            print(f"  COL    : {column}  |  ADD={ops['add']}  DELETE={ops['delete']}")

            col_status = existing_status or ""

            # ── ADD TAGS ───────────────────────────────────────────
            if ops["add"]:
                r = api_request("PUT", f"{ENTITY_API}/{guid}/labels",
                                json_body=ops["add"], label=f"Add tags {column}")
                if r is not None and r.status_code in [200, 204]:
                    print(f"  ADD    : OK  {ops['add']}")
                    df.at[idx, "add_tags"] = ", ".join(ops["add"])
                    add_ok += 1
                    col_status = "Tag-Applied"
                else:
                    err = r.text if r else "No response"
                    print(f"  ADD    : FAILED -> {err}")
                    col_status = "Failed"
                    failed += 1

            # ── DELETE TAGS ────────────────────────────────────────
            if ops["delete"]:
                r = api_request("DELETE", f"{ENTITY_API}/{guid}/labels",
                                json_body=ops["delete"], label=f"Del tags {column}")
                if r is not None and r.status_code in [200, 204]:
                    print(f"  DELETE : OK  {ops['delete']}")
                    df.at[idx, "delete_tags"] = ", ".join(ops["delete"])
                    del_ok += 1
                    col_status = "Tag-Updated" if ops["add"] else "Tag-Deleted"
                else:
                    err = r.text if r else "No response"
                    print(f"  DELETE : FAILED -> {err}")
                    col_status = "Failed"
                    failed += 1

            statuses[idx] = col_status

        for idx, status in statuses.items():
            df.at[idx, "Status"] = status
        updated_dfs[sheet] = df
        grand_add_ok += add_ok
        grand_del_ok += del_ok
        grand_failed += failed

        print(f"\n  Added: {add_ok}  Deleted: {del_ok}  Failed: {failed}  Skipped: {skipped}")

    with pd.ExcelWriter(catalog_file, engine="openpyxl") as writer:
        for sheet, df in updated_dfs.items():
            df.to_excel(writer, sheet_name=sheet, index=False)

    print(f"\n{'='*60}")
    print(f"  GRAND SUMMARY")
    print(f"  Tags added    : {grand_add_ok}")
    print(f"  Tags deleted  : {grand_del_ok}")
    print(f"  Failed        : {grand_failed}")
    print(f"  No match      : {grand_no_match}")
    print(f"  Excel updated : {catalog_file}")
    print("="*60)

# ------------------------------------------------
# MAIN
# ------------------------------------------------
#
# RUN MODES:
#
#   python purview_tagger.py
#     -> Scan Purview, generate purview_tags.xlsx (runtime output)
#     -> If tag_params.xlsx has data → also apply/delete tags
#     -> If tag_params.xlsx is empty  → scan only (no changes to Purview)
#
# WORKFLOW:
#   1. First run  → scan generates purview_tags.xlsx
#   2. Download purview_tags.xlsx from artifacts
#   3. Fill in add_tags / delete_tags columns for specific rows
#   4. Save as tag_params.xlsx and commit
#   5. Next run   → scan + apply/delete based on tag_params.xlsx
# ------------------------------------------------

def main():
    args = sys.argv[1:]

    print("="*60)
    print("  Purview Tagger")
    print(f"  Account       : {PURVIEW_ACCOUNT}")
    print(f"  Runtime Excel : {TAG_EXCEL_FILE}")
    print(f"  Params Excel  : {TAG_PARAMS_FILE}")
    print("="*60)

    # ── python purview_tagger.py tag_params.xlsx purview_tags.xlsx ──
    # 2 args passed → skip scan, go straight to apply/delete
    # No need to re-scan Purview when you already have the catalog
    if len(args) == 2:
        params_file  = args[0]
        catalog_file = args[1]
        if not os.path.exists(params_file):
            print(f"  [ERROR] Params file not found: {params_file}")
            return
        if not os.path.exists(catalog_file):
            print(f"  [ERROR] Catalog file not found: {catalog_file}")
            print(f"          Run scan first: python purview_tagger.py")
            return
        print(f"  [MODE] APPLY/DELETE ONLY — using existing catalog")
        print(f"  Params  : {params_file}")
        print(f"  Catalog : {catalog_file}")
        process_params(params_file, catalog_file)
        return

    # ── python purview_tagger.py ────────────────────────────────────
    # No args → SCAN ONLY — never auto-applies
    # To apply tags you must explicitly pass the files:
    #   python purview_tagger.py tag_params.xlsx purview_tags.xlsx
    scan_tags()
    print(f"\n  Scan complete.")
    print(f"  To apply/delete tags run:")
    print(f"    python purview_tagger.py {TAG_PARAMS_FILE} {TAG_EXCEL_FILE}")


if __name__ == "__main__":
    main()