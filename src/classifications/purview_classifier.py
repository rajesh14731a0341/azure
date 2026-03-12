import os
import sys
import json
import time
import requests
import pandas as pd
import configparser
from collections import defaultdict

# ------------------------------------------------
# CONFIG — credentials (secret from GitHub Actions)
# ------------------------------------------------

CLIENT_ID       = "2c98dd46-5ec9-4198-b265-058e18078125"
CLIENT_SECRET   = os.environ.get("PURVIEW_CLIENT_SECRET", "")
TENANT_ID       = "5f9bacc0-ffe8-41f7-8d25-215d55cb0f96"
PURVIEW_ACCOUNT = "finastrapurview"

SEARCH_API       = f"https://{PURVIEW_ACCOUNT}.purview.azure.com/datamap/api/search/query?api-version=2023-09-01"
ENTITY_API       = f"https://{PURVIEW_ACCOUNT}.purview.azure.com/datamap/api/atlas/v2/entity/guid"
COLLECTIONS_API  = f"https://{PURVIEW_ACCOUNT}.purview.azure.com/account/collections?api-version=2019-11-01-preview"
DATASOURCES_API  = f"https://{PURVIEW_ACCOUNT}.purview.azure.com/scan/datasources?api-version=2022-07-01-preview"
BULK_ENTITY_API  = f"https://{PURVIEW_ACCOUNT}.purview.azure.com/datamap/api/atlas/v2/entity/bulk"
BATCH_SIZE       = 100  # entities per bulk fetch

# ------------------------------------------------
# LOAD CONFIG FROM config.ini
# ------------------------------------------------
# All file names and filters are read from config.ini
# Edit config.ini to change settings — no need to touch this file
# ------------------------------------------------

_cfg = configparser.ConfigParser()
_cfg.read(os.path.join(os.path.dirname(__file__), "config.ini"))

EXCEL_FILE        = _cfg["FILES"]["EXCEL_FILE"]
RAW_CATALOG_FILE  = _cfg["FILES"]["RAW_CATALOG_FILE"]
CATALOG_SNAPSHOT  = _cfg["FILES"]["CATALOG_SNAPSHOT"]
CDE_FILE          = _cfg["FILES"]["CDE_FILE"]
CDE_DELETE_FILE   = _cfg["FILES"]["CDE_DELETE_FILE"]

_col = _cfg["FILTERS"]["FILTER_COLLECTIONS"].strip()
_ds  = _cfg["FILTERS"]["FILTER_DATA_SOURCES"].strip()

FILTER_COLLECTIONS  = None if _col == "None" else [x.strip() for x in _col.split(",") if x.strip()]
FILTER_DATA_SOURCES = None if _ds  == "None" else [x.strip() for x in _ds.split(",")  if x.strip()]

# ------------------------------------------------
# RETRY & PERFORMANCE CONFIG
# ------------------------------------------------

# Max assets per search page (Purview API max is 1000)
SEARCH_PAGE_SIZE  = 1000

# Retry settings — applies to every API call
MAX_RETRIES       = 5          # total attempts before giving up
RETRY_BACKOFF     = 2          # seconds — doubles each attempt (2, 4, 8, 16, 32)
RETRY_STATUS_CODES = {429, 500, 502, 503, 504}  # retry on these HTTP codes

# Token refresh — re-fetch token after this many minutes to avoid expiry mid-run
TOKEN_REFRESH_MINUTES = 50

# ------------------------------------------------
# CONSTANTS
# ------------------------------------------------

# ------------------------------------------------
# DYNAMIC ENTITY TYPE HANDLING
# ------------------------------------------------
# No hardcoded list of entity types — the script accepts ANY entity type
# returned by Purview (tables, views, blobs, cosmos, synapse, etc.)
#
# Rules for tab name derivation from entity type string:
#   mssql_table / mssql_view           -> "SQL Server"
#   azure_sql_table / azure_sql_view   -> "Azure SQL Database"
#   oracle_table / oracle_view         -> "Oracle"
#   postgresql_table / postgresql_view -> "PostgreSQL"
#   azure_blob_*                       -> "Azure Blob Storage"
#   azure_cosmosdb_*                   -> "Azure Cosmos DB"
#   azure_synapse_*                    -> "Azure Synapse"
#   azure_datalake_*                   -> "Azure Data Lake"
#   mysql_*                            -> "MySQL"
#   db2_*                              -> "DB2"
#   sap_*                              -> "SAP"
#   snowflake_*                        -> "Snowflake"
#   databricks_*                       -> "Databricks"
#   ... any other type                 -> derived from prefix (auto-titled)
# ------------------------------------------------

# Known excluded entity objectTypes — not column-bearing assets
EXCLUDED_OBJECT_TYPES = {
    "Process", "Column", "Schema", "Database", "Server",
    "Account", "Namespace", "Topic", "Subscription", "Queue",
}

# System fields to skip for Cosmos DB
COSMOS_SYSTEM_FIELDS = {
    "_rid", "_self", "_etag", "_attachments", "_ts",
    "_lsn", "_metadata", "_docs", "_sprocs", "_triggers",
    "_udfs", "_conflicts"
}

# Known tab name mappings for common prefixes
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


def entity_type_to_tab(entity_type: str) -> str:
    """
    Derives a human-readable Excel tab name from any Purview entity type string.
    Works for any data source — no hardcoded list required.

    Strategy:
      1. Try known prefix mappings (longest match first)
      2. Fall back to auto-title from the prefix before first underscore
    """
    et = entity_type.lower().strip()

    # Try longest prefix match first
    for prefix in sorted(_PREFIX_TO_TAB, key=len, reverse=True):
        if et.startswith(prefix):
            return _PREFIX_TO_TAB[prefix]

    # Auto-derive: take everything before the last underscore segment
    # e.g. "my_custom_db_table" -> "My Custom Db"
    parts = et.rsplit("_", 1)
    base  = parts[0] if len(parts) > 1 else et
    return base.replace("_", " ").title()


def is_column_bearing_asset(asset: dict) -> bool:
    """
    Returns True if this asset is likely to have columns/fields we can classify.
    Accepts any entity type that is NOT in the excluded object types set.
    This means new source types added to Purview in future work automatically.
    """
    obj_type = asset.get("objectType", "")
    return obj_type not in EXCLUDED_OBJECT_TYPES and bool(asset.get("entityType", ""))

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
# AUTH  —  auto-refresh token before expiry
# ------------------------------------------------

_token_value    = None
_token_fetched  = 0.0   # epoch seconds when token was last fetched


def get_token():
    """Fetch a fresh OAuth2 token. Called automatically when needed."""
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
    """
    Returns auth headers, refreshing the token if it is older than
    TOKEN_REFRESH_MINUTES to avoid mid-run 401 expiry errors.
    """
    global _token_value, _token_fetched
    age_minutes = (time.time() - _token_fetched) / 60
    if _token_value is None or age_minutes >= TOKEN_REFRESH_MINUTES:
        print(f"  [AUTH] {'Refreshing' if _token_value else 'Fetching'} token...")
        _token_value   = get_token()
        _token_fetched = time.time()
    return {"Authorization": f"Bearer {_token_value}", "Content-Type": "application/json"}


# ------------------------------------------------
# CENTRAL HTTP WRAPPER  —  retry on failure
# ------------------------------------------------

def api_request(method, url, *, json_body=None, form_data=None, label=""):
    """
    Wrapper around requests.get / requests.post with:
      - Automatic retry on network errors and transient HTTP errors
      - Exponential back-off (RETRY_BACKOFF doubles each attempt)
      - Auto token refresh on 401
      - Never raises on connection loss — returns None after all retries exhausted
        so the caller can skip the asset gracefully instead of crashing

    Returns: requests.Response  or  None (all retries failed)
    """
    delay = RETRY_BACKOFF
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            hdrs = get_headers()
            if method == "GET":
                r = requests.get(url, headers=hdrs, timeout=60)
            elif method == "DELETE":
                r = requests.delete(url, headers=hdrs, timeout=60)
            elif method == "PUT":
                r = requests.put(url, headers=hdrs, json=json_body, timeout=60)
            else:
                r = requests.post(url, headers=hdrs, json=json_body,
                                  data=form_data, timeout=60)

            # Token expired — refresh and retry immediately
            if r.status_code == 401:
                print(f"  [AUTH] 401 on {label or url} — refreshing token (attempt {attempt})")
                global _token_value
                _token_value = None
                continue

            # Transient server error — wait and retry
            if r.status_code in RETRY_STATUS_CODES:
                wait = delay * attempt
                print(f"  [RETRY] HTTP {r.status_code} on {label or url} "
                      f"— waiting {wait}s (attempt {attempt}/{MAX_RETRIES})")
                time.sleep(wait)
                continue

            return r   # success (or a non-retryable error like 400/404)

        except (requests.exceptions.ConnectionError,
                requests.exceptions.Timeout,
                requests.exceptions.ChunkedEncodingError) as exc:
            wait = delay * attempt
            print(f"  [RETRY] Network error on {label or url}: {exc} "
                  f"— waiting {wait}s (attempt {attempt}/{MAX_RETRIES})")
            time.sleep(wait)

    print(f"  [FAIL]  Gave up after {MAX_RETRIES} attempts: {label or url}")
    return None

# ------------------------------------------------
# COLLECTION FILTER  —  name/path -> collectionId
# ------------------------------------------------

def fetch_all_collections():
    r = api_request("GET", COLLECTIONS_API, label="Collections API")
    if r is None or r.status_code != 200:
        print(f"  [WARN] Collections API failed — collection filter disabled.")
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

    r = api_request("GET", DATASOURCES_API, label="Scan datasources API")
    if r is None or r.status_code != 200:
        print(f"  [WARN] Scan datasources API failed.")
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
            # Bare host/IP — normalize comma separator to colon (SQL Server uses host,port)
            # e.g. "10.192.254.117,1433" -> "10.192.254.117:1433"
            raw_lower = raw.lower().replace(",", ":")
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
    print(f"  Page size: {SEARCH_PAGE_SIZE} assets per request")
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
    consecutive_fails  = 0

    while True:
        body = {"keywords": "*", "limit": SEARCH_PAGE_SIZE}

        if collection_ids:
            body["filter"] = (
                {"collectionId": collection_ids[0]}
                if len(collection_ids) == 1
                else {"or": [{"collectionId": cid} for cid in collection_ids]}
            )

        if continuation_token:
            body["continuationToken"] = continuation_token

        r = api_request("POST", SEARCH_API, json_body=body, label=f"Search page {page}")

        # If this page failed entirely after all retries — skip and try to continue
        if r is None:
            consecutive_fails += 1
            print(f"  [WARN] Page {page} failed — skipping (consecutive fails: {consecutive_fails})")
            if consecutive_fails >= 3:
                print(f"  [WARN] 3 consecutive page failures — stopping pagination early.")
                break
            # Can't continue without a valid continuation token, stop gracefully
            break

        if r.status_code != 200:
            print(f"  [WARN] Page {page} returned HTTP {r.status_code} — stopping pagination.")
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
    with open(RAW_CATALOG_FILE, "w") as f:
        json.dump(all_assets, f, indent=2)
    print(f"  Raw catalog saved      -> {RAW_CATALOG_FILE}")

    return all_assets


# ------------------------------------------------
# FETCH ENTITY  —  single + bulk
# ------------------------------------------------

def fetch_entity(guid, mini=False):
    """Fetch a single entity by GUID."""
    url = f"{ENTITY_API}/{guid}?minExtInfo={'true' if mini else 'false'}"
    r   = api_request("GET", url, label=f"Entity {guid[:8]}...")
    if r is None or r.status_code != 200:
        return None
    try:
        return r.json()
    except Exception:
        return None


def fetch_entities_bulk(guids):
    """
    Fetch up to BATCH_SIZE entities in one API call using the bulk endpoint.
    Returns a dict: { guid -> entity_json } for all successfully fetched entities.
    Falls back to individual fetches if bulk fails.
    """
    results = {}
    for i in range(0, len(guids), BATCH_SIZE):
        batch = guids[i:i + BATCH_SIZE]
        params = "&".join(f"guid={g}" for g in batch)
        url    = f"{BULK_ENTITY_API}?{params}&minExtInfo=true&ignoreRelationships=false"
        r      = api_request("GET", url, label=f"Bulk fetch {len(batch)} entities")

        if r is not None and r.status_code == 200:
            try:
                data = r.json()
                entities  = data.get("entities", [])
                referred  = data.get("referredEntities", {})
                # Wrap each entity the same shape as single-entity response
                for ent in entities:
                    g = ent.get("guid")
                    if g:
                        results[g] = {"entity": ent, "referredEntities": referred}
                continue   # bulk succeeded — skip fallback
            except Exception:
                pass

        # Fallback: fetch individually
        print(f"  [WARN] Bulk fetch failed for batch {i//BATCH_SIZE + 1} — falling back to individual fetches")
        for g in batch:
            single = fetch_entity(g)
            if single:
                results[g] = single

    return results

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

    def _inject_guid(g, obj):
        """Referred entity objects don't have 'guid' at top level — inject it so
        col.get('guid') works correctly when building Excel rows."""
        if obj.get("guid") != g:
            obj = dict(obj)   # shallow copy — don't mutate the shared referredEntities dict
            obj["guid"] = g
        return obj

    # 1. SQL / Oracle / PostgreSQL
    for key in ["columns", "table_columns"]:
        for ref in relationships.get(key, []):
            g = ref.get("guid")
            if g in referred:
                columns[g] = _inject_guid(g, referred[g])

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
                columns[g] = _inject_guid(g, obj)

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
                        columns[g] = _inject_guid(g, obj)
                for ref in ts_rels.get("columns", []):
                    g = ref.get("guid")
                    if g and g in ts_referred and g not in columns:
                        columns[g] = _inject_guid(g, ts_referred[g])

    return list(columns.values())

# ------------------------------------------------
# SCAN MODE
# ------------------------------------------------

def scan_catalog():
    print("\n  ACTIVE FILTERS")
    print(f"  Collections  : {FILTER_COLLECTIONS  if FILTER_COLLECTIONS  else 'ALL'}")
    print(f"  Data sources : {FILTER_DATA_SOURCES if FILTER_DATA_SOURCES else 'ALL'}")

    all_assets   = fetch_catalog()

    # Accept ANY column-bearing asset — no hardcoded type list
    table_assets = [a for a in all_assets if is_column_bearing_asset(a)]

    # Resolve data source endpoints then filter
    resolve_datasource_endpoints(all_assets)
    if FILTER_DATA_SOURCES:
        before       = len(table_assets)
        table_assets = [a for a in table_assets if asset_matches_datasource(a)]
        print(f"\n  Data source filter: {before} -> {len(table_assets)} assets kept")
        if not table_assets:
            print(f"  [WARN] No assets matched. Check source names match exactly what is in Purview.")
            return

    print(f"\n  Column-bearing assets to process -> {len(table_assets)}")

    # Group by (tab_name, instance_host) for per-instance console output
    # Instance host = "protocol://hostname" extracted from qualifiedName
    # e.g. oracle://10.1.2.3, mssql://server1, azure_sql://mydb.database.windows.net
    def get_instance_key(asset):
        tab = entity_type_to_tab(asset.get("entityType", "unknown"))
        qn  = asset.get("qualifiedName", "")
        if "://" in qn:
            parts = qn.split("/")
            host  = "/".join(parts[:3])   # "protocol://hostname"
        else:
            host = qn.split("/")[0] if "/" in qn else qn[:50]
        return (tab, host)

    # instance_groups: { (tab_name, host) -> [assets] }  — for printing per instance
    instance_groups = defaultdict(list)
    for a in table_assets:
        instance_groups[get_instance_key(a)].append(a)

    # source_groups: { tab_name -> [assets] }  — for Excel tab grouping (unchanged)
    source_groups = defaultdict(list)
    for a in table_assets:
        tab = entity_type_to_tab(a.get("entityType", "unknown"))
        source_groups[tab].append(a)

    print("\n" + "="*60)
    print(f"  STEP 2 — Fetching entity details & columns  (batch size: {BATCH_SIZE})")
    print("="*60)

    # unclassified rows per tab  ->  written to "<tab> - Unclassified" sheet
    unclassified_by_tab = defaultdict(list)
    # classified rows per tab    ->  written to "<tab> - Classified" sheet
    classified_by_tab   = defaultdict(list)
    snapshot_sources    = {}

    # ── BULK FETCH all GUIDs at once across ALL instances in parallel ──
    all_guids     = [a.get("id") for a in table_assets if a.get("id")]
    guid_to_asset = {a.get("id"): a for a in table_assets if a.get("id")}

    print(f"\n  Bulk fetching ALL {len(all_guids)} entities across all data sources...")
    entity_map = fetch_entities_bulk(all_guids)   # { guid -> entity_json }
    print(f"  Total fetched: {len(entity_map)} entities\n")
    # ──────────────────────────────────────────────────────────────────

    # Now process and PRINT per instance (each server/host shown separately)
    for (tab_name, instance_host), instance_assets in sorted(instance_groups.items()):
        instance_guids = [a.get("id") for a in instance_assets if a.get("id")]

        print(f"\n{'='*60}")
        print(f"  DATA SOURCE  : {tab_name}")
        print(f"  INSTANCE     : {instance_host}")
        print(f"  Assets       : {len(instance_assets)}")
        print(f"{'='*60}")

        source_snapshot    = []
        total_classified   = 0
        total_unclassified = 0

        for guid in instance_guids:
            entity_json = entity_map.get(guid)
            if not entity_json:
                print(f"  [WARN] No entity data returned for GUID {guid[:8]}... — skipping")
                continue

            asset          = guid_to_asset.get(guid, {})
            qualified_name = asset.get("qualifiedName", "")
            entity_type    = asset.get("entityType", "")
            collection_id  = asset.get("collectionId", "")

            columns = extract_columns(entity_json)

            classified_cols   = []
            unclassified_cols = []

            asset_snapshot = {
                "qualifiedName": qualified_name,
                "entityType":    entity_type,
                "collectionId":  collection_id,
                "tab":           tab_name,
                "columns":       []
            }

            if not columns:
                # Table exists in Purview but has no column metadata returned
                # Still print it so user knows it was processed
                print(f"\n  ASSET        : {qualified_name}")
                print(f"  COLLECTION   : {collection_id}  |  TYPE: {entity_type}")
                print(f"  COLS         : 0 — no column metadata returned (table may not have been scanned)")
                source_snapshot.append(asset_snapshot)
                continue

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

                row = {
                    "DataSource":        tab_name,
                    "EntityType":        entity_type,
                    "FullQualifiedName": qualified_name,
                    "Column":            col_name,
                    "ColumnGUID":        col.get("guid") or "",
                    "Classification":    "",
                    "Status":            ""
                }

                if not row["ColumnGUID"]:
                    print(f"  [WARN] No GUID for column '{col_name}' in '{qualified_name}' — will not be deletable")

                if class_list:
                    classified_cols.append(col_name)
                    total_classified += 1
                    row["Classification"] = ", ".join(class_list)
                    classified_by_tab[tab_name].append(row)
                else:
                    unclassified_cols.append(col_name)
                    total_unclassified += 1
                    unclassified_by_tab[tab_name].append(row)

            source_snapshot.append(asset_snapshot)

            total = len(classified_cols) + len(unclassified_cols)
            lines = [
                f"\n  ASSET        : {qualified_name}",
                f"  COLLECTION   : {collection_id}  |  TYPE: {entity_type}",
                f"  COLS         : {total} total | {len(classified_cols)} classified | {len(unclassified_cols)} unclassified",
            ]
            if classified_cols:
                lines.append(f"  [+] Classified   : {', '.join(classified_cols)}")
            if unclassified_cols:
                lines.append(f"  [-] Unclassified : {', '.join(unclassified_cols)}")
            print("\n".join(lines))

        print(f"\n  -- {tab_name} | {instance_host} SUMMARY --")
        print(f"     Assets            : {len(source_snapshot)}")
        print(f"     Classified cols   : {total_classified}")
        print(f"     Unclassified cols : {total_unclassified}")
        print(f"{'='*60}\n")

        # Accumulate snapshot keyed by tab (merged across instances for JSON)
        if tab_name not in snapshot_sources:
            snapshot_sources[tab_name] = []
        snapshot_sources[tab_name].extend(source_snapshot)

    with open(CATALOG_SNAPSHOT, "w") as f:
        json.dump(snapshot_sources, f, indent=2)

    # ── Dynamic tab order ──────────────────────────────────────────────
    PREFERRED_TAB_ORDER = [
        "SQL Server", "Azure SQL Database", "Oracle", "PostgreSQL",
        "MySQL", "DB2", "Snowflake", "Databricks", "Teradata",
        "SAP HANA", "SAP ECC", "Azure Blob Storage", "Azure Data Lake",
        "Azure Synapse", "Azure Cosmos DB", "Amazon RDS", "Amazon S3",
        "Hive", "HDFS",
    ]
    all_tabs  = set(unclassified_by_tab.keys()) | set(classified_by_tab.keys())
    tab_order = [t for t in PREFERRED_TAB_ORDER if t in all_tabs]
    tab_order += sorted(all_tabs - set(tab_order))

    # ── Write Excel: two sheets per source ────────────────────────────
    # Sheet name limit in Excel is 31 chars — truncate if needed
    def sheet_name(tab, suffix):
        name = f"{tab} - {suffix}"
        return name[:31]

    total_unclassified_rows = 0
    total_classified_rows   = 0

    with pd.ExcelWriter(EXCEL_FILE, engine="openpyxl") as writer:
        for tab in tab_order:
            # Unclassified sheet — for APPLY operations
            u_rows = unclassified_by_tab.get(tab, [])
            df_u   = pd.DataFrame(u_rows)
            if not df_u.empty:
                df_u = df_u.drop_duplicates(subset=["FullQualifiedName", "Column"])
            df_u.to_excel(writer, sheet_name=sheet_name(tab, "Unclassified"), index=False)
            total_unclassified_rows += len(df_u)

            # Classified sheet — for DELETE operations
            c_rows = classified_by_tab.get(tab, [])
            df_c   = pd.DataFrame(c_rows)
            if not df_c.empty:
                df_c = df_c.drop_duplicates(subset=["FullQualifiedName", "Column"])
            df_c.to_excel(writer, sheet_name=sheet_name(tab, "Classified"), index=False)
            total_classified_rows += len(df_c)

            print(f"  [{tab}]  Unclassified: {len(df_u)}  |  Classified: {len(df_c)}")

    print(f"\n{'='*60}")
    print(f"  Snapshot -> {CATALOG_SNAPSHOT}")
    print(f"  Raw      -> {RAW_CATALOG_FILE}")
    print(f"  Excel    -> {EXCEL_FILE}")
    print(f"             Unclassified cols : {total_unclassified_rows}  (use for APPLY)")
    print(f"             Classified cols   : {total_classified_rows}   (use for DELETE)")
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

            r = api_request(
                "POST",
                f"{ENTITY_API}/{guid}/classifications",
                json_body=[{"typeName": str(classification).strip()}],
                label=f"Classify {column}"
            )

            print(f"\n  ASSET : {asset}")
            print(f"  COL   : {column}  |  CLASS: {classification}")

            if r is not None and r.status_code in [200, 204]:
                print("  STATUS: UPDATED [OK]")
                statuses[idx] = "Updated"
                updated += 1
            else:
                err = r.text if r is not None else "No response (all retries failed)"
                print(f"  STATUS: FAILED  -> {err}")
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
            if not pd.isna(col) and not pd.isna(cls) and str(col).strip() and str(cls).strip():
                mapping[str(col).strip().lower()] = str(cls).strip()
        cde_lookup[tab.lower()] = mapping
        print(f"  CDE [{tab}] -> {len(mapping)} rules")

    if not cde_lookup:
        print("  [ERROR] No CDE rules found.")
        return

    updated_dfs   = {}
    grand_matched = grand_updated = grand_failed = grand_no_match = 0

    all_sheets = pd.ExcelFile(catalog_file).sheet_names

    for sheet in all_sheets:
        df = pd.read_excel(catalog_file, sheet_name=sheet,
                           dtype={"Classification": str, "Status": str})

        # APPLY only works on "- Unclassified" sheets — skip Classified sheets
        if "- classified" in sheet.lower():
            updated_dfs[sheet] = df
            continue

        if df.empty:
            updated_dfs[sheet] = df
            continue

        print(f"\n{'='*60}")
        print(f"  TAB: {sheet}  |  Rows: {len(df)}")
        print(f"{'='*60}")

        # Strip " - Unclassified" suffix when matching CDE tab name
        sl = sheet.lower().replace(" - unclassified", "").strip()
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

            r = api_request(
                "POST",
                f"{ENTITY_API}/{guid}/classifications",
                json_body=[{"typeName": classification}],
                label=f"CDE-Classify {column}"
            )

            print(f"\n  ASSET  : {asset}")
            print(f"  COL    : {column}  |  CLASS: {classification}")

            if r is not None and r.status_code in [200, 204]:
                print("  STATUS : CDE-Applied [OK]")
                statuses[idx]               = "CDE-Applied"
                df.at[idx, "Classification"] = classification
                updated += 1
            else:
                err = r.text if r is not None else "No response (all retries failed)"
                print(f"  STATUS : FAILED -> {err}")
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
# DELETE MODE
# ------------------------------------------------

def delete_cde_classifications(delete_file, catalog_file):
    """
    Reads delete_file (same format as rajesh_test.xlsx: column_name + classification per tab).
    Reads the "- Classified" sheets from catalog_file to get column GUIDs.
    Deletes matching classifications directly from Purview.
    Updates Status in the Classified sheet to "CDE-Deleted" on success.
    """
    print("\n" + "="*60)
    print("  CDE DELETE CLASSIFICATION MODE")
    print(f"  Delete rules : {delete_file}")
    print(f"  Catalog      : {catalog_file}")
    print("="*60)

    # Build delete lookup from delete_file: { tab_lower -> { col_lower -> classification } }
    delete_lookup = {}
    for tab in pd.ExcelFile(delete_file).sheet_names:
        df = pd.read_excel(delete_file, sheet_name=tab)
        df.columns = [c.strip().lower() for c in df.columns]
        if "column_name" not in df.columns or "classification" not in df.columns:
            print(f"  [WARN] Tab '{tab}' missing column_name/classification — skipped")
            continue
        mapping = {}
        for _, row in df.iterrows():
            col = row.get("column_name", "")
            cls = row.get("classification", "")
            if not pd.isna(col) and not pd.isna(cls) and str(col).strip() and str(cls).strip():
                mapping[str(col).strip().lower()] = str(cls).strip()
        delete_lookup[tab.lower()] = mapping
        print(f"  Delete rules [{tab}] -> {len(mapping)} rules")

    if not delete_lookup:
        print("  [ERROR] No delete rules found.")
        return

    all_sheets  = pd.ExcelFile(catalog_file).sheet_names
    updated_dfs = {}
    grand_matched = grand_deleted = grand_failed = 0

    for sheet in all_sheets:
        df = pd.read_excel(catalog_file, sheet_name=sheet,
                           dtype={"Classification": str, "Status": str})

        # DELETE only works on "- Classified" sheets — skip Unclassified sheets
        if "- classified" not in sheet.lower():
            updated_dfs[sheet] = df
            continue

        if df.empty:
            updated_dfs[sheet] = df
            continue

        print(f"\n{'='*60}")
        print(f"  TAB: {sheet}  |  Rows: {len(df)}")
        print(f"{'='*60}")

        # Strip " - Classified" suffix to match delete_lookup tab name
        sl      = sheet.lower().replace(" - classified", "").strip()
        del_tab = sl if sl in delete_lookup else next(
            (k for k in delete_lookup if k in sl or sl in k), None
        )

        if not del_tab:
            print(f"  [SKIP] No delete rules matched tab '{sheet}'")
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
            existing_class  = str(row.get("Classification", "")).strip()

            # Skip already deleted
            if existing_status == "CDE-Deleted":
                statuses[idx] = existing_status
                skipped += 1
                continue

            col_lower = str(column).strip().lower()
            if col_lower not in rules:
                statuses[idx] = existing_status
                continue

            # IMPORTANT: Use the Classification value from the Excel row (actual typeName
            # captured during scan) — NOT the value from rajesh_delete.xlsx.
            # The delete file only tells us WHICH column to target; the real typeName
            # (e.g. "MICROSOFT.PERSONAL.ADDRESS") lives in the Classified sheet already.
            # Fall back to the delete_file value only if the Excel cell is empty.
            classification = existing_class if existing_class and existing_class.lower() != "nan" else rules[col_lower]
            if not classification:
                print(f"  [SKIP] {column} — no classification to delete")
                continue

            # Handle multiple classifications on one column (comma-separated in Excel)
            class_list = [c.strip() for c in classification.split(",") if c.strip()]
            matched += 1
            grand_matched += 1

            # Guard: skip if GUID is missing (column wasn't resolvable during scan)
            if not str(guid).strip() or str(guid).strip().lower() in ["", "nan", "none"]:
                print(f"\n  [SKIP] {column} in '{asset}' — ColumnGUID is empty (re-run scan to fix)")
                statuses[idx] = "No GUID"
                continue

            print(f"\n  ASSET  : {asset}")
            print(f"  COL    : {column}  |  CLASSES: {class_list}")

            # Guard: GUID must be a valid non-empty value
            # Empty GUID means the Excel was generated before the guid-inject fix —
            # user must re-run scan first.
            if not guid or str(guid).strip() in ("", "nan", "None"):
                print(f"  [SKIP] {column} — ColumnGUID is empty. Re-run scan first:")
                print(f"         python purview_classifier.py")
                statuses[idx] = "No GUID - rescan needed"
                continue

            col_deleted = 0
            for cls in class_list:
                # Method 1: DELETE /entity/guid/{columnGUID}/classifications/{typeName}
                url = f"{ENTITY_API}/{guid}/classification/{cls}"
                print(f"  URL    : DELETE .../{guid[:8]}.../{cls}")
                r   = api_request("DELETE", url, label=f"Delete {column}/{cls}")

                if r is not None and r.status_code in [200, 204]:
                    print(f"  STATUS : DELETED [OK]  ({cls})")
                    col_deleted += 1

                elif r is not None and r.status_code == 404:
                    # Method 2: PUT /entity/guid/{columnGUID} with empty classifications []
                    # This tells Purview to clear only this column's classifications
                    print(f"  STATUS : Direct DELETE 404 — trying PUT fallback on column GUID...")
                    put_url  = f"{ENTITY_API}/{guid}"
                    put_body = {
                        "entity": {
                            "guid":            guid,
                            "typeName":        row.get("EntityType", ""),
                            "classifications": []
                        }
                    }
                    r2 = api_request("PUT", put_url, json_body=put_body,
                                     label=f"PUT clear {column}/{cls}")
                    if r2 is not None and r2.status_code in [200, 204]:
                        print(f"  STATUS : DELETED via PUT [OK]  ({cls})")
                        col_deleted += 1
                    else:
                        err = r2.text[:300] if r2 else "No response"
                        print(f"  STATUS : FAILED (both methods failed) -> {err}")
                        print(f"  DETAIL : Column GUID={guid}  Classification={cls}")
                        statuses[idx] = "Failed"
                        failed += 1
                        grand_failed += 1

                else:
                    err = r.text[:300] if r is not None else "No response (all retries failed)"
                    print(f"  STATUS : FAILED [{r.status_code if r else 'N/A'}] -> {err}")
                    statuses[idx] = "Failed"
                    failed += 1
                    grand_failed += 1

            if col_deleted == len(class_list):
                statuses[idx] = "CDE-Deleted"
                df.at[idx, "Classification"] = ""
                deleted += 1
                grand_deleted += 1
            elif col_deleted > 0:
                statuses[idx] = "Partial-Deleted"
                deleted += 1
                grand_deleted += 1

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

def main():
    args = sys.argv[1:]

    # ── No args → scan only ────────────────────────────────────────────
    if len(args) == 0:
        scan_catalog()

    # ── 1 arg → manual apply on existing Excel ─────────────────────────
    elif len(args) == 1:
        if not os.path.exists(args[0]):
            print(f"  [ERROR] File not found: {args[0]}")
            return
        apply_classifications(args[0])

    # ── 2 args → CDE apply OR delete, auto-detected by filename ─────────
    # Usage:  python purview_classifier.py <rules_file> <catalog_file>
    #
    # If rules_file matches CDE_FILE          -> apply classifications
    # If rules_file matches CDE_DELETE_FILE   -> delete classifications
    # Both files share the same format: column_name + classification columns per tab
    #
    # Requires catalog_file to already exist from a prior scan.
    # To scan first, run:  python purview_classifier.py
    # ────────────────────────────────────────────────────────────────────
    elif len(args) == 2:
        rules_file, catalog_file = args

        if not os.path.exists(rules_file):
            print(f"  [ERROR] Rules file not found: {rules_file}")
            return

        if not os.path.exists(catalog_file):
            print(f"  [ERROR] Catalog file not found: {catalog_file}")
            print(f"          Run 'python purview_classifier.py' first to generate it.")
            return

        # Auto-detect mode by comparing filename to configured variables
        rules_basename = os.path.basename(rules_file).lower()
        delete_basename = os.path.basename(CDE_DELETE_FILE).lower()
        apply_basename  = os.path.basename(CDE_FILE).lower()

        if rules_basename == delete_basename:
            print(f"  [MODE] DELETE — matched CDE_DELETE_FILE: {CDE_DELETE_FILE}")
            delete_cde_classifications(rules_file, catalog_file)
        elif rules_basename == apply_basename:
            print(f"  [MODE] APPLY — matched CDE_FILE: {CDE_FILE}")
            apply_cde_classifications(rules_file, catalog_file)
        else:
            # Unknown file — ask user to confirm intent or update config
            print(f"  [WARN] '{rules_file}' does not match CDE_FILE or CDE_DELETE_FILE.")
            print(f"         CDE_FILE        = '{CDE_FILE}'")
            print(f"         CDE_DELETE_FILE = '{CDE_DELETE_FILE}'")
            print(f"  Defaulting to APPLY mode. Update CDE_FILE or CDE_DELETE_FILE at the top of the script to avoid this warning.")
            apply_cde_classifications(rules_file, catalog_file)

    else:
        print(f"\n  Usage:")
        print(f"    python purview_classifier.py")
        print(f"      -> Scan: discover all assets and generate Excel")
        print(f"")
        print(f"    python purview_classifier.py {EXCEL_FILE}")
        print(f"      -> Manual apply: apply classifications filled in Excel")
        print(f"")
        print(f"    python purview_classifier.py {CDE_FILE} {EXCEL_FILE}")
        print(f"      -> CDE apply: auto-match and apply CDE rules (no scan)")
        print(f"")
        print(f"    python purview_classifier.py {CDE_DELETE_FILE} {EXCEL_FILE}")
        print(f"      -> CDE delete: auto-match and DELETE classifications (no scan)")

if __name__ == "__main__":
    main()