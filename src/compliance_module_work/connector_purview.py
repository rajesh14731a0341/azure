"""
connector_purview.py
====================
Microsoft Purview connector for scan.py.

WHAT THIS FILE IS
─────────────────
This is the ONLY file that knows anything about Microsoft Purview.
It speaks the Atlas API, handles Azure AD token auth, and returns
data in the standard shape that scan.py expects.

HOW TO SWAP THIS FOR ANOTHER TOOL (Collibra, Alation, etc.)
─────────────────────────────────────────────────────────────
1. Create a new connector file, e.g. connector_collibra.py
2. Implement the same five functions with the same signatures:
       get_connector_name()   → str
       fetch_collections()    → (coll_map, coll_paths, raw_items)
       fetch_glossary_terms() → list[dict]
       fetch_assets()         → list[dict]
       fetch_entities(guids)  → {guid: {"entity": {...}, "referredEntities": {...}}}
       fetch_single_entity(guid) → {"entity": {...}, "referredEntities": {...}} | None
       extract_columns(ejson) → list[dict]
3. In scan.py change ONE import line at the top:
       from connector_purview  import *   →   from connector_collibra import *
   That's it.  scan.py doesn't change at all.

STANDARD RETURN SHAPES  (scan.py depends on these exactly)
──────────────────────────────────────────────────────────
fetch_collections()
  coll_map   : {collection_id: friendly_name_str}
  coll_paths : {collection_id: "Root / Sub / Leaf"}
  raw_items  : [raw collection dicts, each with _hierarchy_path added]

fetch_assets()
  list of dicts, each must have at minimum:
    id            – unique asset GUID
    qualifiedName – full path string
    entityType    – e.g. "azure_sql_table"
    objectType    – e.g. "Tables"
    collectionId  – matches a key in coll_map

fetch_entities(guids)
  {guid: {"entity": {full entity dict}, "referredEntities": {guid: col_dict}}}

fetch_single_entity(guid)
  {"entity": {...}, "referredEntities": {...}} or None

extract_columns(ejson)
  list of column dicts from referredEntities or child API calls.
  Each column dict must have at minimum:
    guid              – column GUID
    attributes.name   – column name
    classifications   – list of classification dicts
    labels            – list of sensitivity label strings
    businessAttributes– {group: {attr: value}}
    updateTime / lastModifiedTS / createTime (for timeframe filter)

WHAT STAYS IN THIS FILE ONLY
──────────────────────────────
  ✓ Azure AD client-credentials token fetch
  ✓ HTTP retry wrapper that uses Bearer tokens
  ✓ All Purview Atlas API endpoint URLs
  ✓ fetch_collections / fetch_glossary_terms / fetch_assets
  ✓ fetch_entities (parallel bulk) + fetch_single_entity
  ✓ extract_columns (Atlas referredEntities + attachedSchema + tabular_schema)
  ✓ is_leaf   — Atlas-specific logic (objectType / entityType classification)
  ✓ ds_type   — Purview entity-type prefix → human datasource name
  ✓ get_instance / get_schema_path — Atlas qualifiedName parsing

WHAT MUST NOT BE IN THIS FILE
────────────────────────────────
  ✗ Config file reading
  ✗ JSON file writing
  ✗ Snapshot building
  ✗ Column detail enrichment (build_column_detail)
  ✗ Timeframe filter logic (col_in_timeframe)
  ✗ Collection path matching
  ✗ Any UI / logging that mentions "Purview" by name
"""

import os, time, threading, hashlib
import requests, concurrent.futures
import re as _re

# ═══════════════════════════════════════════════════════════════════════
#  CONNECTOR IDENTITY  — scan.py calls this to show the tool name
# ═══════════════════════════════════════════════════════════════════════

def get_connector_name() -> str:
    """Return the display name of this connector."""
    return "Microsoft Purview"

# ═══════════════════════════════════════════════════════════════════════
#  CREDENTIALS  (set here or via env var)
# ═══════════════════════════════════════════════════════════════════════

CLIENT_ID       = "c636fbbb-132d-4be2-9a2d-9f1352cd0e58"
CLIENT_SECRET   = os.environ.get("PURVIEW_CLIENT_SECRET",
                                 "Jg18Q~OgLpY3EtHXU2~qQd4do2RQ~jbxlUfApalR")
TENANT_ID       = "5f9bacc0-ffe8-41f7-8d25-215d55cb0f96"
PURVIEW_ACCOUNT = "finastrapurview"

# ═══════════════════════════════════════════════════════════════════════
#  API ENDPOINTS  (all Purview Atlas / account API URLs live here)
# ═══════════════════════════════════════════════════════════════════════

_BASE           = f"https://{PURVIEW_ACCOUNT}.purview.azure.com"
SEARCH_API      = f"{_BASE}/datamap/api/search/query?api-version=2023-09-01"
ENTITY_API      = f"{_BASE}/datamap/api/atlas/v2/entity/guid"
BULK_ENTITY_API = f"{_BASE}/datamap/api/atlas/v2/entity/bulk"
COLLECTION_API  = f"{_BASE}/account/collections?api-version=2019-11-01-preview"
ATLAS_BASE      = f"{_BASE}/datamap/api/atlas/v2"

# ═══════════════════════════════════════════════════════════════════════
#  TUNING CONSTANTS  (connector-specific, can differ per tool)
# ═══════════════════════════════════════════════════════════════════════

SEARCH_PAGE_SIZE   = 1000   # max results per search page
BATCH_SIZE         = 100    # GUIDs per bulk entity request
MAX_RETRIES        = 5
RETRY_BACKOFF      = 2      # seconds; multiplied by attempt number
RETRY_STATUS_CODES = {429, 500, 502, 503, 504}
TOKEN_REFRESH_MINS = 50     # refresh before the 60-min Azure token expires
MIN_VALID_MS       = 946_684_800_000   # 2000-01-01 — epoch-0 sentinel threshold

# ═══════════════════════════════════════════════════════════════════════
#  STRUCTURAL FILTER CONSTANTS  (Purview Atlas entity taxonomy)
#  Used by is_leaf() to distinguish data assets from structural nodes.
# ═══════════════════════════════════════════════════════════════════════

STRUCTURAL_OBJECT_TYPES = {
    "Process", "Column", "Schema", "Database", "Server", "Account",
    "Namespace", "Subscription", "Queue", "ResourceGroup",
    "Tenant", "Cluster", "Workspace",
}
STRUCTURAL_KINDS = {
    "schema", "server", "db", "database", "instance", "account",
    "container", "folder", "namespace", "service", "warehouse",
    "cluster", "catalog", "pipeline", "workspace", "location",
    "subscription", "resourcegroup", "tenant", "filesystem", "directory",
}
COSMOS_SYSTEM_FIELDS = {
    "_rid", "_self", "_etag", "_attachments", "_ts", "_lsn",
    "_metadata", "_docs", "_sprocs", "_triggers", "_udfs", "_conflicts",
}
_PREFIX_TO_DS = {
    "mssql":          "SQL Server",
    "azure_sql":      "Azure SQL Database",
    "oracle":         "Oracle",
    "postgresql":     "PostgreSQL",
    "mysql":          "MySQL",
    "snowflake":      "Snowflake",
    "databricks":     "Databricks",
    "azure_blob":     "Azure Blob Storage",
    "azure_adls":     "Azure Data Lake",
    "azure_datalake": "Azure Data Lake",
    "azure_cosmosdb": "Azure Cosmos DB",
    "azure_cosmos":   "Azure Cosmos DB",
    "azure_synapse":  "Azure Synapse",
    "teradata":       "Teradata",
    "amazon_rds":     "Amazon RDS",
    "amazon_s3":      "Amazon S3",
    "hive":           "Hive",
    "sap_hana":       "SAP HANA",
    "sap_ecc":        "SAP ECC",
}

# ═══════════════════════════════════════════════════════════════════════
#  AUTHENTICATION  — Azure AD client-credentials flow
#  Only this connector knows about Azure AD tokens.
#  scan.py never sees a token — it only calls fetch_*() functions.
# ═══════════════════════════════════════════════════════════════════════

_tok     = None
_tok_ts  = 0.0
_tok_lock = threading.Lock()

def _get_token() -> str:
    """Return a valid Bearer token, refreshing if needed."""
    global _tok, _tok_ts
    with _tok_lock:
        if _tok is None or (time.time() - _tok_ts) / 60 >= TOKEN_REFRESH_MINS:
            r = requests.post(
                f"https://login.microsoftonline.com/{TENANT_ID}/oauth2/token",
                data={
                    "grant_type":    "client_credentials",
                    "client_id":     CLIENT_ID,
                    "client_secret": CLIENT_SECRET,
                    "resource":      "https://purview.azure.net",
                },
                timeout=30,
            )
            r.raise_for_status()
            _tok, _tok_ts = r.json()["access_token"], time.time()
        return _tok

def _hdrs() -> dict:
    return {"Authorization": f"Bearer {_get_token()}",
            "Content-Type":  "application/json"}

# ═══════════════════════════════════════════════════════════════════════
#  HTTP HELPER  — retries with exponential backoff
#  All network calls go through here — one place to add rate-limit
#  handling, logging, circuit-breakers, etc.
# ═══════════════════════════════════════════════════════════════════════

def _api(method: str, url: str, body=None, label: str = ""):
    """
    Make a GET or POST request with retry logic.
    Returns the requests.Response object on success, None on failure.
    """
    global _tok
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            h = _hdrs()
            r = (requests.get(url,  headers=h, timeout=60) if method == "GET"
                 else requests.post(url, headers=h, json=body, timeout=60))
            if r.status_code == 401:
                with _tok_lock: _tok = None   # force token refresh on next call
                continue
            if r.status_code in RETRY_STATUS_CODES:
                wait = RETRY_BACKOFF * attempt
                print(f"[WARN] HTTP {r.status_code} [{label}] — retry {attempt}/{MAX_RETRIES} in {wait}s",
                      flush=True)
                time.sleep(wait)
                continue
            return r
        except (requests.exceptions.ConnectionError,
                requests.exceptions.Timeout,
                requests.exceptions.ChunkedEncodingError) as e:
            print(f"[WARN] Network error [{label}]: {e} — retry {attempt}", flush=True)
            time.sleep(RETRY_BACKOFF * attempt)
    print(f"[ERR]  Gave up after {MAX_RETRIES} attempts: {label}", flush=True)
    return None

# ═══════════════════════════════════════════════════════════════════════
#  HELPERS  — Purview / Atlas specific utility functions
# ═══════════════════════════════════════════════════════════════════════

def sid(val: str) -> str:
    """Stable fallback GUID when the API doesn't provide one."""
    return hashlib.md5(str(val).encode()).hexdigest()

def is_leaf(asset: dict) -> bool:
    """
    Return True if this asset is a data asset (table, file, container)
    rather than a structural node (schema, database, server, etc.).
    Purview Atlas uses objectType and entityType suffix for this.
    """
    ot = asset.get("objectType", "")
    et = asset.get("entityType", "").lower().strip()
    if ot in STRUCTURAL_OBJECT_TYPES:
        return False
    if not et:
        return False
    return et.rsplit("_", 1)[-1] not in STRUCTURAL_KINDS

def ds_type(entity_type: str) -> str:
    """Map Atlas entity type prefix to a human-readable datasource name."""
    et = entity_type.lower().strip()
    for prefix in sorted(_PREFIX_TO_DS, key=len, reverse=True):
        if et.startswith(prefix):
            return _PREFIX_TO_DS[prefix]
    return et.replace("_", " ").title()

def get_instance(qn: str) -> str:
    """Extract server / account name from an Atlas qualified name."""
    if "://" not in qn:
        return qn.split("/")[0] if "/" in qn else qn[:80]
    after = qn.split("://", 1)[1]
    parts = [p for p in after.split("/") if p]
    if not parts:
        return qn[:80]
    _struct = {"servers", "server", "accounts", "account", "instances", "instance",
               "hosts", "host", "nodes", "node", "clusters", "cluster"}
    return parts[1] if parts[0].lower() in _struct and len(parts) > 1 else parts[0]

def get_schema_path(qn: str) -> str | None:
    """Extract the schema/database portion of an Atlas qualified name."""
    if "://" in qn:
        segs = [s for s in qn.split("/")[3:] if s]
        if len(segs) >= 2:   return "/".join(segs[:-1])
        elif len(segs) == 1: return segs[0]
    return None

def short_name(qn: str) -> str:
    """Return the last path segment of a qualified name as the asset display name."""
    parts = [p for p in str(qn).rstrip("/").split("/") if p]
    return parts[-1] if parts else qn

# ═══════════════════════════════════════════════════════════════════════
#  FETCH COLLECTIONS
#  Returns:
#    coll_map   {collection_id: friendly_name}
#    coll_paths {collection_id: "Root / Sub / Leaf"}  (computed locally)
#    raw_items  [raw collection dicts + _hierarchy_path added]
# ═══════════════════════════════════════════════════════════════════════

def fetch_collections():
    r = _api("GET", COLLECTION_API, label="Collections")
    if r is None or r.status_code != 200:
        print("[WARN] Collections API failed.", flush=True)
        return {}, {}, []

    items = r.json().get("value", [])
    print(f"[OK]   Collections fetched: {len(items)}", flush=True)

    by_id = {c.get("name", ""): c for c in items}

    def _build_path(cid, seen=None):
        seen = seen or set()
        if cid in seen or cid not in by_id:
            return ""
        seen.add(cid)
        c  = by_id[cid]
        p  = (c.get("parentCollection") or {}).get("referenceName", "")
        n  = c.get("friendlyName") or c.get("name", "")
        if p and p in by_id and p != cid:
            pp = _build_path(p, seen)
            return f"{pp} / {n}" if pp else n
        return n

    coll_paths = {cid: _build_path(cid) for cid in by_id}
    coll_map   = {c.get("name", ""): c.get("friendlyName", "") for c in items}
    enriched   = [{**c, "_hierarchy_path": coll_paths.get(c.get("name", ""), "")}
                  for c in items]
    return coll_map, coll_paths, enriched

# ═══════════════════════════════════════════════════════════════════════
#  FETCH GLOSSARY TERMS
#  Strategy 1: list all glossary GUIDs, then fetch terms per glossary.
#  Strategy 2: flat /glossary/terms endpoint (fallback).
# ═══════════════════════════════════════════════════════════════════════

def fetch_glossary_terms() -> list:
    terms = []

    # Strategy 1
    glossaries = []
    r_gl = _api("GET", f"{ATLAS_BASE}/glossary?limit=100&offset=0",
                label="Glossary list")
    if r_gl and r_gl.status_code == 200:
        raw = r_gl.json()
        glossaries = (raw if isinstance(raw, list)
                      else raw.get("value", [raw]) if "value" in raw else [raw])

    if glossaries:
        for gl in glossaries:
            gl_guid = gl.get("guid") or gl.get("glossaryId") or gl.get("name")
            gl_name = gl.get("name", gl_guid)
            if not gl_guid:
                continue
            offset, limit = 0, 1000
            while True:
                r = _api("GET",
                         f"{ATLAS_BASE}/glossary/{gl_guid}/terms"
                         f"?limit={limit}&offset={offset}&sort=ASC",
                         label=f"Glossary '{gl_name}' offset={offset}")
                if r is None or r.status_code != 200:
                    print(f"[WARN] Glossary '{gl_name}' failed at offset {offset}.", flush=True)
                    break
                batch = r.json()
                if not isinstance(batch, list):
                    batch = batch.get("value", []) if isinstance(batch, dict) else []
                if not batch:
                    break
                terms.extend(batch)
                print(f"[OK]   Glossary '{gl_name}' offset {offset}: "
                      f"{len(batch)} terms | total: {len(terms)}", flush=True)
                if len(batch) < limit:
                    break
                offset += limit
    else:
        # Strategy 2: flat endpoint fallback
        print("[WARN] No glossaries via list — trying flat /glossary/terms.", flush=True)
        offset, limit = 0, 1000
        while True:
            r = _api("GET",
                     f"{ATLAS_BASE}/glossary/terms"
                     f"?limit={limit}&offset={offset}&sort=ASC",
                     label=f"Glossary flat offset={offset}")
            if r is None or r.status_code != 200:
                break
            batch = r.json()
            if not isinstance(batch, list):
                batch = batch.get("value", []) if isinstance(batch, dict) else []
            if not batch:
                break
            terms.extend(batch)
            if len(batch) < limit:
                break
            offset += limit

    print(f"[OK]   Glossary terms fetched: {len(terms)}", flush=True)
    return terms

# ═══════════════════════════════════════════════════════════════════════
#  FETCH ASSETS  — search API, paginated
#  Returns list of raw search result dicts.
# ═══════════════════════════════════════════════════════════════════════

def fetch_assets() -> list:
    items, page, tk = [], 1, None
    while True:
        body = {"keywords": "*", "limit": SEARCH_PAGE_SIZE}
        if tk:
            body["continuationToken"] = tk
        r = _api("POST", SEARCH_API, body=body, label=f"Search p{page}")
        if r is None or r.status_code != 200:
            print(f"[WARN] Search page {page} failed.", flush=True)
            break
        data  = r.json()
        batch = data.get("value", [])
        items.extend(batch)
        print(f"[OK]     Page {page}: {len(batch)} assets | "
              f"running total: {len(items):,}", flush=True)
        tk = data.get("continuationToken")
        if not tk or not batch:
            break
        page += 1
    print(f"[OK]   Total assets fetched: {len(items):,}", flush=True)
    return items

# ═══════════════════════════════════════════════════════════════════════
#  FETCH ENTITIES  — parallel bulk fetch
#  Returns {guid: {"entity": {...}, "referredEntities": {...}}}
# ═══════════════════════════════════════════════════════════════════════

def _fetch_one_batch(bn: int, batch: list, total: int) -> dict:
    """Fetch one batch of GUIDs using the bulk entity endpoint."""
    params = "&".join(f"guid={g}" for g in batch)
    url    = f"{BULK_ENTITY_API}?{params}&minExtInfo=true&ignoreRelationships=false"
    r      = _api("GET", url, label=f"Bulk {bn}/{total}")
    out    = {}
    if r and r.status_code == 200:
        try:
            data = r.json()
            refs = data.get("referredEntities", {})
            for e in data.get("entities", []):
                g = e.get("guid")
                if g:
                    out[g] = {"entity": e, "referredEntities": refs}
            print(f"[OK]   Bulk {bn}/{total} — {len(data.get('entities', []))} entities",
                  flush=True)
            return out
        except Exception as ex:
            print(f"[WARN] Bulk {bn} parse error: {ex}", flush=True)

    # Fallback: single entity fetches one by one
    print(f"[WARN] Bulk {bn} failed — falling back to single fetches", flush=True)
    for g in batch:
        r2 = _api("GET",
                  f"{ENTITY_API}/{g}?minExtInfo=true&ignoreRelationships=false",
                  label=f"Single {g[:8]}")
        if r2 and r2.status_code == 200:
            try:
                raw = r2.json()
                out[g] = {"entity":           raw.get("entity", {}),
                          "referredEntities": raw.get("referredEntities", {})}
            except Exception:
                pass
    return out


def fetch_entities(guids: list, max_workers: int = 100) -> dict:
    """
    Fetch full entity detail for all guids in parallel batches.
    max_workers is passed in from scan.py (read from config there).
    """
    total   = max(1, (len(guids) + BATCH_SIZE - 1) // BATCH_SIZE)
    batches = [(i // BATCH_SIZE + 1, guids[i:i + BATCH_SIZE], total)
               for i in range(0, len(guids), BATCH_SIZE)]
    results = {}
    with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as ex:
        futs = {ex.submit(_fetch_one_batch, b[0], b[1], b[2]): b[0] for b in batches}
        for f in concurrent.futures.as_completed(futs):
            try:
                results.update(f.result())
            except Exception as e:
                print(f"[WARN] Batch future error: {e}", flush=True)
    print(f"[OK]   Entities fetched: {len(results):,}", flush=True)
    return results


def fetch_single_entity(guid: str) -> dict | None:
    """Fetch a single entity by GUID. Used as a fallback when bulk fetch misses one."""
    r = _api("GET",
             f"{ENTITY_API}/{guid}?minExtInfo=true&ignoreRelationships=false",
             label=f"Single {guid[:8]}")
    if r and r.status_code == 200:
        try:
            raw = r.json()
            return {"entity":           raw.get("entity", {}),
                    "referredEntities": raw.get("referredEntities", {})}
        except Exception:
            pass
    return None

# ═══════════════════════════════════════════════════════════════════════
#  EXTRACT COLUMNS
#  Pulls column objects out of the Atlas entity response.
#  Handles three Atlas patterns:
#    1. entity.relationshipAttributes.columns / table_columns
#    2. entity.relationshipAttributes.attachedSchema  (some source types)
#    3. entity.relationshipAttributes.tabular_schema  (CosmosDB, ADLS, etc.)
#  Returns raw column dicts — scan.py's build_column_detail() reads them.
# ═══════════════════════════════════════════════════════════════════════

def extract_columns(ejson: dict) -> list:
    """
    Return a list of raw column dicts from an entity fetch response.
    Each dict is a referredEntity object with guid injected.
    """
    cols   = {}
    entity = ejson.get("entity", {})
    rels   = entity.get("relationshipAttributes", {})
    refs   = ejson.get("referredEntities", {})

    def _inj(g, obj):
        if obj.get("guid") != g:
            obj = dict(obj)
            obj["guid"] = g
        return obj

    # Pattern 1: standard relational columns
    for key in ("columns", "table_columns"):
        for ref in rels.get(key, []):
            g = ref.get("guid")
            if g and g in refs:
                cols[g] = _inj(g, refs[g])

    # Pattern 2: schema-attached columns (Hive, SAP, some Azure sources)
    for s in rels.get("attachedSchema", []):
        sg = s.get("guid")
        if not sg:
            continue
        r = _api("GET", f"{ENTITY_API}/{sg}?minExtInfo=false",
                 label=f"Schema {sg[:8]}")
        if r and r.status_code == 200:
            try:
                for g, obj in r.json().get("referredEntities", {}).items():
                    if any(x in obj.get("typeName", "").lower()
                           for x in ("column", "field", "attribute")):
                        cols[g] = _inj(g, obj)
            except Exception:
                pass

    # Pattern 3: tabular schema (CosmosDB, blob resource sets, ADLS gen2, etc.)
    ts_ref = rels.get("tabular_schema", {})
    tg     = ts_ref.get("guid") if isinstance(ts_ref, dict) else None
    if tg:
        r = _api("GET", f"{ENTITY_API}/{tg}?minExtInfo=true",
                 label=f"TabSchema {tg[:8]}")
        if r and r.status_code == 200:
            try:
                tj  = r.json()
                tr  = tj.get("referredEntities", {})
                trl = tj.get("entity", {}).get("relationshipAttributes", {})
                for g, obj in tr.items():
                    if any(x in obj.get("typeName", "").lower()
                           for x in ("column", "field", "attribute")):
                        cols[g] = _inj(g, obj)
                for ref in trl.get("columns", []):
                    g = ref.get("guid")
                    if g and g in tr and g not in cols:
                        cols[g] = _inj(g, tr[g])
            except Exception:
                pass

    return list(cols.values())