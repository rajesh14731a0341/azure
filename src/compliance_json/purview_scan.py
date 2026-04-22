"""
purview_scan.py
===============
Microsoft Purview → JSON only.  No SQL.  No database.  No external
dependencies beyond:   pip install requests

AUTO CONFIG DETECTION (searches in order):
  1. First CLI argument:  python purview_scan.py my.ini
  2. purview_config.ini next to this script
  3. purview_config.ini in the current working directory

OUTPUT  (JSON_OUTPUT_FOLDER from config, default = json_output/)
────────────────────────────────────────────────────────────────────
  collections.json      raw Purview collection objects + _hierarchy_path
  glossary_terms.json   raw glossary term objects
  assets.json           raw leaf-asset search results
  entities.json         raw entity detail objects (bulk API)
  purview_snapshot.json ENRICHED ASSETS ONLY:
                          • Only assets that have ≥1 enriched column
                            (classified, labeled, or tagged) are written.
                          • Each asset's "columns" list contains ONLY its
                            enriched columns — unenriched columns excluded.
                          • total_columns on each asset still reflects the
                            full count from the API so you know the real size.
                          • collection_breakdown section in the snapshot
                            gives per-collection counts of assets, columns,
                            classifications, labels, tags, and cls types.

COLLECTIONS FILTER
────────────────────────────────────────────────────────────────────
  COLLECTIONS_FILTER = none                → all assets
  COLLECTIONS_FILTER = POC_Finastra/Azure SQL Task, POC_Finastra/Lending-POS
    → only assets whose resolved hierarchy path starts with any listed value.
    Matching is case-insensitive and ignores whitespace around "/", so
    "POC_Finastra/Azure SQL Task" matches "POC_Finastra / Azure SQL Task".

TIMEFRAME FILTER  (LAST_RUN)
────────────────────────────────────────────────────────────────────
  LAST_RUN = all        → fetch every asset
  LAST_RUN = 7d / 2hr / 30min
    → only columns whose classification timestamp (or asset updateTime for
      MICROSOFT.* auto-scan with epoch-0 ts) falls within the window.
      Labels and business-tags have no API timestamp — when present they
      always qualify.  Asset created/updated timestamps are never used
      as the filter signal.

Usage
─────
    python purview_scan.py                      # auto-finds config
    python purview_scan.py purview_config.ini   # explicit path
"""

import os, sys, json, time, uuid, hashlib, datetime
import requests, configparser, concurrent.futures, threading
from pathlib import Path
from collections import defaultdict
import re as _re
import json

# ═══════════════════════════════════════════════════════════════════════
#  CREDENTIALS  (client secret falls back to env var)
# ═══════════════════════════════════════════════════════════════════════

CLIENT_ID       = "c636fbbb-132d-4be2-9a2d-9f1352cd0e58"
CLIENT_SECRET   = os.environ.get("PURVIEW_CLIENT_SECRET",
                                 "Jg18Q~OgLpY3EtHXU2~qQd4do2RQ~jbxlUfApalR")
TENANT_ID       = "5f9bacc0-ffe8-41f7-8d25-215d55cb0f96"
PURVIEW_ACCOUNT = "finastrapurview"

# ═══════════════════════════════════════════════════════════════════════
#  AUTO CONFIG DETECTION
# ═══════════════════════════════════════════════════════════════════════

def _find_config():
    candidates = []
    if len(sys.argv) > 1:
        candidates.append(Path(sys.argv[1]))
    candidates.append(Path(__file__).parent / "purview_config.ini")
    candidates.append(Path.cwd() / "purview_config.ini")
    for p in candidates:
        if p.exists():
            return str(p)
    return None

_config_path = _find_config()
_cfg = configparser.ConfigParser()
if _config_path:
    _cfg.read(_config_path)

def _get(key, fallback=""):
    if _cfg.has_section("PURVIEW"):
        return _cfg["PURVIEW"].get(key, fallback).strip()
    return fallback

JSON_OUTPUT_FOLDER = _get("JSON_OUTPUT_FOLDER", "json_output")
SNAPSHOT_FILE      = _get("SNAPSHOT_FILE",      "purview_snapshot.json")
_LAST_RUN_RAW      = _get("LAST_RUN",           "all")
_COLL_FILTER_RAW   = _get("COLLECTIONS_FILTER", "none")
MAX_WORKERS        = int(_get("MAX_WORKERS",     "100"))

COLLECTIONS_FILTER = (
    None if _COLL_FILTER_RAW.lower() == "none"
    else [f.strip() for f in _COLL_FILTER_RAW.split(",") if f.strip()]
)

# ═══════════════════════════════════════════════════════════════════════
#  API ENDPOINTS
# ═══════════════════════════════════════════════════════════════════════

_BASE           = f"https://{PURVIEW_ACCOUNT}.purview.azure.com"
SEARCH_API      = f"{_BASE}/datamap/api/search/query?api-version=2023-09-01"
ENTITY_API      = f"{_BASE}/datamap/api/atlas/v2/entity/guid"
BULK_ENTITY_API = f"{_BASE}/datamap/api/atlas/v2/entity/bulk"
COLLECTION_API  = f"{_BASE}/account/collections?api-version=2019-11-01-preview"
ATLAS_BASE      = f"{_BASE}/datamap/api/atlas/v2"

# ═══════════════════════════════════════════════════════════════════════
#  TUNING
# ═══════════════════════════════════════════════════════════════════════

SEARCH_PAGE_SIZE   = 1000
BATCH_SIZE         = 100
MAX_RETRIES        = 5
RETRY_BACKOFF      = 2
RETRY_STATUS_CODES = {429, 500, 502, 503, 504}
TOKEN_REFRESH_MINS = 50
MIN_VALID_MS       = 946_684_800_000   # 2000-01-01 — below this is epoch-0 sentinel

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
#  LOGGING  (same format, thread-safe)
# ═══════════════════════════════════════════════════════════════════════

_print_lock = threading.Lock()

def log(msg, level="INFO"):
    ts   = datetime.datetime.now(datetime.timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
    icon = {"INFO": "[INFO]", "OK": "[OK]  ",
            "WARN": "[WARN]", "ERROR": "[ERR] "}.get(level, "     ")
    with _print_lock:
        print(f"[{ts}] {icon} {msg}", flush=True)

def log_section(title):
    with _print_lock:
        print(f"\n{'='*70}\n  {title}\n{'='*70}", flush=True)

def fmt_dur(s):
    s = int(s); h, r = divmod(s, 3600); m, s = divmod(r, 60)
    return f"{h}h {m}m {s}s" if h else (f"{m}m {s}s" if m else f"{s}s")

# ═══════════════════════════════════════════════════════════════════════
#  TIMEFRAME HELPERS
# ═══════════════════════════════════════════════════════════════════════

def parse_last_run(raw):
    val = raw.strip().lower()
    if val == "all":
        return None, "ALL — full load (no time filter)"
    for unit, secs in [("min", 60), ("hr", 3600), ("d", 86400)]:
        if val.endswith(unit):
            try:
                n   = int(val[:-len(unit)])
                cut = int((time.time() - n * secs) * 1000)
                return cut, f"last {n}{unit}  (changes after {ms_to_iso(cut)})"
            except ValueError:
                pass
    raise ValueError(f"Invalid LAST_RUN '{raw}'. Use: all | <N>min | <N>hr | <N>d")

def ms_to_iso(ms):
    if ms is None: return None
    try:
        v = int(ms)
        if v < MIN_VALID_MS: return None
        return datetime.datetime.fromtimestamp(
            v / 1000, datetime.timezone.utc
        ).strftime("%Y-%m-%d %H:%M:%S")
    except Exception:
        return None

def _safe_ms(val):
    if val is None: return None
    try:
        v = int(val)
        return v if v >= MIN_VALID_MS else None
    except Exception:
        return None

def _get_ts(obj, *keys):
    """Try each key on obj then on obj['attributes'], return first valid epoch-ms."""
    for k in keys:
        v = _safe_ms(obj.get(k))
        if v: return v
    attrs = obj.get("attributes") or {}
    for k in keys:
        v = _safe_ms(attrs.get(k))
        if v: return v
    return None

# ═══════════════════════════════════════════════════════════════════════
#  TIMEFRAME FILTER  (classification / label / tag driven)
#
#  col_in_timeframe decides whether a column falls inside the LAST_RUN
#  window.  The logic is:
#    Full load (cutoff_ms=None) → always True
#    Incremental:
#      Gate A1 — any ACTIVE classification with real timestamp ≥ cutoff
#      Gate A2 — MICROSOFT.* auto-scan cls have lastModifiedTS=0 (epoch
#                sentinel); fall back to parent asset updateTime
#      Gate B  — sensitivity label present → column updateTime ≥ cutoff
#                (Purview bumps column updateTime when label applied)
#      Gate C  — business tag present → same proxy as Gate B
#  Asset created/updated timestamps are NEVER used as the filter signal
#  except as the fallback proxy in Gates A2/B/C.
# ═══════════════════════════════════════════════════════════════════════

def col_in_timeframe(col, cutoff_ms, a_upd_ms=None):
    if cutoff_ms is None:
        return True

    # Gate A1 + A2 — classifications
    has_epoch_zero_cls = False
    for c in (col.get("classifications") or []):
        if c.get("entityStatus", "ACTIVE") == "DELETED":
            continue
        raw_ms = _get_ts(c, "lastModifiedTS", "updateTime", "createTime")
        if raw_ms and raw_ms >= cutoff_ms:
            return True
        if not raw_ms:
            has_epoch_zero_cls = True   # epoch-0 sentinel present

    # Gate A2: MICROSOFT.* auto-scan epoch-0 → use asset updateTime as proxy
    if has_epoch_zero_cls and a_upd_ms and a_upd_ms >= cutoff_ms:
        return True

    col_upd_ms = _get_ts(col, "updateTime", "lastModifiedTS", "createTime")

    # Gate B — sensitivity labels
    labels = [l for l in (col.get("labels") or []) if l]
    if labels:
        if col_upd_ms and col_upd_ms >= cutoff_ms:
            return True
        if not col_upd_ms and a_upd_ms and a_upd_ms >= cutoff_ms:
            return True

    # Gate C — business tags
    has_tags = any(isinstance(gv, dict) and gv
                   for gv in (col.get("businessAttributes") or {}).values())
    if has_tags:
        if col_upd_ms and col_upd_ms >= cutoff_ms:
            return True
        if not col_upd_ms and a_upd_ms and a_upd_ms >= cutoff_ms:
            return True

    return False

# ═══════════════════════════════════════════════════════════════════════
#  AUTHENTICATION
# ═══════════════════════════════════════════════════════════════════════

_tok, _tok_ts, _tok_lock = None, 0.0, threading.Lock()

def get_token():
    global _tok, _tok_ts
    with _tok_lock:
        if _tok is None or (time.time() - _tok_ts) / 60 >= TOKEN_REFRESH_MINS:
            log(("Refreshing" if _tok else "Fetching") + " auth token...")
            r = requests.post(
                f"https://login.microsoftonline.com/{TENANT_ID}/oauth2/token",
                data={"grant_type":    "client_credentials",
                      "client_id":     CLIENT_ID,
                      "client_secret": CLIENT_SECRET,
                      "resource":      "https://purview.azure.net"},
                timeout=30)
            r.raise_for_status()
            _tok, _tok_ts = r.json()["access_token"], time.time()
            log("Token ready.", "OK")
        return _tok

def hdrs():
    return {"Authorization": f"Bearer {get_token()}",
            "Content-Type":  "application/json"}

# ═══════════════════════════════════════════════════════════════════════
#  HTTP HELPER — retries + exponential backoff
# ═══════════════════════════════════════════════════════════════════════

def api(method, url, body=None, label=""):
    global _tok
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            h = hdrs()
            r = (requests.get(url, headers=h, timeout=60)
                 if method == "GET"
                 else requests.post(url, headers=h, json=body, timeout=60))
            if r.status_code == 401:
                with _tok_lock: _tok = None
                continue
            if r.status_code in RETRY_STATUS_CODES:
                wait = RETRY_BACKOFF * attempt
                log(f"HTTP {r.status_code} [{label}] — retry {attempt}/{MAX_RETRIES} in {wait}s",
                    "WARN")
                time.sleep(wait)
                continue
            return r
        except (requests.exceptions.ConnectionError,
                requests.exceptions.Timeout,
                requests.exceptions.ChunkedEncodingError) as e:
            log(f"Network error [{label}]: {e} — retry {attempt} in {RETRY_BACKOFF*attempt}s",
                "WARN")
            time.sleep(RETRY_BACKOFF * attempt)
    log(f"Gave up after {MAX_RETRIES} attempts: {label}", "ERROR")
    return None

# ═══════════════════════════════════════════════════════════════════════
#  HELPERS
# ═══════════════════════════════════════════════════════════════════════

_UUID_RE = _re.compile(
    r'^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$', _re.I)

def sid(val):
    """Stable fallback GUID from qualified_name + column_name."""
    return hashlib.md5(str(val).encode()).hexdigest()

def short_name(qn):
    parts = [p for p in str(qn).rstrip("/").split("/") if p]
    return parts[-1] if parts else qn

def is_leaf(asset):
    ot = asset.get("objectType", "")
    et = asset.get("entityType", "").lower().strip()
    if ot in STRUCTURAL_OBJECT_TYPES: return False
    if not et: return False
    return et.rsplit("_", 1)[-1] not in STRUCTURAL_KINDS

def ds_type(et):
    et = et.lower().strip()
    for prefix in sorted(_PREFIX_TO_DS, key=len, reverse=True):
        if et.startswith(prefix): return _PREFIX_TO_DS[prefix]
    return et.replace("_", " ").title()

def get_instance(qn):
    if "://" not in qn:
        return qn.split("/")[0] if "/" in qn else qn[:80]
    after = qn.split("://", 1)[1]
    parts = [p for p in after.split("/") if p]
    if not parts: return qn[:80]
    _struct = {"servers","server","accounts","account","instances","instance",
               "hosts","host","nodes","node","clusters","cluster"}
    return parts[1] if parts[0].lower() in _struct and len(parts) > 1 else parts[0]

def get_schema_path(qn):
    if "://" in qn:
        segs = [s for s in qn.split("/")[3:] if s]
        if len(segs) >= 2:   return "/".join(segs[:-1])
        elif len(segs) == 1: return segs[0]
    return None

def _resolve_by(raw):
    if not raw: return None
    s = str(raw).strip()
    if not s: return None
    if _UUID_RE.match(s):
        return f"Automated Scan ({s[:8]}...)"
    return s

def _norm_path(s):
    """Normalize a collection path for comparison: collapse spaces around '/', lowercase."""
    s = _re.sub(r'\s*/\s*', '/', s)
    return ' '.join(s.split()).lower()

def coll_matches(coll_p):
    if not COLLECTIONS_FILTER: return True
    norm = _norm_path(coll_p)
    return any(norm.startswith(_norm_path(cf)) for cf in COLLECTIONS_FILTER)

# ═══════════════════════════════════════════════════════════════════════
#  FETCH — COLLECTIONS
# ═══════════════════════════════════════════════════════════════════════

def fetch_collections():
    log_section("STEP 1 — Collections (full load)")
    r = api("GET", COLLECTION_API, label="Collections")
    if r is None or r.status_code != 200:
        log("Collections API failed.", "WARN")
        return {}, {}, []
    items = r.json().get("value", [])
    log(f"Collections fetched: {len(items)}", "OK")

    by_id = {c.get("name", ""): c for c in items}

    def build_path(cid, seen=None):
        seen = seen or set()
        if cid in seen or cid not in by_id: return ""
        seen.add(cid)
        c  = by_id[cid]
        p  = (c.get("parentCollection") or {}).get("referenceName", "")
        n  = c.get("friendlyName") or c.get("name", "")
        if p and p in by_id and p != cid:
            pp = build_path(p, seen)
            return f"{pp} / {n}" if pp else n
        return n

    coll_paths = {cid: build_path(cid) for cid in by_id}
    coll_map   = {c.get("name", ""): c.get("friendlyName", "") for c in items}
    # Enrich with resolved path for the raw JSON file
    enriched   = [{**c, "_hierarchy_path": coll_paths.get(c.get("name", ""), "")}
                  for c in items]
    return coll_map, coll_paths, enriched

# ═══════════════════════════════════════════════════════════════════════
#  FETCH — GLOSSARY TERMS
# ═══════════════════════════════════════════════════════════════════════

def fetch_glossary_terms():
    log_section("STEP 2 — Glossary terms (full load)")
    terms = []

    # Strategy 1: list all glossaries, then fetch per-glossary terms
    glossaries = []
    r_gl = api("GET", f"{ATLAS_BASE}/glossary?limit=100&offset=0",
               label="Glossary list")
    if r_gl and r_gl.status_code == 200:
        raw = r_gl.json()
        glossaries = (raw if isinstance(raw, list)
                      else raw.get("value", [raw]) if "value" in raw else [raw])

    if glossaries:
        for gl in glossaries:
            gl_guid = gl.get("guid") or gl.get("glossaryId") or gl.get("name")
            gl_name = gl.get("name", gl_guid)
            if not gl_guid: continue
            offset, limit = 0, 1000
            while True:
                r = api("GET",
                        f"{ATLAS_BASE}/glossary/{gl_guid}/terms"
                        f"?limit={limit}&offset={offset}&sort=ASC",
                        label=f"Glossary '{gl_name}' offset={offset}")
                if r is None or r.status_code != 200:
                    log(f"Glossary '{gl_name}' failed at offset {offset}.", "WARN"); break
                batch = r.json()
                if not isinstance(batch, list):
                    batch = batch.get("value", []) if isinstance(batch, dict) else []
                if not batch: break
                terms.extend(batch)
                log(f"  '{gl_name}' offset {offset}: {len(batch)} terms | total: {len(terms)}", "OK")
                if len(batch) < limit: break
                offset += limit
    else:
        # Strategy 2: flat endpoint fallback
        log("No glossaries via list — trying flat /glossary/terms.", "WARN")
        offset, limit = 0, 1000
        while True:
            r = api("GET",
                    f"{ATLAS_BASE}/glossary/terms?limit={limit}&offset={offset}&sort=ASC",
                    label=f"Glossary flat offset={offset}")
            if r is None or r.status_code != 200:
                log(f"Glossary flat failed at offset {offset}.", "WARN"); break
            batch = r.json()
            if not isinstance(batch, list):
                batch = batch.get("value", []) if isinstance(batch, dict) else []
            if not batch: break
            terms.extend(batch)
            log(f"  Flat offset {offset}: {len(batch)} terms | total: {len(terms)}", "OK")
            if len(batch) < limit: break
            offset += limit

    log(f"Glossary terms fetched: {len(terms)}", "OK")
    return terms

# ═══════════════════════════════════════════════════════════════════════
#  FETCH — ASSETS  (search API, paginated)
# ═══════════════════════════════════════════════════════════════════════

def fetch_assets():
    log_section("STEP 3 — Fetch all assets (search)")
    items, page, tk = [], 1, None
    while True:
        body = {"keywords": "*", "limit": SEARCH_PAGE_SIZE}
        if tk: body["continuationToken"] = tk
        r = api("POST", SEARCH_API, body=body, label=f"Search p{page}")
        if r is None or r.status_code != 200:
            log(f"Search page {page} failed.", "WARN"); break
        data  = r.json()
        batch = data.get("value", [])
        items.extend(batch)
        log(f"  Page {page}: {len(batch)} assets | running total: {len(items):,}", "OK")
        tk = data.get("continuationToken")
        if not tk or not batch: break
        page += 1
    log(f"Total assets fetched: {len(items):,}", "OK")
    return items

# ═══════════════════════════════════════════════════════════════════════
#  FETCH — ENTITIES  (bulk parallel, MAX_WORKERS threads)
# ═══════════════════════════════════════════════════════════════════════

def _fetch_one_batch(bn, batch, total):
    params = "&".join(f"guid={g}" for g in batch)
    url    = f"{BULK_ENTITY_API}?{params}&minExtInfo=true&ignoreRelationships=false"
    r      = api("GET", url, label=f"Bulk {bn}")
    out    = {}
    if r and r.status_code == 200:
        try:
            data = r.json()
            refs = data.get("referredEntities", {})
            for e in data.get("entities", []):
                g = e.get("guid")
                if g: out[g] = {"entity": e, "referredEntities": refs}
            log(f"Bulk {bn}/{total} — {len(data.get('entities', []))} entities", "OK")
            return out
        except Exception as ex:
            log(f"Bulk {bn} parse error: {ex}", "WARN")
    log(f"Bulk {bn} failed — falling back to single fetches", "WARN")
    for g in batch:
        r2 = api("GET",
                 f"{ENTITY_API}/{g}?minExtInfo=true&ignoreRelationships=false",
                 label=f"Single {g[:8]}")
        if r2 and r2.status_code == 200:
            try:
                raw = r2.json()
                out[g] = {"entity": raw.get("entity", {}),
                          "referredEntities": raw.get("referredEntities", {})}
            except Exception: pass
    return out

def fetch_entities(guids):
    log_section(f"STEP 4 — Bulk entity fetch ({len(guids):,} GUIDs, {MAX_WORKERS} workers)")
    total   = max(1, (len(guids) + BATCH_SIZE - 1) // BATCH_SIZE)
    batches = [(i // BATCH_SIZE + 1, guids[i:i+BATCH_SIZE], total)
               for i in range(0, len(guids), BATCH_SIZE)]
    results = {}
    with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_WORKERS) as ex:
        futs = {ex.submit(_fetch_one_batch, b[0], b[1], b[2]): b[0] for b in batches}
        for f in concurrent.futures.as_completed(futs):
            try: results.update(f.result())
            except Exception as e: log(f"Batch future error: {e}", "WARN")
    log(f"Entities fetched: {len(results):,}", "OK")
    return results

# ═══════════════════════════════════════════════════════════════════════
#  COLUMN EXTRACTION  (handles relational, schema-attached, CosmosDB)
# ═══════════════════════════════════════════════════════════════════════

def extract_columns(ejson):
    cols   = {}
    entity = ejson.get("entity", {})
    rels   = entity.get("relationshipAttributes", {})
    refs   = ejson.get("referredEntities", {})

    def inj(g, obj):
        if obj.get("guid") != g: obj = dict(obj); obj["guid"] = g
        return obj

    # 1. Relational: columns / table_columns
    for key in ["columns", "table_columns"]:
        for ref in rels.get(key, []):
            g = ref.get("guid")
            if g and g in refs: cols[g] = inj(g, refs[g])

    # 2. Schema-attached (some source types)
    for s in rels.get("attachedSchema", []):
        sg = s.get("guid")
        if not sg: continue
        r = api("GET", f"{ENTITY_API}/{sg}?minExtInfo=false", label=f"Schema {sg[:8]}")
        if r and r.status_code == 200:
            try:
                for g, obj in r.json().get("referredEntities", {}).items():
                    if any(x in obj.get("typeName", "").lower()
                           for x in ("column", "field", "attribute")):
                        cols[g] = inj(g, obj)
            except Exception: pass

    # 3. Tabular schema (CosmosDB, blob resource sets, ADLS, etc.)
    ts_ref = rels.get("tabular_schema", {})
    tg     = ts_ref.get("guid") if isinstance(ts_ref, dict) else None
    if tg:
        r = api("GET", f"{ENTITY_API}/{tg}?minExtInfo=true", label=f"TabSchema {tg[:8]}")
        if r and r.status_code == 200:
            try:
                tj  = r.json()
                tr  = tj.get("referredEntities", {})
                trl = tj.get("entity", {}).get("relationshipAttributes", {})
                for g, obj in tr.items():
                    if any(x in obj.get("typeName", "").lower()
                           for x in ("column", "field", "attribute")):
                        cols[g] = inj(g, obj)
                for ref in trl.get("columns", []):
                    g = ref.get("guid")
                    if g and g in tr and g not in cols:
                        cols[g] = inj(g, tr[g])
            except Exception: pass

    return list(cols.values())

# ═══════════════════════════════════════════════════════════════════════
#  COLUMN DETAIL BUILDER
#
#  Produces a self-contained record for every column:
#    • Column identity (guid, name, type, position, description, flags)
#    • All timestamps (column created/updated, ISO + raw ms)
#    • All classifications: name, applied_at, applied_by, source,
#      confidence, entity_status — nothing omitted
#    • Sensitivity labels (list)
#    • Business tags (full group → attribute dict)
#    • Enrichment flags: has_classification, has_sensitivity_label,
#      has_business_tag, is_enriched
# ═══════════════════════════════════════════════════════════════════════

def build_column_detail(col, col_number, total_cols, asset_upd_at, cutoff_ms, a_upd_ms):
    attr     = col.get("attributes") or {}
    col_name = attr.get("name") or col.get("name") or ""
    col_guid = col.get("guid") or sid(col_name)

    # Skip CosmosDB internal system fields entirely
    if col_name in COSMOS_SYSTEM_FIELDS:
        return None

    # Timestamps
    col_crt_ms = _get_ts(col, "createTime",  "createdTS")
    col_upd_ms = _get_ts(col, "updateTime",  "lastModifiedTS")

    # ── Classifications — full detail ─────────────────────────────────
    classifications = []
    for c in (col.get("classifications") or []):
        cn = c.get("typeName", "")
        if not cn: continue
        status = c.get("entityStatus", "ACTIVE")
        raw_ms = _get_ts(c, "lastModifiedTS", "updateTime", "createTime")
        raw_by = (c.get("source") or c.get("createdBy") or
                  (c.get("attributes") or {}).get("source") or
                  (c.get("attributes") or {}).get("createdBy"))
        readable_by = _resolve_by(raw_by)

        # For epoch-0 MICROSOFT.* auto-scan: fall back to asset update time
        applied_at = ms_to_iso(raw_ms) if raw_ms else (
            asset_upd_at if status != "DELETED" else None)

        classifications.append({
            "name":          cn,
            "entity_status": status,
            "applied_at":    applied_at,
            "applied_at_ms": raw_ms,
            "applied_by":    readable_by or ("Automated Scan" if status == "ACTIVE" else None),
            "source":        c.get("source"),
            "confidence":    (c.get("attributes") or {}).get("confidence"),
            "validity_periods": c.get("validityPeriods"),
        })

    active_cls = [c for c in classifications if c["entity_status"] != "DELETED"]

    # ── Sensitivity labels ────────────────────────────────────────────
    sensitivity_labels = [l for l in (col.get("labels") or []) if l]

    # ── Business tags (full group → attribute dict) ───────────────────
    business_tags = {}
    for grp, gvals in (col.get("businessAttributes") or {}).items():
        if isinstance(gvals, dict):
            business_tags[grp] = gvals

    has_cls = bool(active_cls)
    has_lbl = bool(sensitivity_labels)
    has_tag = bool(business_tags)
    is_enriched = has_cls or has_lbl or has_tag

    # Timeframe check (for snapshot summary counters)
    in_window = col_in_timeframe(col, cutoff_ms, a_upd_ms)

    return {
        # ── Identity ─────────────────────────────────────────────────
        "column_number":     col_number,
        "total_columns":     total_cols,
        "column_guid":       col_guid,
        "column_name":       col_name,
        "data_type":         (attr.get("data_type") or attr.get("type") or
                              attr.get("dataType") or attr.get("primitiveType")),
        "description":       attr.get("description") or attr.get("comment"),
        "position":          attr.get("position") or attr.get("ordinalPosition"),
        "is_nullable":       attr.get("isNullable"),
        "is_primary_key":    attr.get("isPrimaryKey") or attr.get("primaryKey"),
        "is_foreign_key":    attr.get("isForeignKey") or attr.get("foreignKey"),
        "is_unique":         attr.get("isUnique"),
        "default_value":     attr.get("defaultValue") or attr.get("default"),
        "max_length":        attr.get("maxLength") or attr.get("length"),
        "precision":         attr.get("precision"),
        "scale":             attr.get("scale"),
        "entity_type_name":  col.get("typeName"),
        "column_status":     col.get("status"),
        "version":           col.get("version"),
        # ── Timestamps ───────────────────────────────────────────────
        "column_created_at":    ms_to_iso(col_crt_ms),
        "column_created_at_ms": col_crt_ms,
        "column_updated_at":    ms_to_iso(col_upd_ms),
        "column_updated_at_ms": col_upd_ms,
        "column_created_by":    col.get("createdBy"),
        "column_updated_by":    col.get("updatedBy") or col.get("modifiedBy"),
        # ── Enrichment flags ─────────────────────────────────────────
        "is_enriched":              is_enriched,
        "in_timeframe_window":      in_window,
        "has_classification":       has_cls,
        "has_sensitivity_label":    has_lbl,
        "has_business_tag":         has_tag,
        "active_classification_count": len(active_cls),
        # ── Classifications (full detail) ─────────────────────────────
        "classifications":          classifications,
        "classification_names":     [c["name"] for c in active_cls],
        # ── Sensitivity labels ────────────────────────────────────────
        "sensitivity_labels":       sensitivity_labels,
        # ── Business tags ─────────────────────────────────────────────
        "business_tags":            business_tags,
    }

# ═══════════════════════════════════════════════════════════════════════
#  WRITE HELPERS
# ═══════════════════════════════════════════════════════════════════════

from pathlib import Path
import json

def write_json(path, data, label):
    try:
        # ✅ Step 4.1: Ensure folder exists
        folder = Path(path).parent

        if not folder.exists():
            folder.mkdir(parents=True, exist_ok=True)
            print(f"[INFO] Created folder: {folder}")

        # ✅ Step 4.2: Write JSON
        with open(path, "w", encoding="utf-8") as f:
            json.dump(data, f, indent=2, ensure_ascii=False)

        # ✅ Step 4.3: Logging
        size = Path(path).stat().st_size
        print(f"[OK]   {label}: {Path(path).name} ({size:,} bytes) [{len(data)} records]")

    except Exception as e:
        print(f"[ERR]  Failed writing {label}: {e}")

# ═══════════════════════════════════════════════════════════════════════
#  MAIN
# ═══════════════════════════════════════════════════════════════════════

def main():
    t0     = time.time()
    run_id = str(uuid.uuid4())
    dt0    = datetime.datetime.now(datetime.timezone.utc)
    ts     = dt0.strftime("%Y-%m-%d %H:%M:%S")
    outdir = Path(JSON_OUTPUT_FOLDER)
    outdir.mkdir(parents=True, exist_ok=True)

    try:
        cutoff_ms, run_label = parse_last_run(_LAST_RUN_RAW)
    except ValueError as e:
        print(f"\n[ERROR] {e}\n"); sys.exit(1)

    log_section("PURVIEW → JSON  |  purview_scan.py")
    log(f"Config        : {_config_path or '(none — using defaults)'}")
    log(f"Account       : {PURVIEW_ACCOUNT}")
    log(f"Output folder : {outdir.resolve()}/")
    log(f"Snapshot file : {SNAPSHOT_FILE}")
    log(f"LAST_RUN      : {run_label}")
    log(f"Workers       : {MAX_WORKERS}")
    log(f"Start         : {ts} UTC")
    if COLLECTIONS_FILTER:
        log(f"Coll filter   : {len(COLLECTIONS_FILTER)} collection(s) specified:")
        for i, cf in enumerate(COLLECTIONS_FILTER, 1):
            log(f"               [{i}] {cf}")
    else:
        log(f"Coll filter   : none — full search across all collections")

    if not CLIENT_SECRET:
        log("PURVIEW_CLIENT_SECRET not set.", "ERROR"); sys.exit(1)

    # ── STEP 1: Collections ───────────────────────────────────────────
    coll_map, coll_paths, coll_items = fetch_collections()
    write_json(outdir / "collections.json", coll_items, "collections")

    # ── STEP 2: Glossary terms ────────────────────────────────────────
    glossary_terms = fetch_glossary_terms()
    write_json(outdir / "glossary_terms.json", glossary_terms, "glossary_terms")

    # ── STEP 3: Assets (search) ───────────────────────────────────────
    all_assets  = fetch_assets()
    leaf_assets = [a for a in all_assets if is_leaf(a)]
    log(f"Leaf: {len(leaf_assets):,}  |  Structural skipped: "
        f"{len(all_assets) - len(leaf_assets):,}")
    write_json(outdir / "assets.json", leaf_assets, "assets")

    guids         = [a["id"] for a in leaf_assets if a.get("id")]
    guid_to_asset = {a["id"]: a for a in leaf_assets if a.get("id")}

    # ── STEP 4: Entities (bulk parallel) ─────────────────────────────
    entity_map = fetch_entities(guids)
    # Write raw entities JSON
    raw_entities = []
    for g, ej in entity_map.items():
        e = ej.get("entity", {})
        raw_entities.append({**e, "guid": g,
                             "_referred_entities_count":
                                 len(ej.get("referredEntities", {}))})
    write_json(outdir / "entities.json", raw_entities, "entities")

    # ── STEP 5: Build snapshot — every asset with every column ────────
    log_section("STEP 5 — Build snapshot (all assets × all columns)")

    snapshot_assets = []
    total_cols_seen = 0
    total_enriched  = 0
    total_cls_count = 0
    total_lbl_count = 0
    total_tag_count = 0
    assets_filtered = 0

    # Per-collection stats — built while processing, printed after the loop
    coll_stats = defaultdict(lambda: {
        "assets": 0, "total_columns": 0, "enriched_columns": 0,
        "classified_columns": 0, "labeled_columns": 0, "tagged_columns": 0,
        "cls_types": set(),
    })

    # Group by datasource → instance for ordered on-screen output
    ds_groups = defaultdict(lambda: defaultdict(list))
    for g in guids:
        a = guid_to_asset.get(g, {})
        ds_groups[ds_type(a.get("entityType", ""))][
            get_instance(a.get("qualifiedName", ""))].append(g)

    for ds, instances in sorted(ds_groups.items()):
        for instance, g_list in sorted(instances.items()):
            for guid in g_list:
                a       = guid_to_asset.get(guid, {})
                ej      = entity_map.get(guid)
                qn      = a.get("qualifiedName", "")
                et      = a.get("entityType", "")
                ot      = a.get("objectType", "")
                coll_id = a.get("collectionId", "")
                coll_nm = coll_map.get(coll_id, "")
                coll_p  = coll_paths.get(coll_id, coll_nm)
                a_name  = short_name(qn)

                # Collection filter (applied after path is fully resolved).
                # Bug fix: if coll_p is empty AND a filter is active, the asset's
                # collection could not be resolved — exclude it rather than silently
                # passing it through, which caused multi-collection runs to show
                # assets that didn't belong to any listed collection.
                if COLLECTIONS_FILTER:
                    if not coll_p or not coll_matches(coll_p):
                        assets_filtered += 1
                        continue

                if not ej:
                    continue

                e_raw    = ej.get("entity", {})
                a_upd_ms = _safe_ms(e_raw.get("updateTime") or
                                    e_raw.get("lastModifiedTS"))
                a_crt_ms = _safe_ms(e_raw.get("createTime") or
                                    e_raw.get("createdTS"))
                a_upd_at = ms_to_iso(a_upd_ms)
                a_crt_at = ms_to_iso(a_crt_ms)
                a_upd_by = e_raw.get("updatedBy") or e_raw.get("modifiedBy")
                a_crt_by = e_raw.get("createdBy")

                # Extract columns (with fallback single-fetch)
                cols = extract_columns(ej)
                if not cols:
                    r2 = api("GET",
                             f"{ENTITY_API}/{guid}"
                             f"?minExtInfo=true&ignoreRelationships=false",
                             label=f"Fallback {guid[:8]}")
                    if r2 and r2.status_code == 200:
                        try:
                            fb   = r2.json()
                            cols = extract_columns(fb)
                            if not a_upd_at:
                                a_upd_ms = _safe_ms(fb.get("entity", {})
                                                      .get("updateTime"))
                                a_upd_at = ms_to_iso(a_upd_ms)
                            if not a_upd_by:
                                a_upd_by = fb.get("entity", {}).get("updatedBy")
                        except Exception: pass

                # Build column detail for EVERY column (not just enriched)
                column_records = []
                asset_cls_set  = set()
                asset_cls_cnt  = 0
                cols_in_log    = []   # for on-screen print

                for col_num, col in enumerate(cols, 1):
                    detail = build_column_detail(
                        col, col_num, len(cols), a_upd_at, cutoff_ms, a_upd_ms)
                    if detail is None:   # CosmosDB system field
                        continue

                    column_records.append(detail)
                    total_cols_seen += 1

                    if detail["has_classification"]:
                        asset_cls_cnt += 1
                        asset_cls_set.update(detail["classification_names"])
                        total_cls_count += len(detail["classification_names"])
                    if detail["has_sensitivity_label"]:
                        total_lbl_count += len(detail["sensitivity_labels"])
                    if detail["has_business_tag"]:
                        total_tag_count += len(detail["business_tags"])
                    if detail["is_enriched"]:
                        total_enriched += 1
                        cols_in_log.append(detail)

                # ── On-screen asset log (same format as before) ────────
                if cols_in_log:
                    with _print_lock:
                        print(f"\n  ASSET  : {qn}")
                        if ot:       print(f"  TYPE   : {ot} | {et}")
                        if coll_p:   print(f"  COLL   : {coll_p}")
                        if a_upd_at: print(f"  UPDATED: {a_upd_at}  "
                                           f"by: {a_upd_by or '?'}")
                        n_e = len(cols_in_log)
                        print(f"  COLS   : {len(column_records)} total  |  "
                              f"{n_e} with enrichment")
                        for d in cols_in_log:
                            rparts = [f"    [{d['column_number']:>4}/{len(cols)}]"
                                      f" {d['column_name']:<35}"]
                            if d["data_type"]:
                                rparts.append(f"type={d['data_type']}")
                            if d["classification_names"]:
                                rparts.append(
                                    f"cls={', '.join(d['classification_names'])}")
                                first_cls = d["classifications"][0]
                                if first_cls.get("applied_at"):
                                    rparts.append(f"applied={first_cls['applied_at']}")
                                if first_cls.get("applied_by"):
                                    rparts.append(f"by={first_cls['applied_by']}")
                            if d["sensitivity_labels"]:
                                rparts.append(
                                    f"lbl={', '.join(d['sensitivity_labels'])}")
                            if d["business_tags"]:
                                flat_tags = [
                                    f"{grp}.{k}={v}"
                                    for grp, gvals in d["business_tags"].items()
                                    for k, v in gvals.items()]
                                rparts.append(f"tags={', '.join(flat_tags)}")
                            print("  ".join(rparts))
                        if asset_cls_set:
                            print(f"  CLASSIFIED: {asset_cls_cnt}/"
                                  f"{len(column_records)} cols  "
                                  f"types: {', '.join(sorted(asset_cls_set))}")

                # ── Build snapshot records — one entry per ENRICHED column ────
                # SNAPSHOT POLICY:
                #   Each record = ONE enriched column, fully self-contained.
                #   By looking at a single snapshot record you can answer:
                #     - What is the column name and type?
                #     - Which table/asset does it belong to?
                #     - Which datasource and instance?
                #     - Which collection and what is the full hierarchy path?
                #     - What classifications are on it, who applied them, when?
                #     - What sensitivity labels are on it?
                #     - What business tags are on it?
                #     - When was the column last updated?
                #   Only enriched columns (cls / label / tag) are written.
                #   Unenriched columns are never written to the snapshot.
                if cols_in_log:   # only process assets that have enriched columns
                    coll_stats[coll_p]["assets"] += 1
                    coll_stats[coll_p]["total_columns"] += len(column_records)
                    coll_stats[coll_p]["enriched_columns"] += len(cols_in_log)
                    coll_stats[coll_p]["classified_columns"] += asset_cls_cnt
                    coll_stats[coll_p]["cls_types"].update(asset_cls_set)
                    for d in cols_in_log:
                        if d["sensitivity_labels"]:
                            coll_stats[coll_p]["labeled_columns"] += 1
                        if d["business_tags"]:
                            coll_stats[coll_p]["tagged_columns"] += 1

                    for d in cols_in_log:
                        snapshot_assets.append({
                            # ── Run context ──────────────────────────────────
                            "scan_run_id":          run_id,
                            "scan_timestamp":       ts,

                            # ── Column identity ───────────────────────────────
                            "column_guid":          d["column_guid"],
                            "column_name":          d["column_name"],
                            "column_number":        d["column_number"],
                            "total_columns_in_asset": len(column_records),
                            "data_type":            d["data_type"],
                            "description":          d["description"],
                            "position":             d["position"],
                            "is_nullable":          d["is_nullable"],
                            "is_primary_key":       d["is_primary_key"],
                            "is_foreign_key":       d["is_foreign_key"],
                            "is_unique":            d["is_unique"],
                            "default_value":        d["default_value"],
                            "max_length":           d["max_length"],
                            "precision":            d["precision"],
                            "scale":                d["scale"],
                            "entity_type_name":     d["entity_type_name"],
                            "column_status":        d["column_status"],

                            # ── Column timestamps ────────────────────────────
                            "column_created_at":    d["column_created_at"],
                            "column_created_by":    d["column_created_by"],
                            "column_updated_at":    d["column_updated_at"],
                            "column_updated_by":    d["column_updated_by"],

                            # ── Parent asset ─────────────────────────────────
                            "asset_guid":                   guid,
                            "asset_name":                   a_name,
                            "asset_qualified_name":         qn,
                            "asset_entity_type":            et,
                            "asset_object_type":            ot,
                            "asset_created_at":             a_crt_at,
                            "asset_created_by":             a_crt_by,
                            "asset_updated_at":             a_upd_at,
                            "asset_updated_by":             a_upd_by,

                            # ── Datasource ───────────────────────────────────
                            "datasource_type":      ds,
                            "datasource_instance":  instance,
                            "schema_path":          get_schema_path(qn),

                            # ── Collection ───────────────────────────────────
                            "collection_id":                coll_id,
                            "collection_name":              coll_nm,
                            "collection_hierarchy_path":    coll_p,

                            # ── Enrichment flags ────────────────────────────
                            "is_enriched":              d["is_enriched"],
                            "has_classification":       d["has_classification"],
                            "has_sensitivity_label":    d["has_sensitivity_label"],
                            "has_business_tag":         d["has_business_tag"],
                            "active_classification_count": d["active_classification_count"],

                            # ── Classifications (full detail per classification)
                            # Each entry: name, applied_at, applied_by, source,
                            #             confidence, entity_status
                            "classifications":          d["classifications"],
                            "classification_names":     d["classification_names"],

                            # ── Sensitivity labels ───────────────────────────
                            "sensitivity_labels":        d["sensitivity_labels"],

                            # ── Business tags ────────────────────────────────
                            # Full group → {attribute: value} dict
                            "business_tags":             d["business_tags"],
                        })

    log(f"  Assets in snapshot : {len(snapshot_assets):,}", "INFO")
    log(f"  Total columns seen : {total_cols_seen:,}", "INFO")
    log(f"  Enriched columns   : {total_enriched:,}", "INFO")
    log(f"  Classifications    : {total_cls_count:,}", "INFO")
    log(f"  Sensitivity labels : {total_lbl_count:,}", "INFO")
    log(f"  Business tags      : {total_tag_count:,}", "INFO")
    if COLLECTIONS_FILTER:
        log(f"  Assets filtered    : {assets_filtered:,}  (outside collection filter)",
            "INFO")

    # ── Collection breakdown log ──────────────────────────────────────
    if coll_stats or COLLECTIONS_FILTER:
        with _print_lock:
            print(f"\n{'='*70}")
            if COLLECTIONS_FILTER:
                print(f"  COLLECTION FILTER RESULTS")
                print(f"  Requested : {len(COLLECTIONS_FILTER)} collection(s)")
                print(f"  Filter    : {_COLL_FILTER_RAW}")
            else:
                print(f"  COLLECTION BREAKDOWN  (full search — all collections with enrichment)")
            print(f"{'='*70}")

            print(f"  {'COLLECTION PATH':<46} {'ASSETS':>6} "
                  f"{'COLS':>6} {'ENRICH':>6} "
                  f"{'CLS':>5} {'LBL':>5} {'TAG':>5}  STATUS")
            print(f"  {'─'*46} {'─'*6} {'─'*6} {'─'*6} {'─'*5} {'─'*5} {'─'*5}  {'─'*10}")

            total_a = total_c = total_e = total_clsc = total_lblc = total_tagc = 0

            if COLLECTIONS_FILTER:
                # Show each REQUESTED collection in the order the user listed them.
                # Mark each as FOUND (has data) or NOT FOUND (0 enriched assets).
                for cf in COLLECTIONS_FILTER:
                    # Find matching coll_stats keys (resolved path may have spaces around /)
                    matched_keys = [k for k in coll_stats
                                    if _norm_path(k) == _norm_path(cf) or
                                    _norm_path(k).startswith(_norm_path(cf))]
                    if matched_keys:
                        for mk in sorted(matched_keys):
                            st      = coll_stats[mk]
                            display = mk[:46]
                            status  = "FOUND"
                            print(f"  {display:<46} {st['assets']:>6} "
                                  f"{st['total_columns']:>6} {st['enriched_columns']:>6} "
                                  f"{st['classified_columns']:>5} "
                                  f"{st['labeled_columns']:>5} "
                                  f"{st['tagged_columns']:>5}  {status}")
                            if st["cls_types"]:
                                cls_line = ", ".join(sorted(st["cls_types"]))
                                while cls_line:
                                    print(f"    cls: {cls_line[:78]}")
                                    cls_line = cls_line[78:]
                            total_a    += st["assets"]
                            total_c    += st["total_columns"]
                            total_e    += st["enriched_columns"]
                            total_clsc += st["classified_columns"]
                            total_lblc += st["labeled_columns"]
                            total_tagc += st["tagged_columns"]
                    else:
                        # Collection was requested but yielded nothing
                        display = cf[:46]
                        print(f"  {display:<46} {'0':>6} {'0':>6} {'0':>6} "
                              f"{'0':>5} {'0':>5} {'0':>5}  NOT FOUND")
            else:
                # Full search — show all collections that have enrichment
                for cpath, st in sorted(coll_stats.items()):
                    display = (cpath or "(no collection)")[:46]
                    print(f"  {display:<46} {st['assets']:>6} "
                          f"{st['total_columns']:>6} {st['enriched_columns']:>6} "
                          f"{st['classified_columns']:>5} "
                          f"{st['labeled_columns']:>5} "
                          f"{st['tagged_columns']:>5}")
                    if st["cls_types"]:
                        cls_line = ", ".join(sorted(st["cls_types"]))
                        while cls_line:
                            print(f"    cls: {cls_line[:78]}")
                            cls_line = cls_line[78:]
                    total_a    += st["assets"]
                    total_c    += st["total_columns"]
                    total_e    += st["enriched_columns"]
                    total_clsc += st["classified_columns"]
                    total_lblc += st["labeled_columns"]
                    total_tagc += st["tagged_columns"]

            print(f"  {'─'*46} {'─'*6} {'─'*6} {'─'*6} {'─'*5} {'─'*5} {'─'*5}")
            print(f"  {'TOTAL':<46} {total_a:>6} "
                  f"{total_c:>6} {total_e:>6} "
                  f"{total_clsc:>5} {total_lblc:>5} {total_tagc:>5}")
            print(f"\n  COLUMN HEADERS:")
            print(f"    ASSETS  = enriched tables/files (have ≥1 classified/labeled/tagged col)")
            print(f"    COLS    = all columns in those assets (total from API)")
            print(f"    ENRICH  = columns with at least one classification, label, or tag")
            print(f"    CLS     = columns with classification applied")
            print(f"    LBL     = columns with sensitivity label applied")
            print(f"    TAG     = columns with business tag applied")
            print(f"    STATUS  = FOUND (has enriched data) | NOT FOUND (no enriched assets)")
            print(f"{'='*70}")

    # ── STEP 6: Write snapshot JSON ───────────────────────────────────
    log_section("STEP 6 — Write snapshot JSON")
    elapsed_so_far = fmt_dur(time.time() - t0)
    end_dt = datetime.datetime.now(datetime.timezone.utc)

    snapshot = {
        "scan_run_id":        run_id,
        "scan_time":          ts,
        "scan_end":           end_dt.strftime("%Y-%m-%d %H:%M:%S"),
        "duration":           elapsed_so_far,
        "config_file":        _config_path,
        "purview_account":    PURVIEW_ACCOUNT,
        "last_run_filter":    run_label,
        "collections_filter": _COLL_FILTER_RAW,

        # ── What is in this file ────────────────────────────────────────
        "snapshot_policy": (
            "ONE RECORD PER ENRICHED COLUMN. Only columns that have at least one "
            "classification, sensitivity label, or business tag are written. "
            "Unenriched columns are never included. "
            "Each record is fully self-contained: column identity + data type + "
            "all timestamps + parent asset + datasource + collection hierarchy + "
            "full classification detail (name, applied_at, applied_by, source, "
            "confidence) + sensitivity labels + business tags. "
            "You can answer every question about a column from a single record."
        ),

        # ── Run summary ─────────────────────────────────────────────────
        "summary": {
            "total_search_assets":         len(all_assets),
            "leaf_assets":                 len(leaf_assets),
            "entities_fetched":            len(entity_map),
            "collections_fetched":         len(coll_items),
            "glossary_terms_fetched":      len(glossary_terms),
            "assets_collection_filtered":  assets_filtered,
            "enriched_assets":             len(set(r["asset_guid"] for r in snapshot_assets)),
            "total_columns_seen":          total_cols_seen,
            "enriched_columns_in_snapshot": len(snapshot_assets),
            "total_classifications":       total_cls_count,
            "total_sensitivity_labels":    total_lbl_count,
            "total_business_tags":         total_tag_count,
            "collections_requested": COLLECTIONS_FILTER or "none (full search)",
            "collections_with_data": sorted(coll_stats.keys()),
            "collections_not_found": (
                [cf for cf in (COLLECTIONS_FILTER or [])
                 if not any(_norm_path(k) == _norm_path(cf) or
                            _norm_path(k).startswith(_norm_path(cf))
                            for k in coll_stats)]
                if COLLECTIONS_FILTER else []
            ),
        },

        # ── Per-collection breakdown ─────────────────────────────────────
        "collection_breakdown": {
            cpath: {
                "enriched_assets":      st["assets"],
                "total_columns":        st["total_columns"],
                "enriched_columns":     st["enriched_columns"],
                "classified_columns":   st["classified_columns"],
                "labeled_columns":      st["labeled_columns"],
                "tagged_columns":       st["tagged_columns"],
                "classification_types": sorted(st["cls_types"]),
            }
            for cpath, st in sorted(coll_stats.items())
        },

        # ── Output files ────────────────────────────────────────────────
        "output_files": {
            "collections":    str(outdir / "collections.json"),
            "glossary_terms": str(outdir / "glossary_terms.json"),
            "assets":         str(outdir / "assets.json"),
            "entities":       str(outdir / "entities.json"),
            "snapshot":       str(outdir / SNAPSHOT_FILE),
        },

        # ── The enriched column records ──────────────────────────────────
        # Each record = one enriched column, fully self-contained.
        # Keys present in every record:
        #   scan_run_id, scan_timestamp
        #   column_guid, column_name, column_number, total_columns_in_asset,
        #   data_type, description, position, is_nullable, is_primary_key,
        #   is_foreign_key, is_unique, default_value, max_length, precision, scale,
        #   entity_type_name, column_status, column_created_at, column_created_by,
        #   column_updated_at, column_updated_by,
        #   asset_guid, asset_name, asset_qualified_name, asset_entity_type,
        #   asset_object_type, asset_created_at, asset_created_by,
        #   asset_updated_at, asset_updated_by,
        #   datasource_type, datasource_instance, schema_path,
        #   collection_id, collection_name, collection_hierarchy_path,
        #   is_enriched, has_classification, has_sensitivity_label, has_business_tag,
        #   active_classification_count,
        #   classifications (list of {name, applied_at, applied_by, source,
        #                             confidence, entity_status}),
        #   classification_names (list of strings),
        #   sensitivity_labels (list of strings),
        #   business_tags ({group: {attr: value}})
        "columns": snapshot_assets,
    }

    snap_path = outdir / SNAPSHOT_FILE
    with open(snap_path, "w", encoding="utf-8") as f:
        json.dump(snapshot, f, indent=2, ensure_ascii=False, default=str)
    size_kb = snap_path.stat().st_size / 1024
    enriched_assets_in_snap = len(set(r["asset_guid"] for r in snapshot_assets))
    log(f"  Written: {snap_path.name}  "
        f"({len(snapshot_assets)} enriched columns across "
        f"{enriched_assets_in_snap} assets,  {size_kb:,.1f} KB)", "OK")

    # ── Summary ───────────────────────────────────────────────────────
    elapsed = fmt_dur(time.time() - t0)
    enriched_assets_count = len(set(r["asset_guid"] for r in snapshot_assets))
    not_found = [cf for cf in (COLLECTIONS_FILTER or [])
                 if not any(_norm_path(k) == _norm_path(cf) or
                            _norm_path(k).startswith(_norm_path(cf))
                            for k in coll_stats)]

    print(f"""
{'='*70}
  COMPLETE — purview_scan.py
{'='*70}
  Scan run ID    : {run_id}
  Start          : {ts} UTC
  End            : {end_dt.strftime('%Y-%m-%d %H:%M:%S')} UTC
  Duration       : {elapsed}
  LAST_RUN       : {run_label}
  ──────────────────────────────────────────────────────
  COLLECTION FILTER
  Requested      : {', '.join(COLLECTIONS_FILTER) if COLLECTIONS_FILTER else 'none (full search across all collections)'}
  Found data     : {', '.join(sorted(coll_stats.keys())) or 'none'}""")
    if not_found:
        print(f"  NOT FOUND      : {', '.join(not_found)}  (no enriched assets in these collections)")
    print(f"""  ──────────────────────────────────────────────────────
  FETCH COUNTS
  Assets searched   : {len(all_assets):>7,}  (search API, all pages)
  Leaf assets       : {len(leaf_assets):>7,}  (structural types excluded)
  Entities fetched  : {len(entity_map):>7,}  (bulk parallel, {MAX_WORKERS} workers)
  Collections       : {len(coll_items):>7,}  (full load)
  Glossary terms    : {len(glossary_terms):>7,}  (full load)
  Coll-filtered out : {assets_filtered:>7,}  (outside collection filter)
  ──────────────────────────────────────────────────────
  SNAPSHOT  →  {snap_path}
  Structure          : one record per enriched column (fully self-contained)
  Total cols seen    : {total_cols_seen:>7,}  (all columns from API)
  Enriched columns   : {len(snapshot_assets):>7,}  (written to snapshot)
  Enriched assets    : {enriched_assets_count:>7,}  (have ≥1 cls/label/tag column)
  Classifications    : {total_cls_count:>7,}
  Sensitivity labels : {total_lbl_count:>7,}
  Business tags      : {total_tag_count:>7,}
  ──────────────────────────────────────────────────────
  SNAPSHOT RECORD FIELDS (every record has all of these):
    column_guid, column_name, data_type, description, position
    column_created_at/by, column_updated_at/by
    asset_guid, asset_name, asset_qualified_name, asset_entity_type
    asset_created_at/by, asset_updated_at/by
    datasource_type, datasource_instance, schema_path
    collection_id, collection_name, collection_hierarchy_path
    classifications  [ name, applied_at, applied_by, source, confidence ]
    sensitivity_labels  [ list of label names ]
    business_tags  {{ group: {{attr: value}} }}
  ──────────────────────────────────────────────────────
  OUTPUT FILES
  collections.json       {len(coll_items):>7,}  {outdir}/collections.json
  glossary_terms.json    {len(glossary_terms):>7,}  {outdir}/glossary_terms.json
  assets.json            {len(leaf_assets):>7,}  {outdir}/assets.json
  entities.json          {len(entity_map):>7,}  {outdir}/entities.json
  {SNAPSHOT_FILE:<23}{len(snapshot_assets):>7,}  {outdir}/{SNAPSHOT_FILE}
  ──────────────────────────────────────────────────────
  Output : {outdir.resolve()}
{'='*70}
""", flush=True)


if __name__ == "__main__":
    main()