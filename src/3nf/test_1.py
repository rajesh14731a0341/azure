"""
purview_final.py
================
5-TABLE MODE  —  data sourced exclusively from raw Purview API JSON dumps.

HOW IT WORKS
────────────
STEP 1-4  Fetch raw data from 4 Purview APIs.
STEP 5    Save raw API responses as JSON files:
            json_output/raw_search_assets.json    ← Search API
            json_output/raw_entities.json         ← Bulk Entity API
            json_output/raw_collections.json      ← Collections API
            json_output/raw_glossary_terms.json   ← Glossary API
STEP 6    Screen log of what was found.
STEP 7    Read those raw JSON files and map every field into SQL MERGE
          statements for the 5 DB tables.
STEP 8    Optionally push all SQL part-files to Azure SQL via sqlcmd.

TABLES (all 5 names REQUIRED in config — script exits if any is missing)
──────
  TBL_ASSET_REGISTRY  → asset_registry
      One row per Purview asset (table/file/container).
      Source: raw_search_assets.json + raw_entities.json
      PK: asset_guid

  TBL_COLLECTIONS     → purview_collections
      One row per Purview collection.
      Source: raw_collections.json
      PK: collection_name

  TBL_ENTITIES        → purview_entities
      One row per entity from the Bulk Entity API.
      Source: raw_entities.json
      PK: guid

  TBL_GLOSSARY        → purview_glossary_terms
      One row per glossary term.
      Source: raw_glossary_terms.json
      PK: guid

  TBL_SEARCH          → purview_search_assets
      One row per asset returned by the Search API.
      Source: raw_search_assets.json
      PK: asset_id

NULL POLICY
───────────
Only fields with real values are written to the DB.  Null / empty fields
are omitted from every MERGE statement — the DB column keeps its DEFAULT.

DUPLICATE POLICY
────────────────
Every table uses MERGE on its PK.  Re-running the same file is safe:
  WHEN MATCHED     → UPDATE non-null fields + last_seen_at = GETUTCDATE()
  WHEN NOT MATCHED → INSERT new row

TIMEFRAME FILTER (LAST_RUN)
────────────────────────────
Applied to Search assets and Entities.  Assets/entities whose updateTime
falls within the window are included.  Collections and Glossary are always
fetched in full regardless of LAST_RUN.

COLLECTIONS_FILTER
──────────────────
Set to 'none' to load all collections.
Or provide comma-separated full hierarchical paths to restrict loading
(e.g. "Finance / Europe, HR / APAC").  Applied during asset processing.
"""

import os, sys, uuid, json, time, hashlib, datetime, requests, shutil, subprocess, re
import configparser, concurrent.futures, threading
from collections import defaultdict
from pathlib import Path

# ═══════════════════════════════════════════════════════════════════════
#  CREDENTIALS
# ═══════════════════════════════════════════════════════════════════════

CLIENT_ID       = "c636fbbb-132d-4be2-9a2d-9f1352cd0e58"
CLIENT_SECRET   = os.environ.get("PURVIEW_CLIENT_SECRET",
                                 "Jg18Q~OgLpY3EtHXU2~qQd4do2RQ~jbxlUfApalR")
TENANT_ID       = "5f9bacc0-ffe8-41f7-8d25-215d55cb0f96"
PURVIEW_ACCOUNT = "finastrapurview"

# ═══════════════════════════════════════════════════════════════════════
#  CONFIG
# ═══════════════════════════════════════════════════════════════════════

_config_path = sys.argv[1] if len(sys.argv) > 1 else "purview_config.ini"
_cfg = configparser.ConfigParser()
_cfg.read(_config_path)

if not _cfg.has_section("PURVIEW"):
    print(f"\n[WARN] '{_config_path}' not found — creating default config.\n")
    Path(_config_path).write_text("""\
# =============================================================
# purview_config.ini  (FINAL — 5-Table Mode)
# =============================================================
[PURVIEW]

# ── Output ────────────────────────────────────────────────────
SCHEMA_NAME        = compliance
SQL_OUTPUT_FOLDER  = sql_output
JSON_OUTPUT_FOLDER = json_output
SNAPSHOT_FILE      = purview_snapshot.json

# ── Collection Filter ─────────────────────────────────────────
# Use 'none' to fetch all collections globally.
# Otherwise, provide a comma-separated list of full hierarchical
# paths. The script will ONLY load assets from these collections.
# Example: Finance / Europe, HR / APAC
COLLECTIONS_FILTER = none

# ── 5 Tables (ALL MANDATORY) ──────────────────────────────────
# The script exits immediately if any of these is missing or blank.
TBL_ASSET_REGISTRY = asset_registry
TBL_COLLECTIONS    = purview_collections
TBL_ENTITIES       = purview_entities
TBL_GLOSSARY       = purview_glossary_terms
TBL_SEARCH         = purview_search_assets

# ── Time window ───────────────────────────────────────────────
# Options: all, 30min, 2hr, 2d, 7d
LAST_RUN = 7d

# ── Run SQL against Azure SQL after generating files ──────────
RUN_SQL_IN_DB = false

# ── Azure SQL connection details ──────────────────────────────
SQL_SERVER   = yourserver.database.windows.net
SQL_DATABASE = yourdb
SQL_USER     = youruser
SQL_PASSWORD = yourpassword
""", encoding="utf-8")
    print(f"Default config written to '{_config_path}'. Edit it and re-run.\n")
    sys.exit(0)


def _get(key, fallback=""):
    return _cfg["PURVIEW"].get(key, fallback).strip()


def _require(key):
    """
    Read a REQUIRED config key.
    If missing or blank the script exits immediately with a clear message.
    All 5 table names are required — no fallback defaults are used.
    """
    val = _cfg["PURVIEW"].get(key, "").strip()
    if not val:
        print(f"\n[ERROR] Required config key '{key}' is missing or blank in '{_config_path}'.")
        print(f"        Add it under [PURVIEW] and re-run.\n")
        print(f"        All 5 table keys are required:")
        for k in ("TBL_ASSET_REGISTRY", "TBL_COLLECTIONS", "TBL_ENTITIES",
                  "TBL_GLOSSARY", "TBL_SEARCH"):
            print(f"          {k} = <your_table_name>")
        print()
        sys.exit(1)
    return val


SCHEMA_NAME        = _get("SCHEMA_NAME",        "compliance")
SQL_OUTPUT_FOLDER  = _get("SQL_OUTPUT_FOLDER",  "sql_output")
JSON_OUTPUT_FOLDER = _get("JSON_OUTPUT_FOLDER", "json_output")
SNAPSHOT_FILE      = _get("SNAPSHOT_FILE",      "purview_snapshot.json")
MAX_ROWS_PER_FILE  = max(1, int(_get("MAX_ROWS_PER_FILE", "500")))
_LAST_RUN_RAW      = _get("LAST_RUN",           "all")
RUN_SQL_IN_DB      = _get("RUN_SQL_IN_DB",      "false").lower() == "true"
SQL_SERVER         = _get("SQL_SERVER",         "")
SQL_DATABASE       = _get("SQL_DATABASE",       "")
SQL_USER           = _get("SQL_USER",           "")
SQL_PASSWORD       = _get("SQL_PASSWORD",       "") or os.environ.get("SQL_PASSWORD", "")

# Collections filter
_COLLECTIONS_FILTER_RAW = _get("COLLECTIONS_FILTER", "none").strip()
COLLECTIONS_FILTER = None
if _COLLECTIONS_FILTER_RAW.lower() != "none" and _COLLECTIONS_FILTER_RAW:
    COLLECTIONS_FILTER = {c.strip().lower()
                          for c in _COLLECTIONS_FILTER_RAW.split(",") if c.strip()}

# ── 5 Table names — ALL REQUIRED — fail immediately if any missing ─────
T_ASSET       = _require("TBL_ASSET_REGISTRY")
T_COLLECTIONS = _require("TBL_COLLECTIONS")
T_ENTITIES    = _require("TBL_ENTITIES")
T_GLOSSARY    = _require("TBL_GLOSSARY")
T_SEARCH      = _require("TBL_SEARCH")

# ═══════════════════════════════════════════════════════════════════════
#  API ENDPOINTS
# ═══════════════════════════════════════════════════════════════════════

SEARCH_API      = f"https://{PURVIEW_ACCOUNT}.purview.azure.com/datamap/api/search/query?api-version=2023-09-01"
ENTITY_API      = f"https://{PURVIEW_ACCOUNT}.purview.azure.com/datamap/api/atlas/v2/entity/guid"
BULK_ENTITY_API = f"https://{PURVIEW_ACCOUNT}.purview.azure.com/datamap/api/atlas/v2/entity/bulk"
COLLECTION_API  = f"https://{PURVIEW_ACCOUNT}.purview.azure.com/account/collections?api-version=2019-11-01-preview"
GLOSSARY_BASE   = f"https://{PURVIEW_ACCOUNT}.purview.azure.com/datamap/api/atlas/v2/glossary"

SEARCH_PAGE_SIZE   = 1000
BATCH_SIZE         = 100
MAX_RETRIES        = 5
RETRY_BACKOFF      = 2
RETRY_STATUS_CODES = {429, 500, 502, 503, 504}
TOKEN_REFRESH_MINS = 50
MAX_WORKERS        = 50
MIN_VALID_MS       = 946_684_800_000   # 2000-01-01T00:00:00Z

STRUCTURAL_OBJECT_TYPES = {
    "Process","Column","Schema","Database","Server","Account",
    "Namespace","Subscription","Queue","ResourceGroup",
    "Tenant","Cluster","Workspace",
}
STRUCTURAL_KINDS = {
    "schema","server","db","database","instance","account","container",
    "folder","namespace","service","warehouse","cluster","catalog",
    "pipeline","workspace","location","subscription","resourcegroup",
    "tenant","filesystem","directory",
}
_PREFIX_TO_DS = {
    "mssql":"SQL Server","azure_sql":"Azure SQL Database","oracle":"Oracle",
    "postgresql":"PostgreSQL","mysql":"MySQL","snowflake":"Snowflake",
    "databricks":"Databricks","azure_blob":"Azure Blob Storage",
    "azure_adls":"Azure Data Lake","azure_datalake":"Azure Data Lake",
    "azure_cosmosdb":"Azure Cosmos DB","azure_cosmos":"Azure Cosmos DB",
    "azure_synapse":"Azure Synapse","teradata":"Teradata",
    "amazon_rds":"Amazon RDS","amazon_s3":"Amazon S3",
    "hive":"Hive","sap_hana":"SAP HANA","sap_ecc":"SAP ECC",
}


def entity_type_to_datasource(et):
    et = et.lower().strip()
    for prefix in sorted(_PREFIX_TO_DS, key=len, reverse=True):
        if et.startswith(prefix): return _PREFIX_TO_DS[prefix]
    return et.replace("_", " ").title()


def get_instance(qn):
    if "://" not in qn:
        return qn.split("/")[0] if "/" in qn else qn[:60]
    after = qn.split("://", 1)[1]
    parts = [p for p in after.split("/") if p]
    if not parts: return qn[:60]
    _struct = {"servers","server","accounts","account","instances","instance",
               "hosts","host","nodes","node","clusters","cluster"}
    if parts[0].lower() in _struct and len(parts) > 1:
        return parts[1]
    return parts[0]


def get_schema_path(qn):
    if "://" in qn:
        segs = [s for s in qn.split("/")[3:] if s]
        if len(segs) >= 2:    return "/".join(segs[:-1])
        elif len(segs) == 1:  return segs[0]
    return None


def short_name(qn):
    parts = [p for p in str(qn).rstrip("/").split("/") if p]
    return parts[-1] if parts else qn


def is_leaf(asset):
    ot = asset.get("objectType", "")
    et = asset.get("entityType", "").lower().strip()
    if ot in STRUCTURAL_OBJECT_TYPES: return False
    if not et: return False
    return et.rsplit("_", 1)[-1] not in STRUCTURAL_KINDS

# ═══════════════════════════════════════════════════════════════════════
#  TIMEFRAME
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
                return cut, f"last {n}{unit}  (after {ms_to_iso(cut)})"
            except ValueError:
                pass
    raise ValueError(f"Invalid LAST_RUN '{raw}'. Use: all | <N>min | <N>hr | <N>d")


def ms_to_iso(ms):
    if ms is None: return None
    try:
        v = int(ms)
        if v < MIN_VALID_MS: return None
        return datetime.datetime.utcfromtimestamp(v / 1000).strftime("%Y-%m-%d %H:%M:%S")
    except (ValueError, TypeError, OSError):
        return None


def _safe_ms(val):
    if val is None: return None
    try:
        v = int(val)
        return v if v >= MIN_VALID_MS else None
    except (ValueError, TypeError):
        return None


def _in_timeframe(epoch_ms_val, cutoff_ms):
    """True if epoch_ms_val >= cutoff_ms, or if cutoff_ms is None."""
    if cutoff_ms is None:
        return True
    v = _safe_ms(epoch_ms_val)
    if v is None:
        return True   # no timestamp → conservative include
    return v >= cutoff_ms

# ═══════════════════════════════════════════════════════════════════════
#  LOGGING
# ═══════════════════════════════════════════════════════════════════════

_print_lock = threading.Lock()


def log(msg, level="INFO"):
    ts   = datetime.datetime.now(datetime.timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
    icon = {"INFO": "[INFO]", "OK": "[OK]  ", "WARN": "[WARN]", "ERROR": "[ERR] "}.get(level, "     ")
    with _print_lock:
        print(f"[{ts}] {icon} {msg}")


def log_section(title):
    with _print_lock:
        print(f"\n{'='*70}\n  {title}\n{'='*70}")


def fmt_dur(s):
    s = int(s); h, r = divmod(s, 3600); m, s = divmod(r, 60)
    return f"{h}h {m}m {s}s" if h else (f"{m}m {s}s" if m else f"{s}s")

# ═══════════════════════════════════════════════════════════════════════
#  AUTH
# ═══════════════════════════════════════════════════════════════════════

_tok, _tok_ts, _tok_lock = None, 0.0, threading.Lock()


def get_token():
    global _tok, _tok_ts
    with _tok_lock:
        if _tok is None or (time.time() - _tok_ts) / 60 >= TOKEN_REFRESH_MINS:
            log(("Refreshing" if _tok else "Fetching") + " auth token...")
            r = requests.post(
                f"https://login.microsoftonline.com/{TENANT_ID}/oauth2/token",
                data={"grant_type": "client_credentials", "client_id": CLIENT_ID,
                      "client_secret": CLIENT_SECRET, "resource": "https://purview.azure.net"},
                timeout=30)
            r.raise_for_status()
            _tok, _tok_ts = r.json()["access_token"], time.time()
            log("Token ready.", "OK")
        return _tok


def hdrs():
    return {"Authorization": f"Bearer {get_token()}", "Content-Type": "application/json"}

# ═══════════════════════════════════════════════════════════════════════
#  HTTP
# ═══════════════════════════════════════════════════════════════════════

def api(method, url, body=None, label=""):
    global _tok
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            h = hdrs()
            r = (requests.get(url, headers=h, timeout=60) if method == "GET"
                 else requests.post(url, headers=h, json=body, timeout=60))
            if r.status_code == 401:
                with _tok_lock: _tok = None
                continue
            if r.status_code in RETRY_STATUS_CODES:
                wait = RETRY_BACKOFF * attempt
                log(f"HTTP {r.status_code} on {label} — retry {attempt}/{MAX_RETRIES} in {wait}s", "WARN")
                time.sleep(wait); continue
            return r
        except (requests.exceptions.ConnectionError,
                requests.exceptions.Timeout,
                requests.exceptions.ChunkedEncodingError) as e:
            log(f"Network error on {label}: {e} — retry {attempt} in {RETRY_BACKOFF*attempt}s", "WARN")
            time.sleep(RETRY_BACKOFF * attempt)
    log(f"Gave up after {MAX_RETRIES} attempts: {label}", "ERROR")
    return None

# ═══════════════════════════════════════════════════════════════════════
#  SQL ESCAPE HELPERS
# ═══════════════════════════════════════════════════════════════════════

def esc(val):
    """Escape to N'...' string literal, or return 'NULL' (never written to DB)."""
    if val is None or val == "" or (isinstance(val, list) and not val):
        return "NULL"
    s = str(val).strip()
    return "NULL" if not s else "N'" + s.replace("'", "''") + "'"


def esc_int(val):
    if val is None: return "NULL"
    try:    return str(int(val))
    except: return "NULL"


def esc_bigint(val):
    if val is None: return "NULL"
    try:    return str(int(val))
    except: return "NULL"


def esc_bit(val):
    if val is None: return "NULL"
    return "1" if val else "0"


def esc_guid(val):
    """Escape a GUID/UUID as NVARCHAR, returns NULL if empty."""
    if not val or str(val).strip().lower() in ("", "none", "null"):
        return "NULL"
    return esc(str(val).strip())


def has_value(val):
    if val is None or val == "": return False
    if isinstance(val, list):    return len(val) > 0
    return True

# ═══════════════════════════════════════════════════════════════════════
#  SMART MERGE BUILDER  (null-suppressing, duplicate-safe)
# ═══════════════════════════════════════════════════════════════════════

def _smart_merge(S, table, pk_col, pk_val_sql, fields_sql):
    """
    Build a MERGE statement from pre-escaped field values.

    pk_val_sql : already-escaped PK value, e.g. N'abc-123'
    fields_sql : dict of {col_name: escaped_sql_value}
                 Any value equal to "NULL" is OMITTED entirely.
    """
    non_null = [(c, v) for c, v in fields_sql.items()
                if c != pk_col and v != "NULL"]

    if not non_null:
        return None   # nothing to write beyond PK — skip row

    insert_cols = [pk_col] + [c for c, _ in non_null]
    insert_vals = [pk_val_sql] + [v for _, v in non_null]
    update_sets = [f"T.[{c}] = {v}" for c, v in non_null]
    update_sets.append("T.[last_seen_at] = GETUTCDATE()")

    return (
        f"MERGE [{S}].[{table}] AS T\n"
        f"USING (SELECT {pk_val_sql} AS [{pk_col}]) AS S ON T.[{pk_col}] = S.[{pk_col}]\n"
        f"WHEN MATCHED THEN UPDATE SET\n"
        f"    {', '.join(update_sets)}\n"
        f"WHEN NOT MATCHED THEN INSERT\n"
        f"    ({', '.join(f'[{c}]' for c in insert_cols)})\n"
        f"    VALUES ({', '.join(insert_vals)});\n"
    )

# ═══════════════════════════════════════════════════════════════════════
#  FETCH: SEARCH ASSETS
# ═══════════════════════════════════════════════════════════════════════

def fetch_search_assets(cutoff_ms):
    """Fetch all assets from the Purview Search API.  Returns raw list."""
    def _pages():
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
            log(f"  Page {page}: {len(batch)} assets  |  running total: {len(items)}", "OK")
            tk = data.get("continuationToken")
            if not tk or not batch: break
            page += 1
        return items

    if cutoff_ms is None:
        log_section("STEP 1 — Fetch ALL search assets (full load)")
    else:
        log_section(f"STEP 1 — Fetch search assets  [after {ms_to_iso(cutoff_ms)}]")
    assets = _pages()
    log(f"  Total assets fetched: {len(assets)}", "OK")
    return assets

# ═══════════════════════════════════════════════════════════════════════
#  FETCH: BULK ENTITIES
# ═══════════════════════════════════════════════════════════════════════

def _one_batch(bn, batch, total):
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
        r2 = api("GET", f"{ENTITY_API}/{g}?minExtInfo=true&ignoreRelationships=false",
                 label=f"Single {g[:8]}")
        if r2 and r2.status_code == 200:
            try:
                raw = r2.json()
                out[g] = {"entity": raw.get("entity", {}),
                          "referredEntities": raw.get("referredEntities", {})}
            except Exception: pass
    return out


def fetch_entities(guids):
    log_section(f"STEP 2 — Bulk entity fetch  ({len(guids)} GUIDs, {MAX_WORKERS} workers)")
    total   = max(1, (len(guids) + BATCH_SIZE - 1) // BATCH_SIZE)
    batches = [(i // BATCH_SIZE + 1, guids[i:i+BATCH_SIZE], total)
               for i in range(0, len(guids), BATCH_SIZE)]
    results = {}
    with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_WORKERS) as ex:
        futs = {ex.submit(_one_batch, b[0], b[1], b[2]): b[0] for b in batches}
        for f in concurrent.futures.as_completed(futs):
            try:    results.update(f.result())
            except Exception as e: log(f"Batch future error: {e}", "WARN")
    log(f"Entities fetched: {len(results)}", "OK")
    return results

# ═══════════════════════════════════════════════════════════════════════
#  FETCH: COLLECTIONS
# ═══════════════════════════════════════════════════════════════════════

def fetch_collections():
    log_section("STEP 3 — Collections")
    r = api("GET", COLLECTION_API, label="Collections")
    if r is None or r.status_code != 200:
        log("Collections API failed.", "WARN"); return {}, [], {}
    items      = r.json().get("value", [])
    log(f"Collections fetched: {len(items)}", "OK")
    coll_map   = {c.get("name", ""): c.get("friendlyName", "") for c in items}
    by_id      = {c.get("name", ""): c for c in items}

    def _path(cid, seen=None):
        seen = seen or set()
        if cid in seen or cid not in by_id: return ""
        seen.add(cid)
        c = by_id[cid]
        p = c.get("parentCollection", {}).get("referenceName", "")
        n = c.get("friendlyName") or c.get("name", "")
        if p and p in by_id and p != cid:
            pp = _path(p, seen)
            return f"{pp} / {n}" if pp else n
        return n

    coll_paths = {cid: _path(cid) for cid in by_id}
    return coll_map, items, coll_paths

# ═══════════════════════════════════════════════════════════════════════
#  FETCH: GLOSSARY TERMS
# ═══════════════════════════════════════════════════════════════════════

def _glossary_terms_for_guid(g_guid, g_name):
    terms, offset, limit = [], 0, 1000
    while True:
        url = f"{GLOSSARY_BASE}/{g_guid}/terms?limit={limit}&offset={offset}&sort=ASC"
        r   = api("GET", url, label=f"Glossary({g_name}) offset={offset}")
        if r is None or r.status_code != 200:
            log(f"Glossary({g_name}) failed at offset {offset}", "WARN"); break
        try:    raw = r.json()
        except Exception as ex:
            log(f"Glossary({g_name}) parse error: {ex}", "WARN"); break
        if isinstance(raw, list):   batch = raw
        elif isinstance(raw, dict): batch = raw.get("value", raw.get("entities", []))
        else:                       batch = []
        if not batch: break
        terms.extend(batch)
        log(f"  Glossary({g_name}) offset={offset}: {len(batch)} terms", "OK")
        if len(batch) < limit: break
        offset += limit
    return terms


def fetch_glossary():
    log_section("STEP 4 — Glossary terms")
    all_terms  = []
    r          = api("GET", f"{GLOSSARY_BASE}?limit=100&offset=0", label="Glossary list")
    glossaries = []
    if r is not None and r.status_code == 200:
        try:
            raw = r.json()
            if isinstance(raw, list):
                glossaries = raw
            elif isinstance(raw, dict):
                glossaries = raw.get("value") or raw.get("entities") or ([raw] if raw.get("guid") else [])
            log(f"Glossaries found: {len(glossaries)}", "OK")
        except Exception as ex:
            log(f"Glossary list parse error: {ex}", "WARN")
    else:
        log("Glossary list failed — trying /glossary/detailed fallback", "WARN")

    if glossaries:
        for g in glossaries:
            g_guid = g.get("guid", "")
            g_name = g.get("name") or g.get("qualifiedName") or g_guid[:8]
            if not g_guid: continue
            batch = _glossary_terms_for_guid(g_guid, g_name)
            all_terms.extend(batch)
            log(f"  Glossary '{g_name}': {len(batch)} terms", "OK")
    else:
        log("No glossaries via list — trying /glossary/detailed fallback", "INFO")
        fb = api("GET", f"{GLOSSARY_BASE}/detailed", label="Glossary detailed")
        if fb and fb.status_code == 200:
            try:
                raw2      = fb.json()
                candidate = raw2.get("terms", raw2.get("termInfo", [])) if isinstance(raw2, dict) else raw2
                all_terms = list(candidate.values()) if isinstance(candidate, dict) else (candidate or [])
                log(f"Fallback /glossary/detailed: {len(all_terms)} terms", "OK")
            except Exception as ex:
                log(f"Glossary detailed parse error: {ex}", "WARN")

    log(f"Glossary total: {len(all_terms)}", "OK")
    return all_terms

# ═══════════════════════════════════════════════════════════════════════
#  SAVE RAW JSON FILES
#  These 4 files are the single source of truth for all 5 DB tables.
#  Each file is completely overwritten on every run.
# ═══════════════════════════════════════════════════════════════════════

def save_raw_json(json_dir, scan_run_id, scan_ts, run_label,
                  all_assets, entity_map, coll_items, gloss_items):
    json_dir = Path(json_dir)
    json_dir.mkdir(parents=True, exist_ok=True)

    def _write(filename, data, label):
        p = json_dir / filename
        with open(p, "w", encoding="utf-8") as f:
            json.dump(data, f, indent=2, ensure_ascii=False, default=str)
        log(f"  Raw dump: {p.name}  ({p.stat().st_size:,} bytes)  [{label}]", "OK")
        return p

    _write("raw_search_assets.json", {
        "purview_account": PURVIEW_ACCOUNT, "api_endpoint": SEARCH_API,
        "fetched_at": scan_ts, "scan_run_id": scan_run_id,
        "last_run_filter": run_label, "total_count": len(all_assets),
        "assets": all_assets,
    }, f"{len(all_assets)} assets")

    _write("raw_entities.json", {
        "purview_account": PURVIEW_ACCOUNT, "api_endpoint": BULK_ENTITY_API,
        "fetched_at": scan_ts, "scan_run_id": scan_run_id,
        "total_fetched": len(entity_map),
        "entities": [
            {"guid": g, "entity": v.get("entity", {}),
             "referredEntities": v.get("referredEntities", {})}
            for g, v in entity_map.items()
        ],
    }, f"{len(entity_map)} entities")

    _write("raw_collections.json", {
        "purview_account": PURVIEW_ACCOUNT, "api_endpoint": COLLECTION_API,
        "fetched_at": scan_ts, "scan_run_id": scan_run_id,
        "total_count": len(coll_items), "collections": coll_items,
    }, f"{len(coll_items)} collections")

    _write("raw_glossary_terms.json", {
        "purview_account": PURVIEW_ACCOUNT,
        "api_endpoint": f"{GLOSSARY_BASE}/{{guid}}/terms",
        "fetched_at": scan_ts, "scan_run_id": scan_run_id,
        "total_count": len(gloss_items), "terms": gloss_items,
    }, f"{len(gloss_items)} terms")

    log(f"  All raw JSON files written to: {json_dir.resolve()}", "OK")

# ═══════════════════════════════════════════════════════════════════════
#  ROW BUILDERS
#  Each function reads from raw JSON and returns (pk_val_sql, fields_sql)
#  where every value is already a valid escaped SQL literal.
#  Returns None if the row should be skipped.
# ═══════════════════════════════════════════════════════════════════════

def _row_asset_registry(search_asset, entity_payload, coll_paths,
                        scan_run_id, scan_ts, cutoff_ms):
    """
    Source: raw_search_assets.json asset + raw_entities.json payload.
    Maps to asset_registry table columns.
    """
    guid = search_asset.get("id") or search_asset.get("guid", "")
    if not guid:
        return None

    # Timeframe filter
    if not _in_timeframe(
        search_asset.get("updateTime") or search_asset.get("lastModifiedTS"),
        cutoff_ms
    ):
        return None

    qn      = search_asset.get("qualifiedName", "")
    name    = search_asset.get("name") or search_asset.get("displayText") or short_name(qn)
    et      = search_asset.get("entityType", "")
    ot      = search_asset.get("objectType", "")
    coll_id = search_asset.get("collectionId", "")
    coll_raw = search_asset.get("collection", {})
    coll_nm  = coll_raw.get("name", "") if isinstance(coll_raw, dict) else ""
    coll_p   = coll_paths.get(coll_id, coll_nm)
    ds_type  = entity_type_to_datasource(et) if et else None
    sch_p    = get_schema_path(qn) if qn else None
    inst     = get_instance(qn) if qn else None

    # Collections filter
    if COLLECTIONS_FILTER:
        path_lower = (coll_p or coll_nm or "").lower()
        if not any(f in path_lower for f in COLLECTIONS_FILTER):
            return None

    # Entity enrichment
    crt_at = crt_by = upd_at = upd_by = None
    total_c = cls_c = 0
    cls_set = set()

    if entity_payload:
        e_obj  = entity_payload.get("entity", {})
        attrs  = e_obj.get("attributes", {})
        upd_at = ms_to_iso(e_obj.get("updateTime") or e_obj.get("lastModifiedTS"))
        upd_by = e_obj.get("updatedBy") or e_obj.get("modifiedBy") or None
        crt_at = ms_to_iso(e_obj.get("createTime") or e_obj.get("createdTS"))
        crt_by = e_obj.get("createdBy") or attrs.get("owner") or None
        for ref_obj in entity_payload.get("referredEntities", {}).values():
            tn = ref_obj.get("typeName", "").lower()
            if any(x in tn for x in ("column", "field", "attribute")):
                total_c += 1
                active = [c for c in (ref_obj.get("classifications") or [])
                          if c.get("entityStatus", "ACTIVE") != "DELETED" and c.get("typeName")]
                if active:
                    cls_c += 1
                    for c in active: cls_set.add(c["typeName"])

    cls_csv = ", ".join(sorted(cls_set)) if cls_set else None
    pk_sql  = esc(guid)

    return pk_sql, {
        "asset_guid":                  pk_sql,
        "asset_name":                  esc(name),
        "asset_qualified_name":        esc(qn),
        "asset_entity_type":           esc(et),
        "asset_object_type":           esc(ot),
        "datasource_type":             esc(ds_type),
        "datasource_instance":         esc(inst),
        "schema_path":                 esc(sch_p),
        "collection_id":               esc(coll_id),
        "collection_name":             esc(coll_nm),
        "collection_hierarchy_path":   esc(coll_p),
        "total_columns":               esc_int(total_c) if total_c else "NULL",
        "total_classified_columns":    esc_int(cls_c) if cls_c else "NULL",
        "has_classified_columns":      esc_bit(cls_c > 0),
        "classification_types_found":  esc(cls_csv),
        "asset_created_at":            esc(crt_at),
        "asset_created_by":            esc(crt_by),
        "asset_last_updated_at":       esc(upd_at),
        "asset_last_updated_by":       esc(upd_by),
        "scan_run_id":                 esc(scan_run_id),
        "scan_timestamp":              esc(scan_ts),
        "scan_status":                 esc("Scanned"),
    }


def _row_collections(coll_item, scan_run_id, scan_ts):
    """Source: raw_collections.json.  Maps to purview_collections."""
    coll_name = coll_item.get("name", "")
    if not coll_name:
        return None

    sys_data = coll_item.get("systemData") or {}
    pk_sql   = esc(coll_name)

    return pk_sql, {
        "collection_name":                pk_sql,
        "friendly_name":                  esc(coll_item.get("friendlyName")),
        "description":                    esc(coll_item.get("description")),
        "parent_collection_name":         esc((coll_item.get("parentCollection") or {}).get("referenceName")),
        "created_by":                     esc(sys_data.get("createdBy")),
        "created_by_type":                esc(sys_data.get("createdByType")),
        "created_at":                     esc(sys_data.get("createdAt")),
        "last_modified_by":               esc(sys_data.get("lastModifiedBy")),
        "last_modified_by_type":          esc(sys_data.get("lastModifiedByType")),
        "last_modified_at":               esc(sys_data.get("lastModifiedAt")),
        "collection_provisioning_state":  esc(coll_item.get("collectionProvisioningState")),
        "purview_account":                esc(PURVIEW_ACCOUNT),
        "api_endpoint":                   esc(COLLECTION_API),
        "fetched_at":                     esc(scan_ts),
        "scan_run_id":                    esc(scan_run_id),
    }


def _row_entities(guid, entity_payload, scan_run_id, scan_ts):
    """Source: raw_entities.json.  Maps to purview_entities."""
    if not guid:
        return None

    e_obj  = entity_payload.get("entity", {})
    attrs  = e_obj.get("attributes", {})
    qn     = attrs.get("qualifiedName") or e_obj.get("qualifiedName", "")
    name   = attrs.get("name") or short_name(qn)
    pk_sql = esc(guid)

    return pk_sql, {
        "guid":               pk_sql,
        "entity_type_name":   esc(e_obj.get("typeName")),
        "entity_name":        esc(name),
        "qualified_name":     esc(qn),
        "owner_name":         esc(attrs.get("owner")),
        "modified_time":      esc_bigint(attrs.get("modifiedTime")),
        "total_size_bytes":   esc_bigint(attrs.get("size") or attrs.get("totalSizeBytes")),
        "partition_count":    esc_int(attrs.get("partitionCount")),
        "schema_count":       esc_int(attrs.get("schemaCount")),
        "last_modified_ts":   esc(str(e_obj.get("lastModifiedTS", "")) or None),
        "is_incomplete":      esc_bit(e_obj.get("isIncomplete")),
        "provenance_type":    esc_int(e_obj.get("provenanceType")),
        "status":             esc(e_obj.get("status")),
        "created_by":         esc(e_obj.get("createdBy")),
        "updated_by":         esc(e_obj.get("updatedBy")),
        "create_time_epoch":  esc_bigint(e_obj.get("createTime")),
        "update_time_epoch":  esc_bigint(e_obj.get("updateTime")),
        "version_no":         esc_int(e_obj.get("version")),
        "is_indexed":         esc_bit(e_obj.get("isIndexed")),
        "source_name":        esc(e_obj.get("source") or attrs.get("sourceName")),
        "scan_resource_id":   esc(attrs.get("scanResourceId")),
        "collection_id":      esc(e_obj.get("collectionId")),
        "domain_id":          esc(e_obj.get("domainId")),
        "display_text":       esc(e_obj.get("displayText")),
        "proxy_flag":         esc_bit(e_obj.get("proxy")),
        "fetched_at":         esc(scan_ts),
        "scan_run_id":        esc(scan_run_id),
    }


def _row_glossary(term, scan_run_id, scan_ts):
    """Source: raw_glossary_terms.json.  Maps to purview_glossary_terms."""
    guid = term.get("guid", "")
    if not guid:
        return None

    anchor       = term.get("anchor") or {}
    glossary_guid = anchor.get("glossaryGuid") or anchor.get("guid") or None

    related  = term.get("antonyms") or term.get("isA") or []
    rel_guid = None
    if related and isinstance(related, list) and isinstance(related[0], dict):
        rel_guid = related[0].get("termGuid") or related[0].get("guid")

    synonyms = term.get("seeAlso") or term.get("synonyms") or []
    syn_text = None
    if synonyms and isinstance(synonyms, list) and isinstance(synonyms[0], dict):
        syn_text = synonyms[0].get("displayText")

    pk_sql = esc(guid)

    return pk_sql, {
        "guid":                  pk_sql,
        "qualified_name":        esc(term.get("qualifiedName") or term.get("name")),
        "term_name":             esc(term.get("name")),
        "long_description":      esc(term.get("longDescription") or term.get("shortDescription")),
        "last_modified_ts":      esc(str(term.get("lastModifiedTS", "")) or None),
        "created_by":            esc(term.get("createdBy")),
        "updated_by":            esc(term.get("updatedBy")),
        "create_time_epoch":     esc_bigint(term.get("createTime")),
        "update_time_epoch":     esc_bigint(term.get("updateTime")),
        "domain_id":             esc(term.get("domainId")),
        "abbreviation":          esc(term.get("abbreviation")),
        "status":                esc(term.get("status")),
        "glossary_guid":         esc_guid(glossary_guid),
        "relation_guid":         esc_guid(rel_guid),
        "synonym_display_text":  esc(syn_text),
        "fetched_at":            esc(scan_ts),
        "scan_run_id":           esc(scan_run_id),
    }


def _row_search(asset, scan_run_id, scan_ts, cutoff_ms):
    """Source: raw_search_assets.json.  Maps to purview_search_assets."""
    asset_id = asset.get("id") or asset.get("guid", "")
    if not asset_id:
        return None

    if not _in_timeframe(
        asset.get("updateTime") or asset.get("lastModifiedTS"), cutoff_ms
    ):
        return None

    asset_type_raw = asset.get("assetType")
    if isinstance(asset_type_raw, list):
        asset_type_str = ", ".join(str(x) for x in asset_type_raw if x)
    else:
        asset_type_str = str(asset_type_raw) if asset_type_raw else None

    pk_sql = esc(asset_id)

    return pk_sql, {
        "asset_id":           pk_sql,
        "asset_name":         esc(asset.get("name") or asset.get("displayText")),
        "display_text":       esc(asset.get("displayText")),
        "qualified_name":     esc(asset.get("qualifiedName")),
        "entity_type":        esc(asset.get("entityType")),
        "object_type":        esc(asset.get("objectType")),
        "description":        esc(asset.get("description")),
        "collection_id":      esc(asset.get("collectionId")),
        "domain_id":          esc(asset.get("domainId")),
        "create_by":          esc(asset.get("createBy") or asset.get("createdBy")),
        "update_by":          esc(asset.get("updateBy") or asset.get("updatedBy")),
        "create_time_epoch":  esc_bigint(asset.get("createTime")),
        "update_time_epoch":  esc_bigint(asset.get("updateTime") or asset.get("lastModifiedTS")),
        "is_indexed":         esc_bit(asset.get("isIndexed")),
        "asset_type":         esc(asset_type_str),
        "search_score":       "NULL",   # not available in standard Search API response
        "fetched_at":         esc(scan_ts),
        "scan_run_id":        esc(scan_run_id),
    }

# ═══════════════════════════════════════════════════════════════════════
#  DDL — 5 TABLES
#  Matches the exact schema from the provided CREATE TABLE statements.
#  GUIDs stored as NVARCHAR (not UNIQUEIDENTIFIER) for Purview compatibility.
# ═══════════════════════════════════════════════════════════════════════

def _schema_ddl(S):
    return (
        f"IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = N'{S}')\n"
        f"    EXEC('CREATE SCHEMA [{S}]');\nGO\n\n"
    )


def _asset_registry_ddl(S, tbl):
    return f"""\
IF OBJECT_ID(N'[{S}].[{tbl}]', N'U') IS NULL
BEGIN
CREATE TABLE [{S}].[{tbl}] (
    asset_guid                   NVARCHAR(64)    NOT NULL,
    asset_name                   NVARCHAR(255)   NULL,
    asset_qualified_name         NVARCHAR(MAX)   NULL,
    asset_entity_type            NVARCHAR(100)   NULL,
    asset_object_type            NVARCHAR(50)    NULL,
    datasource_type              NVARCHAR(100)   NULL,
    datasource_instance          NVARCHAR(500)   NULL,
    schema_path                  NVARCHAR(500)   NULL,
    collection_id                NVARCHAR(50)    NULL,
    collection_name              NVARCHAR(255)   NULL,
    collection_hierarchy_path    NVARCHAR(1000)  NULL,
    total_columns                INT             NULL,
    total_classified_columns     INT             NULL,
    has_classified_columns       BIT             NULL,
    classification_types_found   NVARCHAR(1000)  NULL,
    asset_created_at             NVARCHAR(30)    NULL,
    asset_created_by             NVARCHAR(255)   NULL,
    asset_last_updated_at        NVARCHAR(30)    NULL,
    asset_last_updated_by        NVARCHAR(255)   NULL,
    scan_run_id                  NVARCHAR(36)    NULL,
    scan_timestamp               NVARCHAR(30)    NULL,
    scan_status                  NVARCHAR(100)   NULL,
    first_seen_at                DATETIME2       NOT NULL DEFAULT GETUTCDATE(),
    last_seen_at                 DATETIME2       NOT NULL DEFAULT GETUTCDATE(),
    CONSTRAINT PK_{tbl} PRIMARY KEY (asset_guid)
);
PRINT '{tbl} created.';
END
ELSE PRINT '{tbl} already exists — skipping DDL.';
GO

"""


def _collections_ddl(S, tbl):
    return f"""\
IF OBJECT_ID(N'[{S}].[{tbl}]', N'U') IS NULL
BEGIN
CREATE TABLE [{S}].[{tbl}] (
    collection_name                  NVARCHAR(100)  NOT NULL,
    friendly_name                    NVARCHAR(255)  NULL,
    description                      NVARCHAR(MAX)  NULL,
    parent_collection_name           NVARCHAR(100)  NULL,
    created_by                       NVARCHAR(100)  NULL,
    created_by_type                  NVARCHAR(50)   NULL,
    created_at                       NVARCHAR(50)   NULL,
    last_modified_by                 NVARCHAR(100)  NULL,
    last_modified_by_type            NVARCHAR(50)   NULL,
    last_modified_at                 NVARCHAR(50)   NULL,
    collection_provisioning_state    NVARCHAR(50)   NULL,
    purview_account                  NVARCHAR(100)  NULL,
    api_endpoint                     NVARCHAR(MAX)  NULL,
    fetched_at                       NVARCHAR(30)   NULL,
    scan_run_id                      NVARCHAR(36)   NULL,
    first_seen_at                    DATETIME2      NOT NULL DEFAULT GETUTCDATE(),
    last_seen_at                     DATETIME2      NOT NULL DEFAULT GETUTCDATE(),
    CONSTRAINT PK_{tbl} PRIMARY KEY (collection_name)
);
PRINT '{tbl} created.';
END
ELSE PRINT '{tbl} already exists — skipping DDL.';
GO

"""


def _entities_ddl(S, tbl):
    return f"""\
IF OBJECT_ID(N'[{S}].[{tbl}]', N'U') IS NULL
BEGIN
CREATE TABLE [{S}].[{tbl}] (
    guid                  NVARCHAR(64)   NOT NULL,
    entity_type_name      NVARCHAR(100)  NULL,
    entity_name           NVARCHAR(255)  NULL,
    qualified_name        NVARCHAR(MAX)  NULL,
    owner_name            NVARCHAR(255)  NULL,
    modified_time         BIGINT         NULL,
    total_size_bytes      BIGINT         NULL,
    partition_count       INT            NULL,
    schema_count          INT            NULL,
    last_modified_ts      NVARCHAR(20)   NULL,
    is_incomplete         BIT            NULL,
    provenance_type       INT            NULL,
    status                NVARCHAR(50)   NULL,
    created_by            NVARCHAR(255)  NULL,
    updated_by            NVARCHAR(255)  NULL,
    create_time_epoch     BIGINT         NULL,
    update_time_epoch     BIGINT         NULL,
    version_no            INT            NULL,
    is_indexed            BIT            NULL,
    source_name           NVARCHAR(100)  NULL,
    scan_resource_id      NVARCHAR(500)  NULL,
    collection_id         NVARCHAR(100)  NULL,
    domain_id             NVARCHAR(100)  NULL,
    display_text          NVARCHAR(255)  NULL,
    proxy_flag            BIT            NULL,
    fetched_at            NVARCHAR(30)   NULL,
    scan_run_id           NVARCHAR(36)   NULL,
    first_seen_at         DATETIME2      NOT NULL DEFAULT GETUTCDATE(),
    last_seen_at          DATETIME2      NOT NULL DEFAULT GETUTCDATE(),
    CONSTRAINT PK_{tbl} PRIMARY KEY (guid)
);
PRINT '{tbl} created.';
END
ELSE PRINT '{tbl} already exists — skipping DDL.';
GO

"""


def _glossary_ddl(S, tbl):
    return f"""\
IF OBJECT_ID(N'[{S}].[{tbl}]', N'U') IS NULL
BEGIN
CREATE TABLE [{S}].[{tbl}] (
    guid                   NVARCHAR(64)   NOT NULL,
    qualified_name         NVARCHAR(500)  NULL,
    term_name              NVARCHAR(255)  NULL,
    long_description       NVARCHAR(MAX)  NULL,
    last_modified_ts       NVARCHAR(20)   NULL,
    created_by             NVARCHAR(100)  NULL,
    updated_by             NVARCHAR(100)  NULL,
    create_time_epoch      BIGINT         NULL,
    update_time_epoch      BIGINT         NULL,
    domain_id              NVARCHAR(100)  NULL,
    abbreviation           NVARCHAR(500)  NULL,
    status                 NVARCHAR(50)   NULL,
    glossary_guid          NVARCHAR(64)   NULL,
    relation_guid          NVARCHAR(64)   NULL,
    synonym_display_text   NVARCHAR(255)  NULL,
    fetched_at             NVARCHAR(30)   NULL,
    scan_run_id            NVARCHAR(36)   NULL,
    first_seen_at          DATETIME2      NOT NULL DEFAULT GETUTCDATE(),
    last_seen_at           DATETIME2      NOT NULL DEFAULT GETUTCDATE(),
    CONSTRAINT PK_{tbl} PRIMARY KEY (guid)
);
PRINT '{tbl} created.';
END
ELSE PRINT '{tbl} already exists — skipping DDL.';
GO

"""


def _search_ddl(S, tbl):
    return f"""\
IF OBJECT_ID(N'[{S}].[{tbl}]', N'U') IS NULL
BEGIN
CREATE TABLE [{S}].[{tbl}] (
    asset_id              NVARCHAR(64)    NOT NULL,
    asset_name            NVARCHAR(255)   NULL,
    display_text          NVARCHAR(255)   NULL,
    qualified_name        NVARCHAR(MAX)   NULL,
    entity_type           NVARCHAR(100)   NULL,
    object_type           NVARCHAR(100)   NULL,
    description           NVARCHAR(MAX)   NULL,
    collection_id         NVARCHAR(100)   NULL,
    domain_id             NVARCHAR(100)   NULL,
    create_by             NVARCHAR(255)   NULL,
    update_by             NVARCHAR(255)   NULL,
    create_time_epoch     BIGINT          NULL,
    update_time_epoch     BIGINT          NULL,
    is_indexed            BIT             NULL,
    asset_type            NVARCHAR(255)   NULL,
    search_score          DECIMAL(10,4)   NULL,
    fetched_at            NVARCHAR(30)    NULL,
    scan_run_id           NVARCHAR(36)    NULL,
    first_seen_at         DATETIME2       NOT NULL DEFAULT GETUTCDATE(),
    last_seen_at          DATETIME2       NOT NULL DEFAULT GETUTCDATE(),
    CONSTRAINT PK_{tbl} PRIMARY KEY (asset_id)
);
PRINT '{tbl} created.';
END
ELSE PRINT '{tbl} already exists — skipping DDL.';
GO

"""

# ═══════════════════════════════════════════════════════════════════════
#  WRITE SQL FILES
#  One subfolder per table, part-files of MAX_ROWS_PER_FILE rows each.
#  Data comes exclusively from the raw JSON structures.
# ═══════════════════════════════════════════════════════════════════════

def _write_table_sql(out_dir, S, ts, scan_run_id,
                     tbl_name, ddl_fn, pk_col, rows_iter, label):
    """
    Generic writer: iterate rows_iter → _smart_merge per row → part-files.
    rows_iter must yield (pk_val_sql, fields_sql) tuples or None (skip).
    Returns list of written Path objects.
    """
    tbl_dir = Path(out_dir) / tbl_name
    tbl_dir.mkdir(parents=True, exist_ok=True)

    stmts   = []
    skipped = 0

    for item in rows_iter:
        if item is None:
            skipped += 1
            continue
        pk_sql, fields_sql = item
        sql = _smart_merge(S, tbl_name, pk_col, pk_sql, fields_sql)
        if sql:
            stmts.append(f"-- {label}: {pk_sql}\n{sql}")
        else:
            skipped += 1

    if skipped:
        log(f"  {tbl_name}: {skipped} row(s) skipped (no data / outside timeframe / no PK)", "INFO")

    chunks      = [stmts[i:i+MAX_ROWS_PER_FILE]
                   for i in range(0, max(1, len(stmts)), MAX_ROWS_PER_FILE)]
    if not chunks: chunks = [[]]
    total_parts = len(chunks)
    written     = []

    for part_idx, chunk in enumerate(chunks, 1):
        fname = f"{tbl_name}_part{part_idx}.sql"
        p     = tbl_dir / fname

        if part_idx == 1:
            hdr = (
                f"-- ================================================================\n"
                f"-- TABLE : [{S}].[{tbl_name}]\n"
                f"-- PART  : {part_idx} of {total_parts}   ROWS: {len(stmts)} total\n"
                f"-- RUN   : {scan_run_id}   TIME: {ts}\n"
                f"-- SOURCE: raw JSON files in {JSON_OUTPUT_FOLDER}/\n"
                f"-- NULL  : only non-null values written — DB keeps its DEFAULT\n"
                f"-- MERGE : on PK [{pk_col}] — safe to re-run (no duplicates)\n"
                f"-- ================================================================\n\n"
                f"SET NOCOUNT ON;\nGO\n\n"
                f"{_schema_ddl(S)}"
                f"{ddl_fn(S, tbl_name)}"
            )
        else:
            hdr = (
                f"-- ================================================================\n"
                f"-- TABLE : [{S}].[{tbl_name}]   Part {part_idx} of {total_parts}\n"
                f"-- RUN   : {scan_run_id}   TIME: {ts}\n"
                f"-- Data rows only — DDL is in part 1.\n"
                f"-- ================================================================\n\n"
                f"SET NOCOUNT ON;\nGO\n\n"
            )

        trail = (
            f"\nGO\n"
            f"PRINT '{tbl_name} part {part_idx}/{total_parts} — {len(chunk)} rows loaded.';\nGO\n"
        )
        with open(p, "w", encoding="utf-8") as f:
            f.write(hdr + "".join(chunk) + trail)
        written.append(p)

    log(f"  {tbl_name}: {len(stmts)} rows → {total_parts} part-file(s) in {tbl_dir.name}/", "OK")
    return written


def write_all_sql_files(out_dir, S, ts, scan_run_id, scan_ts,
                        all_assets, entity_map, coll_items, coll_paths,
                        gloss_items, cutoff_ms):
    """
    Build and write SQL files for all 5 tables from the raw JSON data.
    Returns (all_files_list, ordered_files_list).
    """
    out_dir = Path(out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    log(f"  Output folder: {out_dir.resolve()}", "OK")

    all_files = []

    # ── 1. asset_registry ─────────────────────────────────────────────
    # Source: raw_search_assets.json + raw_entities.json
    all_files += _write_table_sql(
        out_dir, S, ts, scan_run_id,
        T_ASSET, _asset_registry_ddl, "asset_guid",
        (
            _row_asset_registry(a, entity_map.get(a.get("id") or a.get("guid", "")),
                                coll_paths, scan_run_id, scan_ts, cutoff_ms)
            for a in all_assets
        ),
        "asset")

    # ── 2. purview_collections ─────────────────────────────────────────
    # Source: raw_collections.json
    all_files += _write_table_sql(
        out_dir, S, ts, scan_run_id,
        T_COLLECTIONS, _collections_ddl, "collection_name",
        (_row_collections(c, scan_run_id, scan_ts) for c in coll_items),
        "collection")

    # ── 3. purview_entities ────────────────────────────────────────────
    # Source: raw_entities.json
    all_files += _write_table_sql(
        out_dir, S, ts, scan_run_id,
        T_ENTITIES, _entities_ddl, "guid",
        (_row_entities(g, v, scan_run_id, scan_ts) for g, v in entity_map.items()),
        "entity")

    # ── 4. purview_glossary_terms ─────────────────────────────────────
    # Source: raw_glossary_terms.json
    all_files += _write_table_sql(
        out_dir, S, ts, scan_run_id,
        T_GLOSSARY, _glossary_ddl, "guid",
        (_row_glossary(t, scan_run_id, scan_ts) for t in gloss_items),
        "term")

    # ── 5. purview_search_assets ──────────────────────────────────────
    # Source: raw_search_assets.json
    all_files += _write_table_sql(
        out_dir, S, ts, scan_run_id,
        T_SEARCH, _search_ddl, "asset_id",
        (_row_search(a, scan_run_id, scan_ts, cutoff_ms) for a in all_assets),
        "search")

    # ── Runner scripts ─────────────────────────────────────────────────
    # Load order: collections first, then assets, entities, glossary, search
    ordered = []
    for tbl in [T_COLLECTIONS, T_ASSET, T_ENTITIES, T_GLOSSARY, T_SEARCH]:
        tbl_dir = out_dir / tbl
        if tbl_dir.exists():
            parts = sorted(
                tbl_dir.glob(f"{tbl}_part*.sql"),
                key=lambda p: int(re.search(r'_part(\d+)\.sql$', p.name).group(1)))
            ordered.extend(parts)

    bat = [
        "@echo off",
        f"REM Purview SQL loader — 5-table mode — schema [{S}]",
        f"REM Load order: {T_COLLECTIONS} → {T_ASSET} → {T_ENTITIES} → {T_GLOSSARY} → {T_SEARCH}",
        "REM Set env vars: SQL_SERVER  SQL_DATABASE  SQL_USER  SQL_PASSWORD",
        f"REM Total files: {len(ordered)}", "",
    ]
    sh = [
        "#!/bin/bash",
        f"# Purview SQL loader — 5-table mode — schema [{S}]",
        f"# Load order: {T_COLLECTIONS} → {T_ASSET} → {T_ENTITIES} → {T_GLOSSARY} → {T_SEARCH}",
        "# export SQL_SERVER=... SQL_DATABASE=... SQL_USER=... SQL_PASSWORD=...",
        f"# Total files: {len(ordered)}", "",
    ]
    for pf in ordered:
        rel = pf.relative_to(out_dir)
        bat += [
            f'echo Loading {rel}...',
            f'sqlcmd -S %SQL_SERVER% -d %SQL_DATABASE% -U %SQL_USER% -P %SQL_PASSWORD% -b -I -i "{rel}"',
            f'if %ERRORLEVEL% NEQ 0 (echo FAILED: {rel} & exit /b 1)',
        ]
        sh += [
            f'echo "Loading {rel}..."',
            f'sqlcmd -S "$SQL_SERVER" -d "$SQL_DATABASE" -U "$SQL_USER" -P "$SQL_PASSWORD" -b -I -i "{rel}"',
            f'if [ $? -ne 0 ]; then echo "FAILED: {rel}"; exit 1; fi',
        ]
    bat.append("echo All files loaded successfully.")
    sh.append("echo 'All files loaded successfully.'")

    bat_p = out_dir / "run_all.bat";  sh_p = out_dir / "run_all.sh"
    with open(bat_p, "w", encoding="utf-8") as f: f.write("\n".join(bat))
    with open(sh_p,  "w", encoding="utf-8") as f: f.write("\n".join(sh))
    try: sh_p.chmod(0o755)
    except Exception: pass
    log(f"  Runner scripts: run_all.bat + run_all.sh ({len(ordered)} file(s))", "OK")

    return all_files, ordered

# ═══════════════════════════════════════════════════════════════════════
#  PUSH TO AZURE SQL
# ═══════════════════════════════════════════════════════════════════════

def push_to_azure_sql(sql_files, table_counts):
    sqlcmd = shutil.which("sqlcmd")
    if not sqlcmd:
        log("sqlcmd not found in PATH. Run run_all.bat/run_all.sh manually.", "WARN")
        return False
    if not all([SQL_SERVER, SQL_DATABASE, SQL_USER, SQL_PASSWORD]):
        log("SQL_SERVER/SQL_DATABASE/SQL_USER/SQL_PASSWORD not fully set.", "WARN")
        return False

    log_section(f"PUSH TO DB — {SQL_SERVER}/{SQL_DATABASE}")
    log(f"  Schema      : {SCHEMA_NAME}")
    log(f"  Tables      : {T_COLLECTIONS}, {T_ASSET}, {T_ENTITIES}, {T_GLOSSARY}, {T_SEARCH}")
    log(f"  Total files : {len(sql_files)}")
    for tbl, cnt in table_counts.items():
        log(f"  {tbl:<40}: {cnt:>6} rows", "INFO")

    t_start = time.time()

    for file_idx, sql_file in enumerate(sql_files, 1):
        fname     = sql_file.name
        tbl_label = next((t for t in [T_COLLECTIONS, T_ASSET, T_ENTITIES,
                                       T_GLOSSARY, T_SEARCH]
                          if fname.startswith(t)), "unknown")

        part_match = re.search(r'_part(\d+)\.sql$', fname)
        part_num   = int(part_match.group(1)) if part_match else 1

        print(f"\n{'─'*70}")
        print(f"  [{file_idx}/{len(sql_files)}] TABLE : [{SCHEMA_NAME}].[{tbl_label}]  "
              f"part {part_num}")
        print(f"  FILE  : {fname}")
        print(f"{'─'*70}")

        log(f"  Sending to DB: {fname} ...", "INFO")
        push_t = time.time()
        result = subprocess.run(
            [sqlcmd, "-S", SQL_SERVER, "-d", SQL_DATABASE,
             "-U", SQL_USER, "-P", SQL_PASSWORD,
             "-i", str(sql_file), "-b", "-I"],
            capture_output=True, text=True)
        elapsed = time.time() - push_t

        if result.returncode == 0:
            log(f"  OK  [{fname}]  ({elapsed:.1f}s)", "OK")
            for line in [ln.strip() for ln in result.stdout.splitlines() if ln.strip()]:
                log(f"    DB › {line}", "INFO")
        else:
            log(f"  FAILED [{fname}]  (exit {result.returncode}, {elapsed:.1f}s)", "ERROR")
            for line in (result.stderr or result.stdout or "").splitlines()[:20]:
                if line.strip(): log(f"    ERR › {line.strip()}", "ERROR")
            log("  Stopping push — remaining files NOT loaded.", "ERROR")
            return False

    log(f"All {len(sql_files)} file(s) pushed in {fmt_dur(time.time() - t_start)}.", "OK")
    return True

# ═══════════════════════════════════════════════════════════════════════
#  MAIN
# ═══════════════════════════════════════════════════════════════════════

def main():
    t0  = time.time()
    dt0 = datetime.datetime.now(datetime.timezone.utc)
    ts  = dt0.strftime("%Y-%m-%d %H:%M:%S")

    try:
        cutoff_ms, run_label = parse_last_run(_LAST_RUN_RAW)
    except ValueError as e:
        print(f"\n[ERROR] {e}\n"); sys.exit(1)

    log_section("PURVIEW → AZURE SQL  |  purview_final.py  (5-table mode)")
    log(f"Config        : {_config_path}")
    log(f"Account       : {PURVIEW_ACCOUNT}")
    log(f"Schema        : {SCHEMA_NAME}")
    log(f"Tables        : {T_ASSET}, {T_COLLECTIONS}, {T_ENTITIES}, {T_GLOSSARY}, {T_SEARCH}")
    log(f"Output folder : {SQL_OUTPUT_FOLDER}/")
    log(f"JSON folder   : {JSON_OUTPUT_FOLDER}/")
    log(f"LAST_RUN      : {run_label}")
    log(f"Coll filter   : {_COLLECTIONS_FILTER_RAW}")
    log(f"RUN_SQL_IN_DB : {RUN_SQL_IN_DB}")
    log(f"Workers       : {MAX_WORKERS}")
    log(f"Start         : {ts} UTC")
    log(f"NULL policy   : only non-null values written — DB keeps DEFAULT")
    log(f"Data source   : raw JSON files in {JSON_OUTPUT_FOLDER}/")

    if not CLIENT_SECRET:
        log("PURVIEW_CLIENT_SECRET not set.", "ERROR"); sys.exit(1)

    scan_run_id = str(uuid.uuid4())
    scan_ts     = ts
    S           = SCHEMA_NAME
    out_dir     = Path(SQL_OUTPUT_FOLDER)
    json_dir    = Path(JSON_OUTPUT_FOLDER)
    json_dir.mkdir(parents=True, exist_ok=True)

    # ── STEP 1: Fetch search assets ────────────────────────────────────
    all_assets = fetch_search_assets(cutoff_ms)
    leaf       = [a for a in all_assets if is_leaf(a)]
    log(f"Leaf assets: {len(leaf)}  |  Structural skipped: {len(all_assets) - len(leaf)}")
    guids = [a["id"] for a in leaf if a.get("id")]

    # ── STEP 2: Fetch bulk entities ────────────────────────────────────
    entity_map = fetch_entities(guids)

    # ── STEP 3: Fetch collections ──────────────────────────────────────
    coll_map, coll_items, coll_paths = fetch_collections()

    # ── STEP 4: Fetch glossary terms ───────────────────────────────────
    gloss_items = fetch_glossary()

    # ── STEP 5: Save raw JSON files ───────────────────────────────────
    log_section("STEP 5 — Save raw Purview API dumps → JSON files")
    log("  These 4 files are the source of truth for all 5 DB tables.", "INFO")
    save_raw_json(json_dir, scan_run_id, scan_ts, run_label,
                  all_assets, entity_map, coll_items, gloss_items)

    # ── STEP 6: Screen log — assets overview ──────────────────────────
    log_section("STEP 6 — Assets overview (from raw data)")
    ds_groups = defaultdict(list)
    for a in leaf:
        ds_groups[entity_type_to_datasource(a.get("entityType", "Unknown"))].append(a)

    for ds_type, assets in sorted(ds_groups.items()):
        with _print_lock:
            print(f"\n  DATASOURCE : {ds_type}  ({len(assets)} assets)")
        for a in assets[:8]:
            qn      = a.get("qualifiedName", "")
            name    = a.get("name") or a.get("displayText") or short_name(qn)
            coll_id = a.get("collectionId", "")
            coll_p  = coll_paths.get(coll_id, coll_map.get(coll_id, ""))
            upd     = ms_to_iso(a.get("updateTime") or a.get("lastModifiedTS"))
            with _print_lock:
                print(f"  ASSET  : {name}")
                if qn:     print(f"  QN     : {qn}")
                if coll_p: print(f"  COLL   : {coll_p}")
                if upd:    print(f"  UPDATED: {upd}")
        if len(assets) > 8:
            with _print_lock:
                print(f"  ... and {len(assets)-8} more assets in {ds_type}")

    # ── STEP 7: Generate SQL files from raw JSON ───────────────────────
    log_section(f"STEP 7 — Write SQL files → {out_dir}/")
    log("  Source: raw JSON files (not processed/transformed data)", "INFO")

    sql_files, ordered_files = write_all_sql_files(
        out_dir, S, ts, scan_run_id, scan_ts,
        all_assets, entity_map, coll_items, coll_paths,
        gloss_items, cutoff_ms)

    # ── STEP 8: Push to Azure SQL ──────────────────────────────────────
    table_counts = {
        T_ASSET:       len(all_assets),
        T_COLLECTIONS: len(coll_items),
        T_ENTITIES:    len(entity_map),
        T_GLOSSARY:    len(gloss_items),
        T_SEARCH:      len(all_assets),
    }

    push_ok = False
    if RUN_SQL_IN_DB:
        push_ok = push_to_azure_sql(ordered_files, table_counts)
    else:
        log("RUN_SQL_IN_DB=false — SQL files generated but NOT pushed to DB.", "INFO")
        log(f"  To load: cd {out_dir.resolve()} && run_all.bat  (or ./run_all.sh)", "INFO")

    # ── Summary ────────────────────────────────────────────────────────
    elapsed = fmt_dur(time.time() - t0)
    end_dt  = datetime.datetime.now(datetime.timezone.utc)

    print(f"""
{'='*70}
  COMPLETE — purview_final.py  (5-table mode)
{'='*70}
  Scan run ID    : {scan_run_id}
  Start          : {dt0.strftime('%Y-%m-%d %H:%M:%S')} UTC
  End            : {end_dt.strftime('%Y-%m-%d %H:%M:%S')} UTC
  Duration       : {elapsed}
  LAST_RUN       : {run_label}
  Coll filter    : {_COLLECTIONS_FILTER_RAW}
  NULL policy    : only non-null values written — DB keeps DEFAULT
  Data source    : raw JSON files in {json_dir.resolve()}/
  ──────────────────────────────────────────────────────
  SQL output     : {out_dir.resolve()}/
  SQL files      : {len(sql_files)} part-file(s) across 5 table subfolders
  JSON output    : {json_dir.resolve()}/
  Raw files      : raw_search_assets.json   raw_entities.json
                   raw_collections.json     raw_glossary_terms.json
  ──────────────────────────────────────────────────────
  TABLE                             ROWS    SOURCE JSON FILE
  {T_ASSET:<33}: {table_counts[T_ASSET]:>6}    raw_search_assets + raw_entities
  {T_COLLECTIONS:<33}: {table_counts[T_COLLECTIONS]:>6}    raw_collections
  {T_ENTITIES:<33}: {table_counts[T_ENTITIES]:>6}    raw_entities
  {T_GLOSSARY:<33}: {table_counts[T_GLOSSARY]:>6}    raw_glossary_terms
  {T_SEARCH:<33}: {table_counts[T_SEARCH]:>6}    raw_search_assets
  ──────────────────────────────────────────────────────
  Total assets fetched  : {len(all_assets):>6}
  Leaf assets           : {len(leaf):>6}
  Entity payloads       : {len(entity_map):>6}
  Collections           : {len(coll_items):>6}
  Glossary terms        : {len(gloss_items):>6}
  ──────────────────────────────────────────────────────
  RUN_SQL_IN_DB  : {'SUCCESS' if push_ok else ('FAILED' if RUN_SQL_IN_DB else 'false — not pushed')}
  ──────────────────────────────────────────────────────
  Quick queries:
    -- All assets in a collection
    SELECT asset_name, asset_qualified_name, datasource_type,
           collection_hierarchy_path, scan_status
    FROM [{S}].[{T_ASSET}]
    WHERE collection_hierarchy_path LIKE N'%Finance%';

    -- Entity detail with collection path
    SELECT e.entity_name, e.qualified_name, e.entity_type_name,
           a.collection_hierarchy_path, a.datasource_type
    FROM [{S}].[{T_ENTITIES}] e
    JOIN [{S}].[{T_ASSET}] a ON a.asset_guid = e.guid;

    -- All glossary terms
    SELECT term_name, qualified_name, status, long_description
    FROM [{S}].[{T_GLOSSARY}];

    -- Collections hierarchy
    SELECT collection_name, friendly_name, parent_collection_name
    FROM [{S}].[{T_COLLECTIONS}]
    ORDER BY parent_collection_name, collection_name;

    -- Search asset type breakdown
    SELECT entity_type, COUNT(*) AS cnt
    FROM [{S}].[{T_SEARCH}]
    GROUP BY entity_type ORDER BY cnt DESC;
  ──────────────────────────────────────────────────────
  To load manually:
    cd {out_dir.resolve()}
    run_all.bat   (Windows)
    ./run_all.sh  (Linux / Mac)
{'='*70}
""")


if __name__ == "__main__":
    main()