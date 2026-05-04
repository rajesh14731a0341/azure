"""
purview_final.py
================
Standalone. Needs purview_config.ini.

TWO TABLES ONLY
───────────────
1. unified_catalog  — One row per column.  Every single fact about that
                      column in a single row: column identity, parent table,
                      data source, collection path, classification, labels,
                      tags, timestamps.  PK = column_guid.  Uses MERGE —
                      safe to re-run as many times as needed.

2. change_audit_log — Append-only.  One row per detected change event.
                      Captures: COLUMN_NEW, COLUMN_CLASSIFIED_ADDED,
                      COLUMN_CLASSIFIED_REMOVED, COLUMN_LABELED_ADDED,
                      COLUMN_LABELED_REMOVED, COLUMN_TAGGED_CHANGED.
                      PK = log_entry_id (UUID, unique per row, per run).

NO GLOSSARY — glossary fetch removed entirely.

NULL POLICY
───────────
Only fields with real values are written.  Empty / None fields are
omitted from every MERGE and INSERT — the DB column keeps its DEFAULT.

DB PUSH LOGGING
───────────────
When RUN_SQL_IN_DB = true, every file push prints on screen:
  • Which table is being loaded (unified_catalog vs audit)
  • Part number and total parts for that table
  • How many rows in this part-file
  • Per-row progress for the first file of each table (so you can watch)
  • DB PRINT messages from sqlcmd (so you see confirmations live)
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
[PURVIEW]
SCHEMA_NAME        = compliance
SQL_OUTPUT_FOLDER  = sql_output
JSON_OUTPUT_FOLDER = json_output
SNAPSHOT_FILE      = purview_snapshot.json
TBL_UNIFIED_CATALOG = unified_catalog
TBL_AUDIT_LOG       = change_audit_log
MAX_ROWS_PER_FILE   = 500
LAST_RUN            = all
RUN_SQL_IN_DB       = false
SQL_SERVER          = yourserver.database.windows.net
SQL_DATABASE        = yourdb
SQL_USER            = youruser
SQL_PASSWORD        = yourpassword
""", encoding="utf-8")
    print(f"Default config written. Edit it and re-run.\n")
    sys.exit(0)

def _get(key, fallback=""):
    return _cfg["PURVIEW"].get(key, fallback).strip()

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

# ── The only two table names ──────────────────────────────────────────
T_CATALOG = _get("TBL_UNIFIED_CATALOG")
T_AUDIT   = _get("TBL_AUDIT_LOG")

if not T_CATALOG or not T_AUDIT:
    print("\n[ERROR] Both TBL_UNIFIED_CATALOG and TBL_AUDIT_LOG must be defined in config file.\n")
    sys.exit(1)

# ═══════════════════════════════════════════════════════════════════════
#  API ENDPOINTS
# ═══════════════════════════════════════════════════════════════════════

SEARCH_API      = f"https://{PURVIEW_ACCOUNT}.purview.azure.com/datamap/api/search/query?api-version=2023-09-01"
ENTITY_API      = f"https://{PURVIEW_ACCOUNT}.purview.azure.com/datamap/api/atlas/v2/entity/guid"
BULK_ENTITY_API = f"https://{PURVIEW_ACCOUNT}.purview.azure.com/datamap/api/atlas/v2/entity/bulk"
COLLECTION_API  = f"https://{PURVIEW_ACCOUNT}.purview.azure.com/account/collections?api-version=2019-11-01-preview"

SEARCH_PAGE_SIZE   = 1000
BATCH_SIZE         = 100
MAX_RETRIES        = 5
RETRY_BACKOFF      = 2
RETRY_STATUS_CODES = {429, 500, 502, 503, 504}
TOKEN_REFRESH_MINS = 50
MAX_WORKERS        = 100
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
COSMOS_SYSTEM_FIELDS = {
    "_rid","_self","_etag","_attachments","_ts","_lsn",
    "_metadata","_docs","_sprocs","_triggers","_udfs","_conflicts",
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
                return cut, f"last {n}{unit}  (changes after {ms_to_iso(cut)})"
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

# ═══════════════════════════════════════════════════════════════════════
#  LOGGING  ← kept exactly as-is (you love this format)
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

def sid(val):
    return hashlib.md5(str(val).encode()).hexdigest()[:16]

def esc(val):
    """Escape to N'...' or return 'NULL' sentinel (never written to DB)."""
    if val is None or val == "" or (isinstance(val, list) and not val):
        return "NULL"
    s = str(val).strip()
    return "NULL" if not s else "N'" + s.replace("'", "''") + "'"

def esc_int(val):
    if val is None: return "NULL"
    try:    return str(int(val))
    except: return "NULL"

def esc_bit(val):
    if val is None: return "NULL"
    return "1" if val else "0"

def csv_null(lst):
    c = [str(x) for x in lst if x and str(x).strip()]
    return ", ".join(c) if c else None

def has_value(val):
    if val is None or val == "": return False
    if isinstance(val, list):    return len(val) > 0
    return True

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
#  TIMEFRAME FILTER
# ═══════════════════════════════════════════════════════════════════════

def col_in_timeframe(col, cutoff_ms):
    if cutoff_ms is None:
        return True

    def _valid(val):
        if val is None: return None
        try:
            v = int(val)
            return v if v >= MIN_VALID_MS else None
        except (ValueError, TypeError):
            return None

    # ONLY classification timestamps (no column timestamps)
    for c in (col.get("classifications") or []):
        if c.get("entityStatus", "ACTIVE") == "DELETED":
            continue

        raw_ms = None

        # direct keys
        for key in ("lastModifiedTS", "updateTime", "createTime"):
            raw_ms = _valid(c.get(key))
            if raw_ms:
                break

        # fallback inside attributes
        if not raw_ms:
            attrs = c.get("attributes") or {}
            for key in ("lastModifiedTS", "updateTime", "createTime"):
                raw_ms = _valid(attrs.get(key))
                if raw_ms:
                    break

        if raw_ms and raw_ms >= cutoff_ms:
            return True

    return False

_UUID_RE = re.compile(
    r'^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$', re.I)

def _resolve_by(raw, scan_label=None):
    if not raw: return None
    s = str(raw).strip()
    if not s: return None
    if _UUID_RE.match(s):
        return f"Automated Scan ({scan_label or s[:8]}...)"
    return s

# ═══════════════════════════════════════════════════════════════════════
#  FETCH: ASSETS
# ═══════════════════════════════════════════════════════════════════════

def fetch_assets(cutoff_ms):
    def _pages(extra):
        items, page, tk = [], 1, None
        while True:
            body = {"keywords": "*", "limit": SEARCH_PAGE_SIZE}
            body.update(extra)
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
        log_section("STEP 1 — Fetch ALL assets (full load)")
    else:
        log_section(f"STEP 1 — Fetch ALL assets  [timeframe filter: after {ms_to_iso(cutoff_ms)}]")
    assets = _pages({})
    log(f"  Total assets: {len(assets)}", "OK")
    return assets

# ═══════════════════════════════════════════════════════════════════════
#  FETCH: ENTITIES BULK
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
        log("Collections API failed.", "WARN"); return {}, []
    items = r.json().get("value", [])
    log(f"Collections: {len(items)}", "OK")
    return {c.get("name", ""): c.get("friendlyName", "") for c in items}, items

def build_coll_paths(items):
    by_id = {c.get("name", ""): c for c in items}
    def path(cid, seen=None):
        seen = seen or set()
        if cid in seen or cid not in by_id: return ""
        seen.add(cid)
        c = by_id[cid]
        p = c.get("parentCollection", {}).get("referenceName", "")
        n = c.get("friendlyName") or c.get("name", "")
        if p and p in by_id and p != cid:
            pp = path(p, seen)
            return f"{pp} / {n}" if pp else n
        return n
    return {cid: path(cid) for cid in by_id}

# ═══════════════════════════════════════════════════════════════════════
#  COLUMN EXTRACTION
# ═══════════════════════════════════════════════════════════════════════

def extract_columns(ejson):
    cols   = {}
    entity = ejson.get("entity", {})
    et     = entity.get("typeName", "").lower()
    rels   = entity.get("relationshipAttributes", {})
    refs   = ejson.get("referredEntities", {})

    def inj(g, obj):
        if obj.get("guid") != g: obj = dict(obj); obj["guid"] = g
        return obj

    for key in ["columns", "table_columns"]:
        for ref in rels.get(key, []):
            g = ref.get("guid")
            if g and g in refs: cols[g] = inj(g, refs[g])

    for s in rels.get("attachedSchema", []):
        sg = s.get("guid")
        if not sg: continue
        r = api("GET", f"{ENTITY_API}/{sg}?minExtInfo=false", label=f"Schema {sg[:8]}")
        if r and r.status_code == 200:
            try:
                for g, obj in r.json().get("referredEntities", {}).items():
                    tn = obj.get("typeName", "").lower()
                    if any(x in tn for x in ("column", "field", "attribute")):
                        cols[g] = inj(g, obj)
            except Exception: pass

    if "cosmosdb" in et:
        ts = rels.get("tabular_schema", {})
        tg = ts.get("guid") if isinstance(ts, dict) else None
        if tg:
            r = api("GET", f"{ENTITY_API}/{tg}?minExtInfo=true", label=f"Cosmos {tg[:8]}")
            if r and r.status_code == 200:
                try:
                    tj  = r.json()
                    tr  = tj.get("referredEntities", {})
                    trl = tj.get("entity", {}).get("relationshipAttributes", {})
                    for g, obj in tr.items():
                        if "column" in obj.get("typeName", "").lower(): cols[g] = inj(g, obj)
                    for ref in trl.get("columns", []):
                        g = ref.get("guid")
                        if g and g in tr and g not in cols: cols[g] = inj(g, tr[g])
                except Exception: pass
    return list(cols.values())

# ═══════════════════════════════════════════════════════════════════════
#  TIMESTAMP HELPERS
# ═══════════════════════════════════════════════════════════════════════

def _e_upd_at(ej):
    e = ej.get("entity", {})
    return ms_to_iso(e.get("updateTime") or e.get("lastModifiedTS"))

def _e_crt_at(ej):
    e = ej.get("entity", {})
    return ms_to_iso(e.get("createTime") or e.get("createdTS"))

def _e_upd_by(ej):
    e = ej.get("entity", {})
    return e.get("updatedBy") or e.get("modifiedBy") or None

def _e_crt_by(ej):
    e = ej.get("entity", {})
    return e.get("createdBy") or e.get("attributes", {}).get("owner") or None

def _col_upd_at(col):
    return ms_to_iso(col.get("updateTime")) or ms_to_iso(col.get("lastModifiedTS"))

def _col_crt_at(col):
    return ms_to_iso(col.get("createTime")) or ms_to_iso(col.get("createdTS"))

def _col_upd_by(col):
    return col.get("updatedBy") or col.get("modifiedBy") or None

def _cls_details(col, asset_update_time=None):
    """
    Extract best classification timestamp + who applied it.
    MICROSOFT.* auto-scan: lastModifiedTS=0 → fall back to asset scan time.
    Manual/API: real timestamp and user UPN.
    """
    best_ms = None; best_by = None; fallback_by = None
    names   = []; details = []

    for c in (col.get("classifications") or []):
        cn = c.get("typeName", "")
        if not cn or c.get("entityStatus", "ACTIVE") == "DELETED":
            continue
        names.append(cn)
        raw_ms = None
        for key in ("lastModifiedTS", "updateTime", "createTime"):
            raw_ms = _safe_ms(c.get(key))
            if raw_ms: break
        if not raw_ms:
            attrs = c.get("attributes") or {}
            for key in ("lastModifiedTS", "updateTime", "createTime"):
                raw_ms = _safe_ms(attrs.get(key))
                if raw_ms: break
        raw_by = (c.get("source") or c.get("createdBy") or
                  (c.get("attributes") or {}).get("source") or
                  (c.get("attributes") or {}).get("createdBy") or None)
        readable_by = _resolve_by(raw_by)
        if raw_ms:
            ts_iso = ms_to_iso(raw_ms)
            if best_ms is None or raw_ms > best_ms:
                best_ms = raw_ms; best_by = readable_by
            via_fallback = False
        elif asset_update_time:
            ts_iso = asset_update_time; via_fallback = True
            if fallback_by is None and readable_by: fallback_by = readable_by
        else:
            ts_iso = None; via_fallback = False
        parts = [cn, f"Applied by: {readable_by or 'Automated Scan'}"]
        if ts_iso:
            parts.append(f"Scan completed: {ts_iso} (approx)" if via_fallback
                         else f"Applied at: {ts_iso}")
        details.append("  |  ".join(parts))

    if not names:
        return None, None, None, None

    applied_at = ms_to_iso(best_ms) if best_ms else asset_update_time
    applied_by = best_by or fallback_by or "Automated Scan"
    return applied_at, applied_by, csv_null(names), csv_null(details)

def _lbl_details(col):
    labels = [l for l in (col.get("labels") or []) if l]
    return None, None, csv_null(labels)   # label timestamps not in Purview API

# ═══════════════════════════════════════════════════════════════════════
#  SMART MERGE  (null-suppressing)
# ═══════════════════════════════════════════════════════════════════════

def _smart_merge(S, table, pk_col, pk_val, fields):
    """Build MERGE that only writes non-NULL fields."""
    pk_esc = esc(pk_val)
    non_null_pairs = []
    for col, val in fields.items():
        if col == pk_col: continue
        if isinstance(val, bool):   sql_val = esc_bit(val)
        elif isinstance(val, int):  sql_val = str(val)
        else:                       sql_val = esc(val)
        if sql_val != "NULL":
            non_null_pairs.append((col, sql_val))
    if not non_null_pairs:
        return None
    insert_cols = [pk_col] + [c for c, _ in non_null_pairs]
    insert_vals = [pk_esc]  + [v for _, v in non_null_pairs]
    update_sets = [f"T.[{c}] = {v}" for c, v in non_null_pairs if c != pk_col]
    update_sets.append("T.[last_seen_at] = GETUTCDATE()")
    return (
        f"MERGE [{S}].[{table}] AS T\n"
        f"USING (SELECT {pk_esc} AS [{pk_col}]) AS S ON T.[{pk_col}] = S.[{pk_col}]\n"
        f"WHEN MATCHED THEN UPDATE SET\n"
        f"    {', '.join(update_sets)}\n"
        f"WHEN NOT MATCHED THEN INSERT\n"
        f"    ({', '.join(f'[{c}]' for c in insert_cols)})\n"
        f"    VALUES ({', '.join(insert_vals)});\n"
    )

# ═══════════════════════════════════════════════════════════════════════
#  UNIFIED CATALOG SQL
#  One MERGE + inline audit INSERTs that fire only when something changed.
# ═══════════════════════════════════════════════════════════════════════

def unified_catalog_sql(S, tbl_cat, tbl_audit, row, scan_run_id, scan_ts):
    """
    Generate MERGE for unified_catalog + conditional audit INSERT statements.
    Audit rows are written ONLY when a classification / label / tag value
    changed from the previous run — not every time.
    """
    col_guid  = row["column_guid"]
    col_name  = row.get("column_name", "")
    a_name    = row.get("asset_name", "")
    a_guid    = row.get("asset_guid", "")
    qn        = row.get("asset_qualified_name", "")
    dst       = row.get("datasource_type", "")
    coll_id   = row.get("collection_id", "")
    coll_nm   = row.get("collection_name", "")
    coll_p    = row.get("collection_hierarchy_path", "")
    cua       = row.get("column_last_updated_at", "")
    cub       = row.get("column_last_updated_by", "")
    cls_csv   = row.get("classifications", "")
    lbl_csv   = row.get("sensitivity_labels", "")
    tgs_csv   = row.get("business_tags", "")
    cls_at    = row.get("classification_applied_at", "")
    cls_by    = row.get("classification_applied_by", "")

    merge = _smart_merge(S, tbl_cat, "column_guid", col_guid, row)
    if not merge:
        return ""

    cg   = esc(col_guid)
    cnm  = esc(col_name)
    nqn  = esc(qn)
    ndst = esc(dst)
    ncn  = esc(coll_nm)
    ncid = esc(coll_id)
    ncph = esc(coll_p)
    nag  = esc(a_guid)
    nam  = esc(a_name)
    ncua = esc(cua)
    ncub = esc(cub)
    ncls = esc(cls_csv)
    nlbl = esc(lbl_csv)
    ntgs = esc(tgs_csv)
    ncat = esc(cls_at)
    ncby = esc(cls_by)
    sri  = esc(scan_run_id)
    sts  = esc(scan_ts)

    def _u(): return esc(str(uuid.uuid4()))

    # ── COLUMN_NEW: fires only on first appearance ─────────────────────
    audit_new = (
        f"IF NOT EXISTS (SELECT 1 FROM [{S}].[{tbl_audit}]\n"
        f"    WHERE column_guid = {cg} AND change_type = N'COLUMN_NEW')\n"
        f"INSERT INTO [{S}].[{tbl_audit}]\n"
        f"    ([log_entry_id],[scan_run_id],[scan_timestamp],[change_type],\n"
        f"     [column_guid],[column_name],[asset_guid],[asset_name],\n"
        f"     [asset_qualified_name],[datasource_type],[collection_id],\n"
        f"     [collection_name],[collection_hierarchy_path],\n"
        f"     [changed_field],[new_value],[changed_at_in_purview],[changed_by_in_purview])\n"
        f"VALUES ({_u()},{sri},{sts},N'COLUMN_NEW',\n"
        f"    {cg},{cnm},{nag},{nam},\n"
        f"    {nqn},{ndst},{ncid},{ncn},{ncph},\n"
        f"    N'column_guid',{cg},{ncua},{ncub});\n"
    )

    def _chg(change_type, field, old_expr, new_expr, at_expr, by_expr, where_extra=""):
        """Conditional audit INSERT — fires only when old_value ≠ new_value."""
        u = _u()
        return (
            f"IF EXISTS (SELECT 1 FROM [{S}].[{tbl_cat}] AS T\n"
            f"    WHERE T.column_guid = {cg}{where_extra})\n"
            f"INSERT INTO [{S}].[{tbl_audit}]\n"
            f"    ([log_entry_id],[scan_run_id],[scan_timestamp],[change_type],\n"
            f"     [column_guid],[column_name],[asset_guid],[asset_name],\n"
            f"     [asset_qualified_name],[datasource_type],[collection_id],\n"
            f"     [collection_name],[collection_hierarchy_path],\n"
            f"     [changed_field],[old_value],[new_value],\n"
            f"     [changed_at_in_purview],[changed_by_in_purview])\n"
            f"SELECT {u},{sri},{sts},N'{change_type}',\n"
            f"    {cg},{cnm},{nag},{nam},\n"
            f"    {nqn},{ndst},{ncid},{ncn},{ncph},\n"
            f"    N'{field}',{old_expr},{new_expr},{at_expr},{by_expr}\n"
            f"FROM [{S}].[{tbl_cat}] AS T WHERE T.column_guid = {cg}{where_extra};\n"
        )

    audit_parts = [audit_new]

    # Classification added
    if ncls != "NULL":
        audit_parts.append(_chg(
            "COLUMN_CLASSIFIED_ADDED", "classifications",
            "T.classifications", ncls, ncat, ncby,
            f" AND ISNULL(T.classifications,N'')<>ISNULL({ncls},N'')"))

    # Classification removed
    audit_parts.append(_chg(
        "COLUMN_CLASSIFIED_REMOVED", "classifications",
        "T.classifications", "NULL", ncua, ncub,
        f" AND T.classifications IS NOT NULL AND {ncls} IS NULL"))

    # Label added
    if nlbl != "NULL":
        audit_parts.append(_chg(
            "COLUMN_LABELED_ADDED", "sensitivity_labels",
            "T.sensitivity_labels", nlbl, ncua, ncub,
            f" AND ISNULL(T.sensitivity_labels,N'')<>ISNULL({nlbl},N'')"))

    # Label removed
    audit_parts.append(_chg(
        "COLUMN_LABELED_REMOVED", "sensitivity_labels",
        "T.sensitivity_labels", "NULL", ncua, ncub,
        f" AND T.sensitivity_labels IS NOT NULL AND {nlbl} IS NULL"))

    # Business tag changed
    if ntgs != "NULL":
        audit_parts.append(_chg(
            "COLUMN_TAGGED_CHANGED", "business_tags",
            "T.business_tags", ntgs, ncua, ncub,
            f" AND ISNULL(T.business_tags,N'')<>ISNULL({ntgs},N'')"))

    return (
        f"-- ── Column: {col_name or col_guid}  │  Asset: {a_name or qn}\n"
        f"-- ── Source: {dst}  │  Collection: {coll_p or coll_nm}\n"
        f"-- ── cls: {cls_csv or '(none)'}  │  labels: {lbl_csv or '(none)'}\n"
        f"{merge}{''.join(audit_parts)}\n"
    )

# ═══════════════════════════════════════════════════════════════════════
#  DDL — TWO TABLES ONLY
# ═══════════════════════════════════════════════════════════════════════

def _schema_ddl(S):
    return (
        f"IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = N'{S}')\n"
        f"    EXEC('CREATE SCHEMA [{S}]');\nGO\n\n"
    )

def _unified_catalog_ddl(S, tbl):
    return f"""\
IF OBJECT_ID(N'[{S}].[{tbl}]', N'U') IS NULL
BEGIN
CREATE TABLE [{S}].[{tbl}] (
    -- ── Column identity ────────────────────────────────────────────
    column_guid                     NVARCHAR(64)    NOT NULL,
    column_name                     NVARCHAR(500)   NOT NULL,
    data_type                       NVARCHAR(200)   NULL,
    column_description              NVARCHAR(2000)  NULL,
    column_position                 INT             NULL,
    -- ── Parent asset (table / file / container) ────────────────────
    asset_guid                      NVARCHAR(64)    NULL,
    asset_name                      NVARCHAR(500)   NULL,
    asset_qualified_name            NVARCHAR(2000)  NULL,   -- full qualified name of the table
    asset_entity_type               NVARCHAR(200)   NULL,   -- e.g. azure_sql_table
    asset_object_type               NVARCHAR(200)   NULL,   -- Table | File | etc.
    schema_path                     NVARCHAR(1000)  NULL,   -- schema within the database
    -- ── Data source ────────────────────────────────────────────────
    datasource_type                 NVARCHAR(200)   NULL,   -- Azure SQL Database | Snowflake | ...
    datasource_instance             NVARCHAR(500)   NULL,   -- server / account name
    -- ── Collection ─────────────────────────────────────────────────
    collection_id                   NVARCHAR(64)    NULL,
    collection_name                 NVARCHAR(500)   NULL,
    collection_hierarchy_path       NVARCHAR(2000)  NULL,   -- Root / SubCollection / ...
    -- ── Classification ────────────────────────────────────────────
    is_classified                   BIT             NOT NULL DEFAULT 0,
    classifications                 NVARCHAR(2000)  NULL,   -- CSV: MICROSOFT.PERSONAL.EMAIL, ...
    classification_source           NVARCHAR(20)    NULL,   -- System | Custom | Mixed
    classification_applied_at       NVARCHAR(30)    NULL,
    classification_applied_by       NVARCHAR(500)   NULL,
    classification_applied_detail   NVARCHAR(MAX)   NULL,   -- full detail per classification
    -- ── Sensitivity labels ────────────────────────────────────────
    sensitivity_labels              NVARCHAR(2000)  NULL,
    label_applied_at                NVARCHAR(30)    NULL,   -- not exposed by Purview API
    label_applied_by                NVARCHAR(500)   NULL,   -- not exposed by Purview API
    -- ── Business tags ─────────────────────────────────────────────
    business_tags                   NVARCHAR(2000)  NULL,
    -- ── Timestamps ────────────────────────────────────────────────
    column_created_at               NVARCHAR(30)    NULL,
    column_last_updated_at          NVARCHAR(30)    NULL,
    column_last_updated_by          NVARCHAR(200)   NULL,
    asset_last_updated_at           NVARCHAR(30)    NULL,
    asset_last_updated_by           NVARCHAR(200)   NULL,
    -- ── Scan metadata ─────────────────────────────────────────────
    scan_run_id                     NVARCHAR(36)    NULL,
    scan_timestamp                  NVARCHAR(30)    NULL,
    -- ── Row lifecycle ─────────────────────────────────────────────
    first_seen_at                   DATETIME2       NOT NULL DEFAULT GETUTCDATE(),
    last_seen_at                    DATETIME2       NOT NULL DEFAULT GETUTCDATE(),
    CONSTRAINT PK_{tbl} PRIMARY KEY (column_guid)
);
PRINT '{tbl} ready.';
END
GO

"""

def _audit_log_ddl(S, tbl):
    return f"""\
IF OBJECT_ID(N'[{S}].[{tbl}]', N'U') IS NULL
BEGIN
-- APPEND-ONLY. Never UPDATE or DELETE rows here.
-- One row per detected change. PK = log_entry_id (UUID).
-- change_type values:
--   COLUMN_NEW               → column seen for the first time
--   COLUMN_CLASSIFIED_ADDED  → new classification applied
--   COLUMN_CLASSIFIED_REMOVED→ classification was removed
--   COLUMN_LABELED_ADDED     → sensitivity label applied
--   COLUMN_LABELED_REMOVED   → sensitivity label removed
--   COLUMN_TAGGED_CHANGED    → business tag changed
CREATE TABLE [{S}].[{tbl}] (
    log_entry_id                NVARCHAR(36)    NOT NULL,   -- UUID, unique per row
    scan_run_id                 NVARCHAR(36)    NULL,       -- which run detected this change
    scan_timestamp              NVARCHAR(30)    NULL,
    change_type                 NVARCHAR(60)    NOT NULL,
    -- ── What changed ─────────────────────────────────────────────
    column_guid                 NVARCHAR(64)    NULL,
    column_name                 NVARCHAR(500)   NULL,
    asset_guid                  NVARCHAR(64)    NULL,
    asset_name                  NVARCHAR(500)   NULL,
    asset_qualified_name        NVARCHAR(2000)  NULL,
    datasource_type             NVARCHAR(200)   NULL,
    collection_id               NVARCHAR(64)    NULL,
    collection_name             NVARCHAR(500)   NULL,
    collection_hierarchy_path   NVARCHAR(2000)  NULL,
    changed_field               NVARCHAR(200)   NULL,
    old_value                   NVARCHAR(2000)  NULL,
    new_value                   NVARCHAR(2000)  NULL,
    -- ── Who changed it and when (from Purview) ───────────────────
    changed_at_in_purview       NVARCHAR(30)    NULL,
    changed_by_in_purview       NVARCHAR(500)   NULL,
    -- ── When this script detected the change ─────────────────────
    detected_at                 DATETIME2       NOT NULL DEFAULT GETUTCDATE(),
    CONSTRAINT PK_{tbl} PRIMARY KEY (log_entry_id)
);
PRINT '{tbl} ready.';
END
GO

"""

# ═══════════════════════════════════════════════════════════════════════
#  WRITE SQL FILES  (2 tables only, part-file splitting)
# ═══════════════════════════════════════════════════════════════════════

def write_sql_files(out_dir, S, ts, scan_run_id, scan_ts, catalog_rows, row_metadata):
    """
    Write SQL part-files for unified_catalog and change_audit_log.

    OUTPUT STRUCTURE
    ────────────────
    sql_output/
      unified_catalog/
        unified_catalog_part1.sql   ← DDL + audit DDL + first N rows
        unified_catalog_part2.sql   ← next N rows (no DDL)
        ...
      run_all.bat  /  run_all.sh
    """
    out_dir = Path(out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    log(f"  Output folder: {out_dir.resolve()}", "OK")

    # ── Build all SQL statements ───────────────────────────────────────
    stmts    = []
    skipped  = 0
    total    = len(catalog_rows)

    for idx, row in enumerate(catalog_rows, 1):
        col_name = row.get("column_name", "?")
        a_name   = row.get("asset_name", "?")
        dst      = row.get("datasource_type", "?")
        coll_p   = row.get("collection_hierarchy_path") or row.get("collection_name", "?")
        cls_csv  = row.get("classifications", "") or "(none)"
        lbl_csv  = row.get("sensitivity_labels", "") or "(none)"

        # Enrich row_metadata (used by push_to_azure_sql for console output)
        row_metadata.append({
            "idx":      idx,
            "total":    total,
            "column":   col_name,
            "asset":    a_name,
            "source":   dst,
            "coll":     coll_p,
            "cls":      cls_csv,
            "lbl":      lbl_csv,
        })

        sql = unified_catalog_sql(S, T_CATALOG, T_AUDIT, row, scan_run_id, scan_ts)
        if sql:
            stmts.append(sql)
        else:
            skipped += 1

    if skipped:
        log(f"  Skipped (no data beyond PK): {skipped} column rows", "INFO")

    # ── Split into part-files ──────────────────────────────────────────
    all_files = []
    tbl_dir   = out_dir / T_CATALOG
    tbl_dir.mkdir(parents=True, exist_ok=True)

    chunks      = [stmts[i:i+MAX_ROWS_PER_FILE]
                   for i in range(0, max(1, len(stmts)), MAX_ROWS_PER_FILE)]
    if not chunks: chunks = [[]]
    total_parts = len(chunks)

    # Audit DDL is embedded in part-1 so the table exists before data lands
    audit_ddl = _schema_ddl(S) + _audit_log_ddl(S, T_AUDIT)

    for part_idx, chunk in enumerate(chunks, 1):
        fname = f"{T_CATALOG}_part{part_idx}.sql"
        p     = tbl_dir / fname

        # Part 1: full header with DDL
        if part_idx == 1:
            hdr = (
                f"-- ================================================================\n"
                f"-- TABLE  : [{S}].[{T_CATALOG}]\n"
                f"-- PART   : {part_idx} of {total_parts}\n"
                f"-- ROWS   : {len(stmts)} columns total\n"
                f"-- RUN ID : {scan_run_id}\n"
                f"-- TIME   : {ts}\n"
                f"-- NULL policy: only non-null values written to DB\n"
                f"-- ================================================================\n\n"
                f"SET NOCOUNT ON;\nGO\n\n"
                f"{_schema_ddl(S)}"
                f"{_unified_catalog_ddl(S, T_CATALOG)}"
                f"{audit_ddl}"     # create audit table before any data references it
            )
        else:
            hdr = (
                f"-- ================================================================\n"
                f"-- TABLE  : [{S}].[{T_CATALOG}]   Part {part_idx} of {total_parts}\n"
                f"-- RUN ID : {scan_run_id}   TIME: {ts}\n"
                f"-- Data rows only — DDL is in part 1.\n"
                f"-- ================================================================\n\n"
                f"SET NOCOUNT ON;\nGO\n\n"
            )

        body  = "".join(chunk)
        trail = (
            f"\nGO\n"
            f"PRINT 'unified_catalog part {part_idx}/{total_parts} loaded — "
            f"{len(chunk)} rows.';\nGO\n"
        )
        with open(p, "w", encoding="utf-8") as f:
            f.write(hdr + body + trail)
        all_files.append(p)

    log(f"  {T_CATALOG}: {len(stmts)} rows → {total_parts} part-file(s) "
        f"in {tbl_dir.name}/", "OK")

    # ── Runner scripts ─────────────────────────────────────────────────
    ordered = sorted(
        tbl_dir.glob(f"{T_CATALOG}_part*.sql"),
        key=lambda p: int(re.search(r'_part(\d+)\.sql$', p.name).group(1)))

    bat = [
        "@echo off",
        "REM Purview SQL loader — 2 tables: unified_catalog + change_audit_log",
        "REM Set env vars: SQL_SERVER, SQL_DATABASE, SQL_USER, SQL_PASSWORD",
        f"REM Total: {len(ordered)} part-file(s)",
        "",
    ]
    sh = [
        "#!/bin/bash",
        "# Purview SQL loader — 2 tables: unified_catalog + change_audit_log",
        "# export SQL_SERVER=... SQL_DATABASE=... SQL_USER=... SQL_PASSWORD=...",
        f"# Total: {len(ordered)} part-file(s)",
        "",
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
    log(f"  Runner scripts: run_all.bat + run_all.sh ({len(ordered)} part-file(s))", "OK")

    return all_files, ordered

# ═══════════════════════════════════════════════════════════════════════
#  PUSH TO AZURE SQL  — with rich per-row / per-file console logging
# ═══════════════════════════════════════════════════════════════════════

def push_to_azure_sql(sql_files, row_metadata, total_catalog_rows):
    """
    Execute each SQL file via sqlcmd and print detailed progress on screen.

    For every file pushed you will see on screen:
      • TABLE being loaded and which part
      • How many rows are in this part-file
      • For the FIRST part-file: a per-row preview table showing exactly
        what column / asset / classification is being loaded
      • Every PRINT message that sqlcmd outputs from the SQL (live)
      • Success / failure status per file
    """
    sqlcmd = shutil.which("sqlcmd")
    if not sqlcmd:
        log("sqlcmd not found in PATH. Run run_all.bat/run_all.sh manually.", "WARN")
        return False
    if not all([SQL_SERVER, SQL_DATABASE, SQL_USER, SQL_PASSWORD]):
        log("SQL_SERVER/SQL_DATABASE/SQL_USER/SQL_PASSWORD not fully set.", "WARN")
        return False

    log_section(f"PUSH TO DB — {SQL_SERVER}/{SQL_DATABASE}")
    log(f"  Schema   : {SCHEMA_NAME}")
    log(f"  Tables   : {T_CATALOG}  |  {T_AUDIT}")
    log(f"  Files    : {len(sql_files)} SQL part-file(s)")
    log(f"  Total rows in unified_catalog: {total_catalog_rows}")

    rows_loaded = 0
    t_push_start = time.time()

    for file_idx, sql_file in enumerate(sql_files, 1):
        fname = sql_file.name

        # Determine how many rows this file covers from metadata
        rows_in_file = min(MAX_ROWS_PER_FILE, total_catalog_rows - (file_idx-1)*MAX_ROWS_PER_FILE)
        rows_in_file = max(0, rows_in_file)

        part_match = re.search(r'_part(\d+)\.sql$', fname)
        part_num   = int(part_match.group(1)) if part_match else file_idx
        total_parts = (total_catalog_rows + MAX_ROWS_PER_FILE - 1) // MAX_ROWS_PER_FILE or 1

        print(f"\n{'─'*70}")
        print(f"  LOADING  : [{SCHEMA_NAME}].[{T_CATALOG}]  "
              f"part {part_num}/{total_parts}")
        print(f"  FILE     : {fname}")
        print(f"  ROWS     : ~{rows_in_file} column rows in this batch")
        print(f"  PROGRESS : {rows_loaded}/{total_catalog_rows} rows pushed so far")
        print(f"{'─'*70}")

        # For part 1: print a per-row preview table (first 20 rows)
        if part_num == 1 and row_metadata:
            first_batch = row_metadata[:min(20, rows_in_file)]
            print(f"\n  COLUMN PREVIEW (first {len(first_batch)} rows being loaded):")
            print(f"  {'#':>4}  {'COLUMN':<28} {'ASSET':<25} {'SOURCE':<22} {'CLASSIFICATION'}")
            print(f"  {'─'*4}  {'─'*28} {'─'*25} {'─'*22} {'─'*35}")
            for m in first_batch:
                cls_short = (m["cls"] or "(none)")[:40]
                print(f"  {m['idx']:>4}  {m['column'][:28]:<28} "
                      f"{m['asset'][:25]:<25} "
                      f"{m['source'][:22]:<22} "
                      f"{cls_short}")
            if len(row_metadata) > 20:
                print(f"  ... and {len(row_metadata) - 20} more rows")
            print()

        # Execute sqlcmd
        log(f"  Executing sqlcmd → {fname} ...", "INFO")
        push_t = time.time()
        result = subprocess.run(
            [sqlcmd,
             "-S", SQL_SERVER, "-d", SQL_DATABASE,
             "-U", SQL_USER,   "-P", SQL_PASSWORD,
             "-i", str(sql_file), "-b", "-I"],
            capture_output=True, text=True)

        elapsed_push = time.time() - push_t

        if result.returncode == 0:
            rows_loaded += rows_in_file
            log(f"  OK  [{fname}] — {elapsed_push:.1f}s", "OK")

            # Print PRINT messages from the SQL (confirmations, row counts)
            stdout_lines = [ln.strip() for ln in result.stdout.splitlines() if ln.strip()]
            if stdout_lines:
                print(f"  ── DB Messages ──────────────────────────")
                for line in stdout_lines:
                    log(f"    DB › {line}", "INFO")

            # Per-batch summary line
            pct = int(100 * rows_loaded / total_catalog_rows) if total_catalog_rows else 100
            print(f"  ── Batch summary ────────────────────────")
            print(f"  Rows in this batch : {rows_in_file}")
            print(f"  Total rows loaded  : {rows_loaded}/{total_catalog_rows}  ({pct}%)")
            bar_filled = int(pct / 5)
            bar = "█" * bar_filled + "░" * (20 - bar_filled)
            print(f"  Progress           : [{bar}] {pct}%")

        else:
            log(f"  FAILED [{fname}] — exit code {result.returncode}", "ERROR")
            print(f"\n  ── Error Output ─────────────────────────")
            for line in (result.stderr or result.stdout or "").splitlines()[:20]:
                if line.strip(): log(f"    ERR › {line.strip()}", "ERROR")
            log(f"  Stopping push. Remaining files were NOT loaded.", "ERROR")
            return False

    total_push = fmt_dur(time.time() - t_push_start)
    print(f"\n{'─'*70}")
    log(f"All {len(sql_files)} file(s) pushed successfully in {total_push}.", "OK")
    log(f"  {total_catalog_rows} rows loaded into [{SCHEMA_NAME}].[{T_CATALOG}]", "OK")
    log(f"  Change events written to [{SCHEMA_NAME}].[{T_AUDIT}] (inline, count varies)", "OK")
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

    log_section("PURVIEW → AZURE SQL  |  purview_final.py  (2-table mode)")
    log(f"Config        : {_config_path}")
    log(f"Account       : {PURVIEW_ACCOUNT}")
    log(f"Schema        : {SCHEMA_NAME}")
    log(f"Tables        : [{T_CATALOG}]  +  [{T_AUDIT}]  (2 tables only)")
    log(f"Output folder : {SQL_OUTPUT_FOLDER}/")
    log(f"JSON folder   : {JSON_OUTPUT_FOLDER}/")
    log(f"Snapshot      : {SNAPSHOT_FILE}")
    log(f"LAST_RUN      : {run_label}")
    log(f"RUN_SQL_IN_DB : {RUN_SQL_IN_DB}")
    log(f"Workers       : {MAX_WORKERS}")
    log(f"Start         : {ts} UTC")
    log(f"NULL policy   : only non-null values written — DB never gets NULL noise")

    if not CLIENT_SECRET:
        log("PURVIEW_CLIENT_SECRET not set.", "ERROR"); sys.exit(1)

    scan_run_id = str(uuid.uuid4())
    scan_ts     = ts
    S           = SCHEMA_NAME
    out_dir     = Path(SQL_OUTPUT_FOLDER)
    json_dir    = Path(JSON_OUTPUT_FOLDER)
    json_dir.mkdir(parents=True, exist_ok=True)

    # ── STEP 1–3: Fetch from Purview API ──────────────────────────────
    all_assets             = fetch_assets(cutoff_ms)
    leaf                   = [a for a in all_assets if is_leaf(a)]
    log(f"Leaf: {len(leaf)}  |  Structural skipped: {len(all_assets) - len(leaf)}")

    guids                  = [a["id"] for a in leaf if a.get("id")]
    guid_to_asset          = {a["id"]: a for a in leaf if a.get("id")}
    entity_map             = fetch_entities(guids)
    coll_map, coll_items   = fetch_collections()
    coll_paths             = build_coll_paths(coll_items)

    # NOTE: Glossary fetch removed entirely as requested.

    # ── STEP 4: Process assets + columns ──────────────────────────────
    log_section("STEP 4 — Process assets and build unified_catalog rows")

    catalog_rows = []     # one dict per column — becomes one DB row
    total_assets = total_cols = total_cls = total_lbl = total_tag = cols_in_tf = 0

    ds_groups = defaultdict(lambda: defaultdict(list))
    for g in guids:
        a = guid_to_asset.get(g, {})
        ds_groups[entity_type_to_datasource(a.get("entityType", ""))][
            get_instance(a.get("qualifiedName", ""))].append(g)

    for ds_type, instances in sorted(ds_groups.items()):
        for instance, g_list in sorted(instances.items()):
            for guid in g_list:
                ej      = entity_map.get(guid)
                a       = guid_to_asset.get(guid, {})
                qn      = a.get("qualifiedName", "")
                et      = a.get("entityType", "")
                ot      = a.get("objectType", "")
                coll_id = a.get("collectionId", "")
                coll_nm = coll_map.get(coll_id, "")
                coll_p  = coll_paths.get(coll_id, coll_nm)
                a_name  = short_name(qn)
                sch_p   = get_schema_path(qn)

                a_upd_at = _e_upd_at(ej) if ej else None
                a_upd_by = _e_upd_by(ej) if ej else None
                a_crt_at = _e_crt_at(ej) if ej else None
                a_crt_by = _e_crt_by(ej) if ej else None

                if not ej:
                    continue

                cols = extract_columns(ej)
                if not cols:
                    r2 = api("GET",
                             f"{ENTITY_API}/{guid}?minExtInfo=true&ignoreRelationships=false",
                             label=f"Fallback {guid[:8]}")
                    if r2 and r2.status_code == 200:
                        try:
                            fb   = r2.json();  cols = extract_columns(fb)
                            if not a_upd_at: a_upd_at = _e_upd_at(fb)
                            if not a_upd_by: a_upd_by = _e_upd_by(fb)
                        except Exception: pass

                total_assets += 1
                asset_col_cnt = 0;  asset_cls_cnt = 0;  asset_cls_set = set()
                cols_in_tf_list = []

                for col_num, col in enumerate(cols, 1):
                    attr     = col.get("attributes", {})
                    col_name = attr.get("name")
                    if not col_name or col_name in COSMOS_SYSTEM_FIELDS:
                        continue

                    col_guid = col.get("guid") or sid(f"{qn}/{col_name}")
                    dtype    = attr.get("data_type") or attr.get("type") or None
                    col_desc = attr.get("description") or attr.get("userDescription") or None
                    col_pos  = attr.get("position") or attr.get("columnOrder") or None
                    c_upd_at = _col_upd_at(col);  c_crt_at = _col_crt_at(col)
                    c_upd_by = _col_upd_by(col)

                    in_tf = col_in_timeframe(col, cutoff_ms)
                    asset_col_cnt += 1;  total_cols += 1

                    if not in_tf:
                        continue

                    # Enrichment gate: only columns with at least one enrichment value
                    _has_cls  = any(c.get("typeName") and c.get("entityStatus","ACTIVE")!="DELETED"
                                    for c in (col.get("classifications") or []))
                    _has_lbl  = bool([l for l in (col.get("labels") or []) if l])
                    _has_tag  = any(isinstance(gv, dict) and gv
                                    for gv in (col.get("businessAttributes") or {}).values())
                    if not (_has_cls or _has_lbl or _has_tag):
                        continue

                    cols_in_tf += 1

                    # ── Extract enrichment ────────────────────────────
                    cls_app_at, cls_app_by, cls_names_csv, cls_detail = _cls_details(
                        col, asset_update_time=a_upd_at)

                    cls_names = [c.get("typeName","")
                                 for c in (col.get("classifications") or [])
                                 if c.get("typeName") and c.get("entityStatus","ACTIVE")!="DELETED"]

                    if cls_names:
                        asset_cls_cnt += 1
                        asset_cls_set.update(cls_names)
                        total_cls += len(cls_names)

                    # Determine classification source (System / Custom / Mixed)
                    if cls_names:
                        has_sys    = any(cn.upper().startswith("MICROSOFT.") for cn in cls_names)
                        has_custom = any(not cn.upper().startswith("MICROSOFT.") for cn in cls_names)
                        cls_source = ("Mixed" if has_sys and has_custom
                                      else "System" if has_sys else "Custom")
                    else:
                        cls_source = None

                    lbl_app_at, lbl_app_by, lbl_names_csv = _lbl_details(col)
                    lbl_names = [l for l in (col.get("labels") or []) if l]
                    if lbl_names: total_lbl += len(lbl_names)

                    tag_display = []
                    for grp, gvals in (col.get("businessAttributes") or {}).items():
                        if not isinstance(gvals, dict): continue
                        for attr_nm, attr_val in gvals.items():
                            tk = f"{grp}.{attr_nm}"
                            tv = str(attr_val) if attr_val is not None else None
                            tag_display.append(f"{tk}={tv}" if tv else tk)
                            total_tag += 1

                    # ── Build the single unified_catalog row ──────────
                    row = {
                        # Column
                        "column_guid":                   col_guid,
                        "column_name":                   col_name,
                        "data_type":                     dtype,
                        "column_description":            col_desc,
                        "column_position":               int(col_pos) if col_pos is not None else None,
                        # Asset
                        "asset_guid":                    guid,
                        "asset_name":                    a_name or None,
                        "asset_qualified_name":          qn or None,
                        "asset_entity_type":             et or None,
                        "asset_object_type":             ot or None,
                        "schema_path":                   sch_p,
                        # Data source
                        "datasource_type":               ds_type or None,
                        "datasource_instance":           instance or None,
                        # Collection
                        "collection_id":                 coll_id or None,
                        "collection_name":               coll_nm or None,
                        "collection_hierarchy_path":     coll_p or None,
                        # Classification
                        "is_classified":                 len(cls_names) > 0,
                        "classifications":               cls_names_csv,
                        "classification_source":         cls_source,
                        "classification_applied_at":     cls_app_at,
                        "classification_applied_by":     cls_app_by,
                        "classification_applied_detail": cls_detail,
                        # Labels
                        "sensitivity_labels":            lbl_names_csv,
                        "label_applied_at":              lbl_app_at,
                        "label_applied_by":              lbl_app_by,
                        # Tags
                        "business_tags":                 csv_null(tag_display),
                        # Timestamps
                        "column_created_at":             c_crt_at,
                        "column_last_updated_at":        c_upd_at,
                        "column_last_updated_by":        c_upd_by,
                        "asset_last_updated_at":         a_upd_at,
                        "asset_last_updated_by":         a_upd_by,
                        # Scan
                        "scan_run_id":                   scan_run_id,
                        "scan_timestamp":                scan_ts,
                    }
                    # Strip None / empty values (except mandatory identity fields)
                    mandatory = {"column_guid", "column_name", "is_classified"}
                    row = {k: v for k, v in row.items() if has_value(v) or k in mandatory}
                    catalog_rows.append(row)

                    # Console output (exact format kept from test_cloude.py)
                    cols_in_tf_list.append((
                        col_num, len(cols), col_name, dtype,
                        cls_names, cls_app_at, cls_app_by,
                        lbl_names, tag_display,
                    ))

                if not cols_in_tf_list:
                    continue

                _full = (cutoff_ms is None)
                with _print_lock:
                    print(f"\n  ASSET  : {qn}")
                    if ot:       print(f"  TYPE   : {ot} | {et}")
                    if coll_p:   print(f"  COLL   : {coll_p}")
                    if a_upd_at: print(f"  UPDATED: {a_upd_at}  by: {a_upd_by or '?'}")
                    n_tf = len(cols_in_tf_list)
                    print(f"  COLS   : {asset_col_cnt} total  |  "
                          f"{n_tf} {'with enrichment' if _full else 'in timeframe'}")
                    for (rn, tot, rname, rdtype,
                         rcls, rcla, rclb, rlbl, rtgs) in cols_in_tf_list:
                        rparts = [f"    [{rn:>4}/{tot}] {rname:<35}"]
                        if rdtype: rparts.append(f"type={rdtype}")
                        if rcls:
                            rparts.append(f"cls={', '.join(rcls)}")
                            if rcla: rparts.append(f"applied={rcla}")
                            if rclb: rparts.append(f"by={rclb}")
                        if rlbl:   rparts.append(f"lbl={', '.join(rlbl)}")
                        if rtgs:   rparts.append(f"tags={', '.join(rtgs)}")
                        print("  ".join(rparts))
                    if asset_cls_set:
                        print(f"  CLASSIFIED: {asset_cls_cnt}/{asset_col_cnt} cols  "
                              f"types: {', '.join(sorted(asset_cls_set))}")

    # ── STEP 5: Write SQL files ────────────────────────────────────────
    log_section(f"STEP 5 — Write SQL files → {out_dir}/")
    log(f"  Total catalog rows to write: {len(catalog_rows)}", "INFO")
    row_metadata = []
    sql_files, ordered_files = write_sql_files(
        out_dir, S, ts, scan_run_id, scan_ts, catalog_rows, row_metadata)

    # ── STEP 6: Write JSON snapshot ────────────────────────────────────
    log_section("STEP 6 — Write JSON snapshot")
    snapshot_path = json_dir / SNAPSHOT_FILE
    snapshot = {
        "scan_run_id":        scan_run_id,
        "scan_time":          scan_ts,
        "config_file":        _config_path,
        "last_run_filter":    run_label,
        "schema_name":        S,
        "sql_output_folder":  str(out_dir),
        "json_output_folder": str(json_dir),
        "tables": {
            "unified_catalog": T_CATALOG,
            "audit_log":       T_AUDIT,
        },
        "null_policy": "Only non-null values written to DB",
        "summary": {
            "total_search_assets":  len(all_assets),
            "leaf_assets":          len(leaf),
            "entities_fetched":     len(entity_map),
            "total_columns_seen":   total_cols,
            "catalog_rows_written": len(catalog_rows),
            "columns_in_timeframe": cols_in_tf,
            "total_classifications": total_cls,
            "total_labels":         total_lbl,
            "total_tags":           total_tag,
        },
        "unified_catalog": catalog_rows,
    }
    with open(snapshot_path, "w", encoding="utf-8") as f:
        json.dump(snapshot, f, indent=2, ensure_ascii=False)
    log(f"Snapshot written: {snapshot_path}  ({snapshot_path.stat().st_size:,} bytes)", "OK")

    # ── STEP 7: Push to Azure SQL ──────────────────────────────────────
    push_ok = False
    if RUN_SQL_IN_DB:
        push_ok = push_to_azure_sql(ordered_files, row_metadata, len(catalog_rows))
    else:
        log("RUN_SQL_IN_DB=false — SQL files generated but NOT pushed to DB.", "INFO")
        log(f"  To load manually: cd {out_dir.resolve()} && run_all.bat (or ./run_all.sh)", "INFO")

    # ── Summary ────────────────────────────────────────────────────────
    elapsed = fmt_dur(time.time() - t0)
    end_dt  = datetime.datetime.now(datetime.timezone.utc)

    print(f"""
{'='*70}
  COMPLETE — purview_final.py  (2-table mode)
{'='*70}
  Scan run ID    : {scan_run_id}
  Start          : {dt0.strftime('%Y-%m-%d %H:%M:%S')} UTC
  End            : {end_dt.strftime('%Y-%m-%d %H:%M:%S')} UTC
  Duration       : {elapsed}
  LAST_RUN       : {run_label}
  NULL policy    : only non-null values written
  ──────────────────────────────────────────────────────
  SQL folder     : {out_dir.resolve()}/
  Files          : {len(sql_files)} SQL part-file(s)
  Snapshot       : {snapshot_path}
  ──────────────────────────────────────────────────────
  TABLE                         ROWS    DESCRIPTION
  {T_CATALOG:<29} {len(catalog_rows):>6}    PK: column_guid — one row per column
  {T_AUDIT:<29}  inline    PK: log_entry_id — append-only changes
  ──────────────────────────────────────────────────────
  Assets searched   : {len(all_assets):>6}
  Leaf assets       : {len(leaf):>6}
  Total columns     : {total_cols:>6}
  Catalog rows      : {len(catalog_rows):>6}  (columns with enrichment data)
  Columns in window : {cols_in_tf:>6}
  Classifications   : {total_cls:>6}  (total tags across all columns)
  Labels            : {total_lbl:>6}
  Business tags     : {total_tag:>6}
  ──────────────────────────────────────────────────────
  RUN_SQL_IN_DB  : {'SUCCESS' if push_ok else ('FAILED' if RUN_SQL_IN_DB else 'false — not pushed')}
  ──────────────────────────────────────────────────────
  Quick queries:
    -- Everything about one table
    SELECT * FROM [{S}].[{T_CATALOG}]
    WHERE asset_name = 'your_table_name';

    -- All classified columns with who applied and when
    SELECT column_name, asset_name, datasource_type,
           collection_hierarchy_path,
           classifications, classification_applied_at, classification_applied_by
    FROM [{S}].[{T_CATALOG}]
    WHERE is_classified = 1
    ORDER BY classification_applied_at DESC;

    -- Columns with labels
    SELECT column_name, asset_name, sensitivity_labels,
           collection_hierarchy_path
    FROM [{S}].[{T_CATALOG}]
    WHERE sensitivity_labels IS NOT NULL;

    -- Change history for this run
    SELECT change_type, column_name, asset_name, datasource_type,
           collection_hierarchy_path, changed_field,
           old_value, new_value, changed_at_in_purview, changed_by_in_purview
    FROM [{S}].[{T_AUDIT}]
    WHERE scan_run_id = '{scan_run_id}'
    ORDER BY detected_at DESC;

    -- What changed in the last 7 days
    SELECT change_type, column_name, asset_name,
           new_value, changed_at_in_purview, changed_by_in_purview
    FROM [{S}].[{T_AUDIT}]
    WHERE detected_at >= DATEADD(day, -7, GETUTCDATE())
    ORDER BY detected_at DESC;
  ──────────────────────────────────────────────────────
  To load manually:
    cd {out_dir.resolve()}
    run_all.bat   (Windows)
    ./run_all.sh  (Linux / Mac)
{'='*70}
""")


if __name__ == "__main__":
    main()