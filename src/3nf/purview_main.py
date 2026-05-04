"""
purview_main.py
===============
Main orchestration script — Microsoft Purview → Azure SQL  (5-table mode).

Reads purview_config.ini (or path passed as first argument).
Fetches data from Purview APIs, assembles row-sets for 5 tables,
delegates all SQL / DDL / DB-write work to purview_sql_model.py.

FIVE TABLES
───────────
1. purview_search_assets   — raw search catalog (all leaf assets found)
2. purview_collections     — all collections (always full load)
3. purview_entities        — bulk entity details for fetched GUIDs
4. purview_glossary_terms  — glossary terms (always full load)
5. asset_registry          — enriched assets (cls / label / tag applied)

NULL POLICY
───────────
Only fields with real values are written.  Empty / None fields are
stripped before SQL generation.  Mandatory PK fields are validated in
purview_sql_model before any SQL is produced — rows missing PKs are
skipped with a WARN.

DUPLICATE POLICY
────────────────
Every table uses MERGE on its PK.  Re-running any file is safe.

COLUMN-ENRICHMENT FILTER
────────────────────────
asset_registry captures an asset only when ≥1 of its columns has:
  • classification applied        (timestamp-based — primary signal)
  • sensitivity label applied     (column updateTime ≥ LAST_RUN cutoff)
  • business attribute / tag set  (column updateTime ≥ LAST_RUN cutoff)
The timeframe window (LAST_RUN) is applied via classification timestamps
ONLY — asset created / updated timestamps are never used as a filter.

COLLECTIONS FILTER
──────────────────
Set COLLECTIONS_FILTER = none  to load all collections.
Or supply comma-separated hierarchy paths to restrict which assets are
loaded into asset_registry, e.g.:
    COLLECTIONS_FILTER = Finance / Europe, HR / APAC

Usage
─────
    python purview_main.py [purview_config.ini]
"""

import os, sys, uuid, json, time, hashlib, datetime
import requests, configparser, concurrent.futures, threading
from collections import defaultdict
from pathlib import Path

# ── Import the SQL modeling layer ──────────────────────────────────────
from purview_sql_model import (
    write_sql_files,
    push_to_azure_sql,
    csv_null,
    has_value,
)

# ═══════════════════════════════════════════════════════════════════════
#  CREDENTIALS  (client secret falls back to env var)
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
COLLECTIONS_FILTER = none
TBL_ASSET_REGISTRY = asset_registry
TBL_COLLECTIONS    = purview_collections
TBL_ENTITIES       = purview_entities
TBL_GLOSSARY       = purview_glossary_terms
TBL_SEARCH         = purview_search_assets
MAX_ROWS_PER_FILE  = 500
LAST_RUN           = all
RUN_SQL_IN_DB      = false
SQL_SERVER         = yourserver.database.windows.net
SQL_DATABASE       = yourdb
SQL_USER           = youruser
SQL_PASSWORD       = yourpassword
""", encoding="utf-8")
    print("Default config written.  Edit it and re-run.\n")
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

# ── Collections filter ────────────────────────────────────────────────
_COLL_FILTER_RAW   = _get("COLLECTIONS_FILTER", "none")
COLLECTIONS_FILTER = (None if _COLL_FILTER_RAW.lower() == "none"
                      else [f.strip() for f in _COLL_FILTER_RAW.split(",") if f.strip()])

# ── Five table names — ALL MANDATORY ─────────────────────────────────
_TABLE_KEYS = {
    "asset_registry": "TBL_ASSET_REGISTRY",
    "collections":    "TBL_COLLECTIONS",
    "entities":       "TBL_ENTITIES",
    "glossary_terms": "TBL_GLOSSARY",
    "search_assets":  "TBL_SEARCH",
}

TABLE_NAMES = {}
_missing = []
for _key, _cfg_key in _TABLE_KEYS.items():
    _val = _get(_cfg_key, "")
    if not _val:
        _missing.append(_cfg_key)
    TABLE_NAMES[_key] = _val

if _missing:
    print(f"\n[ERROR] These config keys are required but missing or blank:\n"
          f"  {', '.join(_missing)}\n"
          f"Please set them in [{_config_path}] and re-run.\n")
    sys.exit(1)

# ═══════════════════════════════════════════════════════════════════════
#  API ENDPOINTS
# ═══════════════════════════════════════════════════════════════════════

SEARCH_API      = (f"https://{PURVIEW_ACCOUNT}.purview.azure.com"
                   f"/datamap/api/search/query?api-version=2023-09-01")
ENTITY_API      = (f"https://{PURVIEW_ACCOUNT}.purview.azure.com"
                   f"/datamap/api/atlas/v2/entity/guid")
BULK_ENTITY_API = (f"https://{PURVIEW_ACCOUNT}.purview.azure.com"
                   f"/datamap/api/atlas/v2/entity/bulk")
COLLECTION_API  = (f"https://{PURVIEW_ACCOUNT}.purview.azure.com"
                   f"/account/collections?api-version=2019-11-01-preview")
GLOSSARY_API    = (f"https://{PURVIEW_ACCOUNT}.purview.azure.com"
                   f"/datamap/api/atlas/v2/glossary/terms"
                   f"?api-version=2023-09-01&limit=1000&offset=0&sort=ASC")

SEARCH_PAGE_SIZE   = 1000
BATCH_SIZE         = 100
MAX_RETRIES        = 5
RETRY_BACKOFF      = 2
RETRY_STATUS_CODES = {429, 500, 502, 503, 504}
TOKEN_REFRESH_MINS = 50
MAX_WORKERS        = 50
MIN_VALID_MS       = 946_684_800_000      # 2000-01-01T00:00:00Z

# ── Structural asset filters (excluded from leaf detection) ───────────
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
    "_metadata", "_docs", "_sprocs", "_triggers", "_udfs",
    "_conflicts",
}

_PREFIX_TO_DS = {
    "mssql":         "SQL Server",
    "azure_sql":     "Azure SQL Database",
    "oracle":        "Oracle",
    "postgresql":    "PostgreSQL",
    "mysql":         "MySQL",
    "snowflake":     "Snowflake",
    "databricks":    "Databricks",
    "azure_blob":    "Azure Blob Storage",
    "azure_adls":    "Azure Data Lake",
    "azure_datalake":"Azure Data Lake",
    "azure_cosmosdb":"Azure Cosmos DB",
    "azure_cosmos":  "Azure Cosmos DB",
    "azure_synapse": "Azure Synapse",
    "teradata":      "Teradata",
    "amazon_rds":    "Amazon RDS",
    "amazon_s3":     "Amazon S3",
    "hive":          "Hive",
    "sap_hana":      "SAP HANA",
    "sap_ecc":       "SAP ECC",
}

# ═══════════════════════════════════════════════════════════════════════
#  LOGGING  (format preserved exactly)
# ═══════════════════════════════════════════════════════════════════════

_print_lock = threading.Lock()


def log(msg, level="INFO"):
    ts   = datetime.datetime.now(datetime.timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
    icon = {"INFO": "[INFO]", "OK": "[OK]  ",
            "WARN": "[WARN]", "ERROR": "[ERR] "}.get(level, "     ")
    with _print_lock:
        print(f"[{ts}] {icon} {msg}")


def log_section(title):
    with _print_lock:
        print(f"\n{'='*70}\n  {title}\n{'='*70}")


def fmt_dur(s):
    s = int(s); h, r = divmod(s, 3600); m, s = divmod(r, 60)
    return f"{h}h {m}m {s}s" if h else (f"{m}m {s}s" if m else f"{s}s")

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
#  HTTP helper — retries with back-off
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
                log(f"HTTP {r.status_code} on {label} — retry "
                    f"{attempt}/{MAX_RETRIES} in {wait}s", "WARN")
                time.sleep(wait)
                continue
            return r
        except (requests.exceptions.ConnectionError,
                requests.exceptions.Timeout,
                requests.exceptions.ChunkedEncodingError) as e:
            log(f"Network error on {label}: {e} — "
                f"retry {attempt} in {RETRY_BACKOFF*attempt}s", "WARN")
            time.sleep(RETRY_BACKOFF * attempt)
    log(f"Gave up after {MAX_RETRIES} attempts: {label}", "ERROR")
    return None

# ═══════════════════════════════════════════════════════════════════════
#  HELPER UTILITIES
# ═══════════════════════════════════════════════════════════════════════

import re as _re
_UUID_RE = _re.compile(
    r'^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$',
    _re.I)


def sid(val):
    return hashlib.md5(str(val).encode()).hexdigest()[:16]


def short_name(qn):
    parts = [p for p in str(qn).rstrip("/").split("/") if p]
    return parts[-1] if parts else qn


def is_leaf(asset):
    ot = asset.get("objectType", "")
    et = asset.get("entityType", "").lower().strip()
    if ot in STRUCTURAL_OBJECT_TYPES: return False
    if not et: return False
    return et.rsplit("_", 1)[-1] not in STRUCTURAL_KINDS


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
    _struct = {"servers", "server", "accounts", "account", "instances",
               "instance", "hosts", "host", "nodes", "node",
               "clusters", "cluster"}
    if parts[0].lower() in _struct and len(parts) > 1:
        return parts[1]
    return parts[0]


def get_schema_path(qn):
    if "://" in qn:
        segs = [s for s in qn.split("/")[3:] if s]
        if len(segs) >= 2:   return "/".join(segs[:-1])
        elif len(segs) == 1: return segs[0]
    return None


def _resolve_by(raw, scan_label=None):
    if not raw: return None
    s = str(raw).strip()
    if not s: return None
    if _UUID_RE.match(s):
        return f"Automated Scan ({scan_label or s[:8]}...)"
    return s

# ═══════════════════════════════════════════════════════════════════════
#  TIMEFRAME FILTER  (classification / label / tag — all timestamp-driven)
#
#  Returns True if the column should be included in the current window.
#  Logic:
#    • Full load (cutoff_ms=None)  → always True
#    • Incremental load:
#        1. Classification   — include if any ACTIVE cls has timestamp ≥ cutoff_ms
#                              Gate A2: epoch-0 MICROSOFT.* cls → use asset updateTime
#        2. Sensitivity label— include if any label present AND column/asset
#                              updateTime ≥ cutoff_ms (Purview has no label timestamp;
#                              column updateTime is bumped when label is applied)
#        3. Business tag     — include if any tag present AND column updateTime
#                              ≥ cutoff_ms (same proxy as labels)
#                              Fallback: asset updateTime if column has no timestamp
#  Asset created/updated timestamps are ONLY used as a fallback proxy for
#  signals that carry no direct timestamp (labels, tags, epoch-0 cls).
# ═══════════════════════════════════════════════════════════════════════

def col_in_timeframe(col, cutoff_ms, a_upd_ms=None):
    if cutoff_ms is None:
        return True

    # ── 1. Classification timestamps (primary, reliable signal) ───────
    has_epoch_zero_cls = False
    for c in (col.get("classifications") or []):
        if c.get("entityStatus", "ACTIVE") == "DELETED":
            continue
        raw_ms = None
        for key in ("lastModifiedTS", "updateTime", "createTime"):
            raw_ms = _safe_ms(c.get(key))
            if raw_ms: break
        if not raw_ms:
            for key in ("lastModifiedTS", "updateTime", "createTime"):
                raw_ms = _safe_ms((c.get("attributes") or {}).get(key))
                if raw_ms: break
        if raw_ms and raw_ms >= cutoff_ms:
            return True
        if not raw_ms:
            # epoch-0 sentinel — track for Gate A2
            has_epoch_zero_cls = True

    # ── Gate A2: epoch-0 MICROSOFT.* auto-scan classifications ────────
    # Purview sets lastModifiedTS=0 for all auto-scan cls. If the asset
    # itself was updated within the window, include the column.
    if has_epoch_zero_cls and a_upd_ms and a_upd_ms >= cutoff_ms:
        return True

    # ── 2. Sensitivity labels — filter by column's own updateTime ─────
    # Purview has no label-applied-at timestamp.  When a sensitivity label
    # is applied, Purview bumps the column entity's updateTime — use that.
    labels = [l for l in (col.get("labels") or []) if l]
    if labels:
        col_upd_ms = None
        for key in ("updateTime", "lastModifiedTS", "createTime"):
            col_upd_ms = _safe_ms(col.get(key))
            if col_upd_ms: break
        if not col_upd_ms:
            attrs = col.get("attributes") or {}
            for key in ("updateTime", "lastModifiedTS", "createTime"):
                col_upd_ms = _safe_ms(attrs.get(key))
                if col_upd_ms: break
        if col_upd_ms and col_upd_ms >= cutoff_ms:
            return True
        if not col_upd_ms and a_upd_ms and a_upd_ms >= cutoff_ms:
            return True

    # ── 3. Business tags — filter by column's own updateTime ──────────
    # Purview does not expose a per-tag applied-at timestamp.
    # When a business attribute is set on a column, Purview bumps the
    # column entity's updateTime / lastModifiedTS.  We use that as the
    # applied-at proxy — the same approach as Gate A2 for classifications.
    has_tags = any(isinstance(gv, dict) and gv
                   for gv in (col.get("businessAttributes") or {}).values())
    if has_tags:
        # Extract column-level update timestamp
        col_upd_ms = None
        for key in ("updateTime", "lastModifiedTS", "createTime"):
            col_upd_ms = _safe_ms(col.get(key))
            if col_upd_ms: break
        if not col_upd_ms:
            attrs = col.get("attributes") or {}
            for key in ("updateTime", "lastModifiedTS", "createTime"):
                col_upd_ms = _safe_ms(attrs.get(key))
                if col_upd_ms: break

        if col_upd_ms and col_upd_ms >= cutoff_ms:
            return True
        # No column timestamp — fall back to asset updateTime (same as Gate A2)
        if not col_upd_ms and a_upd_ms and a_upd_ms >= cutoff_ms:
            return True

    return False

# ═══════════════════════════════════════════════════════════════════════
#  CLASSIFICATION / LABEL / TAG DETAIL EXTRACTORS
# ═══════════════════════════════════════════════════════════════════════

def _cls_details(col, asset_update_time=None):
    """
    Returns (applied_at, applied_by, cls_names_csv, detail_csv).
    MICROSOFT.* auto-scan classifications may have lastModifiedTS=0;
    those fall back to the asset scan time.
    Returns (None, None, None, None) when no active classifications.
    """
    best_ms = None; best_by = None; fallback_by = None
    names = []; details = []

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
            for key in ("lastModifiedTS", "updateTime", "createTime"):
                raw_ms = _safe_ms((c.get("attributes") or {}).get(key))
                if raw_ms: break

        raw_by = (c.get("source") or c.get("createdBy") or
                  (c.get("attributes") or {}).get("source") or
                  (c.get("attributes") or {}).get("createdBy") or None)
        readable_by = _resolve_by(raw_by)

        if raw_ms:
            if best_ms is None or raw_ms > best_ms:
                best_ms = raw_ms; best_by = readable_by
            via_fallback = False
        elif asset_update_time:
            via_fallback = True
            if fallback_by is None and readable_by:
                fallback_by = readable_by
        else:
            via_fallback = False

        ts_iso = (ms_to_iso(raw_ms) if raw_ms
                  else (asset_update_time if via_fallback else None))
        parts = [cn, f"Applied by: {readable_by or 'Automated Scan'}"]
        if ts_iso:
            parts.append(f"Scan completed: {ts_iso} (approx)"
                         if via_fallback else f"Applied at: {ts_iso}")
        details.append("  |  ".join(parts))

    if not names:
        return None, None, None, None

    applied_at = ms_to_iso(best_ms) if best_ms else asset_update_time
    applied_by = best_by or fallback_by or "Automated Scan"
    return applied_at, applied_by, csv_null(names), csv_null(details)


def _lbl_details(col):
    labels = [l for l in (col.get("labels") or []) if l]
    return None, None, csv_null(labels)   # Purview API has no label timestamps


def _tag_display_list(col):
    result = []
    for grp, gvals in (col.get("businessAttributes") or {}).items():
        if not isinstance(gvals, dict): continue
        for attr_nm, attr_val in gvals.items():
            tk = f"{grp}.{attr_nm}"
            tv = str(attr_val) if attr_val is not None else None
            result.append(f"{tk}={tv}" if tv else tk)
    return result

# ═══════════════════════════════════════════════════════════════════════
#  TIMESTAMP HELPERS  (entity-level)
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
    return e.get("createdBy") or (e.get("attributes") or {}).get("owner") or None


def _col_upd_at(col):
    return ms_to_iso(col.get("updateTime")) or ms_to_iso(col.get("lastModifiedTS"))


def _col_crt_at(col):
    return ms_to_iso(col.get("createTime")) or ms_to_iso(col.get("createdTS"))


def _col_upd_by(col):
    return col.get("updatedBy") or col.get("modifiedBy") or None

# ═══════════════════════════════════════════════════════════════════════
#  FETCH  —  ASSETS (search API, paginated)
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
            log(f"  Page {page}: {len(batch)} assets  |  "
                f"running total: {len(items)}", "OK")
            tk = data.get("continuationToken")
            if not tk or not batch: break
            page += 1
        return items

    if cutoff_ms is None:
        log_section("STEP 1 — Fetch ALL assets (full load)")
    else:
        log_section(f"STEP 1 — Fetch ALL assets  "
                    f"[timeframe filter: after {ms_to_iso(cutoff_ms)}]")
    assets = _pages({})
    log(f"  Total assets: {len(assets)}", "OK")
    return assets

# ═══════════════════════════════════════════════════════════════════════
#  FETCH  —  ENTITIES (bulk, threaded)
# ═══════════════════════════════════════════════════════════════════════

def _one_batch(bn, batch, total):
    params = "&".join(f"guid={g}" for g in batch)
    url    = (f"{BULK_ENTITY_API}?{params}"
              f"&minExtInfo=true&ignoreRelationships=false")
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
    # fallback: single-fetch
    log(f"Bulk {bn} failed — falling back to single fetches", "WARN")
    for g in batch:
        r2 = api("GET",
                 f"{ENTITY_API}/{g}?minExtInfo=true&ignoreRelationships=false",
                 label=f"Single {g[:8]}")
        if r2 and r2.status_code == 200:
            try:
                raw = r2.json()
                out[g] = {"entity":            raw.get("entity", {}),
                          "referredEntities":   raw.get("referredEntities", {})}
            except Exception:
                pass
    return out


def fetch_entities(guids):
    log_section(
        f"STEP 2 — Bulk entity fetch  ({len(guids)} GUIDs, {MAX_WORKERS} workers)")
    total   = max(1, (len(guids) + BATCH_SIZE - 1) // BATCH_SIZE)
    batches = [(i // BATCH_SIZE + 1, guids[i:i+BATCH_SIZE], total)
               for i in range(0, len(guids), BATCH_SIZE)]
    results = {}
    with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_WORKERS) as ex:
        futs = {ex.submit(_one_batch, b[0], b[1], b[2]): b[0] for b in batches}
        for f in concurrent.futures.as_completed(futs):
            try:
                results.update(f.result())
            except Exception as e:
                log(f"Batch future error: {e}", "WARN")
    log(f"Entities fetched: {len(results)}", "OK")
    return results

# ═══════════════════════════════════════════════════════════════════════
#  FETCH  —  COLLECTIONS
# ═══════════════════════════════════════════════════════════════════════

def fetch_collections():
    log_section("STEP 3 — Collections (full load)")
    r = api("GET", COLLECTION_API, label="Collections")
    if r is None or r.status_code != 200:
        log("Collections API failed.", "WARN")
        return {}, []
    items = r.json().get("value", [])
    log(f"Collections: {len(items)}", "OK")
    coll_map = {c.get("name", ""): c.get("friendlyName", "") for c in items}
    return coll_map, items


def build_coll_paths(items):
    by_id = {c.get("name", ""): c for c in items}

    def path(cid, seen=None):
        seen = seen or set()
        if cid in seen or cid not in by_id: return ""
        seen.add(cid)
        c  = by_id[cid]
        p  = c.get("parentCollection", {}).get("referenceName", "")
        n  = c.get("friendlyName") or c.get("name", "")
        if p and p in by_id and p != cid:
            pp = path(p, seen)
            return f"{pp} / {n}" if pp else n
        return n

    return {cid: path(cid) for cid in by_id}

# ═══════════════════════════════════════════════════════════════════════
#  FETCH  —  GLOSSARY TERMS (always full load, paginated)
# ═══════════════════════════════════════════════════════════════════════

def fetch_glossary_terms():
    log_section("STEP 4 — Glossary terms (full load)")
    terms = []
    base  = f"https://{PURVIEW_ACCOUNT}.purview.azure.com/datamap/api/atlas/v2"

    # ── Strategy 1: list all glossaries, then fetch terms per glossary ──
    # The /glossary/terms endpoint requires a glossary GUID in newer API
    # versions. First discover all glossaries (usually just "Purview"),
    # then page through each one's terms.
    glossaries = []
    r_gl = api("GET", f"{base}/glossary?limit=100&offset=0",
               label="Glossary list")
    if r_gl and r_gl.status_code == 200:
        raw = r_gl.json()
        if isinstance(raw, list):
            glossaries = raw
        elif isinstance(raw, dict):
            glossaries = raw.get("value", [raw]) if "value" in raw else [raw]

    if glossaries:
        for gl in glossaries:
            gl_guid = gl.get("guid") or gl.get("glossaryId") or gl.get("name")
            gl_name = gl.get("name", gl_guid)
            if not gl_guid:
                continue
            offset = 0
            limit  = 1000
            while True:
                url = (f"{base}/glossary/{gl_guid}/terms"
                       f"?limit={limit}&offset={offset}&sort=ASC")
                r = api("GET", url, label=f"Glossary '{gl_name}' offset={offset}")
                if r is None or r.status_code != 200:
                    log(f"Glossary '{gl_name}' terms failed at offset {offset}.",
                        "WARN")
                    break
                batch = r.json()
                if not isinstance(batch, list):
                    batch = batch.get("value", []) if isinstance(batch, dict) else []
                if not batch:
                    break
                terms.extend(batch)
                log(f"  Glossary '{gl_name}' offset {offset}: {len(batch)} terms  |  "
                    f"total so far: {len(terms)}", "OK")
                if len(batch) < limit:
                    break
                offset += limit
    else:
        # ── Strategy 2: fallback — try the flat /glossary/terms endpoint ──
        log("No glossaries found via list — trying flat /glossary/terms.", "WARN")
        offset = 0
        limit  = 1000
        while True:
            url = (f"{base}/glossary/terms"
                   f"?limit={limit}&offset={offset}&sort=ASC")
            r = api("GET", url, label=f"Glossary flat offset={offset}")
            if r is None or r.status_code != 200:
                log(f"Glossary flat API failed at offset {offset}.", "WARN")
                break
            batch = r.json()
            if not isinstance(batch, list):
                batch = batch.get("value", []) if isinstance(batch, dict) else []
            if not batch:
                break
            terms.extend(batch)
            log(f"  Glossary flat offset {offset}: {len(batch)} terms  |  "
                f"total so far: {len(terms)}", "OK")
            if len(batch) < limit:
                break
            offset += limit

    log(f"Glossary terms fetched: {len(terms)}", "OK")
    return terms

# ═══════════════════════════════════════════════════════════════════════
#  COLUMN EXTRACTION  (from entity JSON)
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
        r = api("GET",
                f"{ENTITY_API}/{sg}?minExtInfo=false",
                label=f"Schema {sg[:8]}")
        if r and r.status_code == 200:
            try:
                for g, obj in r.json().get("referredEntities", {}).items():
                    tn = obj.get("typeName", "").lower()
                    if any(x in tn for x in ("column", "field", "attribute")):
                        cols[g] = inj(g, obj)
            except Exception:
                pass

    # ── tabular_schema path (CosmosDB, blob resource sets, ADLS, etc.) ──
    # Any entity type that stores columns via a linked tabular_schema object.
    ts = rels.get("tabular_schema", {})
    tg = ts.get("guid") if isinstance(ts, dict) else None
    if tg:
        r = api("GET",
                f"{ENTITY_API}/{tg}?minExtInfo=true",
                label=f"TabSchema {tg[:8]}")
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
            except Exception:
                pass

    return list(cols.values())

# ═══════════════════════════════════════════════════════════════════════
#  ROW BUILDERS  —  one function per target table
# ═══════════════════════════════════════════════════════════════════════

def build_search_asset_row(asset, scan_run_id, scan_ts):
    """Build a row dict for purview_search_assets from one search result."""
    asset_type = asset.get("assetType")
    if isinstance(asset_type, list):
        asset_type = csv_null(asset_type)

    return {
        "asset_id":         asset.get("id"),
        "asset_name":       asset.get("name"),
        "display_text":     asset.get("displayText"),
        "qualified_name":   asset.get("qualifiedName"),
        "entity_type":      asset.get("entityType"),
        "object_type":      asset.get("objectType"),
        "description":      asset.get("description"),
        "collection_id":    asset.get("collectionId"),
        "domain_id":        asset.get("domainId"),
        "create_by":        asset.get("createBy") or asset.get("createdBy"),
        "update_by":        asset.get("updateBy") or asset.get("updatedBy"),
        "create_time_epoch":asset.get("createTime"),
        "update_time_epoch":asset.get("updateTime"),
        "is_indexed":       asset.get("isIndexed", True),
        "asset_type":       asset_type,
        "search_score":     asset.get("@search.score"),
        "fetched_at":       scan_ts,
        "scan_run_id":      scan_run_id,
    }


def build_collection_row(coll, purview_account, api_endpoint, scan_run_id, scan_ts):
    """Build a row dict for purview_collections from one collection item."""
    sys_data = coll.get("systemData") or {}
    parent   = (coll.get("parentCollection") or {}).get("referenceName")

    return {
        "collection_name":               coll.get("name"),
        "friendly_name":                 coll.get("friendlyName"),
        "description":                   coll.get("description"),
        "parent_collection_name":        parent,
        "created_by":                    sys_data.get("createdBy"),
        "created_by_type":               sys_data.get("createdByType"),
        "created_at":                    sys_data.get("createdAt"),
        "last_modified_by":              sys_data.get("lastModifiedBy"),
        "last_modified_by_type":         sys_data.get("lastModifiedByType"),
        "last_modified_at":              sys_data.get("lastModifiedAt"),
        "collection_provisioning_state": coll.get("collectionProvisioningState"),
        "purview_account":               purview_account,
        "api_endpoint":                  api_endpoint,
        "fetched_at":                    scan_ts,
        "scan_run_id":                   scan_run_id,
    }


def build_entity_row(guid, ej, scan_run_id, scan_ts):
    """Build a row dict for purview_entities from one entity JSON."""
    entity = ej.get("entity", {})
    attrs  = entity.get("attributes") or {}

    lts = entity.get("lastModifiedTS")
    lts_str = str(lts) if lts is not None else None

    return {
        "guid":             guid,
        "entity_type_name": entity.get("typeName"),
        "entity_name":      (attrs.get("name") or
                             short_name(attrs.get("qualifiedName", "")) or None),
        "qualified_name":   attrs.get("qualifiedName"),
        "owner_name":       attrs.get("owner"),
        "modified_time":    attrs.get("modifiedTime"),
        "total_size_bytes": (attrs.get("size") or
                             attrs.get("totalSizeBytes")),
        "partition_count":  attrs.get("partitionCount"),
        "schema_count":     attrs.get("schemaCount"),
        "last_modified_ts": lts_str,
        "is_incomplete":    entity.get("isIncomplete", False),
        "provenance_type":  entity.get("provenanceType"),
        "status":           entity.get("status"),
        "created_by":       entity.get("createdBy"),
        "updated_by":       entity.get("updatedBy"),
        "create_time_epoch":entity.get("createTime"),
        "update_time_epoch":entity.get("updateTime"),
        "version_no":       entity.get("version"),
        "is_indexed":       True,
        "source_name":      (attrs.get("dataSourceName") or
                             (entity.get("source") if entity.get("source") else None)),
        "scan_resource_id": (attrs.get("lastScanResourceId") or
                             attrs.get("scanResourceId")),
        "collection_id":    entity.get("collectionId"),
        "domain_id":        entity.get("domainId"),
        "display_text":     entity.get("displayText") or attrs.get("name"),
        "proxy_flag":       entity.get("proxy", False),
        "fetched_at":       scan_ts,
        "scan_run_id":      scan_run_id,
    }


def build_glossary_row(term, scan_run_id, scan_ts):
    """Build a row dict for purview_glossary_terms from one term object."""
    anchor       = term.get("anchor") or {}
    synonyms     = term.get("synonyms") or []
    syn_text     = csv_null([s.get("displayText") for s in synonyms
                             if s.get("displayText")])
    # relation_guid comes from assignedEntities or seeAlso linkage
    # Use first seeAlso guid if available
    see_also     = term.get("seeAlso") or []
    rel_guid     = (see_also[0].get("termGuid") if see_also else None)

    lts = term.get("lastModifiedTS")

    return {
        "guid":               term.get("guid"),
        "qualified_name":     term.get("qualifiedName"),
        "term_name":          term.get("name"),
        "long_description":   term.get("longDescription"),
        "last_modified_ts":   str(lts) if lts is not None else None,
        "created_by":         term.get("createdBy"),
        "updated_by":         term.get("updatedBy"),
        "create_time_epoch":  term.get("createTime"),
        "update_time_epoch":  term.get("updateTime"),
        "domain_id":          term.get("domainId"),
        "abbreviation":       term.get("abbreviation"),
        "status":             term.get("status"),
        "glossary_guid":      anchor.get("glossaryGuid"),
        "relation_guid":      rel_guid,
        "synonym_display_text": syn_text,
        "fetched_at":         scan_ts,
        "scan_run_id":        scan_run_id,
    }


def build_asset_registry_row(guid, a, ej, coll_map, coll_paths,
                              ds_type, instance,
                              asset_col_cnt, asset_cls_cnt, asset_cls_set,
                              scan_run_id, scan_ts):
    """
    Build a row dict for asset_registry.
    Called only when the asset has ≥1 enriched column.
    """
    qn      = a.get("qualifiedName", "")
    et      = a.get("entityType", "")
    ot      = a.get("objectType", "")
    coll_id = a.get("collectionId", "")
    coll_nm = coll_map.get(coll_id, "")
    coll_p  = coll_paths.get(coll_id, coll_nm)
    a_name  = short_name(qn)
    sch_p   = get_schema_path(qn)

    a_crt_at = _e_crt_at(ej) if ej else None
    a_crt_by = _e_crt_by(ej) if ej else None
    a_upd_at = _e_upd_at(ej) if ej else None
    a_upd_by = _e_upd_by(ej) if ej else None

    cls_found = csv_null(sorted(asset_cls_set)) if asset_cls_set else None

    return {
        "asset_guid":               guid,
        "asset_name":               a_name or None,
        "asset_qualified_name":     qn or None,
        "asset_entity_type":        et or None,
        "asset_object_type":        ot or None,
        "datasource_type":          ds_type or None,
        "datasource_instance":      instance or None,
        "schema_path":              sch_p or None,
        "collection_id":            coll_id or None,
        "collection_name":          coll_nm or None,
        "collection_hierarchy_path":coll_p or None,
        "total_columns":            asset_col_cnt if asset_col_cnt > 0 else None,
        "total_classified_columns": asset_cls_cnt if asset_cls_cnt > 0 else None,
        "has_classified_columns":   (asset_cls_cnt > 0) if asset_col_cnt > 0 else None,
        "classification_types_found": cls_found,
        "asset_created_at":         a_crt_at or None,
        "asset_created_by":         a_crt_by or None,
        "asset_last_updated_at":    a_upd_at or None,
        "asset_last_updated_by":    a_upd_by or None,
        "scan_run_id":              scan_run_id,
        "scan_timestamp":           scan_ts,
        "scan_status":              "ACTIVE",
    }

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

    log_section("PURVIEW → AZURE SQL  |  purview_main.py  (5-table mode)")
    log(f"Config        : {_config_path}")
    log(f"Account       : {PURVIEW_ACCOUNT}")
    log(f"Schema        : {SCHEMA_NAME}")
    log(f"Tables        : {len(TABLE_NAMES)} tables  "
        f"({', '.join(TABLE_NAMES.values())})")
    log(f"Output folder : {SQL_OUTPUT_FOLDER}/")
    log(f"JSON folder   : {JSON_OUTPUT_FOLDER}/")
    log(f"Snapshot      : {SNAPSHOT_FILE}")
    log(f"LAST_RUN      : {run_label}")
    log(f"Coll filter   : {_COLL_FILTER_RAW}")
    log(f"RUN_SQL_IN_DB : {RUN_SQL_IN_DB}")
    log(f"Workers       : {MAX_WORKERS}")
    log(f"Start         : {ts} UTC")
    log(f"NULL policy   : mandatory PKs validated in Python — nulls never reach DB")
    log(f"Dup  policy   : MERGE on PK per table — re-run safe, no duplicates")
    log(f"Enrich filter : classification applied | label applied | tag applied")

    if not CLIENT_SECRET:
        log("PURVIEW_CLIENT_SECRET not set.", "ERROR"); sys.exit(1)

    scan_run_id = str(uuid.uuid4())
    scan_ts     = ts
    S           = SCHEMA_NAME
    out_dir     = Path(SQL_OUTPUT_FOLDER)
    json_dir    = Path(JSON_OUTPUT_FOLDER)
    json_dir.mkdir(parents=True, exist_ok=True)

    # ── STEP 1 — Fetch assets ─────────────────────────────────────────
    all_assets = fetch_assets(cutoff_ms)
    leaf       = [a for a in all_assets if is_leaf(a)]
    log(f"Leaf: {len(leaf)}  |  Structural skipped: "
        f"{len(all_assets) - len(leaf)}")

    # Collections filter is applied AFTER coll_paths is fully resolved
    # (inside the asset_registry loop below, line ~1075).
    # DO NOT filter here — collectionId is a short internal ID that never
    # matches hierarchy path strings like "POC_Finastra / Azure SQL Task".

    guids         = [a["id"] for a in leaf if a.get("id")]
    guid_to_asset = {a["id"]: a for a in leaf if a.get("id")}

    # ── STEP 2 — Fetch entities ───────────────────────────────────────
    entity_map = fetch_entities(guids)

    # ── STEP 3 — Fetch collections ────────────────────────────────────
    coll_map, coll_items = fetch_collections()
    coll_paths = build_coll_paths(coll_items)

    # ── STEP 4 — Fetch glossary terms ─────────────────────────────────
    glossary_terms = fetch_glossary_terms()

    # ── STEP 5 — Build row sets ───────────────────────────────────────
    log_section("STEP 5 — Build row sets for all 5 tables")

    # -- purview_search_assets  (all leaf assets from search)
    search_rows = [build_search_asset_row(a, scan_run_id, scan_ts)
                   for a in leaf]
    log(f"  purview_search_assets  : {len(search_rows)} rows", "INFO")

    # -- purview_collections  (all collections)
    coll_rows = [build_collection_row(c, PURVIEW_ACCOUNT, COLLECTION_API,
                                      scan_run_id, scan_ts)
                 for c in coll_items]
    log(f"  purview_collections    : {len(coll_rows)} rows", "INFO")

    # -- purview_entities  (all fetched entities)
    entity_rows = [build_entity_row(g, ej, scan_run_id, scan_ts)
                   for g, ej in entity_map.items()]
    log(f"  purview_entities       : {len(entity_rows)} rows", "INFO")

    # -- purview_glossary_terms
    glossary_rows = [build_glossary_row(t, scan_run_id, scan_ts)
                     for t in glossary_terms]
    log(f"  purview_glossary_terms : {len(glossary_rows)} rows", "INFO")

    # -- asset_registry  (enriched assets only — classified/labeled/tagged)
    # Process assets grouped by datasource then instance for ordered logging
    registry_rows = []
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

                a_upd_at = _e_upd_at(ej) if ej else None
                a_upd_by = _e_upd_by(ej) if ej else None
                # Raw ms needed for Gate A2 (epoch-0 MICROSOFT.* auto-scan cls)
                a_upd_ms = _safe_ms(
                    (ej.get("entity", {}).get("updateTime") or
                     ej.get("entity", {}).get("lastModifiedTS"))
                    if ej else None
                )

                # Apply collections-hierarchy filter (post full-path resolution).
                # Normalize both sides: collapse whitespace and lowercase so that
                # "POC_Finastra / Azure SQL Task " matches "POC_Finastra / Azure SQL Task".
                if COLLECTIONS_FILTER and coll_p:
                    # Normalise: collapse whitespace, lowercase, AND strip spaces
                    # around "/" so "POC_Finastra/Azure SQL Task" matches
                    # "POC_Finastra / Azure SQL Task" (Purview always adds spaces).
                    def _norm(s):
                        return " ".join(
                            "/".join(p.strip() for p in s.split("/")).split()
                        ).lower()
                    _coll_p_norm = _norm(coll_p)
                    if not any(_coll_p_norm.startswith(_norm(cf))
                               for cf in COLLECTIONS_FILTER):
                        continue

                if not ej:
                    continue

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
                            if not a_upd_at: a_upd_at = _e_upd_at(fb)
                            if not a_upd_by: a_upd_by = _e_upd_by(fb)
                        except Exception:
                            pass

                total_assets  += 1
                asset_col_cnt  = 0
                asset_cls_cnt  = 0
                asset_cls_set  = set()
                cols_in_tf_list = []

                for col_num, col in enumerate(cols, 1):
                    attr     = col.get("attributes", {})
                    col_name = attr.get("name")
                    if not col_name or col_name in COSMOS_SYSTEM_FIELDS:
                        continue

                    # ── Timeframe filter: classification / label / tag
                    #    timestamps only — NOT asset timestamps ──────────
                    in_tf = col_in_timeframe(col, cutoff_ms, a_upd_ms)
                    asset_col_cnt += 1; total_cols += 1

                    if not in_tf:
                        continue

                    # Only include columns that carry enrichment
                    _has_cls = any(
                        c.get("typeName") and
                        c.get("entityStatus", "ACTIVE") != "DELETED"
                        for c in (col.get("classifications") or []))
                    _has_lbl = bool([l for l in (col.get("labels") or []) if l])
                    _has_tag = any(
                        isinstance(gv, dict) and gv
                        for gv in (col.get("businessAttributes") or {}).values())

                    if not (_has_cls or _has_lbl or _has_tag):
                        continue

                    cols_in_tf += 1

                    cls_app_at, cls_app_by, cls_names_csv, cls_detail = \
                        _cls_details(col, asset_update_time=a_upd_at)

                    cls_names = [
                        c.get("typeName", "")
                        for c in (col.get("classifications") or [])
                        if c.get("typeName") and
                        c.get("entityStatus", "ACTIVE") != "DELETED"]

                    if cls_names:
                        asset_cls_cnt += 1
                        asset_cls_set.update(cls_names)
                        total_cls += len(cls_names)

                    lbl_app_at, lbl_app_by, lbl_names_csv = _lbl_details(col)
                    lbl_names = [l for l in (col.get("labels") or []) if l]
                    if lbl_names: total_lbl += len(lbl_names)

                    tag_list = _tag_display_list(col)
                    if tag_list: total_tag += len(tag_list)

                    cols_in_tf_list.append((
                        col_num, len(cols), col_name,
                        attr.get("data_type") or attr.get("type"),
                        cls_names, cls_app_at, cls_app_by,
                        lbl_names, tag_list,
                    ))

                if not cols_in_tf_list:
                    continue     # asset has no enriched columns — skip registry

                # ── Log asset details (same format as before) ──────────
                _full = (cutoff_ms is None)
                with _print_lock:
                    print(f"\n  ASSET  : {qn}")
                    if ot:       print(f"  TYPE   : {ot} | {et}")
                    if coll_p:   print(f"  COLL   : {coll_p}")
                    if a_upd_at: print(f"  UPDATED: {a_upd_at}  by: {a_upd_by or '?'}")
                    n_tf = len(cols_in_tf_list)
                    print(f"  COLS   : {asset_col_cnt} total  |  "
                          f"{n_tf} "
                          f"{'with enrichment' if _full else 'in timeframe'}")
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

                registry_rows.append(
                    build_asset_registry_row(
                        guid, a, ej, coll_map, coll_paths,
                        ds_type, instance,
                        asset_col_cnt, asset_cls_cnt, asset_cls_set,
                        scan_run_id, scan_ts))

    log(f"  asset_registry         : {len(registry_rows)} rows", "INFO")

    # ── STEP 6 — Write SQL files ──────────────────────────────────────
    log_section(f"STEP 6 — Write SQL files → {out_dir}/")

    row_sets = {
        "search_assets":  search_rows,
        "collections":    coll_rows,
        "entities":       entity_rows,
        "glossary_terms": glossary_rows,
        "asset_registry": registry_rows,
    }

    sql_files, ordered_files, row_counts = write_sql_files(
        out_dir, S, ts, scan_run_id, scan_ts,
        row_sets, TABLE_NAMES, MAX_ROWS_PER_FILE)

    # ── STEP 7 — Write JSON snapshot ─────────────────────────────────
    log_section("STEP 7 — Write JSON snapshot")
    snapshot_path = json_dir / SNAPSHOT_FILE
    snapshot = {
        "scan_run_id":        scan_run_id,
        "scan_time":          scan_ts,
        "config_file":        _config_path,
        "last_run_filter":    run_label,
        "schema_name":        S,
        "sql_output_folder":  str(out_dir),
        "json_output_folder": str(json_dir),
        "tables":             TABLE_NAMES,
        "null_policy":  (
            "Mandatory PKs validated in Python.  "
            "Only non-null values written via MERGE."),
        "dup_policy":   (
            "MERGE on PK per table — re-run safe, no duplicates possible."),
        "enrich_filter":(
            "asset_registry filtered by: classification applied, "
            "sensitivity label applied, or business tag applied.  "
            "All three signals use column updateTime as the applied-at "
            "timestamp (asset updateTime used as fallback when column "
            "carries no timestamp)."),
        "collections_filter": _COLL_FILTER_RAW,
        "summary": {
            "total_search_assets":   len(all_assets),
            "leaf_assets":           len(leaf),
            "entities_fetched":      len(entity_map),
            "collections_fetched":   len(coll_items),
            "glossary_terms_fetched":len(glossary_terms),
            "total_assets_processed":total_assets,
            "total_columns_seen":    total_cols,
            "columns_in_timeframe":  cols_in_tf,
            "total_classifications": total_cls,
            "total_labels":          total_lbl,
            "total_tags":            total_tag,
            "registry_rows_written": len(registry_rows),
        },
        "row_counts":         row_counts,
        "asset_registry":     registry_rows,
    }
    with open(snapshot_path, "w", encoding="utf-8") as f:
        json.dump(snapshot, f, indent=2, ensure_ascii=False)
    log(f"Snapshot written: {snapshot_path}  "
        f"({snapshot_path.stat().st_size:,} bytes)", "OK")

    # ── STEP 8 — Push to Azure SQL ────────────────────────────────────
    push_ok = False
    if RUN_SQL_IN_DB:
        push_ok = push_to_azure_sql(
            ordered_files, row_counts, TABLE_NAMES,
            SQL_SERVER, SQL_DATABASE, SQL_USER, SQL_PASSWORD, S)
    else:
        log("RUN_SQL_IN_DB=false — SQL files generated but NOT pushed to DB.",
            "INFO")
        log(f"  To load manually: cd {out_dir.resolve()} "
            f"&& run_all.bat  (or ./run_all.sh)", "INFO")

    # ── Summary ───────────────────────────────────────────────────────
    elapsed = fmt_dur(time.time() - t0)
    end_dt  = datetime.datetime.now(datetime.timezone.utc)

    print(f"""
{'='*70}
  COMPLETE — purview_main.py  (5-table mode)
{'='*70}
  Scan run ID    : {scan_run_id}
  Start          : {dt0.strftime('%Y-%m-%d %H:%M:%S')} UTC
  End            : {end_dt.strftime('%Y-%m-%d %H:%M:%S')} UTC
  Duration       : {elapsed}
  LAST_RUN       : {run_label}
  Coll filter    : {_COLL_FILTER_RAW}
  NULL policy    : mandatory PKs validated in Python — nulls never reach DB
  Dup  policy    : MERGE on PK — re-run safe, no duplicates
  Enrich filter  : classification applied | label applied | tag applied
  ──────────────────────────────────────────────────────
  SQL output     : {out_dir.resolve()}/
  SQL files      : {len(sql_files)} file(s)
  Snapshot       : {snapshot_path}
  ──────────────────────────────────────────────────────
  TABLE                             ROWS    DESCRIPTION
  {TABLE_NAMES['search_assets']:<33} {row_counts.get('search_assets',0):>6}    all leaf assets from search
  {TABLE_NAMES['collections']:<33} {row_counts.get('collections',0):>6}    all Purview collections
  {TABLE_NAMES['entities']:<33} {row_counts.get('entities',0):>6}    entity details (bulk fetch)
  {TABLE_NAMES['glossary_terms']:<33} {row_counts.get('glossary_terms',0):>6}    glossary terms (full load)
  {TABLE_NAMES['asset_registry']:<33} {row_counts.get('asset_registry',0):>6}    enriched assets (cls/lbl/tag)
  ──────────────────────────────────────────────────────
  Assets searched   : {len(all_assets):>6}
  Leaf assets       : {len(leaf):>6}
  Entities fetched  : {len(entity_map):>6}
  Collections       : {len(coll_items):>6}
  Glossary terms    : {len(glossary_terms):>6}
  Total columns     : {total_cols:>6}
  Columns in window : {cols_in_tf:>6}  (cls/lbl/tag in timeframe)
  Classifications   : {total_cls:>6}
  Labels            : {total_lbl:>6}
  Business tags     : {total_tag:>6}
  Registry rows     : {len(registry_rows):>6}  (enriched assets → asset_registry)
  ──────────────────────────────────────────────────────
  RUN_SQL_IN_DB  : {'SUCCESS' if push_ok else ('FAILED' if RUN_SQL_IN_DB else 'false — not pushed')}
  ──────────────────────────────────────────────────────
  To load manually:
    cd {out_dir.resolve()}
    run_all.bat   (Windows)
    ./run_all.sh  (Linux / Mac)
{'='*70}
""")


if __name__ == "__main__":
    main()