"""
scan.py
=======
Tool-agnostic compliance catalog scanner.
Produces a structured JSON snapshot of enriched columns.

ZERO TOOL-SPECIFIC CODE IN THIS FILE
──────────────────────────────────────
This file knows nothing about Purview, Collibra, Alation, or any
other specific tool.  All API calls are imported from a connector.

TO SWITCH TOOLS — CHANGE ONE LINE
───────────────────────────────────
  # Today: Microsoft Purview
  from connector_purview import *

  # Tomorrow: Collibra
  from connector_collibra import *

  # Or Alation
  from connector_alation import *

The connector provides these five functions (see connector_purview.py
for the exact contract each must satisfy):
  get_connector_name()   → str
  fetch_collections()    → (coll_map, coll_paths, raw_items)
  fetch_glossary_terms() → list[dict]
  fetch_assets()         → list[dict]
  fetch_entities(guids, max_workers) → {guid: entity_dict}
  fetch_single_entity(guid) → entity_dict | None
  extract_columns(ejson) → list[dict]

And these helper functions:
  is_leaf(asset)           → bool
  ds_type(entity_type)     → str
  get_instance(qn)         → str
  get_schema_path(qn)      → str | None
  short_name(qn)           → str
  sid(val)                 → str   (fallback GUID hash)
  COSMOS_SYSTEM_FIELDS     → set   (system field names to skip)

CONFIG FILE  (purview_config.ini — tool-neutral settings only)
──────────────────────────────────────────────────────────────
  JSON_OUTPUT_FOLDER  — where to write all output files
  SNAPSHOT_FILE       — name of the enriched snapshot file
  COLLECTIONS_FILTER  — comma-separated hierarchy paths (or "none")
  LAST_RUN            — all | <N>min | <N>hr | <N>d
  MAX_WORKERS         — parallel workers for entity fetch

Usage
─────
    python scan.py                    # auto-finds config
    python scan.py purview_config.ini # explicit path
"""

# ── CONNECTOR IMPORT — change this one line to switch tools ──────────
from connector_purview import (
    get_connector_name,
    fetch_collections,
    fetch_glossary_terms,
    fetch_assets,
    fetch_entities,
    fetch_single_entity,
    extract_columns,
    is_leaf,
    ds_type,
    get_instance,
    get_schema_path,
    short_name,
    sid,
    COSMOS_SYSTEM_FIELDS,
    MIN_VALID_MS,
)
# ─────────────────────────────────────────────────────────────────────

import os, sys, json, time, uuid, hashlib, datetime
import configparser, threading
from pathlib import Path
from collections import defaultdict
import re as _re

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
#  LOGGING  (thread-safe)
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
#  TIMEFRAME HELPERS  — tool-agnostic timestamp logic
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
#  TIMEFRAME FILTER  — classification / label / tag driven
#
#  Gate A1 — any ACTIVE classification with real timestamp >= cutoff
#  Gate A2 — MICROSOFT.* auto-scan epoch-0 cls → asset updateTime proxy
#  Gate B  — sensitivity label present → column updateTime >= cutoff
#  Gate C  — business tag present → same proxy as Gate B
# ═══════════════════════════════════════════════════════════════════════

def col_in_timeframe(col, cutoff_ms, a_upd_ms=None):
    if cutoff_ms is None:
        return True

    has_epoch_zero_cls = False
    for c in (col.get("classifications") or []):
        if c.get("entityStatus", "ACTIVE") == "DELETED":
            continue
        raw_ms = _get_ts(c, "lastModifiedTS", "updateTime", "createTime")
        if raw_ms and raw_ms >= cutoff_ms:
            return True
        if not raw_ms:
            has_epoch_zero_cls = True

    if has_epoch_zero_cls and a_upd_ms and a_upd_ms >= cutoff_ms:
        return True

    col_upd_ms = _get_ts(col, "updateTime", "lastModifiedTS", "createTime")

    labels = [l for l in (col.get("labels") or []) if l]
    if labels:
        if col_upd_ms and col_upd_ms >= cutoff_ms: return True
        if not col_upd_ms and a_upd_ms and a_upd_ms >= cutoff_ms: return True

    has_tags = any(isinstance(gv, dict) and gv
                   for gv in (col.get("businessAttributes") or {}).values())
    if has_tags:
        if col_upd_ms and col_upd_ms >= cutoff_ms: return True
        if not col_upd_ms and a_upd_ms and a_upd_ms >= cutoff_ms: return True

    return False

# ═══════════════════════════════════════════════════════════════════════
#  COLLECTION PATH MATCHING  — tool-agnostic (just string logic)
# ═══════════════════════════════════════════════════════════════════════

def _norm_path(s):
    s = _re.sub(r'\s*/\s*', '/', s)
    return ' '.join(s.split()).lower()

def coll_matches(coll_p):
    if not COLLECTIONS_FILTER: return True
    norm = _norm_path(coll_p)
    return any(norm.startswith(_norm_path(cf)) for cf in COLLECTIONS_FILTER)

# ═══════════════════════════════════════════════════════════════════════
#  COLUMN DETAIL BUILDER  — tool-agnostic
#  Reads from the raw column dict and returns a structured record.
#  The field names used here (attributes.name, classifications, labels,
#  businessAttributes) are what Atlas returns — a Collibra connector
#  would normalize its responses to match this shape.
# ═══════════════════════════════════════════════════════════════════════

_UUID_RE = _re.compile(
    r'^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$', _re.I)

def _resolve_by(raw):
    if not raw: return None
    s = str(raw).strip()
    if not s: return None
    if _UUID_RE.match(s):
        return f"Automated Scan ({s[:8]}...)"
    return s

def build_column_detail(col, col_number, total_cols, asset_upd_at, cutoff_ms, a_upd_ms):
    """
    Build a fully self-contained column record from a raw column dict.
    Returns None for CosmosDB internal system fields.
    """
    attr     = col.get("attributes") or {}
    col_name = attr.get("name") or col.get("name") or ""
    col_guid = col.get("guid") or sid(col_name)

    if col_name in COSMOS_SYSTEM_FIELDS:
        return None

    col_crt_ms = _get_ts(col, "createTime",  "createdTS")
    col_upd_ms = _get_ts(col, "updateTime",  "lastModifiedTS")

    # Classifications — full detail
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
        applied_at  = ms_to_iso(raw_ms) if raw_ms else (
            asset_upd_at if status != "DELETED" else None)
        classifications.append({
            "name":             cn,
            "entity_status":    status,
            "applied_at":       applied_at,
            "applied_at_ms":    raw_ms,
            "applied_by":       readable_by or ("Automated Scan" if status == "ACTIVE" else None),
            "source":           c.get("source"),
            "confidence":       (c.get("attributes") or {}).get("confidence"),
            "validity_periods": c.get("validityPeriods"),
        })

    active_cls         = [c for c in classifications if c["entity_status"] != "DELETED"]
    sensitivity_labels = [l for l in (col.get("labels") or []) if l]
    business_tags      = {grp: gvals
                          for grp, gvals in (col.get("businessAttributes") or {}).items()
                          if isinstance(gvals, dict)}

    has_cls    = bool(active_cls)
    has_lbl    = bool(sensitivity_labels)
    has_tag    = bool(business_tags)
    is_enriched = has_cls or has_lbl or has_tag
    in_window   = col_in_timeframe(col, cutoff_ms, a_upd_ms)

    return {
        "column_number":                col_number,
        "total_columns":                total_cols,
        "column_guid":                  col_guid,
        "column_name":                  col_name,
        "data_type":                    (attr.get("data_type") or attr.get("type") or
                                         attr.get("dataType") or attr.get("primitiveType")),
        "description":                  attr.get("description") or attr.get("comment"),
        "position":                     attr.get("position") or attr.get("ordinalPosition"),
        "is_nullable":                  attr.get("isNullable"),
        "is_primary_key":               attr.get("isPrimaryKey") or attr.get("primaryKey"),
        "is_foreign_key":               attr.get("isForeignKey") or attr.get("foreignKey"),
        "is_unique":                    attr.get("isUnique"),
        "default_value":                attr.get("defaultValue") or attr.get("default"),
        "max_length":                   attr.get("maxLength") or attr.get("length"),
        "precision":                    attr.get("precision"),
        "scale":                        attr.get("scale"),
        "entity_type_name":             col.get("typeName"),
        "column_status":                col.get("status"),
        "version":                      col.get("version"),
        "column_created_at":            ms_to_iso(col_crt_ms),
        "column_created_at_ms":         col_crt_ms,
        "column_updated_at":            ms_to_iso(col_upd_ms),
        "column_updated_at_ms":         col_upd_ms,
        "column_created_by":            col.get("createdBy"),
        "column_updated_by":            col.get("updatedBy") or col.get("modifiedBy"),
        "is_enriched":                  is_enriched,
        "in_timeframe_window":          in_window,
        "has_classification":           has_cls,
        "has_sensitivity_label":        has_lbl,
        "has_business_tag":             has_tag,
        "active_classification_count":  len(active_cls),
        "classifications":              classifications,
        "classification_names":         [c["name"] for c in active_cls],
        "sensitivity_labels":           sensitivity_labels,
        "business_tags":                business_tags,
    }

# ═══════════════════════════════════════════════════════════════════════
#  JSON WRITER
# ═══════════════════════════════════════════════════════════════════════

def write_json(path, data, label):
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2, ensure_ascii=False, default=str)
    size = path.stat().st_size
    count = len(data) if isinstance(data, list) else 1
    log(f"  Written: {path.name}  ({count} records,  {size:,} bytes)", "OK")

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

    connector_name = get_connector_name()

    log_section(f"{connector_name.upper()} → JSON  |  scan.py")
    log(f"Connector     : {connector_name}  (connector_purview.py)")
    log(f"Config        : {_config_path or '(none — using defaults)'}")
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

    # ── STEP 1: Collections ───────────────────────────────────────────
    log_section("STEP 1 — Collections (full load)")
    coll_map, coll_paths, coll_items = fetch_collections()
    write_json(outdir / "collections.json", coll_items, "collections")

    # ── STEP 2: Glossary terms ────────────────────────────────────────
    log_section("STEP 2 — Glossary terms (full load)")
    glossary_terms = fetch_glossary_terms()
    write_json(outdir / "glossary_terms.json", glossary_terms, "glossary_terms")

    # ── STEP 3: Assets ────────────────────────────────────────────────
    log_section("STEP 3 — Fetch all assets (search)")
    all_assets  = fetch_assets()
    leaf_assets = [a for a in all_assets if is_leaf(a)]
    log(f"Leaf: {len(leaf_assets):,}  |  Structural skipped: "
        f"{len(all_assets) - len(leaf_assets):,}")
    write_json(outdir / "assets.json", leaf_assets, "assets")

    guids         = [a["id"] for a in leaf_assets if a.get("id")]
    guid_to_asset = {a["id"]: a for a in leaf_assets if a.get("id")}

    # ── STEP 4: Entities ──────────────────────────────────────────────
    log_section(f"STEP 4 — Bulk entity fetch ({len(guids):,} GUIDs, {MAX_WORKERS} workers)")
    entity_map  = fetch_entities(guids, max_workers=MAX_WORKERS)
    raw_entities = [
        {**ej.get("entity", {}), "guid": g,
         "_referred_entities_count": len(ej.get("referredEntities", {}))}
        for g, ej in entity_map.items()
    ]
    write_json(outdir / "entities.json", raw_entities, "entities")

    # ── STEP 5: Build snapshot ────────────────────────────────────────
    log_section("STEP 5 — Build snapshot (enriched columns only)")

    snapshot_cols   = []
    total_cols_seen = 0
    total_enriched  = 0
    total_cls_count = 0
    total_lbl_count = 0
    total_tag_count = 0
    assets_filtered = 0

    coll_stats = defaultdict(lambda: {
        "assets": 0, "total_columns": 0, "enriched_columns": 0,
        "classified_columns": 0, "labeled_columns": 0, "tagged_columns": 0,
        "cls_types": set(),
    })

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

                # Collection filter
                if COLLECTIONS_FILTER:
                    if not coll_p or not coll_matches(coll_p):
                        assets_filtered += 1
                        continue

                if not ej:
                    continue

                e_raw    = ej.get("entity", {})
                a_upd_ms = _safe_ms(e_raw.get("updateTime") or e_raw.get("lastModifiedTS"))
                a_crt_ms = _safe_ms(e_raw.get("createTime") or e_raw.get("createdTS"))
                a_upd_at = ms_to_iso(a_upd_ms)
                a_crt_at = ms_to_iso(a_crt_ms)
                a_upd_by = e_raw.get("updatedBy") or e_raw.get("modifiedBy")
                a_crt_by = e_raw.get("createdBy")

                # Extract columns — fallback to single entity fetch if none found
                cols = extract_columns(ej)
                if not cols:
                    fb = fetch_single_entity(guid)
                    if fb:
                        cols = extract_columns(fb)
                        if not a_upd_at:
                            a_upd_ms = _safe_ms(fb.get("entity", {}).get("updateTime"))
                            a_upd_at = ms_to_iso(a_upd_ms)
                        if not a_upd_by:
                            a_upd_by = fb.get("entity", {}).get("updatedBy")

                # Build column detail records
                column_records = []
                asset_cls_set  = set()
                asset_cls_cnt  = 0
                cols_in_log    = []

                for col_num, col in enumerate(cols, 1):
                    detail = build_column_detail(
                        col, col_num, len(cols), a_upd_at, cutoff_ms, a_upd_ms)
                    if detail is None:
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

                # On-screen asset log
                if cols_in_log:
                    with _print_lock:
                        print(f"\n  ASSET  : {qn}")
                        if ot:       print(f"  TYPE   : {ot} | {et}")
                        if coll_p:   print(f"  COLL   : {coll_p}")
                        if a_upd_at: print(f"  UPDATED: {a_upd_at}  by: {a_upd_by or '?'}")
                        print(f"  COLS   : {len(column_records)} total  |  "
                              f"{len(cols_in_log)} with enrichment")
                        for d in cols_in_log:
                            rparts = [f"    [{d['column_number']:>4}/{len(cols)}]"
                                      f" {d['column_name']:<35}"]
                            if d["data_type"]:
                                rparts.append(f"type={d['data_type']}")
                            if d["classification_names"]:
                                rparts.append(f"cls={', '.join(d['classification_names'])}")
                                first_cls = d["classifications"][0]
                                if first_cls.get("applied_at"):
                                    rparts.append(f"applied={first_cls['applied_at']}")
                                if first_cls.get("applied_by"):
                                    rparts.append(f"by={first_cls['applied_by']}")
                            if d["sensitivity_labels"]:
                                rparts.append(f"lbl={', '.join(d['sensitivity_labels'])}")
                            if d["business_tags"]:
                                flat = [f"{grp}.{k}={v}"
                                        for grp, gvals in d["business_tags"].items()
                                        for k, v in gvals.items()]
                                rparts.append(f"tags={', '.join(flat)}")
                            print("  ".join(rparts))
                        if asset_cls_set:
                            print(f"  CLASSIFIED: {asset_cls_cnt}/{len(column_records)} cols  "
                                  f"types: {', '.join(sorted(asset_cls_set))}")

                # Build snapshot records — one entry per enriched column
                if cols_in_log:
                    coll_stats[coll_p]["assets"] += 1
                    coll_stats[coll_p]["total_columns"]    += len(column_records)
                    coll_stats[coll_p]["enriched_columns"] += len(cols_in_log)
                    coll_stats[coll_p]["classified_columns"] += asset_cls_cnt
                    coll_stats[coll_p]["cls_types"].update(asset_cls_set)
                    for d in cols_in_log:
                        if d["sensitivity_labels"]: coll_stats[coll_p]["labeled_columns"] += 1
                        if d["business_tags"]:      coll_stats[coll_p]["tagged_columns"]  += 1

                    for d in cols_in_log:
                        snapshot_cols.append({
                            "scan_run_id":              run_id,
                            "scan_timestamp":           ts,
                            "column_guid":              d["column_guid"],
                            "column_name":              d["column_name"],
                            "column_number":            d["column_number"],
                            "total_columns_in_asset":   len(column_records),
                            "data_type":                d["data_type"],
                            "description":              d["description"],
                            "position":                 d["position"],
                            "is_nullable":              d["is_nullable"],
                            "is_primary_key":           d["is_primary_key"],
                            "is_foreign_key":           d["is_foreign_key"],
                            "is_unique":                d["is_unique"],
                            "default_value":            d["default_value"],
                            "max_length":               d["max_length"],
                            "precision":                d["precision"],
                            "scale":                    d["scale"],
                            "entity_type_name":         d["entity_type_name"],
                            "column_status":            d["column_status"],
                            "column_created_at":        d["column_created_at"],
                            "column_created_by":        d["column_created_by"],
                            "column_updated_at":        d["column_updated_at"],
                            "column_updated_by":        d["column_updated_by"],
                            "asset_guid":               guid,
                            "asset_name":               a_name,
                            "asset_qualified_name":     qn,
                            "asset_entity_type":        et,
                            "asset_object_type":        ot,
                            "asset_created_at":         a_crt_at,
                            "asset_created_by":         a_crt_by,
                            "asset_updated_at":         a_upd_at,
                            "asset_updated_by":         a_upd_by,
                            "datasource_type":          ds,
                            "datasource_instance":      instance,
                            "schema_path":              get_schema_path(qn),
                            "collection_id":            coll_id,
                            "collection_name":          coll_nm,
                            "collection_hierarchy_path":coll_p,
                            "is_enriched":              d["is_enriched"],
                            "has_classification":       d["has_classification"],
                            "has_sensitivity_label":    d["has_sensitivity_label"],
                            "has_business_tag":         d["has_business_tag"],
                            "active_classification_count": d["active_classification_count"],
                            "classifications":          d["classifications"],
                            "classification_names":     d["classification_names"],
                            "sensitivity_labels":       d["sensitivity_labels"],
                            "business_tags":            d["business_tags"],
                        })

    log(f"  Enriched columns in snapshot : {len(snapshot_cols):,}", "INFO")
    log(f"  Total columns seen           : {total_cols_seen:,}", "INFO")
    log(f"  Classifications              : {total_cls_count:,}", "INFO")
    log(f"  Sensitivity labels           : {total_lbl_count:,}", "INFO")
    log(f"  Business tags                : {total_tag_count:,}", "INFO")
    if COLLECTIONS_FILTER:
        log(f"  Assets filtered              : {assets_filtered:,}  (outside collection filter)", "INFO")

    # ── Collection breakdown ──────────────────────────────────────────
    if coll_stats or COLLECTIONS_FILTER:
        with _print_lock:
            print(f"\n{'='*70}")
            if COLLECTIONS_FILTER:
                print(f"  COLLECTION FILTER RESULTS  (requested: {len(COLLECTIONS_FILTER)})")
                print(f"  Filter: {_COLL_FILTER_RAW}")
            else:
                print(f"  COLLECTION BREAKDOWN  (full search)")
            print(f"{'='*70}")
            print(f"  {'COLLECTION PATH':<46} {'ASSETS':>6} {'COLS':>6} "
                  f"{'ENRICH':>6} {'CLS':>5} {'LBL':>5} {'TAG':>5}  STATUS")
            print(f"  {'─'*46} {'─'*6} {'─'*6} {'─'*6} {'─'*5} {'─'*5} {'─'*5}  {'─'*10}")
            total_a = total_c = total_e = total_clsc = total_lblc = total_tagc = 0

            if COLLECTIONS_FILTER:
                for cf in COLLECTIONS_FILTER:
                    matched = [k for k in coll_stats
                               if _norm_path(k) == _norm_path(cf) or
                               _norm_path(k).startswith(_norm_path(cf))]
                    if matched:
                        for mk in sorted(matched):
                            st = coll_stats[mk]
                            print(f"  {mk[:46]:<46} {st['assets']:>6} {st['total_columns']:>6} "
                                  f"{st['enriched_columns']:>6} {st['classified_columns']:>5} "
                                  f"{st['labeled_columns']:>5} {st['tagged_columns']:>5}  FOUND")
                            if st["cls_types"]:
                                line = ", ".join(sorted(st["cls_types"]))
                                while line:
                                    print(f"    cls: {line[:78]}"); line = line[78:]
                            total_a += st["assets"]; total_c += st["total_columns"]
                            total_e += st["enriched_columns"]; total_clsc += st["classified_columns"]
                            total_lblc += st["labeled_columns"]; total_tagc += st["tagged_columns"]
                    else:
                        print(f"  {cf[:46]:<46} {'0':>6} {'0':>6} {'0':>6} "
                              f"{'0':>5} {'0':>5} {'0':>5}  NOT FOUND")
            else:
                for cpath, st in sorted(coll_stats.items()):
                    print(f"  {(cpath or '(no collection)')[:46]:<46} {st['assets']:>6} "
                          f"{st['total_columns']:>6} {st['enriched_columns']:>6} "
                          f"{st['classified_columns']:>5} {st['labeled_columns']:>5} "
                          f"{st['tagged_columns']:>5}")
                    if st["cls_types"]:
                        line = ", ".join(sorted(st["cls_types"]))
                        while line:
                            print(f"    cls: {line[:78]}"); line = line[78:]
                    total_a += st["assets"]; total_c += st["total_columns"]
                    total_e += st["enriched_columns"]; total_clsc += st["classified_columns"]
                    total_lblc += st["labeled_columns"]; total_tagc += st["tagged_columns"]

            print(f"  {'─'*46} {'─'*6} {'─'*6} {'─'*6} {'─'*5} {'─'*5} {'─'*5}")
            print(f"  {'TOTAL':<46} {total_a:>6} {total_c:>6} {total_e:>6} "
                  f"{total_clsc:>5} {total_lblc:>5} {total_tagc:>5}")
            print(f"{'='*70}")

    # ── STEP 6: Write snapshot ────────────────────────────────────────
    log_section("STEP 6 — Write snapshot JSON")
    elapsed_so_far = fmt_dur(time.time() - t0)
    end_dt = datetime.datetime.now(datetime.timezone.utc)

    not_found = [cf for cf in (COLLECTIONS_FILTER or [])
                 if not any(_norm_path(k) == _norm_path(cf) or
                            _norm_path(k).startswith(_norm_path(cf))
                            for k in coll_stats)]

    snapshot = {
        "scan_run_id":        run_id,
        "scan_time":          ts,
        "scan_end":           end_dt.strftime("%Y-%m-%d %H:%M:%S"),
        "duration":           elapsed_so_far,
        "config_file":        _config_path,
        "connector":          connector_name,
        "last_run_filter":    run_label,
        "collections_filter": _COLL_FILTER_RAW,
        "snapshot_policy": (
            "ONE RECORD PER ENRICHED COLUMN. Only columns that have at least one "
            "classification, sensitivity label, or business tag are written. "
            "Each record is fully self-contained."
        ),
        "summary": {
            "total_search_assets":          len(all_assets),
            "leaf_assets":                  len(leaf_assets),
            "entities_fetched":             len(entity_map),
            "collections_fetched":          len(coll_items),
            "glossary_terms_fetched":       len(glossary_terms),
            "assets_collection_filtered":   assets_filtered,
            "enriched_assets":              len(set(r["asset_guid"] for r in snapshot_cols)),
            "enriched_columns_in_snapshot": len(snapshot_cols),
            "total_columns_seen":           total_cols_seen,
            "total_classifications":        total_cls_count,
            "total_sensitivity_labels":     total_lbl_count,
            "total_business_tags":          total_tag_count,
            "collections_requested":        COLLECTIONS_FILTER or "none (full search)",
            "collections_with_data":        sorted(coll_stats.keys()),
            "collections_not_found":        not_found,
        },
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
        "output_files": {
            "collections":    str(outdir / "collections.json"),
            "glossary_terms": str(outdir / "glossary_terms.json"),
            "assets":         str(outdir / "assets.json"),
            "entities":       str(outdir / "entities.json"),
            "snapshot":       str(outdir / SNAPSHOT_FILE),
        },
        "columns": snapshot_cols,
    }

    snap_path = outdir / SNAPSHOT_FILE
    with open(snap_path, "w", encoding="utf-8") as f:
        json.dump(snapshot, f, indent=2, ensure_ascii=False, default=str)
    size_kb = snap_path.stat().st_size / 1024
    enriched_assets = len(set(r["asset_guid"] for r in snapshot_cols))
    log(f"  Written: {snap_path.name}  "
        f"({len(snapshot_cols)} enriched columns across "
        f"{enriched_assets} assets,  {size_kb:,.1f} KB)", "OK")

    # ── Final summary ──────────────────────────────────────────────────
    elapsed = fmt_dur(time.time() - t0)
    print(f"""
{'='*70}
  COMPLETE — scan.py  [{connector_name}]
{'='*70}
  Scan run ID    : {run_id}
  Connector      : {connector_name}  (swap by changing the import at top of scan.py)
  Start          : {ts} UTC
  End            : {end_dt.strftime('%Y-%m-%d %H:%M:%S')} UTC
  Duration       : {elapsed}
  LAST_RUN       : {run_label}
  ──────────────────────────────────────────────────────
  COLLECTION FILTER
  Requested      : {', '.join(COLLECTIONS_FILTER) if COLLECTIONS_FILTER else 'none (full search)'}
  Found data     : {', '.join(sorted(coll_stats.keys())) or 'none'}""")
    if not_found:
        print(f"  NOT FOUND      : {', '.join(not_found)}")
    print(f"""  ──────────────────────────────────────────────────────
  FETCH COUNTS
  Assets searched   : {len(all_assets):>7,}
  Leaf assets       : {len(leaf_assets):>7,}
  Entities fetched  : {len(entity_map):>7,}
  Collections       : {len(coll_items):>7,}
  Glossary terms    : {len(glossary_terms):>7,}
  Coll-filtered out : {assets_filtered:>7,}
  ──────────────────────────────────────────────────────
  SNAPSHOT  →  {snap_path}
  Structure          : one record per enriched column (fully self-contained)
  Total cols seen    : {total_cols_seen:>7,}
  Enriched columns   : {len(snapshot_cols):>7,}
  Enriched assets    : {enriched_assets:>7,}
  Classifications    : {total_cls_count:>7,}
  Sensitivity labels : {total_lbl_count:>7,}
  Business tags      : {total_tag_count:>7,}
  ──────────────────────────────────────────────────────
  OUTPUT FILES
  collections.json       {len(coll_items):>7,}  {outdir}/
  glossary_terms.json    {len(glossary_terms):>7,}  {outdir}/
  assets.json            {len(leaf_assets):>7,}  {outdir}/
  entities.json          {len(entity_map):>7,}  {outdir}/
  {SNAPSHOT_FILE:<23}{len(snapshot_cols):>7,}  {outdir}/
  ──────────────────────────────────────────────────────
  TO SWITCH TOOLS: edit scan.py line 1 of imports
    from connector_purview  import *   ← current
    from connector_collibra import *   ← example swap
  Output : {outdir.resolve()}
{'='*70}
""", flush=True)


if __name__ == "__main__":
    main()