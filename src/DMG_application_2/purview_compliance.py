"""
purview_compliance.py
=====================
Standalone Compliance Checker.

Compares a Sensitivity Mapping YAML (local file or GitHub URL)
against the purview_snapshot.json produced by purview_scan.py and
writes a formatted Excel compliance report.

CAN BE USED TWO WAYS
─────────────────────
  1. Called automatically by purview_scan.py after every scan
     (if YAML_SOURCE is set in purview_config.ini):

       from purview_compliance import run_compliance_check
       run_compliance_check(
           snapshot_path = "json_output/purview_snapshot.json",
           yaml_source   = "https://raw.githubusercontent.com/...",
           dmg_filter    = ["DMG0002303"],   # or [] for all
           out_path      = "json_output/compliance_report.xlsx",
       )

  2. Run standalone at any time:

       python purview_compliance.py \\
           --snapshot json_output/purview_snapshot.json \\
           --yaml     https://raw.githubusercontent.com/rajesh14731a0341/azure/classifications/src/compliance_json/Fusion_-_ACHplus__Product_.yaml \\
           --dmg      DMG0002303 \\
           --out      json_output/compliance_report.xlsx

       # All DMGs, folder of YAMLs:
       python purview_compliance.py \\
           --snapshot json_output/purview_snapshot.json \\
           --yaml     https://github.com/rajesh14731a0341/azure/tree/classifications/src/compliance_json

COMPLIANCE LOGIC
─────────────────
  A column is COMPLIANT (YES) when ALL THREE match:
    1. Classification : Purview classification_names contains the YAML
                        purview_classification (case-insensitive).
                        If YAML says "N/A" → Purview must have none.
    2. Sensitivity Label: Purview sensitivity_labels contains the YAML
                        sensitivity_label (case-insensitive).
    3. Column Found   : The column_name from YAML exists in snapshot.

  Any failure → NON-COMPLIANT with a reason in the Reason column.

EXCEL OUTPUT (3 sheets)
─────────────────────────
  Compliance Detail   one row per YAML column — all fields + result
  Summary             per-DMG totals and compliance rate
  Legend              rules, colour codes, field descriptions

DB TABLES (when COMPLIANCE_DB_LOAD = true)
───────────────────────────────────────────
  <schema>.<table>           — main detail table (upsert per column)
  <schema>.<table>_history   — audit log (INSERT on EVERY run, never updated)
  <schema>.<table>_summary   — run-level summary (INSERT on every run, never updated)

DEPENDENCIES
─────────────
  pip install requests pyyaml openpyxl
"""

import argparse
import datetime
import json
import re
import sys
import time
import threading
from collections import defaultdict
from pathlib import Path

# ── Optional imports — warn clearly if missing ────────────────────────
try:
    import requests as _requests
    _HAS_REQUESTS = True
except ImportError:
    _HAS_REQUESTS = False

try:
    import yaml as _yaml
    _HAS_YAML = True
except ImportError:
    _HAS_YAML = False

try:
    import openpyxl
    from openpyxl.styles import Font, PatternFill, Alignment, Border, Side
    from openpyxl.utils import get_column_letter
    _HAS_OPENPYXL = True
except ImportError:
    _HAS_OPENPYXL = False

try:
    import pyodbc as _pyodbc
    _HAS_PYODBC = True
except ImportError:
    _HAS_PYODBC = False

# pymssql — pure-Python TDS driver, no ODBC system drivers required.
# Used automatically as a fallback when pyodbc is present but no ODBC
# driver is installed on the machine.
try:
    import pymssql as _pymssql
    _HAS_PYMSSQL = True
except ImportError:
    _HAS_PYMSSQL = False

# Azure Blob Storage SDK (optional — only needed when YAML_SOURCE_AZURE_BLOB=true)
try:
    from azure.storage.blob import BlobServiceClient as _BlobServiceClient
    _HAS_AZURE_BLOB = True
except ImportError:
    _HAS_AZURE_BLOB = False


# ═══════════════════════════════════════════════════════════════════════
#  LOGGING  (identical format to purview_scan.py — consistent on-screen)
# ═══════════════════════════════════════════════════════════════════════

_print_lock = threading.Lock()

def _log(msg, level="INFO"):
    ts   = datetime.datetime.now(datetime.timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
    icon = {"INFO": "[INFO]", "OK": "[OK]  ",
            "WARN": "[WARN]", "ERROR": "[ERR] "}.get(level, "     ")
    with _print_lock:
        print(f"[{ts}] {icon} {msg}", flush=True)

def _section(title):
    with _print_lock:
        print(f"\n{'='*70}\n  {title}\n{'='*70}", flush=True)

def _rule():
    with _print_lock:
        print(f"  {'─'*66}", flush=True)


# ═══════════════════════════════════════════════════════════════════════
#  YAML LOADER  — 3-way dispatcher: GitHub (public/private), Azure Blob, Local
#
#  Controlled by three true/false flags passed in from the caller
#  (resolved from purview_config.ini by purview_scan.py):
#
#    yaml_source_github     = True  → fetch from GitHub (public or private)
#    yaml_source_azure_blob = True  → fetch from Azure Blob Storage
#    yaml_source_local      = True  → read from local filesystem
#
#  Priority when more than one flag is True: GitHub > Azure Blob > Local.
#  All three defaulting to False falls back to the legacy auto-detect
#  behaviour (URL → GitHub, otherwise local path) for backward-compat.
# ═══════════════════════════════════════════════════════════════════════

def _to_raw_url(url: str) -> str:
    """Convert a GitHub browser/blob URL to raw.githubusercontent.com."""
    if "raw.githubusercontent.com" in url:
        return url
    m = re.match(
        r"https://github\.com/([^/]+)/([^/]+)/blob/([^/]+)/(.*)", url)
    if m:
        owner, repo, branch, path = m.groups()
        return (f"https://raw.githubusercontent.com"
                f"/{owner}/{repo}/{branch}/{path}")
    return url   # already raw or non-GitHub URL


def _github_api_headers(token: str = "") -> dict:
    """Build headers for GitHub API / raw content requests.
    Adds Authorization only when a token is supplied (private repos).
    """
    h = {"Accept": "application/vnd.github+json",
         "X-GitHub-Api-Version": "2022-11-28"}
    if token:
        h["Authorization"] = f"Bearer {token}"
    return h


def _discover_yaml_urls_github(tree_url: str, token: str = "") -> list:
    """
    Given a GitHub tree URL (a folder), return raw URLs for every
    *.yaml file underneath it via the GitHub Trees API.
    Works for both public repos (no token) and private repos (with token).
    Falls back to returning just the URL itself if the API fails.
    """
    m = re.match(
        r"https://github\.com/([^/]+)/([^/]+)/tree/([^/]+)/(.*)", tree_url)
    if not m:
        return [_to_raw_url(tree_url)]
    owner, repo, branch, path = m.groups()
    api_url = (f"https://api.github.com/repos/{owner}/{repo}"
               f"/git/trees/{branch}?recursive=1")
    _log(f"GitHub tree API: {api_url}")
    if token:
        _log("  Private repo mode — using Personal Access Token")
    try:
        r = _requests.get(api_url, headers=_github_api_headers(token), timeout=30)
        if r.status_code == 401:
            _log("GitHub API returned 401 Unauthorized. "
                 "Check that YAML_GITHUB_TOKEN is set and has 'repo' scope "
                 "for private repositories.", "ERROR")
            return []
        if r.status_code == 404:
            _log(f"GitHub API returned 404 — repo or branch not found: {api_url}",
                 "ERROR")
            return []
        r.raise_for_status()
        tree = r.json().get("tree", [])
    except Exception as e:
        _log(f"GitHub API call failed: {e} — will try direct URL", "WARN")
        return [_to_raw_url(tree_url)]

    prefix   = path.rstrip("/") + "/"
    raw_base = (f"https://raw.githubusercontent.com"
                f"/{owner}/{repo}/{branch}/")
    urls = [
        raw_base + item["path"]
        for item in tree
        if item.get("type") == "blob"
        and item["path"].startswith(prefix)
        and item["path"].lower().endswith(".yaml")
    ]
    _log(f"Found {len(urls)} YAML file(s) under {prefix}")
    return urls if urls else [_to_raw_url(tree_url)]


def _load_yaml_text(text: str) -> dict:
    return _yaml.safe_load(text) or {}


# ── Source 1: GitHub (public or private) ──────────────────────────────

def _load_from_github(source: str, token: str = "") -> list:
    """
    Load YAML(s) from GitHub.  source can be:
      - A GitHub tree URL  (folder)    → discovers all *.yaml via Trees API
      - A GitHub blob/raw URL          → fetches that single file
    token must be set for private repos (classic PAT with repo scope or
    fine-grained PAT with Contents: Read).  Leave blank for public repos.
    """
    if not _HAS_REQUESTS:
        _log("requests not installed — run:  pip install requests", "ERROR")
        return []

    results = []

    if "/tree/" in source and "/blob/" not in source:
        urls = _discover_yaml_urls_github(source, token=token)
    else:
        urls = [_to_raw_url(source)]

    for url in urls:
        _log(f"Loading YAML (GitHub) → {url}")
        try:
            # For raw content on private repos the raw.githubusercontent.com
            # endpoint also respects the Authorization header.
            h = {}
            if token:
                h["Authorization"] = f"Bearer {token}"
            r = _requests.get(url, headers=h, timeout=30)
            if r.status_code == 401:
                _log(f"  401 Unauthorized — YAML_GITHUB_TOKEN missing or invalid "
                     f"for private repo.  URL: {url}", "ERROR")
                continue
            if r.status_code == 404:
                _log(f"  404 Not Found — check URL and branch: {url}", "ERROR")
                continue
            r.raise_for_status()
            doc = _load_yaml_text(r.text)
            if doc:
                results.append((url, doc))
                _log(f"  Loaded: v{doc.get('version','')}  "
                     f"products={len(doc.get('data_products',[]))}", "OK")
            else:
                _log(f"  Empty YAML at {url}", "WARN")
        except Exception as e:
            _log(f"Failed to load {url}: {e}", "WARN")

    return results


# ── Source 2: Azure Blob Storage ───────────────────────────────────────

def _load_from_azure_blob(account: str, container: str,
                           prefix: str = "",
                           sas_token: str = "",
                           conn_str: str = "") -> list:
    """
    Load all *.yaml blobs from an Azure Blob Storage container.

    Authentication (in priority order):
      1. conn_str  — full connection string  (YAML_BLOB_CONN_STR)
      2. sas_token — SAS token starting with '?sv=...' (YAML_BLOB_SAS_TOKEN)
         Constructs URL: https://<account>.blob.core.windows.net/<container>
      3. Neither   — anonymous / public container access

    prefix filters blobs to a virtual folder path (e.g. 'compliance_yaml/').
    All blobs ending in .yaml under that prefix are loaded.
    """
    if not _HAS_AZURE_BLOB:
        _log("azure-storage-blob not installed — run:  "
             "pip install azure-storage-blob", "ERROR")
        return []

    try:
        if conn_str:
            _log(f"Azure Blob: connecting via connection string to "
                 f"{account}/{container}")
            client = _BlobServiceClient.from_connection_string(conn_str)
        elif sas_token:
            account_url = f"https://{account}.blob.core.windows.net"
            sas = sas_token if sas_token.startswith("?") else "?" + sas_token
            _log(f"Azure Blob: connecting via SAS token to "
                 f"{account_url}/{container}")
            client = _BlobServiceClient(account_url=account_url + sas)
        else:
            account_url = f"https://{account}.blob.core.windows.net"
            _log(f"Azure Blob: connecting anonymously to "
                 f"{account_url}/{container}  (public container)")
            client = _BlobServiceClient(account_url=account_url)

        container_client = client.get_container_client(container)
        blobs = list(container_client.list_blobs(name_starts_with=prefix or None))
        yaml_blobs = [b for b in blobs if b.name.lower().endswith(".yaml")]
        _log(f"Azure Blob: found {len(yaml_blobs)} YAML blob(s) "
             f"under prefix='{prefix or '(root)'}'")
    except Exception as e:
        _log(f"Azure Blob connection/listing failed: {e}", "ERROR")
        return []

    results = []
    for blob in yaml_blobs:
        _log(f"  Loading YAML (Azure Blob) → {blob.name}")
        try:
            bc   = container_client.get_blob_client(blob.name)
            text = bc.download_blob().readall().decode("utf-8")
            doc  = _load_yaml_text(text)
            if doc:
                label = f"azblob://{account}/{container}/{blob.name}"
                results.append((label, doc))
                _log(f"  Loaded: v{doc.get('version','')}  "
                     f"products={len(doc.get('data_products',[]))}", "OK")
            else:
                _log(f"  Empty YAML: {blob.name}", "WARN")
        except Exception as e:
            _log(f"  Failed to load blob {blob.name}: {e}", "WARN")

    return results


# ── Source 3: Local filesystem ─────────────────────────────────────────

def _load_from_local(source: str) -> list:
    """Load YAML(s) from a local file or directory."""
    p = Path(source)
    results = []

    if p.is_dir():
        for yf in sorted(p.glob("**/*.yaml")):
            _log(f"Loading YAML (local) → {yf}")
            try:
                doc = _load_yaml_text(yf.read_text(encoding="utf-8"))
                if doc:
                    results.append((str(yf), doc))
                    _log(f"  Loaded: v{doc.get('version','')}  "
                         f"products={len(doc.get('data_products',[]))}", "OK")
            except Exception as e:
                _log(f"Failed to load {yf}: {e}", "WARN")
    else:
        _log(f"Loading YAML (local) → {p}")
        try:
            doc = _load_yaml_text(p.read_text(encoding="utf-8"))
            if doc:
                results.append((str(p), doc))
                _log(f"  Loaded: v{doc.get('version','')}  "
                     f"products={len(doc.get('data_products',[]))}", "OK")
        except Exception as e:
            _log(f"Failed to load {p}: {e}", "ERROR")

    return results


# ── Public entry point ─────────────────────────────────────────────────

def load_yaml_sources(source: str,
                      yaml_source_github:     bool = False,
                      yaml_source_azure_blob: bool = False,
                      yaml_source_local:      bool = False,
                      # GitHub auth
                      github_token:           str  = "",
                      # Azure Blob params
                      blob_account:           str  = "",
                      blob_container:         str  = "",
                      blob_prefix:            str  = "",
                      blob_sas_token:         str  = "",
                      blob_conn_str:          str  = "") -> list:
    """
    Load all YAML documents from the configured source.
    Returns list of  (source_label, parsed_dict)  tuples.

    Mode selection (first True wins):
      yaml_source_github     → GitHub public or private repo
      yaml_source_azure_blob → Azure Blob Storage container
      yaml_source_local      → local filesystem path

    When all three are False (legacy / backward-compat):
      URL  → GitHub public (no token)
      else → local path
    """
    if not _HAS_YAML:
        _log("pyyaml not installed — run:  pip install pyyaml", "ERROR")
        return []

    # ── Explicit mode selection ────────────────────────────────────────
    if yaml_source_github:
        _log(f"YAML source mode: GITHUB  ({'private — token supplied' if github_token else 'public — no token'})")
        return _load_from_github(source, token=github_token)

    if yaml_source_azure_blob:
        _log(f"YAML source mode: AZURE BLOB  "
             f"account={blob_account}  container={blob_container}  "
             f"prefix='{blob_prefix}'")
        return _load_from_azure_blob(
            account=blob_account, container=blob_container,
            prefix=blob_prefix, sas_token=blob_sas_token,
            conn_str=blob_conn_str)

    if yaml_source_local:
        _log(f"YAML source mode: LOCAL  path={source}")
        return _load_from_local(source)

    # ── Legacy auto-detect (backward-compat) ──────────────────────────
    _log("YAML source mode: AUTO-DETECT (legacy — set a YAML_SOURCE_* flag "
         "in config for explicit control)")
    if source.startswith("http"):
        return _load_from_github(source, token=github_token)
    return _load_from_local(source)


# ═══════════════════════════════════════════════════════════════════════
#  SNAPSHOT LOADER
#  Builds a fast lookup:  column_name_lower → [snap_record, ...]
#  One column name can appear across many assets — we keep all of them.
# ═══════════════════════════════════════════════════════════════════════

def load_snapshot(snapshot_path: str) -> dict:
    """
    Builds a fast lookup keyed by Purview classification name (lower-case).

      classification_lower → [snap_record, ...]

    One classification can appear across many columns and many assets —
    all of them are kept so the compliance check can count them.
    """
    p = Path(snapshot_path)
    if not p.exists():
        _log(f"Snapshot not found: {snapshot_path}", "ERROR")
        return {}

    size_mb = p.stat().st_size / (1024 * 1024)
    _log(f"Loading snapshot: {p.name}  ({size_mb:.2f} MB)")
    with open(p, encoding="utf-8") as f:
        snap = json.load(f)

    # purview_scan.py wraps records under "columns" key
    if isinstance(snap, dict):
        records = snap.get("columns", [])
    elif isinstance(snap, list):
        records = snap
    else:
        records = []

    # Key by every classification each column carries
    lookup: dict = {}
    for rec in records:
        for cls in (rec.get("classification_names") or []):
            key = cls.strip().lower()
            if key:
                lookup.setdefault(key, []).append(rec)

    total_cols       = len(records)
    unique_cls_count = len(lookup)
    _log(f"Snapshot ready: {total_cols:,} column records  |  "
         f"{unique_cls_count:,} unique classification(s) indexed", "OK")
    _log("NOTE: Lookup is now keyed by Purview classification — "
         "comparison will be YAML purview_classification ↔ Purview classification_names")
    return lookup


# ═══════════════════════════════════════════════════════════════════════
#  COMPLIANCE LOGIC
# ═══════════════════════════════════════════════════════════════════════

def _n(s) -> str:
    """Normalise: lowercase + strip."""
    return (s or "").strip().lower()


def _check_column(yaml_col: dict, snap_records: list) -> dict:
    """
    Compare one YAML column's **purview_classification** against all
    Purview snapshot records that share that same classification.

    New comparison model
    ─────────────────────
      • snap_records   : every Purview column record whose
                         classification_names contains the YAML
                         purview_classification value.
      • Compliance is YES when:
          1. At least one Purview column carries the expected
             classification  (snap_records is non-empty).
          2. The expected sensitivity label is present across those
             matched records  (or YAML label is blank/N/A).

    Returns
    ───────
    {
      purview_classification : str   — the classification that was matched
                                       (empty string when not found)
      purview_sensitivity    : str   — comma-joined distinct labels found
                                       across matched records
      purview_column_count   : int   — # of Purview columns carrying that cls
      purview_asset_count    : int   — # of distinct assets carrying that cls
      collection_path        : str   — up to 3 unique collection paths ("; "-joined)
      compliant              : "YES" | "NO"
      reasons                : list[str]
    }
    """
    yaml_cls_raw = yaml_col.get("purview_classification", "").strip()
    yaml_lbl_raw = yaml_col.get("sensitivity_label", "").strip()
    yaml_cls     = _n(yaml_cls_raw)
    yaml_lbl     = _n(yaml_lbl_raw)
    yaml_cls_na  = yaml_cls in ("n/a", "na", "none", "")

    # ── YAML classification is N/A — not available / not defined ─────
    if yaml_cls_na:
        return {
            "purview_classification": "",
            "purview_sensitivity":    "",
            "purview_column_count":   0,
            "purview_asset_count":    0,
            "collection_path":        "",
            "compliant":              "NO",
            "reasons":                [
                "Purview classification is N/A (not available) — "
                "a valid classification must be defined for this column"
            ],
        }

    # ── Classification lookup miss — nothing in Purview ───────────────
    if not snap_records:
        return {
            "purview_classification": "",
            "purview_sensitivity":    "",
            "purview_column_count":   0,
            "purview_asset_count":    0,
            "collection_path":        "",
            "compliant":              "NO",
            "reasons":                [
                f"Classification '{yaml_cls_raw}' is NOT found in any "
                f"Purview column — 0 columns, 0 assets"
            ],
        }

    # ── Classification found — gather counts and sensitivity info ─────
    col_count   = len(snap_records)
    asset_ids   = {(r.get("asset_qualified_name") or "") for r in snap_records}
    asset_count = len({a for a in asset_ids if a})

    # Collect ALL sensitivity labels across matched records (distinct)
    all_lbl_norm  = []
    all_lbl_raw_u = {}     # normalised → original-cased label
    for rec in snap_records:
        for lbl in (rec.get("sensitivity_labels") or []):
            norm = _n(lbl)
            if norm:
                all_lbl_norm.append(norm)
                all_lbl_raw_u[norm] = lbl.strip()
    unique_lbl_norm = list(dict.fromkeys(all_lbl_norm))   # order-preserving dedup
    p_sensitivity   = ", ".join(all_lbl_raw_u[k] for k in unique_lbl_norm)

    # Collect up to 3 distinct collection paths
    seen_paths: dict = {}
    for rec in snap_records:
        path = (rec.get("collection_hierarchy_path") or "").strip()
        if path and path not in seen_paths:
            seen_paths[path] = None
        if len(seen_paths) >= 3:
            break
    collection_path_str = "; ".join(seen_paths)
    if asset_count > 3:
        collection_path_str += f"  (+{asset_count - 3} more)"

    # ── Sensitivity label check ───────────────────────────────────────
    reasons: list = []
    if yaml_lbl and yaml_lbl not in ("n/a", "na", "none"):
        lbl_ok = yaml_lbl in unique_lbl_norm
        if not lbl_ok:
            got = p_sensitivity or "(none)"
            reasons.append(
                f"Sensitivity label: expected '{yaml_lbl_raw}', "
                f"Purview has '{got}' across {col_count} column(s)"
            )

    return {
        "purview_classification": yaml_cls_raw,   # the classification we matched on
        "purview_sensitivity":    p_sensitivity,
        "purview_column_count":   col_count,
        "purview_asset_count":    asset_count,
        "collection_path":        collection_path_str,
        "compliant":              "YES" if not reasons else "NO",
        "reasons":                reasons,
    }


# ═══════════════════════════════════════════════════════════════════════
#  ROW BUILDER  — iterate YAML docs + run compliance per column
# ═══════════════════════════════════════════════════════════════════════

def build_rows(yaml_sources: list, snap_lookup: dict,
               dmg_filter: list) -> list:
    """
    Walk every data_product in every YAML doc, apply DMG filter,
    and produce one result row per YAML column.

    Comparison key (new model)
    ──────────────────────────
      YAML purview_classification  ↔  Purview classification_names
      (snap_lookup is now keyed by classification, not column name)

    Each result row contains:
      • YAML metadata fields
      • purview_classification  — the classification that was matched
      • purview_sensitivity     — distinct labels across ALL matched columns
      • purview_column_count    — # of Purview columns carrying that classification
      • purview_asset_count     — # of distinct assets carrying that classification
      • collection_path         — up to 3 sample collection paths
      • compliant / reason
    """
    requested = {d.strip() for d in dmg_filter if d.strip()}
    all_rows: list = []

    for src_label, doc in yaml_sources:
        policy_ver = doc.get("version", "")
        _log(f"Processing: v{policy_ver}  source={src_label}")

        for dp in (doc.get("data_products") or []):
            dmg_name = dp.get("dmg_name", "")
            app_name = dp.get("app_name", "")
            region   = dp.get("Region", dp.get("region", ""))

            if requested and dmg_name not in requested:
                _log(f"  [SKIP] DMG {dmg_name} — not in filter")
                continue

            columns = dp.get("columns") or []
            _log(f"  DMG : {dmg_name}  |  App : {app_name}  |  Region : {region}  "
                 f"|  {len(columns)} column(s) to check")
            _rule()

            for idx, col in enumerate(columns, start=1):
                col_name   = col.get("column_name", "")
                col_status = col.get("status", "active")
                yaml_cls   = col.get("purview_classification", "").strip()
                yaml_lbl   = col.get("sensitivity_label", "").strip()

                # ── Lookup by classification (new model) ──────────────
                # Resolve N/A BEFORE touching the snapshot lookup so that
                # "N/A" is never treated as a missing classification.
                _NA_VALUES = {"n/a", "na", "none", ""}
                yaml_cls_na = yaml_cls.strip().lower() in _NA_VALUES
                lookup_key  = "" if yaml_cls_na else yaml_cls.strip().lower()
                snap_recs   = snap_lookup.get(lookup_key, []) if lookup_key else []
                result      = _check_column(col, snap_recs)

                col_count   = result["purview_column_count"]
                asset_count = result["purview_asset_count"]

                # ── Detailed per-column console log ───────────────────
                _log(f"  [{idx}/{len(columns)}] YAML Column : {col_name or '(unnamed)'}")
                _log(f"       YAML Classification   : {yaml_cls or '(blank/N/A)'}")
                _log(f"       YAML Sensitivity Label: {yaml_lbl or '(blank)'}")

                if yaml_cls_na:
                    _log(f"       ↳ N/A — classification not available/not defined "
                         f"→ must be assigned a valid Purview classification", "WARN")
                elif not lookup_key:
                    _log("       ↳ Blank classification in YAML — "
                         "classification check skipped", "WARN")
                else:
                    _log(f"       Lookup key (lower)    : '{lookup_key}'")
                    if col_count == 0:
                        _log(f"       ↳ Classification '{yaml_cls}' — "
                             f"NOT FOUND in any Purview column  "
                             f"(0 columns, 0 assets)", "WARN")
                    else:
                        _log(f"       ↳ Classification '{yaml_cls}' matched "
                             f"→ {col_count} Purview column(s) across "
                             f"{asset_count} asset(s)", "OK")
                        _log(f"       Purview Sensitivity   : "
                             f"{result['purview_sensitivity'] or '(none)'}")
                        if result["collection_path"]:
                            _log(f"       Sample Collection(s)  : "
                                 f"{result['collection_path']}")

                if result["compliant"] == "YES":
                    _log(f"       Result : ✓ COMPLIANT", "OK")
                else:
                    for reason in result["reasons"]:
                        _log(f"       Mismatch : {reason}", "WARN")
                    _log(f"       Result : ✗ NON-COMPLIANT", "WARN")
                _rule()

                all_rows.append({
                    "dmg_name":               dmg_name,
                    "app_name":               app_name,
                    "region":                 region,
                    "yaml_column_name":       col_name,
                    "yaml_purview_cls":       yaml_cls,
                    "yaml_sensitivity_label": yaml_lbl,
                    "yaml_status":            col_status,
                    "yaml_effective_date":    col.get("effective_date", ""),
                    # Purview — counts instead of individual names
                    "purview_classification": result["purview_classification"],
                    "purview_sensitivity":    result["purview_sensitivity"],
                    "purview_column_count":   col_count,
                    "purview_asset_count":    asset_count,
                    "collection_path":        result["collection_path"],
                    # Version + result
                    "yaml_version":           col.get("change_version", policy_ver),
                    "compliant":              result["compliant"],
                    "reason":                 "; ".join(result["reasons"]),
                    # _cls_not_found = True for any row where classification
                    # is missing — including N/A (not available) rows.
                    "_cls_not_found": (yaml_cls_na or (col_count == 0 and bool(lookup_key))),
                })

    # ── Brief per-DMG classification-match summary ────────────────────
    if all_rows:
        _log("")
        _log("Classification match summary by DMG:")
        from collections import Counter
        dmg_cls_cnt: dict = {}
        for r in all_rows:
            key = r["dmg_name"]
            dmg_cls_cnt.setdefault(key, {"total": 0, "matched": 0, "not_found": 0})
            dmg_cls_cnt[key]["total"] += 1
            if r["_cls_not_found"]:
                dmg_cls_cnt[key]["not_found"] += 1
            elif r["purview_column_count"] > 0:
                dmg_cls_cnt[key]["matched"] += 1
        for dmg, c in sorted(dmg_cls_cnt.items()):
            _log(f"  {dmg:40s}  total={c['total']}  "
                 f"cls-matched={c['matched']}  "
                 f"cls-not-found={c['not_found']}")

    return all_rows


# ═══════════════════════════════════════════════════════════════════════
#  EXCEL REPORT
# ═══════════════════════════════════════════════════════════════════════

_C_NAVY   = "FF1F3864"
_C_BLUE   = "FF2E75B6"
_C_GREEN  = "FF00B050"
_C_RED    = "FFFF0000"
_C_GOLD   = "FFFFC000"
_C_PURPLE = "FF7030A0"
_C_TEAL   = "FF00B0F0"
_C_ALT    = "FFF2F2F2"
_C_WHITE  = "FFFFFFFF"
_C_BLACK  = "FF000000"
_C_DKGRN  = "FF375623"
_C_DKRED  = "FF9C0006"

_THIN_BORDER = Border(
    left=Side(style="thin"),  right=Side(style="thin"),
    top=Side(style="thin"),   bottom=Side(style="thin"),
)


def _cell(ws, row, col, value="", bg=_C_WHITE, fg=_C_BLACK, bold=False,
          align="left", wrap=False, size=9, border=True, italic=False):
    c = ws.cell(row=row, column=col, value=value)
    c.font      = Font(bold=bold, color=fg, name="Arial",
                       size=size, italic=italic)
    c.fill      = PatternFill("solid", start_color=bg)
    c.alignment = Alignment(horizontal=align, vertical="center",
                            wrap_text=wrap)
    if border:
        c.border = _THIN_BORDER
    return c


def _hdr(ws, row, col, value, bg=_C_NAVY):
    return _cell(ws, row, col, value, bg=bg, fg=_C_WHITE,
                 bold=True, align="center", wrap=True, size=9)


def _build_excel(rows: list, out_path: str, run_ts: str):
    if not _HAS_OPENPYXL:
        _log("openpyxl not installed — skipping Excel output.  "
             "Run:  pip install openpyxl", "WARN")
        return

    wb = openpyxl.Workbook()

    # ──────────────────────────────────────────────────────────────────
    #  SHEET 1 — COMPLIANCE DETAIL
    # ──────────────────────────────────────────────────────────────────
    ws = wb.active
    ws.title = "Compliance Detail"

    # Banner row
    ws.merge_cells("A1:P1")
    _cell(ws, 1, 1,
          f"PURVIEW COMPLIANCE REPORT  —  "
          f"Generated: {run_ts}",
          bg=_C_NAVY, fg=_C_WHITE, bold=True,
          align="center", size=12)
    ws.row_dimensions[1].height = 26

    # Group-label row
    # Columns: 1-8 YAML | 9-12 Purview Snapshot | 13 col | 14 YAML Version | 15-16 Result
    groups = [
        (1,  8,  "FROM YAML  (Master / Reference)",               _C_BLUE),
        (9,  12, "FROM PURVIEW SNAPSHOT  (Classification-based)", "FF4472C4"),
        (13, 13, "COLLECTION PATH",                               "FF538135"),
        (14, 14, "YAML VERSION",                                  "FF70AD47"),
        (15, 16, "COMPLIANCE RESULT",                             _C_PURPLE),
    ]
    for s, e, lbl, bg in groups:
        ws.merge_cells(start_row=2, start_column=s,
                       end_row=2,   end_column=e)
        _cell(ws, 2, s, lbl, bg=bg, fg=_C_WHITE,
              bold=True, align="center", size=9)
    ws.row_dimensions[2].height = 18

    # Column headers  (16 columns total — Region replaces old policy classification column)
    hdrs = [
        # YAML (1-8)
        ("DMG Name",                        _C_BLUE),
        ("Application Name",                _C_BLUE),
        ("Region",                          _C_BLUE),
        ("YAML Column Name",                _C_BLUE),
        ("YAML Purview Classification",     _C_BLUE),
        ("YAML Sensitivity Label",          _C_BLUE),
        ("YAML Status",                     _C_BLUE),
        ("YAML Effective Date",             _C_BLUE),
        # Purview (9-12)
        ("Purview Classification\n(Matched)", "FF4472C4"),
        ("Purview Sensitivity",             "FF4472C4"),
        ("Purview Column Count\n(with Classification)", "FF4472C4"),
        ("Purview Asset Count\n(with Classification)",  "FF4472C4"),
        # Collection (13)
        ("Collection Path(s)",              "FF538135"),
        # Version (14)
        ("YAML Version",                    "FF70AD47"),
        # Compliance (15-16)
        ("Compliance",                      _C_PURPLE),
        ("Reason (if Non-Compliant)",       _C_PURPLE),
    ]
    for ci, (h, bg) in enumerate(hdrs, start=1):
        _hdr(ws, 3, ci, h, bg=bg)
    ws.row_dimensions[3].height = 36
    ws.freeze_panes = "A4"

    # Data rows
    for ri, row in enumerate(rows, start=4):
        cls_not_found = row.get("_cls_not_found", False)
        base_bg = _C_GOLD if cls_not_found else (_C_ALT if ri % 2 == 0 else _C_WHITE)
        comp    = row["compliant"]
        comp_bg = _C_GREEN if comp == "YES" else _C_RED
        comp_fg = _C_WHITE

        col_count   = row["purview_column_count"]
        asset_count = row["purview_asset_count"]

        vals = [
            row["dmg_name"],
            row["app_name"],
            row.get("region", ""),
            row["yaml_column_name"],
            row["yaml_purview_cls"],
            row["yaml_sensitivity_label"],
            row["yaml_status"],
            row["yaml_effective_date"],
            row["purview_classification"],
            row["purview_sensitivity"],
            col_count,
            asset_count,
            row["collection_path"],
            row["yaml_version"],
            comp,
            row["reason"],
        ]
        for ci, val in enumerate(vals, start=1):
            is_comp = ci == 15
            is_count = ci in (11, 12)
            # Highlight count cells red when 0 and classification was expected
            count_zero = is_count and isinstance(val, int) and val == 0 and bool(row["yaml_purview_cls"])
            bg = comp_bg if is_comp else (_C_RED if count_zero else base_bg)
            fg = comp_fg if is_comp else (_C_WHITE if count_zero else (
                _C_RED if cls_not_found and ci == 9 else _C_BLACK))
            _cell(ws, ri, ci, val,
                  bg=bg, fg=fg, bold=(is_comp or count_zero),
                  align="center" if ci in (1, 3, 8, 11, 12, 14, 15) else "left",
                  wrap=ci in (13, 16))
        ws.row_dimensions[ri].height = 18

    # Column widths  (16 cols — DMG, App, Region, ColName, PurviewCls, SensLabel, Status, Date,
    #                            PurviewCls(M), PurviewSens, ColCnt, AssetCnt, CollPath, Ver, Comp, Reason)
    widths = [14, 26, 10, 26, 36, 24, 10, 14,
              36, 24, 16, 14, 42, 12, 13, 54]
    for i, w in enumerate(widths, start=1):
        ws.column_dimensions[get_column_letter(i)].width = w

    ws.auto_filter.ref = f"A3:{get_column_letter(len(hdrs))}3"

    # ──────────────────────────────────────────────────────────────────
    #  SHEET 2 — SUMMARY
    # ──────────────────────────────────────────────────────────────────
    ws2 = wb.create_sheet("Summary")

    ws2.merge_cells("A1:H1")
    _cell(ws2, 1, 1, "COMPLIANCE SUMMARY  —  Per DMG / Application",
          bg=_C_NAVY, fg=_C_WHITE, bold=True, align="center", size=12)
    ws2.row_dimensions[1].height = 26

    sum_hdrs = ["DMG Name", "Application Name", "Region",
                "Total Columns", "Compliant", "Non-Compliant",
                "Classification Not in Purview", "Compliance Rate"]
    for ci, h in enumerate(sum_hdrs, start=1):
        _hdr(ws2, 2, ci, h)
    ws2.row_dimensions[2].height = 30

    agg = defaultdict(lambda: {"total": 0, "yes": 0, "no": 0, "nf": 0, "region": ""})
    for row in rows:
        k = (row["dmg_name"], row["app_name"])
        agg[k]["total"] += 1
        agg[k]["region"] = row.get("region", "")
        if row["compliant"] == "YES":
            agg[k]["yes"] += 1
        else:
            agg[k]["no"] += 1
        if row.get("_cls_not_found", False):
            agg[k]["nf"] += 1

    for sr, ((dmg, app), c) in enumerate(sorted(agg.items()), start=3):
        bg = _C_ALT if sr % 2 == 0 else _C_WHITE
        _cell(ws2, sr, 1, dmg,         bg=bg)
        _cell(ws2, sr, 2, app,         bg=bg)
        _cell(ws2, sr, 3, c["region"], bg=bg, align="center")
        _cell(ws2, sr, 4, c["total"],  bg=bg, align="center")
        _cell(ws2, sr, 5, c["yes"],    bg=bg, fg="FF375623", bold=True, align="center")
        _cell(ws2, sr, 6, c["no"],     bg=bg,
              fg=_C_RED if c["no"] else _C_BLACK,
              bold=bool(c["no"]), align="center")
        _cell(ws2, sr, 7, c["nf"],     bg=bg,
              fg="FFBF8F00" if c["nf"] else _C_BLACK,
              bold=bool(c["nf"]), align="center")
        rc = ws2.cell(row=sr, column=8, value=f"=E{sr}/D{sr}")
        rc.number_format = "0.0%"
        rc.font      = Font(bold=True, name="Arial", size=9)
        rc.fill      = PatternFill("solid", start_color=bg)
        rc.alignment = Alignment(horizontal="center", vertical="center")
        rc.border    = _THIN_BORDER
        ws2.row_dimensions[sr].height = 18

    last = 2 + len(agg)
    tr   = last + 1
    ws2.merge_cells(f"A{tr}:C{tr}")
    _hdr(ws2, tr, 1, "TOTAL")
    _hdr(ws2, tr, 4, f"=SUM(D3:D{last})")
    _hdr(ws2, tr, 5, f"=SUM(E3:E{last})", bg=_C_GREEN)
    _hdr(ws2, tr, 6, f"=SUM(F3:F{last})", bg=_C_RED)
    _hdr(ws2, tr, 7, f"=SUM(G3:G{last})", bg="FFBF8F00")
    rc2 = ws2.cell(row=tr, column=8, value=f"=E{tr}/D{tr}")
    rc2.number_format = "0.0%"
    rc2.font      = Font(bold=True, color=_C_WHITE, name="Arial", size=10)
    rc2.fill      = PatternFill("solid", start_color=_C_NAVY)
    rc2.alignment = Alignment(horizontal="center", vertical="center")
    rc2.border    = _THIN_BORDER

    for ci, w in enumerate([14, 30, 10, 14, 12, 16, 20, 16], start=1):
        ws2.column_dimensions[get_column_letter(ci)].width = w
    ws2.freeze_panes = "A3"

    wb.save(out_path)


# ═══════════════════════════════════════════════════════════════════════
#  CONSOLE SUMMARY BLOCK
# ═══════════════════════════════════════════════════════════════════════

def _print_summary(rows: list, out_path: str, elapsed: str):
    total     = len(rows)
    compliant = sum(1 for r in rows if r["compliant"] == "YES")
    non_comp  = total - compliant
    not_found = sum(1 for r in rows if r.get("_cls_not_found", False))
    rate      = f"{compliant/total*100:.1f}%" if total else "N/A"

    print(f"""
{'='*70}
  COMPLIANCE CHECK — COMPLETE
{'='*70}
  Duration          : {elapsed}
  ──────────────────────────────────────────────────────
  COLUMN RESULTS  (classification-based comparison)
  Total checked           : {total:>6}
  Compliant         (YES) : {compliant:>6}  {'✓ all good' if non_comp == 0 else ''}
  Non-compliant      (NO) : {non_comp:>6}  {'⚠ action needed' if non_comp else ''}
  Classification not found: {not_found:>6}  {'⚠ classification missing in Purview' if not_found else ''}
  Compliance rate         : {rate:>6}
  ──────────────────────────────────────────────────────
  OUTPUT
  Excel report      : {out_path}
{'='*70}
""", flush=True)


# ═══════════════════════════════════════════════════════════════════════
#  DB PUSH  — upsert into main table + always-insert audit history + summary
#
#  Called only when COMPLIANCE_DB_LOAD = true in config.
#  All DB credentials and target names are passed in from purview_scan.py
#  — this function never reads config itself.
#
#  THREE-TABLE DESIGN (all table names derived from COMPLIANCE_DB_TABLE config)
#  ────────────────────────────────────────────────────────────────────────────
#  MAIN TABLE  ({schema}.{table})
#    • Always holds the current live state of every compliance column record.
#    • Natural key: (dmg_name, app_name, yaml_column_name)
#    • First scan for a record  → INSERT.
#    • Re-scan with no changes  → only last_scanned_at is updated (silent).
#    • Re-scan with any change  → UPDATE the main row (new values).
#
#  HISTORY TABLE  ({schema}.{table}_history)
#    • Append-only audit log — rows are NEVER updated or deleted.
#    • A row is inserted on EVERY run for EVERY compliance record,
#      regardless of whether the data changed.  This gives a complete
#      time-series so the team can see "compliant at 9 AM, non-compliant
#      at 5 PM" across any date range.
#
#  SUMMARY TABLE  ({schema}.{table}_summary)
#    • Append-only run-level summary — INSERT on every run, never updated.
#    • One row per DMG/application per run.
#    • Columns: dmg_name, app_name, region, compliance_status, inserted_at
#    • compliance_status = 'Compliant' when ALL columns are YES, else 'Non-Compliant'.
#
#  RETURNS: (rows_loaded: int, error_message: str | None)
# ═══════════════════════════════════════════════════════════════════════

# ── DDL: main table ────────────────────────────────────────────────────
# Natural key  : (dmg_name, app_name, yaml_column_name)
# Infrastructure: row_id (PK), first_seen_at, last_scanned_at, last_updated_at
_DDL_TABLE = """\
IF OBJECT_ID(N'[{schema}].[{table}]', N'U') IS NULL
BEGIN
CREATE TABLE [{schema}].[{table}] (
    row_id                   NVARCHAR(36)    NOT NULL DEFAULT NEWID(),
    first_seen_at            DATETIME2       NOT NULL DEFAULT GETUTCDATE(),
    last_scanned_at          DATETIME2       NOT NULL DEFAULT GETUTCDATE(),
    last_updated_at          DATETIME2       NULL,
    -- Natural key — uniquely identifies one compliance record
    dmg_name                 NVARCHAR(200)   NOT NULL,
    app_name                 NVARCHAR(500)   NOT NULL,
    region                   NVARCHAR(200)   NULL,
    yaml_column_name         NVARCHAR(500)   NOT NULL,
    -- FROM YAML
    yaml_purview_cls         NVARCHAR(500)   NULL,
    yaml_sensitivity_label   NVARCHAR(500)   NULL,
    yaml_status              NVARCHAR(100)   NULL,
    yaml_version             NVARCHAR(100)   NULL,
    yaml_effective_date      NVARCHAR(100)   NULL,
    -- FROM PURVIEW SNAPSHOT  (classification-based counts)
    purview_classification   NVARCHAR(500)   NULL,
    purview_sensitivity      NVARCHAR(2000)  NULL,
    purview_column_count     INT             NULL,
    purview_asset_count      INT             NULL,
    collection_path          NVARCHAR(2000)  NULL,
    -- COMPLIANCE RESULT
    compliant                NVARCHAR(3)     NULL,
    reason                   NVARCHAR(MAX)   NULL,
    CONSTRAINT PK_{table}     PRIMARY KEY (row_id),
    CONSTRAINT UQ_{table}_key UNIQUE (dmg_name, app_name, yaml_column_name)
);
PRINT '{table} created.';
END
ELSE
    PRINT '{table} already exists.';"""

# ── DDL: history / audit table ─────────────────────────────────────────
# Append-only.  INSERT on EVERY run for EVERY record — full time-series audit log.
# Query pattern:
#   SELECT * FROM history WHERE dmg_name=? AND yaml_column_name=?
#   ORDER BY recorded_at DESC
_DDL_HISTORY_TABLE = """\
IF OBJECT_ID(N'[{schema}].[{history_table}]', N'U') IS NULL
BEGIN
CREATE TABLE [{schema}].[{history_table}] (
    history_id               NVARCHAR(36)    NOT NULL DEFAULT NEWID(),
    -- Denormalised natural key (no join needed for audit queries)
    dmg_name                 NVARCHAR(200)   NOT NULL,
    app_name                 NVARCHAR(500)   NOT NULL,
    region                   NVARCHAR(200)   NULL,
    yaml_column_name         NVARCHAR(500)   NOT NULL,
    -- When was this row recorded?
    recorded_at              DATETIME2       NOT NULL DEFAULT GETUTCDATE(),
    -- Snapshot of EVERY business column at time of this run
    yaml_purview_cls         NVARCHAR(500)   NULL,
    yaml_sensitivity_label   NVARCHAR(500)   NULL,
    yaml_status              NVARCHAR(100)   NULL,
    yaml_version             NVARCHAR(100)   NULL,
    yaml_effective_date      NVARCHAR(100)   NULL,
    purview_classification   NVARCHAR(500)   NULL,
    purview_sensitivity      NVARCHAR(2000)  NULL,
    purview_column_count     INT             NULL,
    purview_asset_count      INT             NULL,
    collection_path          NVARCHAR(2000)  NULL,
    compliant                NVARCHAR(3)     NULL,
    reason                   NVARCHAR(MAX)   NULL,
    CONSTRAINT PK_{history_table} PRIMARY KEY (history_id)
);
PRINT '{history_table} created.';
END
ELSE
    PRINT '{history_table} already exists.';"""

# ── DDL: summary table ─────────────────────────────────────────────────
# Append-only run-level summary.  INSERT on every run, never updated.
# One row per DMG / application per run.
_DDL_SUMMARY_TABLE = """\
IF OBJECT_ID(N'[{schema}].[{summary_table}]', N'U') IS NULL
BEGIN
CREATE TABLE [{schema}].[{summary_table}] (
    summary_id        NVARCHAR(36)    NOT NULL DEFAULT NEWID(),
    dmg_name          NVARCHAR(200)   NOT NULL,
    app_name          NVARCHAR(500)   NOT NULL,
    region            NVARCHAR(200)   NULL,
    compliance_status NVARCHAR(20)    NOT NULL,   -- 'Compliant' | 'Non-Compliant'
    inserted_at       DATETIME2       NOT NULL DEFAULT GETUTCDATE(),
    CONSTRAINT PK_{summary_table} PRIMARY KEY (summary_id)
);
PRINT '{summary_table} created.';
END
ELSE
    PRINT '{summary_table} already exists.';"""

# ── Business columns used for change detection in main table ───────────
# Infrastructure columns (row_id, timestamps) are deliberately excluded.
_COMPARE_COLS = [
    "yaml_purview_cls",
    "yaml_sensitivity_label",
    "yaml_status",
    "yaml_version",
    "yaml_effective_date",
    "purview_classification",
    "purview_sensitivity",
    "purview_column_count",
    "purview_asset_count",
    "collection_path",
    "compliant",
    "reason",
]

# Column positions returned by SELECT * on main table (0-indexed).
# Must stay in sync with _DDL_TABLE column order.
_MAIN_COL_IDX = {
    "row_id":                  0,
    "first_seen_at":           1,
    "last_scanned_at":         2,
    "last_updated_at":         3,
    "dmg_name":                4,
    "app_name":                5,
    "region":                  6,
    "yaml_column_name":        7,
    "yaml_purview_cls":        8,
    "yaml_sensitivity_label":  9,
    "yaml_status":             10,
    "yaml_version":            11,
    "yaml_effective_date":     12,
    "purview_classification":  13,
    "purview_sensitivity":     14,
    "purview_column_count":    15,
    "purview_asset_count":     16,
    "collection_path":         17,
    "compliant":               18,
    "reason":                  19,
}


def _val_str(v) -> str:
    """Normalise a DB value to a comparable string (None → empty string)."""
    if v is None:
        return ""
    return str(v).strip()


def _row_changed(existing_db_row, incoming: dict) -> tuple:
    """
    Compare the DB row (pyodbc Row object) with an incoming result dict.
    Returns (changed: bool, changed_cols: list[str]).
    Only _COMPARE_COLS are examined — infrastructure columns are ignored.
    """
    changed_cols = []
    for col in _COMPARE_COLS:
        db_val  = _val_str(existing_db_row[_MAIN_COL_IDX[col]])
        new_val = _val_str(incoming.get(col))
        if db_val != new_val:
            changed_cols.append(col)
    return bool(changed_cols), changed_cols


def _connect_db(db_server, db_database, db_user, db_password):
    """
    Open an Azure SQL connection using the best available driver.

    Attempt order
    ─────────────
    1. pyodbc + ODBC Driver 18  (fastest, most features)
    2. pyodbc + ODBC Driver 17  (older but still supported)
    3. pymssql                  (pure-Python TDS — no ODBC install needed)

    Returns (conn, db_type, driver_label, error_str).
      db_type      : 'pyodbc' | 'pymssql' | None
      driver_label : human-readable name for logging
      error_str    : None on success, message on failure
    """
    _ODBC_DRIVERS_PREF = [
        "ODBC Driver 18 for SQL Server",
        "ODBC Driver 17 for SQL Server",
    ]

    # ── Attempt 1 & 2: pyodbc ─────────────────────────────────────────
    if _HAS_PYODBC:
        odbc_driver = None
        try:
            available = _pyodbc.drivers()
            for d in _ODBC_DRIVERS_PREF:
                if d in available:
                    odbc_driver = d
                    break
        except Exception:
            pass   # pyodbc.drivers() may not work on all platforms

        if odbc_driver is None:
            # Last-ditch: try each driver string directly
            for d in _ODBC_DRIVERS_PREF:
                try:
                    test_str = (
                        f"DRIVER={{{d}}};"
                        f"SERVER={db_server};DATABASE={db_database};"
                        f"UID={db_user};PWD={db_password};"
                        f"Encrypt=yes;TrustServerCertificate=no;"
                        f"Connection Timeout=5;"
                    )
                    _pyodbc.connect(test_str).close()
                    odbc_driver = d
                    break
                except Exception:
                    continue

        if odbc_driver is not None:
            conn_str = (
                f"DRIVER={{{odbc_driver}}};"
                f"SERVER={db_server};"
                f"DATABASE={db_database};"
                f"UID={db_user};"
                f"PWD={db_password};"
                f"Encrypt=yes;TrustServerCertificate=no;"
                f"Connection Timeout=30;"
            )
            try:
                conn = _pyodbc.connect(conn_str, autocommit=False)
                return conn, "pyodbc", odbc_driver, None
            except Exception as e:
                _log(f"  pyodbc connect failed ({odbc_driver}): {e} — "
                     f"will try pymssql fallback", "WARN")
        else:
            _log("  No ODBC driver found on this machine — "
                 "falling back to pymssql (no ODBC install required)", "WARN")
    else:
        _log("  pyodbc not installed — trying pymssql fallback", "WARN")

    # ── Attempt 3: pymssql (no ODBC drivers needed) ───────────────────
    if _HAS_PYMSSQL:
        try:
            conn = _pymssql.connect(
                server=db_server,
                user=db_user,
                password=db_password,
                database=db_database,
                login_timeout=30,
                tds_version="7.4",   # Azure SQL requires TDS 7.x
            )
            # pymssql defaults to autocommit=False — no extra step needed
            return conn, "pymssql", "pymssql (pure-Python TDS)", None
        except Exception as e:
            return None, None, None, (
                f"pymssql connection failed: {e}\n"
                "  Check server name, credentials, and that TCP port 1433 "
                "is reachable."
            )

    # ── Nothing worked ─────────────────────────────────────────────────
    return None, None, None, (
        "No SQL Server driver available.  Install at least ONE of:\n"
        "\n"
        "  Option A — pyodbc + ODBC driver (fastest):\n"
        "    pip install pyodbc\n"
        "    Then install the system ODBC driver:\n"
        "    https://learn.microsoft.com/sql/connect/odbc/"
        "download-odbc-driver-for-sql-server\n"
        "\n"
        "  Option B — pymssql (no system install, pure Python):\n"
        "    pip install pymssql\n"
    )


def push_to_db(rows:             list,
               scan_run_id:      str,       # kept for signature compat; not stored in DB
               db_server:        str,
               db_database:      str,
               db_user:          str,
               db_password:      str,
               db_schema:        str,
               db_table:         str,
               db_history_table: str,
               db_summary_table: str) -> tuple:
    """
    Upsert compliance rows into the detail table, always-insert every row into the
    history (audit) table, and always-insert a run-level summary row.

    Table names
    ───────────
    All three table names come directly from purview_config.ini:
      detail  : [{db_schema}].[{db_table}]
      history : [{db_schema}].[{db_history_table}]
      summary : [{db_schema}].[{db_summary_table}]

    Algorithm per incoming row (detail table)
    ──────────────────────────────────────────
    Natural key = (dmg_name, app_name, yaml_column_name)
    1. NOT FOUND  → INSERT into detail table.
    2. FOUND, no change  → UPDATE last_scanned_at only (silent).
    3. FOUND, something changed  → UPDATE the detail row with new values
                                    and set last_updated_at.

    History table (always-insert)
    ──────────────────────────────
    Every row from every run is inserted unconditionally — this creates a
    complete time-series so the team can track compliance changes over time
    (e.g. "compliant at 9 AM, non-compliant at 5 PM").

    Summary table (always-insert)
    ──────────────────────────────
    One row per DMG/application is inserted on every run.
    compliance_status = 'Compliant' when ALL columns are YES, else 'Non-Compliant'.
    inserted_at records the exact UTC timestamp of this run.

    Returns (rows_processed: int, error: str | None).
    error is None on success.
    """
    if not _HAS_PYODBC and not _HAS_PYMSSQL:
        return 0, (
            "Neither pyodbc nor pymssql is installed.\n"
            "  Install at least one:\n"
            "    pip install pymssql          # no system ODBC driver needed\n"
            "    pip install pyodbc           # also needs ODBC Driver 17/18\n"
        )
    if not rows:
        return 0, None

    history_table = db_history_table
    summary_table = db_summary_table

    conn, db_type, driver_label, err = _connect_db(
        db_server, db_database, db_user, db_password)
    if err:
        return 0, err

    # Parameter placeholder differs by driver:
    #   pyodbc  → ?       (DB-API 2.0 qmark style)
    #   pymssql → %s      (DB-API 2.0 format style)
    ph = "%s" if db_type == "pymssql" else "?"

    _log(f"  Driver in use     : {driver_label}  [{db_type}]", "OK")
    _log(f"  Placeholder style : {ph!r}")
    _log(f"  Detail  table     : [{db_schema}].[{db_table}]")
    _log(f"  History table     : [{db_schema}].[{history_table}]")
    _log(f"  Summary table     : [{db_schema}].[{summary_table}]")

    cur = conn.cursor()
    try:
        # ── 1. Ensure schema exists ────────────────────────────────────
        cur.execute(f"""
            IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = N'{db_schema}')
                EXEC('CREATE SCHEMA [{db_schema}]')
        """)

        # ── 2. Ensure main table exists ────────────────────────────────
        for stmt in _DDL_TABLE.format(schema=db_schema, table=db_table).split("\nGO\n"):
            stmt = stmt.strip()
            if stmt:
                cur.execute(stmt)

        # ── 3. Ensure history table exists ─────────────────────────────
        for stmt in _DDL_HISTORY_TABLE.format(
                schema=db_schema, history_table=history_table).split("\nGO\n"):
            stmt = stmt.strip()
            if stmt:
                cur.execute(stmt)

        # ── 4. Ensure summary table exists ─────────────────────────────
        for stmt in _DDL_SUMMARY_TABLE.format(
                schema=db_schema, summary_table=summary_table).split("\nGO\n"):
            stmt = stmt.strip()
            if stmt:
                cur.execute(stmt)

        _log(f"  Schema and all three tables ready", "OK")

        # ── 5. Fetch all existing main-table rows for the incoming DMGs ─
        # Pull only the DMGs present in this scan — avoids loading the whole table.
        incoming_dmgs = list({r["dmg_name"] for r in rows if r.get("dmg_name")})
        existing: dict = {}   # (dmg_name, app_name, yaml_column_name) → db_row
        if incoming_dmgs:
            placeholders = ",".join([ph] * len(incoming_dmgs))
            cur.execute(
                f"SELECT * FROM [{db_schema}].[{db_table}] "
                f"WHERE dmg_name IN ({placeholders})",
                incoming_dmgs,
            )
            for db_row in cur.fetchall():
                key = (
                    _val_str(db_row[_MAIN_COL_IDX["dmg_name"]]),
                    _val_str(db_row[_MAIN_COL_IDX["app_name"]]),
                    _val_str(db_row[_MAIN_COL_IDX["yaml_column_name"]]),
                )
                existing[key] = db_row
        _log(f"  Existing records fetched for comparison: {len(existing)}")

        # ── 6. Categorise every incoming row for main table ────────────
        to_insert = []   # brand-new records
        to_update = []   # (new_row_dict, db_row, changed_cols)
        to_touch  = []   # unchanged — only bump last_scanned_at

        for row in rows:
            key = (
                _val_str(row.get("dmg_name")),
                _val_str(row.get("app_name")),
                _val_str(row.get("yaml_column_name")),
            )
            if key not in existing:
                to_insert.append(row)
            else:
                db_row = existing[key]
                changed, changed_cols = _row_changed(db_row, row)
                if changed:
                    to_update.append((row, db_row, changed_cols))
                else:
                    to_touch.append(db_row[_MAIN_COL_IDX["row_id"]])

        _log(f"  New records (INSERT)    : {len(to_insert)}")
        _log(f"  Changed records (UPDATE): {len(to_update)}")
        _log(f"  Unchanged (touch-only)  : {len(to_touch)}")

        # ── 7. INSERT new records into main table ──────────────────────
        if to_insert:
            _phs15 = ", ".join([ph] * 15)
            ins_sql = f"""
                INSERT INTO [{db_schema}].[{db_table}] (
                    dmg_name, app_name, region, yaml_column_name,
                    yaml_purview_cls,
                    yaml_sensitivity_label, yaml_status,
                    yaml_version, yaml_effective_date,
                    purview_classification, purview_sensitivity,
                    purview_column_count, purview_asset_count,
                    collection_path, compliant, reason
                ) VALUES ({ph},{ph},{ph},{ph},{ph},{ph},{ph},{ph},{ph},{ph},{ph},{ph},{ph},{ph},{ph},{ph})
            """
            cur.executemany(ins_sql, [
                (
                    r["dmg_name"],          r["app_name"],
                    r.get("region", ""),    r["yaml_column_name"],
                    r["yaml_purview_cls"],  r["yaml_sensitivity_label"],
                    r["yaml_status"],       r["yaml_version"],
                    r["yaml_effective_date"],
                    r["purview_classification"], r["purview_sensitivity"],
                    r["purview_column_count"],   r["purview_asset_count"],
                    r["collection_path"],   r["compliant"], r["reason"],
                )
                for r in to_insert
            ])
            _log(f"  Inserted {len(to_insert)} new record(s) into main table", "OK")

        # ── 8. UPDATE changed records in main table ────────────────────
        if to_update:
            upd_sql = f"""
                UPDATE [{db_schema}].[{db_table}]
                SET
                    last_scanned_at        = GETUTCDATE(),
                    last_updated_at        = GETUTCDATE(),
                    region                 = {ph},
                    yaml_purview_cls       = {ph},
                    yaml_sensitivity_label = {ph},
                    yaml_status            = {ph},
                    yaml_version           = {ph},
                    yaml_effective_date    = {ph},
                    purview_classification = {ph},
                    purview_sensitivity    = {ph},
                    purview_column_count   = {ph},
                    purview_asset_count    = {ph},
                    collection_path        = {ph},
                    compliant              = {ph},
                    reason                 = {ph}
                WHERE dmg_name = {ph} AND app_name = {ph} AND yaml_column_name = {ph}
            """
            for r, db_row, changed_cols in to_update:
                cur.execute(upd_sql, (
                    r.get("region", ""),
                    r["yaml_purview_cls"],  r["yaml_sensitivity_label"],
                    r["yaml_status"],       r["yaml_version"],
                    r["yaml_effective_date"],
                    r["purview_classification"], r["purview_sensitivity"],
                    r["purview_column_count"],   r["purview_asset_count"],
                    r["collection_path"],        r["compliant"],
                    r["reason"],
                    r["dmg_name"], r["app_name"], r["yaml_column_name"],
                ))
                _log(f"    UPDATED  [{r['dmg_name']}] "
                     f"{r['yaml_column_name']}  — changed: {', '.join(changed_cols)}")
            _log(f"  Updated {len(to_update)} changed record(s) in main table", "OK")

        # ── 9. Touch unchanged records (bump last_scanned_at only) ─────
        if to_touch:
            touch_sql = (
                f"UPDATE [{db_schema}].[{db_table}] "
                f"SET last_scanned_at = GETUTCDATE() "
                f"WHERE row_id = {ph}"
            )
            cur.executemany(touch_sql, [(rid,) for rid in to_touch])

        # ── 10. ALWAYS-INSERT all rows into history table ──────────────
        # Every run, every row — unconditional append for full time-series audit.
        _phs_hist = ", ".join([ph] * 16)
        hist_sql = f"""
            INSERT INTO [{db_schema}].[{history_table}] (
                dmg_name, app_name, region, yaml_column_name,
                yaml_purview_cls,
                yaml_sensitivity_label, yaml_status,
                yaml_version, yaml_effective_date,
                purview_classification, purview_sensitivity,
                purview_column_count, purview_asset_count,
                collection_path, compliant, reason
            ) VALUES ({_phs_hist})
        """
        cur.executemany(hist_sql, [
            (
                r["dmg_name"],          r["app_name"],
                r.get("region", ""),    r["yaml_column_name"],
                r["yaml_purview_cls"],  r["yaml_sensitivity_label"],
                r["yaml_status"],       r["yaml_version"],
                r["yaml_effective_date"],
                r["purview_classification"], r["purview_sensitivity"],
                r["purview_column_count"],   r["purview_asset_count"],
                r["collection_path"],   r["compliant"], r["reason"],
            )
            for r in rows
        ])
        _log(f"  Inserted {len(rows)} row(s) into history table (every run, every row)", "OK")

        # ── 11. ALWAYS-INSERT per-DMG summary rows ─────────────────────
        # Aggregate per (dmg_name, app_name) — compliance_status = 'Compliant'
        # only when ALL columns are YES.
        from collections import defaultdict as _dd
        _sum_agg: dict = {}
        for r in rows:
            k = (r["dmg_name"], r["app_name"])
            if k not in _sum_agg:
                _sum_agg[k] = {"region": r.get("region", ""), "all_yes": True}
            if r["compliant"] != "YES":
                _sum_agg[k]["all_yes"] = False

        sum_sql = f"""
            INSERT INTO [{db_schema}].[{summary_table}]
                (dmg_name, app_name, region, compliance_status)
            VALUES ({ph}, {ph}, {ph}, {ph})
        """
        cur.executemany(sum_sql, [
            (
                dmg, app,
                info["region"],
                "Compliant" if info["all_yes"] else "Non-Compliant",
            )
            for (dmg, app), info in sorted(_sum_agg.items())
        ])
        _log(f"  Inserted {len(_sum_agg)} summary row(s) into summary table", "OK")

        conn.commit()

        total_processed = len(to_insert) + len(to_update) + len(to_touch)
        _log(f"  DB push complete: {total_processed} main-table row(s) processed  "
             f"({len(to_insert)} inserted, {len(to_update)} updated, "
             f"{len(to_touch)} unchanged)  "
             f"| {len(rows)} history rows  | {len(_sum_agg)} summary rows", "OK")
        return total_processed, None

    except Exception as e:
        try:
            conn.rollback()
        except Exception:
            pass
        return 0, str(e)
    finally:
        try:
            cur.close()
        except Exception:
            pass
        try:
            conn.close()
        except Exception:
            pass


# ═══════════════════════════════════════════════════════════════════════
#  PUBLIC ENTRY POINT  (called from purview_scan.py)
# ═══════════════════════════════════════════════════════════════════════

def run_compliance_check(snapshot_path: str,
                         yaml_source:   str,
                         dmg_filter:    list = None,
                         out_path:      str  = "compliance_report.xlsx",
                         # YAML source mode flags
                         yaml_source_github:     bool = False,
                         yaml_source_azure_blob: bool = False,
                         yaml_source_local:      bool = False,
                         # GitHub auth
                         github_token:           str  = "",
                         # Azure Blob params
                         blob_account:           str  = "",
                         blob_container:         str  = "",
                         blob_prefix:            str  = "",
                         blob_sas_token:         str  = "",
                         blob_conn_str:          str  = "",
                         # DB push params (all passed from purview_scan.py)
                         db_load:         bool = False,
                         db_schema:       str  = "",
                         db_table:        str  = "",
                         db_history_table:str  = "",
                         db_summary_table:str  = "",
                         db_server:       str  = "",
                         db_database:     str  = "",
                         db_user:         str  = "",
                         db_password:     str  = "") -> dict:
    """
    Main entry point for integration with purview_scan.py.

    Parameters
    ----------
    snapshot_path : str       Path to purview_snapshot.json
    yaml_source   : str       URL or local path to YAML file(s)
    dmg_filter    : list      DMG IDs to check. Empty/None = all.
    out_path      : str       Excel report path (written next to script,
                              overwritten on every run)
    yaml_source_github     : bool  True → GitHub mode (public or private)
    yaml_source_azure_blob : bool  True → Azure Blob Storage mode
    yaml_source_local      : bool  True → local filesystem mode
    github_token           : str   PAT for private GitHub repos
    blob_account/container/prefix/sas_token/conn_str : Azure Blob params
    db_load          : bool   True = push rows to Azure SQL after Excel
    db_schema        : str    Target schema name (mandatory if db_load=True)
    db_table         : str    Detail table name  (mandatory if db_load=True)
    db_history_table : str    History/audit table name (mandatory if db_load=True)
    db_summary_table : str    Summary table name (mandatory if db_load=True)
    db_server        : str    Azure SQL server hostname
    db_database      : str    Database name
    db_user          : str    SQL login username
    db_password      : str    SQL login password

    Returns
    -------
    dict with keys:
      total, compliant, non_compliant, not_found, report_path,
      db_status ("success"|"skipped"|"failed"|"disabled"),
      db_rows_loaded, db_error
    """
    t0     = time.time()
    run_ts = datetime.datetime.now(
        datetime.timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
    # Use scan_run_id = timestamp-based UUID for DB dedup key
    import uuid as _uuid
    scan_run_id = str(_uuid.uuid4())

    _section("STEP 7 — Compliance Check  (YAML vs Purview Snapshot)")
    _log(f"Snapshot     : {snapshot_path}")
    _log(f"YAML source  : {yaml_source}")
    _log(f"DMG filter   : {dmg_filter or 'all'}")
    _log(f"Report out   : {out_path}  (overwrites previous run)")
    _log(f"DB push      : {'ENABLED → ' + db_schema + '.' + db_table if db_load else 'disabled'}")

    # ── Load YAML(s) ──────────────────────────────────────────────────
    _log("Loading YAML source(s)...")
    yaml_sources = load_yaml_sources(
        yaml_source,
        yaml_source_github=yaml_source_github,
        yaml_source_azure_blob=yaml_source_azure_blob,
        yaml_source_local=yaml_source_local,
        github_token=github_token,
        blob_account=blob_account,
        blob_container=blob_container,
        blob_prefix=blob_prefix,
        blob_sas_token=blob_sas_token,
        blob_conn_str=blob_conn_str,
    )
    if not yaml_sources:
        _log("No YAML documents loaded — compliance check skipped.", "WARN")
        return {"total": 0, "compliant": 0, "non_compliant": 0,
                "not_found": 0, "report_path": "",
                "db_status": "skipped", "db_rows_loaded": 0, "db_error": None}

    # ── Load snapshot ─────────────────────────────────────────────────
    snap_lookup = load_snapshot(snapshot_path)
    if not snap_lookup:
        _log("Snapshot is empty or not found — compliance check skipped.", "WARN")
        return {"total": 0, "compliant": 0, "non_compliant": 0,
                "not_found": 0, "report_path": "",
                "db_status": "skipped", "db_rows_loaded": 0, "db_error": None}

    # ── Run comparison ────────────────────────────────────────────────
    _section("STEP 7b — Column-by-column comparison")
    rows = build_rows(yaml_sources, snap_lookup, dmg_filter or [])

    if not rows:
        _log("No rows produced — check DMG filter and YAML content.", "WARN")
        return {"total": 0, "compliant": 0, "non_compliant": 0,
                "not_found": 0, "report_path": "",
                "db_status": "skipped", "db_rows_loaded": 0, "db_error": None}

    # ── Write Excel — overwrite completely on every run ───────────────
    _section("STEP 7c — Writing compliance Excel report")
    # Ensure the output path's parent exists
    Path(out_path).parent.mkdir(parents=True, exist_ok=True)
    # If a previous Excel exists, delete it first so it is always fresh
    if Path(out_path).exists():
        Path(out_path).unlink()
        _log(f"  Deleted previous report: {Path(out_path).name}")
    _log(f"Writing report: {out_path}")
    _build_excel(rows, out_path, run_ts)
    size_mb = Path(out_path).stat().st_size / (1024 * 1024)
    _log(f"Report written: {out_path}  ({size_mb:.2f} MB)", "OK")

    # ── DB push (only when db_load=True) ─────────────────────────────
    db_status    = "disabled"
    db_rows_done = 0
    db_error_msg = None

    if db_load:
        _section("STEP 7d — Pushing compliance rows to Azure SQL DB")
        _log(f"  Target (detail)  : [{db_schema}].[{db_table}]  on  {db_server}/{db_database}")
        _log(f"  Target (history) : [{db_schema}].[{db_history_table}]  (INSERT every run)")
        _log(f"  Target (summary) : [{db_schema}].[{db_summary_table}]  (INSERT every run)")
        _log(f"  Rows to process  : {len(rows)}")
        _log(f"  Mode: upsert detail / always-insert history + summary")
        db_rows_done, db_error_msg = push_to_db(
            rows             = rows,
            scan_run_id      = scan_run_id,
            db_server        = db_server,
            db_database      = db_database,
            db_user          = db_user,
            db_password      = db_password,
            db_schema        = db_schema,
            db_table         = db_table,
            db_history_table = db_history_table,
            db_summary_table = db_summary_table,
        )
        if db_error_msg:
            _log(f"  DB push FAILED: {db_error_msg}", "ERROR")
            db_status = "failed"
        else:
            db_status = "success"
    elif not rows:
        db_status = "skipped"

    # ── Final summary ─────────────────────────────────────────────────
    elapsed   = _fmt(time.time() - t0)
    total     = len(rows)
    compliant = sum(1 for r in rows if r["compliant"] == "YES")
    not_found = sum(1 for r in rows if r.get("_cls_not_found", False))

    _print_summary(rows, out_path, elapsed)

    return {
        "total":          total,
        "compliant":      compliant,
        "non_compliant":  total - compliant,
        "not_found":      not_found,
        "report_path":    out_path,
        "db_status":      db_status,
        "db_rows_loaded": db_rows_done,
        "db_error":       db_error_msg,
    }


def _fmt(s):
    s = int(s); h, r = divmod(s, 3600); m, s = divmod(r, 60)
    return f"{h}h {m}m {s}s" if h else (f"{m}m {s}s" if m else f"{s}s")


# ═══════════════════════════════════════════════════════════════════════
#  STANDALONE CLI
# ═══════════════════════════════════════════════════════════════════════

def _cli():
    ap = argparse.ArgumentParser(
        description="Compliance check: YAML vs Purview snapshot")
    ap.add_argument("--snapshot", required=True,
                    help="Path to purview_snapshot.json")
    ap.add_argument("--yaml",     required=True,
                    help="URL or local path to YAML (or GitHub tree URL)")
    ap.add_argument("--dmg",      default="",
                    help="Comma-separated DMG IDs to check (default: all)")
    ap.add_argument("--out",      default="compliance_report.xlsx",
                    help="Output Excel path")
    args = ap.parse_args()

    dmg_filter = [d.strip() for d in args.dmg.split(",") if d.strip()]
    run_compliance_check(
        snapshot_path = args.snapshot,
        yaml_source   = args.yaml,
        dmg_filter    = dmg_filter,
        out_path      = args.out,
    )


if __name__ == "__main__":
    _cli()