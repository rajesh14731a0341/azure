"""
purview_compliance.py
=====================
Standalone PIPEDA Compliance Checker.

Compares a PIPEDA Sensitivity Mapping YAML (local file or GitHub URL)
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
#  YAML LOADER  — local file, raw GitHub URL, or GitHub tree (folder)
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


def _discover_yaml_urls(tree_url: str) -> list:
    """
    Given a GitHub tree URL (a folder), return raw URLs for every
    *.yaml file underneath it via the GitHub Trees API.
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
    try:
        r = _requests.get(api_url, timeout=30)
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


def load_yaml_sources(source: str) -> list:
    """
    Load all YAML documents from source (URL, GitHub tree, or local path).
    Returns list of  (source_label, parsed_dict)  tuples.
    """
    if not _HAS_YAML:
        _log("pyyaml not installed — run:  pip install pyyaml", "ERROR")
        return []

    results = []

    if source.startswith("http"):
        # GitHub tree URL → discover all YAMLs in that folder
        if "/tree/" in source and "/blob/" not in source:
            urls = _discover_yaml_urls(source)
        else:
            urls = [_to_raw_url(source)]

        for url in urls:
            _log(f"Loading YAML → {url}")
            try:
                if not _HAS_REQUESTS:
                    _log("requests not installed — run:  pip install requests",
                         "ERROR")
                    continue
                r = _requests.get(url, timeout=30)
                r.raise_for_status()
                doc = _load_yaml_text(r.text)
                if doc:
                    results.append((url, doc))
                    _log(f"  Loaded: policy='{doc.get('policy_name','')}' "
                         f"v{doc.get('version','')}  "
                         f"products={len(doc.get('data_products',[]))}", "OK")
                else:
                    _log(f"  Empty YAML at {url}", "WARN")
            except Exception as e:
                _log(f"Failed to load {url}: {e}", "WARN")
    else:
        p = Path(source)
        if p.is_dir():
            for yf in sorted(p.glob("**/*.yaml")):
                _log(f"Loading YAML → {yf}")
                try:
                    doc = _load_yaml_text(yf.read_text(encoding="utf-8"))
                    if doc:
                        results.append((str(yf), doc))
                        _log(f"  Loaded: v{doc.get('version','')}  "
                             f"products={len(doc.get('data_products',[]))}", "OK")
                except Exception as e:
                    _log(f"Failed to load {yf}: {e}", "WARN")
        else:
            _log(f"Loading YAML → {p}")
            try:
                doc = _load_yaml_text(p.read_text(encoding="utf-8"))
                if doc:
                    results.append((str(p), doc))
                    _log(f"  Loaded: policy='{doc.get('policy_name','')}' "
                         f"v{doc.get('version','')}  "
                         f"products={len(doc.get('data_products',[]))}", "OK")
            except Exception as e:
                _log(f"Failed to load {p}: {e}", "ERROR")

    return results


# ═══════════════════════════════════════════════════════════════════════
#  SNAPSHOT LOADER
#  Builds a fast lookup:  column_name_lower → [snap_record, ...]
#  One column name can appear across many assets — we keep all of them.
# ═══════════════════════════════════════════════════════════════════════

def load_snapshot(snapshot_path: str) -> dict:
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

    lookup = {}
    for rec in records:
        name = (rec.get("column_name") or "").strip().lower()
        if name:
            lookup.setdefault(name, []).append(rec)

    _log(f"Snapshot ready: {len(records):,} enriched column records  |  "
         f"{len(lookup):,} unique column names", "OK")
    return lookup


# ═══════════════════════════════════════════════════════════════════════
#  COMPLIANCE LOGIC
# ═══════════════════════════════════════════════════════════════════════

def _n(s) -> str:
    """Normalise: lowercase + strip."""
    return (s or "").strip().lower()


def _check_column(yaml_col: dict, snap_records: list) -> dict:
    """
    Compare one YAML column entry against all matching snapshot records.

    Returns a result dict with:
      purview_column_name, purview_classification, purview_sensitivity,
      asset_qualified_name, collection_path,
      compliant ("YES"/"NO"), reason (list of failure strings)

    Multi-asset strategy: if the column exists in several assets, pick
    the record that satisfies the most compliance conditions.
    """
    yaml_cls_raw  = yaml_col.get("purview_classification", "").strip()
    yaml_lbl_raw  = yaml_col.get("sensitivity_label", "").strip()
    yaml_cls      = _n(yaml_cls_raw)
    yaml_lbl      = _n(yaml_lbl_raw)
    yaml_cls_na   = yaml_cls in ("n/a", "na", "none", "")

    if not snap_records:
        return {
            "purview_column_name":    "NOT FOUND IN PURVIEW",
            "purview_classification": "",
            "purview_sensitivity":    "",
            "asset_qualified_name":   "",
            "collection_path":        "",
            "compliant":              "NO",
            "reasons":                ["Column not found in Purview snapshot"],
        }

    def _score(rec):
        p_cls = [_n(c) for c in (rec.get("classification_names") or [])]
        p_lbl = [_n(l) for l in (rec.get("sensitivity_labels") or [])]
        cls_ok = (yaml_cls_na and not p_cls) or \
                 (not yaml_cls_na and yaml_cls in p_cls)
        lbl_ok = (not yaml_lbl and not p_lbl) or \
                 (yaml_lbl and yaml_lbl in p_lbl)
        return int(cls_ok) + int(lbl_ok), cls_ok, lbl_ok

    best, b_cls_ok, b_lbl_ok = None, False, False
    best_score = -1
    for rec in snap_records:
        sc, c_ok, l_ok = _score(rec)
        if sc > best_score:
            best_score, best, b_cls_ok, b_lbl_ok = sc, rec, c_ok, l_ok

    p_cls_list = best.get("classification_names") or []
    p_lbl_list = best.get("sensitivity_labels") or []

    reasons = []
    if not b_cls_ok:
        got = ", ".join(p_cls_list) or "(none)"
        reasons.append(
            f"Classification: expected '{yaml_cls_raw}', Purview has '{got}'")
    if not b_lbl_ok:
        got = ", ".join(p_lbl_list) or "(none)"
        reasons.append(
            f"Sensitivity label: expected '{yaml_lbl_raw}', Purview has '{got}'")

    return {
        "purview_column_name":    best.get("column_name", ""),
        "purview_classification": ", ".join(p_cls_list),
        "purview_sensitivity":    ", ".join(p_lbl_list),
        "asset_qualified_name":   best.get("asset_qualified_name", ""),
        "collection_path":        best.get("collection_hierarchy_path", ""),
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
    and produce one result row per column.
    """
    requested = {d.strip() for d in dmg_filter if d.strip()}
    all_rows  = []

    for src_label, doc in yaml_sources:
        policy_ver = doc.get("version", "")
        policy_nm  = doc.get("policy_name", "")
        _log(f"Processing: '{policy_nm}'  v{policy_ver}  source={src_label}")

        for dp in (doc.get("data_products") or []):
            dmg_name = dp.get("dmg_name", "")
            app_name = dp.get("app_name", "")

            if requested and dmg_name not in requested:
                _log(f"  [SKIP] DMG {dmg_name} — not in filter")
                continue

            columns = dp.get("columns") or []
            _log(f"  DMG: {dmg_name}  |  App: {app_name}  "
                 f"|  {len(columns)} column(s) to check")
            _rule()

            for col in columns:
                col_name = col.get("column_name", "")
                col_status = col.get("status", "active")

                snap_recs = snap_lookup.get(col_name.strip().lower(), [])
                result    = _check_column(col, snap_recs)

                # ── Detailed per-column log ─────────────────────────────
                found_count = len(snap_recs)
                found_label = (f"{found_count} asset(s)" if found_count
                               else "NOT FOUND")

                _log(f"  Column   : {col_name}")
                _log(f"  YAML cls : {col.get('purview_classification','')}"
                     f"  |  YAML label : {col.get('sensitivity_label','')}")
                _log(f"  Purview  : cls={result['purview_classification'] or '(none)'}"
                     f"  |  label={result['purview_sensitivity'] or '(none)'}"
                     f"  |  found in {found_label}")

                if result["compliant"] == "YES":
                    _log(f"  Result   : ✓ COMPLIANT", "OK")
                else:
                    for reason in result["reasons"]:
                        _log(f"  Mismatch : {reason}", "WARN")
                    _log(f"  Result   : ✗ NON-COMPLIANT", "WARN")
                _rule()

                all_rows.append({
                    "dmg_name":               dmg_name,
                    "app_name":               app_name,
                    "yaml_column_name":       col_name,
                    "pipeda_classification":  col.get("pipeda_classification", ""),
                    "yaml_sensitivity_label": col.get("sensitivity_label", ""),
                    "yaml_purview_cls":       col.get("purview_classification", ""),
                    "yaml_status":            col_status,
                    "yaml_version":           col.get("change_version", policy_ver),
                    "yaml_effective_date":    col.get("effective_date", ""),
                    "purview_column_name":    result["purview_column_name"],
                    "purview_classification": result["purview_classification"],
                    "purview_sensitivity":    result["purview_sensitivity"],
                    "asset_qualified_name":   result["asset_qualified_name"],
                    "collection_path":        result["collection_path"],
                    "compliant":              result["compliant"],
                    "reason":                 "; ".join(result["reasons"]),
                })

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
    ws.merge_cells("A1:O1")
    _cell(ws, 1, 1,
          f"PURVIEW  ×  PIPEDA  COMPLIANCE REPORT  —  "
          f"Generated: {run_ts}",
          bg=_C_NAVY, fg=_C_WHITE, bold=True,
          align="center", size=12)
    ws.row_dimensions[1].height = 26

    # Group-label row (spans per group)
    groups = [
        (1,  8,  "FROM YAML  (Master / Reference)",        _C_BLUE),
        (9,  13, "FROM PURVIEW SNAPSHOT",                   "FF4472C4"),
        (14, 14, "YAML VERSION",                            "FF70AD47"),
        (15, 16, "COMPLIANCE RESULT",                       _C_PURPLE),
    ]
    for s, e, lbl, bg in groups:
        ws.merge_cells(start_row=2, start_column=s,
                       end_row=2,   end_column=e)
        _cell(ws, 2, s, lbl, bg=bg, fg=_C_WHITE,
              bold=True, align="center", size=9)
    ws.row_dimensions[2].height = 18

    # Column headers
    hdrs = [
        # YAML (1-8)
        ("DMG Name",                _C_BLUE),
        ("Application Name",        _C_BLUE),
        ("YAML Column Name",        _C_BLUE),
        ("PIPEDA Classification",   _C_BLUE),
        ("YAML Purview Class.",     _C_BLUE),
        ("YAML Sensitivity Label",  _C_BLUE),
        ("YAML Status",             _C_BLUE),
        ("YAML Effective Date",     _C_BLUE),
        # Purview (9-13)
        ("Purview Column Name",     "FF4472C4"),
        ("Purview Classification",  "FF4472C4"),
        ("Purview Sensitivity",     "FF4472C4"),
        ("Asset Qualified Name",    "FF4472C4"),
        ("Collection Path",         "FF4472C4"),
        # Version (14)
        ("YAML Version",            "FF70AD47"),
        # Compliance (15-16)
        ("Compliance",              _C_PURPLE),
        ("Reason (if Non-Compliant)", _C_PURPLE),
    ]
    for ci, (h, bg) in enumerate(hdrs, start=1):
        _hdr(ws, 3, ci, h, bg=bg)
    ws.row_dimensions[3].height = 36
    ws.freeze_panes = "A4"

    # Data rows
    for ri, row in enumerate(rows, start=4):
        not_found = "NOT FOUND" in row["purview_column_name"].upper()
        base_bg   = _C_GOLD if not_found else (_C_ALT if ri % 2 == 0 else _C_WHITE)
        comp      = row["compliant"]
        comp_bg   = _C_GREEN if comp == "YES" else _C_RED
        comp_fg   = _C_WHITE

        vals = [
            row["dmg_name"],
            row["app_name"],
            row["yaml_column_name"],
            row["pipeda_classification"],
            row["yaml_purview_cls"],
            row["yaml_sensitivity_label"],
            row["yaml_status"],
            row["yaml_effective_date"],
            row["purview_column_name"],
            row["purview_classification"],
            row["purview_sensitivity"],
            row["asset_qualified_name"],
            row["collection_path"],
            row["yaml_version"],
            comp,
            row["reason"],
        ]
        for ci, val in enumerate(vals, start=1):
            is_comp = ci == 15
            bg      = comp_bg if is_comp else base_bg
            fg      = comp_fg if is_comp else (
                _C_RED if not_found and ci == 9 else _C_BLACK)
            _cell(ws, ri, ci, val,
                  bg=bg, fg=fg, bold=is_comp,
                  align="center" if ci in (1, 7, 14, 15) else "left",
                  wrap=ci in (12, 13, 16))
        ws.row_dimensions[ri].height = 18

    # Column widths
    widths = [14, 26, 26, 26, 34, 24, 10, 14,
              26, 34, 24, 52, 36, 12, 13, 54]
    for i, w in enumerate(widths, start=1):
        ws.column_dimensions[get_column_letter(i)].width = w

    ws.auto_filter.ref = f"A3:{get_column_letter(len(hdrs))}3"

    # ──────────────────────────────────────────────────────────────────
    #  SHEET 2 — SUMMARY
    # ──────────────────────────────────────────────────────────────────
    ws2 = wb.create_sheet("Summary")

    ws2.merge_cells("A1:G1")
    _cell(ws2, 1, 1, "COMPLIANCE SUMMARY  —  Per DMG / Application",
          bg=_C_NAVY, fg=_C_WHITE, bold=True, align="center", size=12)
    ws2.row_dimensions[1].height = 26

    sum_hdrs = ["DMG Name", "Application Name",
                "Total Columns", "Compliant", "Non-Compliant",
                "Not Found in Purview", "Compliance Rate"]
    for ci, h in enumerate(sum_hdrs, start=1):
        _hdr(ws2, 2, ci, h)
    ws2.row_dimensions[2].height = 30

    agg = defaultdict(lambda: {"total": 0, "yes": 0, "no": 0, "nf": 0})
    for row in rows:
        k = (row["dmg_name"], row["app_name"])
        agg[k]["total"] += 1
        if row["compliant"] == "YES":
            agg[k]["yes"] += 1
        else:
            agg[k]["no"] += 1
        if "NOT FOUND" in row["purview_column_name"].upper():
            agg[k]["nf"] += 1

    for sr, ((dmg, app), c) in enumerate(sorted(agg.items()), start=3):
        bg = _C_ALT if sr % 2 == 0 else _C_WHITE
        _cell(ws2, sr, 1, dmg,      bg=bg)
        _cell(ws2, sr, 2, app,      bg=bg)
        _cell(ws2, sr, 3, c["total"], bg=bg, align="center")
        _cell(ws2, sr, 4, c["yes"], bg=bg, fg="FF375623", bold=True, align="center")
        _cell(ws2, sr, 5, c["no"],  bg=bg,
              fg=_C_RED if c["no"] else _C_BLACK,
              bold=bool(c["no"]), align="center")
        _cell(ws2, sr, 6, c["nf"],  bg=bg,
              fg="FFBF8F00" if c["nf"] else _C_BLACK,
              bold=bool(c["nf"]), align="center")
        rc = ws2.cell(row=sr, column=7, value=f"=D{sr}/C{sr}")
        rc.number_format = "0.0%"
        rc.font      = Font(bold=True, name="Arial", size=9)
        rc.fill      = PatternFill("solid", start_color=bg)
        rc.alignment = Alignment(horizontal="center", vertical="center")
        rc.border    = _THIN_BORDER
        ws2.row_dimensions[sr].height = 18

    last = 2 + len(agg)
    tr   = last + 1
    ws2.merge_cells(f"A{tr}:B{tr}")
    _hdr(ws2, tr, 1, "TOTAL")
    _hdr(ws2, tr, 3, f"=SUM(C3:C{last})")
    _hdr(ws2, tr, 4, f"=SUM(D3:D{last})", bg=_C_GREEN)
    _hdr(ws2, tr, 5, f"=SUM(E3:E{last})", bg=_C_RED)
    _hdr(ws2, tr, 6, f"=SUM(F3:F{last})", bg="FFBF8F00")
    rc2 = ws2.cell(row=tr, column=7, value=f"=D{tr}/C{tr}")
    rc2.number_format = "0.0%"
    rc2.font      = Font(bold=True, color=_C_WHITE, name="Arial", size=10)
    rc2.fill      = PatternFill("solid", start_color=_C_NAVY)
    rc2.alignment = Alignment(horizontal="center", vertical="center")
    rc2.border    = _THIN_BORDER

    for ci, w in enumerate([14, 30, 14, 12, 16, 20, 16], start=1):
        ws2.column_dimensions[get_column_letter(ci)].width = w
    ws2.freeze_panes = "A3"

    # ──────────────────────────────────────────────────────────────────
    #  SHEET 3 — LEGEND
    # ──────────────────────────────────────────────────────────────────
    ws3 = wb.create_sheet("Legend")
    ws3.merge_cells("A1:C1")
    _cell(ws3, 1, 1, "COMPLIANCE RULES  &  LEGEND",
          bg=_C_NAVY, fg=_C_WHITE, bold=True, align="center", size=12)
    ws3.row_dimensions[1].height = 26

    legend_rows = [
        ("", "", ""),
        ("COMPLIANCE RULES", "", ""),
        ("A column is COMPLIANT (YES) when ALL of these are true:", "", ""),
        ("Rule 1", "Classification match",
         "Purview classification_names must contain the YAML "
         "purview_classification (case-insensitive). "
         "If YAML says N/A, Purview must have no classification."),
        ("Rule 2", "Sensitivity label match",
         "Purview sensitivity_labels must contain the YAML sensitivity_label "
         "(case-insensitive). If YAML has a label and Purview has none → NO."),
        ("Rule 3", "Column must exist",
         "The YAML column_name must exist in the Purview snapshot. "
         "If not found → NON-COMPLIANT regardless of other fields."),
        ("", "", ""),
        ("COLOUR CODING  (Compliance Detail sheet)", "", ""),
        ("Green cell", "Compliance = YES", "All rules passed"),
        ("Red cell",   "Compliance = NO",
         "One or more rules failed — see Reason column"),
        ("Gold row",   "NOT FOUND IN PURVIEW",
         "Column name from YAML not found in snapshot at all"),
        ("", "", ""),
        ("MATCH STRATEGY", "", ""),
        ("Column name",   "Case-insensitive",
         "YAML column_name matched to snapshot column_name ignoring case/whitespace"),
        ("Multi-asset",   "Best-match scoring",
         "If a column name exists across multiple Purview assets, the record "
         "with the most matching fields is chosen for comparison"),
        ("", "", ""),
        ("DATA SOURCES", "", ""),
        ("YAML file",        "Master / Reference",
         "GitHub YAML — DMG definitions, PIPEDA mappings, expected labels"),
        ("Purview snapshot", "Live scan data",
         "purview_snapshot.json produced by purview_scan.py — "
         "contains actual classifications and sensitivity labels from Purview"),
        ("", "", ""),
        ("COLUMNS (Compliance Detail)", "", ""),
        ("DMG Name",          "YAML",    "dmg_name from data_products[]"),
        ("Application Name",  "YAML",    "app_name from data_products[]"),
        ("YAML Column Name",  "YAML",    "column_name from columns[]"),
        ("PIPEDA Classification", "YAML",
         "pipeda_classification — PIPEDA regulatory category"),
        ("YAML Purview Class.", "YAML",
         "purview_classification — expected classification in Purview"),
        ("YAML Sensitivity Label", "YAML",
         "sensitivity_label — expected label in Purview"),
        ("Purview Column Name",   "Purview",
         "Matched column_name from snapshot (or NOT FOUND IN PURVIEW)"),
        ("Purview Classification","Purview",
         "classification_names from snapshot (comma-separated)"),
        ("Purview Sensitivity",   "Purview",
         "sensitivity_labels from snapshot (comma-separated)"),
        ("Asset Qualified Name",  "Purview",
         "asset_qualified_name — full datasource path in Purview"),
        ("Collection Path",       "Purview",
         "collection_hierarchy_path — Purview collection this asset belongs to"),
        ("YAML Version",          "YAML",
         "change_version from this specific column entry"),
        ("Compliance",            "Result",  "YES or NO"),
        ("Reason",                "Result",
         "Blank when YES. Comma-separated mismatch details when NO."),
    ]

    for lr, (a, b, c) in enumerate(legend_rows, start=2):
        is_section = (not b and not c and a
                      and not a.startswith(("Rule", "Column name",
                                            "Multi-asset", "YAML", "Purview",
                                            "Application", "DMG", "PIPEDA",
                                            "Asset", "Collection",
                                            "Green", "Red", "Gold")))
        bg = _C_BLUE if is_section else _C_WHITE
        fg = _C_WHITE if is_section else _C_BLACK
        for ci, val in enumerate([a, b, c], start=1):
            _cell(ws3, lr, ci, val, bg=bg, fg=fg, bold=is_section,
                  wrap=True, size=9)
        ws3.row_dimensions[lr].height = (
            34 if c and len(c) > 80 else 20 if c else 14)

    for ci, w in enumerate([30, 24, 80], start=1):
        ws3.column_dimensions[get_column_letter(ci)].width = w

    wb.save(out_path)


# ═══════════════════════════════════════════════════════════════════════
#  CONSOLE SUMMARY BLOCK
# ═══════════════════════════════════════════════════════════════════════

def _print_summary(rows: list, out_path: str, elapsed: str):
    total     = len(rows)
    compliant = sum(1 for r in rows if r["compliant"] == "YES")
    non_comp  = total - compliant
    not_found = sum(1 for r in rows
                    if "NOT FOUND" in r["purview_column_name"].upper())
    rate      = f"{compliant/total*100:.1f}%" if total else "N/A"

    print(f"""
{'='*70}
  COMPLIANCE CHECK — COMPLETE
{'='*70}
  Duration          : {elapsed}
  ──────────────────────────────────────────────────────
  COLUMN RESULTS
  Total checked     : {total:>6}
  Compliant   (YES) : {compliant:>6}  {'✓ all good' if non_comp == 0 else ''}
  Non-compliant(NO) : {non_comp:>6}  {'⚠ action needed' if non_comp else ''}
  Not in Purview    : {not_found:>6}  {'⚠ columns missing from snapshot' if not_found else ''}
  Compliance rate   : {rate:>6}
  ──────────────────────────────────────────────────────
  OUTPUT
  Excel report      : {out_path}
{'='*70}
""", flush=True)


# ═══════════════════════════════════════════════════════════════════════
#  DB PUSH  — create schema+table if needed, load rows, overwrite each run
#
#  Called only when COMPLIANCE_DB_LOAD = true in config.
#  All DB credentials and target names are passed in from purview_scan.py
#  — this function never reads config itself.
#
#  TABLE SCHEMA (auto-created if not exists):
#    compliance_schema.compliance_table
#    One row per compliance result row.
#    Existing rows for the current scan_run_id are deleted first so
#    every run is a clean fresh load (no duplicates, no stale rows).
#
#  RETURNS: (rows_loaded: int, error_message: str | None)
# ═══════════════════════════════════════════════════════════════════════

_DDL_SCHEMA = "IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = N'{schema}')\n    EXEC('CREATE SCHEMA [{schema}]');"

_DDL_TABLE = """\
IF OBJECT_ID(N'[{schema}].[{table}]', N'U') IS NULL
BEGIN
CREATE TABLE [{schema}].[{table}] (
    row_id                   NVARCHAR(36)    NOT NULL DEFAULT NEWID(),
    scan_run_id              NVARCHAR(36)    NULL,
    loaded_at                DATETIME2       NOT NULL DEFAULT GETUTCDATE(),
    -- FROM YAML
    dmg_name                 NVARCHAR(200)   NULL,
    app_name                 NVARCHAR(500)   NULL,
    yaml_column_name         NVARCHAR(500)   NULL,
    pipeda_classification    NVARCHAR(500)   NULL,
    yaml_purview_cls         NVARCHAR(500)   NULL,
    yaml_sensitivity_label   NVARCHAR(500)   NULL,
    yaml_status              NVARCHAR(100)   NULL,
    yaml_version             NVARCHAR(100)   NULL,
    yaml_effective_date      NVARCHAR(100)   NULL,
    -- FROM PURVIEW SNAPSHOT
    purview_column_name      NVARCHAR(500)   NULL,
    purview_classification   NVARCHAR(2000)  NULL,
    purview_sensitivity      NVARCHAR(2000)  NULL,
    asset_qualified_name     NVARCHAR(MAX)   NULL,
    collection_path          NVARCHAR(2000)  NULL,
    -- COMPLIANCE RESULT
    compliant                NVARCHAR(3)     NULL,   -- YES | NO
    reason                   NVARCHAR(MAX)   NULL,
    CONSTRAINT PK_{table} PRIMARY KEY (row_id)
);
PRINT '{table} created.';
END
ELSE
    PRINT '{table} already exists.';"""


def push_to_db(rows: list,
               scan_run_id: str,
               db_server:   str,
               db_database: str,
               db_user:     str,
               db_password: str,
               db_schema:   str,
               db_table:    str) -> tuple:
    """
    Push compliance result rows to Azure SQL.

    Steps:
      1. Create schema if it does not exist.
      2. Create table if it does not exist (DDL above).
      3. DELETE rows WHERE scan_run_id = current run (idempotent re-run).
      4. INSERT all rows for this run.

    Returns (rows_loaded: int, error: str | None).
    error is None on success.
    """
    if not _HAS_PYODBC:
        return 0, ("pyodbc not installed — run:  pip install pyodbc  "
                   "(and install the ODBC Driver 17/18 for SQL Server)")
    if not rows:
        return 0, None

    # ── Find the best available ODBC driver ───────────────────────────
    # Try drivers in preference order: 18 (latest) → 17 → fail with
    # a clear install message instead of the cryptic IM002 error.
    _ODBC_DRIVERS_PREF = [
        "ODBC Driver 18 for SQL Server",
        "ODBC Driver 17 for SQL Server",
    ]
    odbc_driver = None
    try:
        available = _pyodbc.drivers()
        for d in _ODBC_DRIVERS_PREF:
            if d in available:
                odbc_driver = d
                break
    except Exception:
        pass   # pyodbc.drivers() may not be available on all platforms

    if odbc_driver is None:
        # Last-ditch: try each driver directly and accept the first that connects
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

    if odbc_driver is None:
        return 0, (
            "No compatible ODBC driver found.\n"
            "  Install one of:\n"
            "    • ODBC Driver 18 for SQL Server  (recommended)\n"
            "    • ODBC Driver 17 for SQL Server\n"
            "  Download: https://learn.microsoft.com/sql/connect/odbc/download-odbc-driver-for-sql-server\n"
            "  Then re-run."
        )

    _log(f"  Using ODBC driver: {odbc_driver}", "INFO")

    conn_str = (
        f"DRIVER={{{odbc_driver}}};"
        f"SERVER={db_server};"
        f"DATABASE={db_database};"
        f"UID={db_user};"
        f"PWD={db_password};"
        f"Encrypt=yes;TrustServerCertificate=no;"
        f"Connection Timeout=30;"
    )

    _log(f"  Connecting to DB: {db_server}/{db_database}")
    try:
        conn = _pyodbc.connect(conn_str, autocommit=False)
        cur  = conn.cursor()
    except Exception as e:
        return 0, f"Connection failed: {e}"

    try:
        # 1. Create schema
        cur.execute(_DDL_SCHEMA.format(schema=db_schema))
        cur.execute("GO" if False else "SELECT 1")   # no-op; GO is batch separator not SQL

        # Execute schema creation via exec since pyodbc can't run GO statements
        cur.execute(f"""
            IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = N'{db_schema}')
                EXEC('CREATE SCHEMA [{db_schema}]')
        """)

        # 2. Create table
        ddl = _DDL_TABLE.format(schema=db_schema, table=db_table)
        # Execute statement by statement (split on GO)
        for stmt in ddl.split("\nGO\n"):
            stmt = stmt.strip()
            if stmt:
                cur.execute(stmt)
        _log(f"  Schema [{db_schema}] and table [{db_table}] ready", "OK")

        # 3. Delete this run's rows (idempotent — safe to re-run)
        cur.execute(
            f"DELETE FROM [{db_schema}].[{db_table}] WHERE scan_run_id = ?",
            scan_run_id)
        deleted = cur.rowcount
        if deleted:
            _log(f"  Deleted {deleted} existing rows for run_id={scan_run_id}")

        # 4. Insert all rows
        insert_sql = f"""
            INSERT INTO [{db_schema}].[{db_table}] (
                scan_run_id, dmg_name, app_name,
                yaml_column_name, pipeda_classification, yaml_purview_cls,
                yaml_sensitivity_label, yaml_status, yaml_version,
                yaml_effective_date, purview_column_name,
                purview_classification, purview_sensitivity,
                asset_qualified_name, collection_path,
                compliant, reason
            ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
        """
        params = [
            (
                scan_run_id,
                row["dmg_name"],
                row["app_name"],
                row["yaml_column_name"],
                row["pipeda_classification"],
                row["yaml_purview_cls"],
                row["yaml_sensitivity_label"],
                row["yaml_status"],
                row["yaml_version"],
                row["yaml_effective_date"],
                row["purview_column_name"],
                row["purview_classification"],
                row["purview_sensitivity"],
                row["asset_qualified_name"],
                row["collection_path"],
                row["compliant"],
                row["reason"],
            )
            for row in rows
        ]
        cur.executemany(insert_sql, params)
        conn.commit()

        loaded = len(params)
        _log(f"  DB push complete: {loaded} rows → "
             f"[{db_schema}].[{db_table}]", "OK")
        return loaded, None

    except Exception as e:
        try: conn.rollback()
        except Exception: pass
        return 0, str(e)
    finally:
        try: cur.close()
        except Exception: pass
        try: conn.close()
        except Exception: pass


# ═══════════════════════════════════════════════════════════════════════
#  PUBLIC ENTRY POINT  (called from purview_scan.py)
# ═══════════════════════════════════════════════════════════════════════

def run_compliance_check(snapshot_path: str,
                         yaml_source:   str,
                         dmg_filter:    list = None,
                         out_path:      str  = "compliance_report.xlsx",
                         # DB push params (all passed from purview_scan.py)
                         db_load:       bool = False,
                         db_schema:     str  = "",
                         db_table:      str  = "",
                         db_server:     str  = "",
                         db_database:   str  = "",
                         db_user:       str  = "",
                         db_password:   str  = "") -> dict:
    """
    Main entry point for integration with purview_scan.py.

    Parameters
    ----------
    snapshot_path : str       Path to purview_snapshot.json
    yaml_source   : str       URL or local path to YAML file(s)
    dmg_filter    : list      DMG IDs to check. Empty/None = all.
    out_path      : str       Excel report path (written next to script,
                              overwritten on every run)
    db_load       : bool      True = push rows to Azure SQL after Excel
    db_schema     : str       Target schema name (mandatory if db_load=True)
    db_table      : str       Target table name  (mandatory if db_load=True)
    db_server     : str       Azure SQL server hostname
    db_database   : str       Database name
    db_user       : str       SQL login username
    db_password   : str       SQL login password

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
    yaml_sources = load_yaml_sources(yaml_source)
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
        _log(f"  Target : [{db_schema}].[{db_table}]  on  {db_server}/{db_database}")
        _log(f"  Rows   : {len(rows)}")
        _log(f"  Run ID : {scan_run_id}  (used to delete+reload on re-run)")
        db_rows_done, db_error_msg = push_to_db(
            rows        = rows,
            scan_run_id = scan_run_id,
            db_server   = db_server,
            db_database = db_database,
            db_user     = db_user,
            db_password = db_password,
            db_schema   = db_schema,
            db_table    = db_table,
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
    not_found = sum(1 for r in rows
                    if "NOT FOUND" in r["purview_column_name"].upper())

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
        description="PIPEDA compliance check: YAML vs Purview snapshot")
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