
import os
import sys
import time
import requests
import configparser
import pandas as pd
from urllib.parse import urlparse
from azure.identity import ClientSecretCredential
from collections import defaultdict

# ===============================
# CONFIGURATION — credentials
# ===============================
CLIENT_ID     = "c636fbbb-132d-4be2-9a2d-9f1352cd0e58"
CLIENT_SECRET   = os.environ.get("PURVIEW_CLIENT_SECRET", "")
TENANT_ID     = "5f9bacc0-ffe8-41f7-8d25-215d55cb0f96"


PURVIEW_ACCOUNT   = "finastrapurview"
CATALOG_ENDPOINT  = f"https://{PURVIEW_ACCOUNT}.purview.azure.com"
API_VERSION       = "2023-09-01"
COLLECTIONS_API   = f"https://{PURVIEW_ACCOUNT}.purview.azure.com/account/collections?api-version=2019-11-01-preview"
DATASOURCES_API   = f"https://{PURVIEW_ACCOUNT}.purview.azure.com/scan/datasources?api-version=2022-07-01-preview"
SEARCH_API        = f"{CATALOG_ENDPOINT}/datamap/api/search/query?api-version={API_VERSION}"

# ===============================
# LOAD CONFIG FROM asset_config.ini
# ===============================
_cfg = configparser.ConfigParser()
_cfg.read(os.path.join(os.path.dirname(__file__), "asset_config.ini"))

if not _cfg.has_section("FILES"):
    print("  [ERROR] asset_config.ini not found or missing [FILES] section.")
    sys.exit(1)

ASSETS_EXCEL = _cfg["FILES"]["ASSETS_EXCEL"].strip()
PARAMS_EXCEL = _cfg["FILES"]["PARAMS_EXCEL"].strip()

_col = _cfg["FILTERS"]["FILTER_COLLECTIONS"].strip()
_ds  = _cfg["FILTERS"]["FILTER_DATA_SOURCES"].strip()

FILTER_COLLECTIONS  = None if _col == "None" else [x.strip() for x in _col.split(",") if x.strip()]
FILTER_DATA_SOURCES = None if _ds  == "None" else [x.strip() for x in _ds.split(",")  if x.strip()]

SCHEME_LABELS = {
    "oracle":     "Oracle Server",
    "mssql":      "SQL Server / Azure SQL",
    "postgresql": "PostgreSQL",
    "mysql":      "MySQL",
    "db2":        "DB2",
    "snowflake":  "Snowflake",
    "databricks": "Databricks",
    "teradata":   "Teradata",
    "https":      "Azure Storage / Cosmos / Web",
    "http":       "HTTP Storage",
    "hdfs":       "HDFS",
    "hive":       "Hive",
    "s3":         "Amazon S3",
    "adl":        "Azure Data Lake",
}

# ===============================
# AUTH
# ===============================
_token_value   = None
_token_fetched = 0.0

def get_token():
    credential = ClientSecretCredential(
        tenant_id=TENANT_ID,
        client_id=CLIENT_ID,
        client_secret=CLIENT_SECRET
    )
    return credential.get_token("https://purview.azure.net/.default").token

def get_headers():
    global _token_value, _token_fetched
    if _token_value is None or (time.time() - _token_fetched) / 60 >= 50:
        print("  [AUTH] Fetching token...")
        _token_value   = get_token()
        _token_fetched = time.time()
    return {"Authorization": f"Bearer {_token_value}", "Content-Type": "application/json"}

# ===============================
# COLLECTION FILTER
# ===============================
def fetch_all_collections():
    r = requests.get(COLLECTIONS_API, headers=get_headers(), timeout=30)
    if r.status_code != 200:
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

# ===============================
# DATA SOURCE FILTER
# ===============================
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
    r = requests.get(DATASOURCES_API, headers=get_headers(), timeout=30)
    if r.status_code != 200:
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

# ===============================
# PAGINATED SEARCH
# ===============================
def fetch_all_assets():
    print("\n  ACTIVE FILTERS")
    print(f"  Collections  : {FILTER_COLLECTIONS  if FILTER_COLLECTIONS  else 'ALL'}")
    print(f"  Data sources : {FILTER_DATA_SOURCES if FILTER_DATA_SOURCES else 'ALL'}")

    collection_ids = []
    if FILTER_COLLECTIONS:
        print(f"\n  Resolving collection paths: {FILTER_COLLECTIONS}")
        collection_ids = resolve_collection_ids(FILTER_COLLECTIONS)

    all_assets   = []
    continuation = None
    page         = 1

    while True:
        body = {"keywords": "*", "limit": 1000}
        if collection_ids:
            body["filter"] = ({"collectionId": collection_ids[0]} if len(collection_ids) == 1
                              else {"or": [{"collectionId": cid} for cid in collection_ids]})
        if continuation:
            body["continuationToken"] = continuation

        r = requests.post(SEARCH_API, headers=get_headers(), json=body, timeout=60)
        if r.status_code != 200:
            break

        data         = r.json()
        assets       = data.get("value", [])
        all_assets.extend(assets)
        print(f"  Page {page:>3}: {len(assets):>5} assets  |  Running total: {len(all_assets)}")

        continuation = data.get("continuationToken")
        if not continuation or not assets:
            break
        page += 1

    print(f"\n  Total assets fetched: {len(all_assets)}")

    if FILTER_DATA_SOURCES:
        resolve_datasource_endpoints(all_assets)
        before     = len(all_assets)
        all_assets = [a for a in all_assets if asset_matches_datasource(a)]
        print(f"  Data source filter: {before} -> {len(all_assets)} assets kept")

    return all_assets

# ===============================
# GROUP BY DATA SOURCE
# ===============================
def group_by_source(assets):
    groups = {}
    for asset in assets:
        qn = asset.get("qualifiedName", "")
        if not qn:
            continue
        parsed = urlparse(qn)
        scheme = parsed.scheme.lower()
        netloc = parsed.netloc or ""
        if not scheme:
            continue
        source_key = f"{scheme}://{netloc}" if netloc else scheme
        label      = SCHEME_LABELS.get(scheme, scheme.title())
        groups.setdefault(source_key, {"label": label, "assets": []})
        groups[source_key]["assets"].append(asset)
    return groups

# ===============================
# SCAN
# No arguments — fetches all assets from Purview and writes
# purview_assets.xlsx with an empty 'delete' column.
# ===============================
def scan_assets():
    print("\n" + "="*60)
    print("  SCAN — Fetching assets from Purview")
    print("="*60)

    assets = fetch_all_assets()
    groups = group_by_source(assets)

    if not groups:
        print("  No data sources found.")
        return

    COLS = ["DataSource", "SourceType", "EntityType", "QualifiedName",
            "AssetName", "AssetGUID", "delete", "Status"]

    rows_by_source = {}
    for source_key, info in sorted(groups.items()):
        rows = []
        for asset in info["assets"]:
            rows.append({
                "DataSource":    source_key,
                "SourceType":    info["label"],
                "EntityType":    asset.get("entityType", ""),
                "QualifiedName": asset.get("qualifiedName", ""),
                "AssetName":     asset.get("name", ""),
                "AssetGUID":     asset.get("id", ""),
                "delete":        "",
                "Status":        ""
            })
        rows_by_source[source_key] = rows

    with pd.ExcelWriter(ASSETS_EXCEL, engine="openpyxl") as writer:
        for source_key, rows in rows_by_source.items():
            df         = pd.DataFrame(rows, columns=COLS)
            sheet_name = source_key.replace("://", "_").replace(".", "_").replace("/", "_")[:31]
            df.to_excel(writer, sheet_name=sheet_name, index=False)
            print(f"  [{sheet_name}]  {len(df)} assets")

    total = sum(len(v) for v in rows_by_source.values())
    print(f"\n{'='*60}")
    print(f"  Scan complete -> {ASSETS_EXCEL}  ({total} assets)")
    print("="*60)


# ===============================
# DELETE
# Receives the Excel file path as argument.
# Deletes every asset where delete=yes, updates Status,
# then clears the entire 'delete' column before saving.
# ===============================
def delete_assets(assets_file):
    print("\n" + "="*60)
    print("  DELETE — Processing marked assets")
    print(f"  File: {assets_file}")
    print("="*60)

    token   = get_token()
    headers = {"Authorization": f"Bearer {token}"}

    all_sheets  = pd.ExcelFile(assets_file).sheet_names
    updated_dfs = {}
    to_delete   = []

    for sheet in all_sheets:
        df = pd.read_excel(assets_file, sheet_name=sheet,
                           dtype={"delete": str, "AssetGUID": str, "Status": str})
        df.columns = [c.strip().lower() for c in df.columns]

        if "assetguid" not in df.columns or "delete" not in df.columns:
            print(f"  [WARN] Sheet '{sheet}' missing AssetGUID or delete column — skipped")
            updated_dfs[sheet] = df
            continue

        for idx, row in df.iterrows():
            guid   = str(row.get("assetguid",     "")).strip()
            delete = str(row.get("delete",         "")).strip().lower()
            name   = str(row.get("assetname",      "")).strip()
            qn     = str(row.get("qualifiedname",  "")).strip()

            if delete != "yes":
                df.at[idx, "status"] = "Skipped"
                continue

            if not guid or guid.lower() in ("nan", "none", ""):
                print(f"  [SKIP] {name} — no GUID")
                df.at[idx, "status"] = "No GUID"
                continue

            to_delete.append({"guid": guid, "name": name, "qualifiedName": qn,
                               "sheet": sheet, "idx": idx})

        updated_dfs[sheet] = df

    if not to_delete:
        print("  No assets marked for deletion.")
        _save_excel(assets_file, updated_dfs, all_sheets, clear_delete=False)
        return

    print(f"\n  Assets to delete: {len(to_delete)}")
    for a in to_delete:
        print(f"    {a['qualifiedName']}")

    guids      = [a["guid"] for a in to_delete]
    guid_index = {a["guid"]: a for a in to_delete}
    batch_size = 20
    deleted    = 0
    failed     = 0

    for i in range(0, len(guids), batch_size):
        chunk = guids[i:i + batch_size]
        query = "&".join([f"guid={g}" for g in chunk])
        url   = (f"{CATALOG_ENDPOINT}/datamap/api/atlas/v2/entity/bulk?"
                 f"{query}&api-version={API_VERSION}")

        resp = requests.delete(url, headers=headers)

        if resp.status_code == 200:
            print(f"  Batch {i // batch_size + 1} — OK  ({len(chunk)} assets)")
            for g in chunk:
                a = guid_index[g]
                updated_dfs[a["sheet"]].at[a["idx"], "status"] = "Deleted"
            deleted += len(chunk)
        else:
            print(f"  Batch {i // batch_size + 1} — FAILED: {resp.text}")
            for g in chunk:
                a = guid_index[g]
                updated_dfs[a["sheet"]].at[a["idx"], "status"] = "Failed"
            failed += len(chunk)

    # Clear delete column so a re-run without new commits is harmless
    _save_excel(assets_file, updated_dfs, all_sheets, clear_delete=True)

    print(f"\n{'='*60}")
    print(f"  Deleted : {deleted}")
    print(f"  Failed  : {failed}")
    print(f"  'delete' column cleared in {assets_file}")
    print("="*60)


def _save_excel(assets_file, updated_dfs, all_sheets, clear_delete=False):
    col_rename = {
        "datasource":    "DataSource",
        "sourcetype":    "SourceType",
        "entitytype":    "EntityType",
        "qualifiedname": "QualifiedName",
        "assetname":     "AssetName",
        "assetguid":     "AssetGUID",
        "delete":        "delete",
        "status":        "Status",
    }
    with pd.ExcelWriter(assets_file, engine="openpyxl") as writer:
        for sheet in all_sheets:
            if sheet not in updated_dfs:
                continue
            df = updated_dfs[sheet].copy()
            if clear_delete and "delete" in df.columns:
                df["delete"] = ""          # wipe all flags — safe re-run
            df.columns = [col_rename.get(c.lower(), c) for c in df.columns]
            df.to_excel(writer, sheet_name=sheet, index=False)


# ===============================
# MAIN
#
#   python purview_asset_deleter.py
#       Scan Purview -> write purview_assets.xlsx (delete column empty)
#
#   python purview_asset_deleter.py purview_assets.xlsx
#       Delete rows where delete=yes -> clear delete column -> update Status
# ===============================
if __name__ == "__main__":
    args = sys.argv[1:]

    print("="*60)
    print("  Purview Asset Deleter")
    print(f"  Account      : {PURVIEW_ACCOUNT}")
    print(f"  Scan output  : {ASSETS_EXCEL}")
    print(f"  Delete input : {PARAMS_EXCEL}")
    print("="*60)

    if not CLIENT_SECRET:
        print("  [ERROR] PURVIEW_CLIENT_SECRET environment variable not set.")
        sys.exit(1)

    if len(args) == 1:
        passed = args[0]

        # Only asset_params.xlsx (PARAMS_EXCEL) is accepted for delete.
        # Passing purview_assets.xlsx or any other file is rejected —
        # purview_assets.xlsx is scan output only and must never be
        # used as a delete input directly.
        if os.path.basename(passed) != os.path.basename(PARAMS_EXCEL):
            print(f"  [ERROR] Delete mode only accepts '{PARAMS_EXCEL}' as input.")
            print(f"          Received : '{passed}'")
            print(f"          Download purview_assets.xlsx from Artifacts, fill the")
            print(f"          delete column with 'yes', save as '{PARAMS_EXCEL}',")
            print(f"          then commit and re-run.")
            sys.exit(1)

        if not os.path.exists(passed):
            print(f"  [ERROR] File not found: {passed}")
            sys.exit(1)

        delete_assets(passed)
    else:
        scan_assets()