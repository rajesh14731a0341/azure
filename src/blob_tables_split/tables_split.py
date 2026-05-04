# blob_sql_to_blob_csv.py
#
# Reads SQL DDL files from Azure Blob Storage
# Parses CREATE TABLE statements
# Saves one CSV per table following hierarchy:
#
#   Output/{Database}/{Schema}/{Table}.csv
#
# Auth    : Azure AD Service Principal
# Config  : blob_config.ini — pipeline container settings
# Secret  : BLOB_CLIENT_SECRET environment variable

import re
import csv
import io
import os
import sys
import configparser
from datetime import datetime

from azure.identity import ClientSecretCredential
from azure.storage.blob import BlobServiceClient

# ------------------------------------------------
# CREDENTIALS — hardcoded (Service Principal)
# ------------------------------------------------

CLIENT_ID     = "c636fbbb-132d-4be2-9a2d-9f1352cd0e58"
CLIENT_SECRET = os.environ.get("PURVIEW_CLIENT_SECRET", "Jg18Q~OgLpY3EtHXU2~qQd4do2RQ~jbxlUfApalR")
TENANT_ID     = "5f9bacc0-ffe8-41f7-8d25-215d55cb0f96"
ACCOUNT_NAME  = "finastrastorage"

# ------------------------------------------------
# LOAD BLOB SETTINGS FROM blob_config.ini
# ------------------------------------------------

config_path = sys.argv[1] if len(sys.argv) > 1 else "blob_config.ini"

_cfg = configparser.ConfigParser()
_cfg.read(config_path)

if not _cfg.has_section("BLOB"):
    print(f"  [ERROR] '{config_path}' not found or missing [BLOB] section.")
    sys.exit(1)

# Read active pipeline and resolve its container names
ACTIVE_PIPELINE = _cfg["BLOB"]["ACTIVE_PIPELINE"].strip().upper()

def get_pipeline_setting(key):
    full_key = f"{ACTIVE_PIPELINE}_{key}"
    if not _cfg.has_option("BLOB", full_key):
        print(f"  [ERROR] Missing config key '{full_key}' for pipeline '{ACTIVE_PIPELINE}'.")
        sys.exit(1)
    return _cfg["BLOB"][full_key].strip()

INPUT_CONTAINER          = get_pipeline_setting("INPUT_CONTAINER")
OUTPUT_CONTAINER         = get_pipeline_setting("OUTPUT_CONTAINER")
INPUT_ARCHIVE_CONTAINER  = get_pipeline_setting("INPUT_ARCHIVE_CONTAINER")
OUTPUT_ARCHIVE_CONTAINER = get_pipeline_setting("OUTPUT_ARCHIVE_CONTAINER")

# Within each container, blobs sit at the root (no sub-folder prefix needed)
# but you can add a prefix constant here if that ever changes.
INPUT_BLOB_PREFIX  = ""   # scan root of input container
BASE_BLOB_OUTPUT   = ""   # write to root of output container

# ------------------------------------------------
# AUTH — Service Principal
# ------------------------------------------------

print("  [AUTH] Authenticating with Service Principal...")

credential = ClientSecretCredential(
    tenant_id     = TENANT_ID,
    client_id     = CLIENT_ID,
    client_secret = CLIENT_SECRET
)

account_url         = f"https://{ACCOUNT_NAME}.blob.core.windows.net"
blob_service_client = BlobServiceClient(account_url=account_url, credential=credential)

def get_container(name):
    """Return a ContainerClient for the given container name."""
    return blob_service_client.get_container_client(name)

input_container_client          = get_container(INPUT_CONTAINER)
output_container_client         = get_container(OUTPUT_CONTAINER)
input_archive_container_client  = get_container(INPUT_ARCHIVE_CONTAINER)
output_archive_container_client = get_container(OUTPUT_ARCHIVE_CONTAINER)

print(f"  [AUTH] Connected to account : {ACCOUNT_NAME}")
print(f"  [PIPELINE] Active           : {ACTIVE_PIPELINE}")
print(f"  [CONTAINERS]")
print(f"    Input          : {INPUT_CONTAINER}")
print(f"    Output         : {OUTPUT_CONTAINER}")
print(f"    Input archive  : {INPUT_ARCHIVE_CONTAINER}")
print(f"    Output archive : {OUTPUT_ARCHIVE_CONTAINER}")

# ------------------------------------------------
# HELPER: ENSURE CONTAINER EXISTS (create if missing)
# ------------------------------------------------

def ensure_container_exists(container_client, container_name):
    try:
        container_client.get_container_properties()
    except Exception:
        print(f"  [CONTAINER] Creating missing container: {container_name}")
        try:
            container_client.create_container()
            print(f"  [CONTAINER] Created: {container_name}")
        except Exception as e:
            print(f"  [WARN] Could not create container '{container_name}': {e}")

def ensure_all_containers():
    print("\n  Ensuring all required containers exist...")
    ensure_container_exists(input_container_client,          INPUT_CONTAINER)
    ensure_container_exists(output_container_client,         OUTPUT_CONTAINER)
    ensure_container_exists(input_archive_container_client,  INPUT_ARCHIVE_CONTAINER)
    ensure_container_exists(output_archive_container_client, OUTPUT_ARCHIVE_CONTAINER)
    print("  All containers verified.")

# ------------------------------------------------
# HELPER: ARCHIVE BLOBS (cross-container move)
# ------------------------------------------------

def move_blobs_to_archive(source_client, source_container_name,
                           archive_client, archive_container_name, timestamp):
    """
    Copy every blob from source_client into archive_client under a
    timestamped virtual folder, then delete the source blob.
    """
    print(f"\n  Archiving '{source_container_name}' → '{archive_container_name}/{timestamp}/'")

    blobs = list(source_client.list_blobs())
    if not blobs:
        print("  No files found to archive.")
        return

    moved_count = 0

    for blob in blobs:
        source_blob_name = blob.name
        target_blob_name = f"{timestamp}/{source_blob_name}"

        source_blob_client = source_client.get_blob_client(source_blob_name)
        target_blob_client = archive_client.get_blob_client(target_blob_name)

        try:
            target_blob_client.start_copy_from_url(source_blob_client.url)
            source_blob_client.delete_blob()
            print(f"  Moved: {source_blob_name} → {target_blob_name}")
            moved_count += 1
        except Exception as e:
            print(f"  Failed: {source_blob_name}: {e}")

    print(f"  Total moved: {moved_count}")

# ------------------------------------------------
# REGEX PATTERNS
# ------------------------------------------------

create_table_regex = re.compile(
    r'CREATE\s+TABLE\s+([\w\.]+)\s*\((.*?)\);',
    re.S | re.I
)

column_dtype_regex = re.compile(
    r'(.+?)\s+(varchar|nvarchar|char|nchar|int|bigint|smallint|tinyint|'
    r'decimal|numeric|double|float|real|date|datetime|datetime2|timestamp|'
    r'boolean|bit|text|ntext|uniqueidentifier|money|xml)',
    re.I
)

# ------------------------------------------------
# MAIN
# ------------------------------------------------

print(f"\n{'='*60}")
print(f"  Blob SQL to CSV Processor")
print(f"  Account  : {ACCOUNT_NAME}")
print(f"  Pipeline : {ACTIVE_PIPELINE}")
print(f"{'='*60}")

timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")

# Step 0 — Ensure all containers exist
ensure_all_containers()

# Step 1 — Archive old output
print(f"\n{'='*60}")
print(f"  STEP 1 — Archive old output")
print(f"{'='*60}")
move_blobs_to_archive(
    output_container_client,         OUTPUT_CONTAINER,
    output_archive_container_client, OUTPUT_ARCHIVE_CONTAINER,
    timestamp
)

# Step 2 — Find SQL files
print(f"\n{'='*60}")
print(f"  STEP 2 — Scanning for SQL files in '{INPUT_CONTAINER}'")
print(f"{'='*60}")

sql_blobs = [
    b for b in input_container_client.list_blobs()
    if b.name.lower().endswith(".sql")
]

print(f"  Found {len(sql_blobs)} SQL file(s).")

if len(sql_blobs) == 0:
    print("  No SQL files found. Exiting.")
    raise SystemExit(0)

# Step 3 — Process SQL files
print(f"\n{'='*60}")
print(f"  STEP 3 — Processing SQL files")
print(f"{'='*60}")

total_tables  = 0
total_uploads = 0

for blob_props in sql_blobs:
    blob_name = blob_props.name
    print(f"\n  File: {blob_name}")

    try:
        data     = input_container_client.get_blob_client(blob_name).download_blob().readall()
        sql_text = data.decode("utf-8", errors="ignore")
    except Exception as e:
        print(f"  [ERROR] Failed reading {blob_name}: {e}")
        continue

    tables = create_table_regex.findall(sql_text)
    print(f"  Found {len(tables)} CREATE TABLE block(s).")
    total_tables += len(tables)

    for table_full_name, columns_block in tables:

        parts = table_full_name.split(".")
        if len(parts) != 3:
            print(f"  [SKIP] '{table_full_name}' — expected Database.Schema.Table")
            continue

        db_name, schema_name, table_name = map(str.strip, parts)

        columns = []
        for raw_line in columns_block.split(","):
            line = raw_line.strip()
            if not line:
                continue
            if line.upper().startswith(("PRIMARY", "FOREIGN", "CONSTRAINT", "UNIQUE", "CHECK", "INDEX")):
                continue
            m = column_dtype_regex.match(line)
            if m:
                columns.append(m.group(1).strip("[]`\" \t"))
            else:
                fallback = line.split()[0].strip("[]`\" ,")
                if fallback.upper() not in ("PRIMARY", "FOREIGN", "CONSTRAINT", "UNIQUE", "CHECK", "INDEX"):
                    columns.append(fallback)

        if not columns:
            print(f"  [SKIP] No columns for {table_full_name}")
            continue

        csv_buffer = io.StringIO()
        csv.writer(csv_buffer).writerow(columns)
        csv_bytes = csv_buffer.getvalue().encode("utf-8")

        # Hierarchy: {Database}/{Schema}/{Table}.csv  (root of output container)
        out_blob_path = f"{db_name}/{schema_name}/{table_name}.csv"

        try:
            output_container_client.get_blob_client(out_blob_path).upload_blob(csv_bytes, overwrite=True)
            print(f"  [OK] {out_blob_path}  ({len(columns)} cols)")
            total_uploads += 1
        except Exception as e:
            print(f"  [ERROR] {out_blob_path}: {e}")

# Step 4 — Archive input SQL files
print(f"\n{'='*60}")
print(f"  STEP 4 — Archive input SQL files")
print(f"{'='*60}")
move_blobs_to_archive(
    input_container_client,         INPUT_CONTAINER,
    input_archive_container_client, INPUT_ARCHIVE_CONTAINER,
    timestamp
)

print(f"\n{'='*60}")
print(f"  SUMMARY")
print(f"  Pipeline         : {ACTIVE_PIPELINE}")
print(f"  Tables found     : {total_tables}")
print(f"  CSVs uploaded    : {total_uploads}")
print(f"  Archive timestamp: {timestamp}")
print(f"  Output container : {OUTPUT_CONTAINER}")
print(f"{'='*60}")