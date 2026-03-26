# blob_sql_to_blob_csv.py
#
# Reads SQL DDL files from Azure Blob Storage
# Parses CREATE TABLE statements
# Saves one CSV per table following hierarchy:
#
#   Output/{Database}/{Schema}/{Table}.csv
#
# Auth    : Azure AD Service Principal
# Config  : blob_config.ini — only blob folder/container settings
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
CLIENT_SECRET = "Jg18Q~OgLpY3EtHXU2~qQd4do2RQ~jbxlUfApalR"
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

CONTAINER_NAME      = _cfg["BLOB"]["CONTAINER_NAME"].strip()
INPUT_BLOB_PREFIX   = _cfg["BLOB"]["INPUT_BLOB_PREFIX"].strip().rstrip("/")   # strip any trailing slash — added back below
INPUT_ARCHIVE_BASE  = _cfg["BLOB"]["INPUT_ARCHIVE_BASE"].strip()
BASE_BLOB_OUTPUT    = _cfg["BLOB"]["OUTPUT_BLOB_BASE"].strip()
OUTPUT_ARCHIVE_BASE = _cfg["BLOB"]["OUTPUT_ARCHIVE_BASE"].strip()

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
container_client    = blob_service_client.get_container_client(CONTAINER_NAME)

print(f"  [AUTH] Connected to: {ACCOUNT_NAME} / {CONTAINER_NAME}")

# ------------------------------------------------
# HELPER: ENSURE FOLDER EXISTS
# ------------------------------------------------

def ensure_folder_exists(prefix):
    blobs      = list(container_client.list_blobs(name_starts_with=prefix))
    real_blobs = [b for b in blobs if not b.name.endswith(".keep")]
    if real_blobs:
        return
    placeholder = f"{prefix}/.keep"
    try:
        container_client.get_blob_client(placeholder).upload_blob(b"", overwrite=True)
        print(f"  [FOLDER] Created: {prefix}/")
    except Exception as e:
        print(f"  [WARN] Could not create folder '{prefix}': {e}")


def ensure_all_folders():
    print("\n  Ensuring all required folders exist...")
    for folder in [INPUT_BLOB_PREFIX, INPUT_ARCHIVE_BASE, BASE_BLOB_OUTPUT, OUTPUT_ARCHIVE_BASE]:
        ensure_folder_exists(folder)
    print("  All folders verified.")

# ------------------------------------------------
# HELPER: ARCHIVE BLOBS
# ------------------------------------------------

def move_blobs_clean(source_prefix, archive_prefix, timestamp):
    print(f"\n  Archiving '{source_prefix}' → '{archive_prefix}/{timestamp}/'")

    blobs = list(container_client.list_blobs(name_starts_with=source_prefix))
    if not blobs:
        print("  No files found to archive.")
        return

    moved_count = 0

    for blob in blobs:
        source_blob_name = blob.name

        if source_blob_name.endswith(".keep"):
            continue
        if source_blob_name.startswith(archive_prefix):
            continue

        relative_check = source_blob_name.replace(source_prefix, "").strip("/")
        first_part     = relative_check.split("/")[0] if relative_check else ""
        if re.match(r"\d{4}-\d{2}-\d{2}_\d{2}-\d{2}-\d{2}", first_part):
            print(f"  Skipping nested archive: {source_blob_name}")
            continue

        if source_blob_name.startswith(source_prefix + "/"):
            relative_path = source_blob_name[len(source_prefix) + 1:]
        else:
            relative_path = source_blob_name[len(source_prefix):]

        target_blob_name   = f"{archive_prefix}/{timestamp}/{relative_path}"
        source_blob_client = container_client.get_blob_client(source_blob_name)
        target_blob_client = container_client.get_blob_client(target_blob_name)

        try:
            target_blob_client.start_copy_from_url(source_blob_client.url)
            source_blob_client.delete_blob()
            print(f"  Moved: {source_blob_name} → {target_blob_name}")
            moved_count += 1
        except Exception as e:
            print(f"  Failed: {source_blob_name}: {e}")

    print(f"  Total moved: {moved_count}")
    ensure_folder_exists(source_prefix)

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
print(f"  Container: {CONTAINER_NAME}")
print(f"  Input    : {INPUT_BLOB_PREFIX}/")
print(f"  Output   : {BASE_BLOB_OUTPUT}")
print(f"{'='*60}")

timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")

# Step 0 — Ensure all folders exist
ensure_all_folders()

# Step 1 — Archive old output
print(f"\n{'='*60}")
print(f"  STEP 1 — Archive old output")
print(f"{'='*60}")
move_blobs_clean(BASE_BLOB_OUTPUT, OUTPUT_ARCHIVE_BASE, timestamp)

# Step 2 — Find SQL files
# FIX: use INPUT_BLOB_PREFIX + "/" so that "Input/" never matches
#      "Input_archive/" — without the slash, list_blobs would return
#      blobs from any folder whose name starts with "Input".
print(f"\n{'='*60}")
print(f"  STEP 2 — Scanning for SQL files")
print(f"{'='*60}")

input_scan_prefix = INPUT_BLOB_PREFIX + "/"   # e.g. "Input/"  not "Input"

sql_blobs = [
    b for b in container_client.list_blobs(name_starts_with=input_scan_prefix)
    if b.name.lower().endswith(".sql")
]

print(f"  Found {len(sql_blobs)} SQL file(s) in '{input_scan_prefix}'.")

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
        data     = container_client.get_blob_client(blob_name).download_blob().readall()
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

        # FIX: hierarchy is now Output/{Database}/{Schema}/{Table}.csv
        #      sql_filename folder level has been removed.
        out_blob_path = f"{BASE_BLOB_OUTPUT}/{db_name}/{schema_name}/{table_name}.csv"

        try:
            container_client.get_blob_client(out_blob_path).upload_blob(csv_bytes, overwrite=True)
            print(f"  [OK] {out_blob_path}  ({len(columns)} cols)")
            total_uploads += 1
        except Exception as e:
            print(f"  [ERROR] {out_blob_path}: {e}")

# Step 4 — Archive input SQL files
print(f"\n{'='*60}")
print(f"  STEP 4 — Archive input SQL files")
print(f"{'='*60}")
move_blobs_clean(INPUT_BLOB_PREFIX, INPUT_ARCHIVE_BASE, timestamp)

print(f"\n{'='*60}")
print(f"  SUMMARY")
print(f"  Tables found     : {total_tables}")
print(f"  CSVs uploaded    : {total_uploads}")
print(f"  Archive timestamp: {timestamp}")
print(f"  Output folder    : {BASE_BLOB_OUTPUT}/")
print(f"{'='*60}")