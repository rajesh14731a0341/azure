import requests
import sys
from datetime import datetime
from azure.identity import ClientSecretCredential

# ===============================
# CONFIGURATION
# ===============================
CLIENT_ID = "2c98dd46-5ec9-4198-b265-058e18078125"
CLIENT_SECRET = "Ijh8Q~s8DdTg~PNkeiFR.Al~rjAPMBb5eovbqcbY"
TENANT_ID = "5f9bacc0-ffe8-41f7-8d25-215d55cb0f96"

PURVIEW_ACCOUNT = "finastrapurview"
CATALOG_ENDPOINT = f"https://{PURVIEW_ACCOUNT}.purview.azure.com"
API_VERSION = "2023-09-01"
OUTPUT_FILE = "catalog_cleanup_preview.txt"

# ===============================
# AUTH
# ===============================
def get_token():
    credential = ClientSecretCredential(
        tenant_id=TENANT_ID,
        client_id=CLIENT_ID,
        client_secret=CLIENT_SECRET
    )
    return credential.get_token("https://purview.azure.net/.default").token

# ===============================
# STEP 1 – LIST ALL DATA SOURCES (EXPANDED FOR ALL DB TYPES)
# ===============================
def list_all_sources(token):
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json"
    }

    print("\nScanning catalog for all database and storage sources...\n")

    # This body uses a wildcard search to find ANY entity that acts as a root/source
    body = {
        "keywords": "*",
        "limit": 1000,
        "filter": {
            "or": [
                # Azure SQL & MSSQL
                { "entityType": "azure_sql_server" },
                { "entityType": "azure_sql_db" },
                { "entityType": "mssql_instance" },
                { "entityType": "mssql_db" },
                { "entityType": "azure_sql_data_warehouse" },
                # Oracle
                { "entityType": "oracle_instance" },
                { "entityType": "oracle_server" },
                { "entityType": "oracle_db" },
                # PostgreSQL
                { "entityType": "postgresql_instance" },
                { "entityType": "postgresql_db" },
                { "entityType": "postgresql_server" },
                # Storage
                { "entityType": "azure_storage_account" },
                { "entityType": "azure_blob_service" }
            ]
        }
    }

    # Search Query endpoint is more powerful for discovering cross-type sources
    search_url = f"{CATALOG_ENDPOINT}/datamap/api/search/query?api-version={API_VERSION}"
    response = requests.post(search_url, headers=headers, json=body)
    response.raise_for_status()

    results = response.json().get("value", [])
    sources = []

    for r in results:
        # Avoid listing columns or low-level assets as "Sources"
        sources.append({
            "guid": r.get("id"),
            "name": r.get("name"),
            "type": r.get("entityType"),
            "qualifiedName": r.get("qualifiedName")
        })

    return sources

# ===============================
# STEP 2 – TRAVERSE FULL HIERARCHY
# ===============================
def collect_entities(token, root_guid):
    headers = {"Authorization": f"Bearer {token}"}
    to_visit = [root_guid]
    visited = set()
    collected = []

    while to_visit:
        guid = to_visit.pop()
        if guid in visited: continue
        visited.add(guid)

        # minExtInfo=false ensures we get the referredEntities (children)
        url = f"{CATALOG_ENDPOINT}/datamap/api/atlas/v2/entity/guid/{guid}?minExtInfo=false&api-version={API_VERSION}"
        response = requests.get(url, headers=headers)
        if response.status_code != 200: continue

        data = response.json()
        entity = data.get("entity", {})
        referred = data.get("referredEntities", {})

        collected.append(entity)
        for ref_guid in referred.keys():
            if ref_guid not in visited:
                to_visit.append(ref_guid)
    return collected

# ===============================
# STEP 3 – PREVIEW + OVERWRITE FILE
# ===============================
def preview_and_save(entities, root):
    with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
        f.write(f"ROOT_GUID={root['guid']}\n")
        f.write("=" * 80 + "\n")

        print("\nALL ENTITIES UNDER ROOT")
        print("=" * 80)
        for e in entities:
            guid = e.get("guid")
            name = e.get("attributes", {}).get("name")
            type_name = e.get("typeName")
            qn = e.get("attributes", {}).get("qualifiedName")
            line = f"GUID={guid} | Name={name} | Type={type_name} | QualifiedName={qn}"
            print(line)
            f.write(line + "\n")

    print(f"\nPreview saved (overwritten) to {OUTPUT_FILE}")

# ===============================
# STEP 4 – OFFICIAL BULK DELETE
# ===============================
def bulk_delete(token):
    headers = {"Authorization": f"Bearer {token}"}
    with open(OUTPUT_FILE, "r", encoding="utf-8") as f:
        lines = f.readlines()

    root_guid = None
    guids = []
    for line in lines:
        if line.startswith("ROOT_GUID="):
            root_guid = line.strip().split("=")[1]
        if line.startswith("GUID="):
            guid = line.split("|")[0].replace("GUID=", "").strip()
            if guid != root_guid:
                guids.append(guid)

    if not guids:
        print("Nothing to delete.")
        return

    print(f"\nDeleting {len(guids)} entities in batches...")
    batch_size = 20
    for i in range(0, len(guids), batch_size):
        chunk = guids[i:i+batch_size]
        query_string = "&".join([f"guid={g}" for g in chunk])
        
        # Format required for bulk delete
        delete_url = f"{CATALOG_ENDPOINT}/datamap/api/atlas/v2/entity/bulk?{query_string}&api-version={API_VERSION}"
        
        response = requests.delete(delete_url, headers=headers)
        if response.status_code == 200:
            print(f"Batch {i//batch_size + 1} deleted successfully.")
        else:
            print(f"Batch failed: {response.text}")

if __name__ == "__main__":
    try:
        print("Authenticating...")
        token = get_token()
        sources = list_all_sources(token)

        if not sources:
            print("No data sources found.")
            exit()

        print("\nAVAILABLE DATA SOURCES")
        print("=" * 80)
        for idx, s in enumerate(sources):
            print(f"{idx + 1}. {s['name']} ({s['type']})")
            print(f"   QualifiedName: {s['qualifiedName']}\n")

        choice = int(input("Select number to inspect/delete: ")) - 1
        root = sources[choice]

        print("\nCollecting hierarchy...")
        entities = collect_entities(token, root["guid"])
        preview_and_save(entities, root)

        confirm = input("\nType YES to delete all child entities: ")
        if confirm.strip().upper() == "YES":
            bulk_delete(token)
        else:
            print("Deletion cancelled.")

    except Exception as e:
        print("Error:", e)