import sys
from azure.purview.scanning import PurviewScanningClient
from azure.identity import ClientSecretCredential
from azure.core.exceptions import HttpResponseError
from azure.purview.administration.account import PurviewAccountClient

# ===============================
# AUTH / PURVIEW CONFIG
# ===============================
CLIENT_ID = "YOUR_CLIENT_ID"
CLIENT_SECRET = "YOUR_CLIENT_SECRET"
TENANT_ID = "YOUR_TENANT_ID"

PURVIEW_ACCOUNT = "Finastrapurview"

# 🔒 USE FULL COLLECTION PATH (SAFE)
COLLECTION_PATH = "POC_Finastra/UK Region/Lending-POS"

# ===============================
# ON-PREM MSSQL CONFIG
# ===============================
DS_NAME = "finastra-onprem-mssql"
SQL_HOST = "192.168.1.10"   # On-prem SQL Server IP or hostname
SQL_PORT = 1433             # Default SQL Server port

# ===============================
# ENDPOINTS
# ===============================
PURVIEW_ENDPOINT = f"https://{PURVIEW_ACCOUNT}.purview.azure.com"
SCAN_ENDPOINT = f"https://{PURVIEW_ACCOUNT}.scan.purview.azure.com"


# ===============================
# AUTH
# ===============================
def get_credentials():
    return ClientSecretCredential(
        tenant_id=TENANT_ID,
        client_id=CLIENT_ID,
        client_secret=CLIENT_SECRET
    )


# ===============================
# HELPERS (SAFE COLLECTION RESOLUTION)
# ===============================
def normalize(x):
    return x.strip().lower()


def build_collection_map(admin_client):
    mapping = {}
    visited = set()

    all_collections = list(admin_client.collections.list_collections())

    child_names = set()
    for col in all_collections:
        children = admin_client.collections.list_child_collection_names(col["name"])
        for child in children:
            child_names.add(child["name"])

    roots = [c for c in all_collections if c["name"] not in child_names]

    def walk(internal_name, friendly_path):
        if internal_name in visited:
            return

        visited.add(internal_name)
        mapping[normalize(friendly_path)] = internal_name

        children = admin_client.collections.list_child_collection_names(internal_name)
        for child in children:
            walk(
                child["name"],
                f"{friendly_path}/{child['friendlyName']}"
            )

    for root in roots:
        walk(root["name"], root["friendlyName"])

    return mapping


# ===============================
# MAIN
# ===============================
def run_registration():
    try:
        creds = get_credentials()

        admin_client = PurviewAccountClient(
            endpoint=PURVIEW_ENDPOINT,
            credential=creds
        )

        scanning_client = PurviewScanningClient(
            endpoint=SCAN_ENDPOINT,
            credential=creds
        )

        # --------------------------------------------------
        # STEP 1: SAFE COLLECTION RESOLUTION
        # --------------------------------------------------
        print(f"Resolving collection path: {COLLECTION_PATH}")

        collection_map = build_collection_map(admin_client)
        internal_id = collection_map.get(normalize(COLLECTION_PATH))

        if not internal_id:
            print(f"ERROR: Collection path '{COLLECTION_PATH}' not found.")
            sys.exit(1)

        print(f"Collection resolved safely. Internal ID = {internal_id}")

        # --------------------------------------------------
        # STEP 2: Define On-Prem MSSQL Data Source
        # --------------------------------------------------
        body_input = {
            "kind": "SqlServer",
            "properties": {
                "host": SQL_HOST,
                "port": SQL_PORT,
                "collection": {
                    "type": "CollectionReference",
                    "referenceName": internal_id
                }
            }
        }

        # --------------------------------------------------
        # STEP 3: Register Data Source
        # --------------------------------------------------
        print(f"Registering On-Prem MSSQL: {DS_NAME}")

        scanning_client.data_sources.create_or_update(
            DS_NAME,
            body=body_input
        )

        print(f"SUCCESS: Data source '{DS_NAME}' registered")

    except HttpResponseError as e:
        print("FAILED to register MSSQL")
        print("Ensure Service Principal has:")
        print("- Purview Data Source Admin (collection scope)")
        print("- Purview Account access")
        print(f"Error details: {e}")


if __name__ == "__main__":
    run_registration()