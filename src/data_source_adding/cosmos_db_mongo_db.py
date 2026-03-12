import sys
from azure.purview.scanning import PurviewScanningClient
from azure.identity import ClientSecretCredential
from azure.core.exceptions import HttpResponseError
from azure.purview.administration.account import PurviewAccountClient

# ===============================
# AUTH CONFIG
# ===============================
CLIENT_ID = "2c98dd46-5ec9-4198-b265-058e18078125"
CLIENT_SECRET = "Ijh8Q~s8DdTg~PNkeiFR.Al~rjAPMBb5eovbqcbY"
TENANT_ID = "5f9bacc0-ffe8-41f7-8d25-215d55cb0f96"

PURVIEW_ACCOUNT = "Finastrapurview"

# 🔒 Full Collection Path (Safe)
COLLECTION_PATH = "POC_Finastra/UK Region/vendor"

# ===============================
# COSMOS DB (SQL API) CONFIG
# ===============================
DS_NAME = "finastracosmos-mongo_db"

COSMOS_RESOURCE_ID = (
    "/subscriptions/430f4e84-391b-4eaf-a24d-9f33c28a8f3d"
    "/resourceGroups/Finastra"
    "/providers/Microsoft.DocumentDB/databaseAccounts/finastracosmodb"
)

COSMOS_ACCOUNT_URI = "https://finastracosmodb.documents.azure.com:443/"

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
# SAFE COLLECTION RESOLUTION
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

        print(f"Resolving collection path: {COLLECTION_PATH}")

        collection_map = build_collection_map(admin_client)
        internal_id = collection_map.get(normalize(COLLECTION_PATH))

        if not internal_id:
            print("ERROR: Collection path not found.")
            sys.exit(1)

        print(f"Collection resolved. Internal ID = {internal_id}")

        # Cosmos SQL API registration (FIXED)
        body_input = {
            "kind": "AzureCosmosDb",
            "properties": {
                "resourceId": COSMOS_RESOURCE_ID,
                "accountUri": COSMOS_ACCOUNT_URI,
                "collection": {
                    "type": "CollectionReference",
                    "referenceName": internal_id
                }
            }
        }

        print(f"Registering Cosmos DB (SQL API): {DS_NAME}")

        scanning_client.data_sources.create_or_update(
            DS_NAME,
            body=body_input
        )

        print("SUCCESS: Cosmos DB (SQL API) registered")

    except HttpResponseError as e:
        print("FAILED to register Cosmos DB")
        print("Ensure:")
        print("- Purview Data Source Admin role on collection")
        print("- Reader role on Cosmos account")
        print(e)


if __name__ == "__main__":
    run_registration()