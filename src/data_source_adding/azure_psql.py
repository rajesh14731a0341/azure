import sys
from azure.purview.scanning import PurviewScanningClient
from azure.identity import ClientSecretCredential
from azure.core.exceptions import HttpResponseError
from azure.purview.administration.account import PurviewAccountClient

# ===============================
# AUTH CONFIG
# ===============================
CLIENT_ID = "YOUR_CLIENT_ID"
CLIENT_SECRET = "YOUR_CLIENT_SECRET"
TENANT_ID = "YOUR_TENANT_ID"

PURVIEW_ACCOUNT = "Finastrapurview"
COLLECTION_PATH = "POC_Finastra/UK Region/Lending-POS"

# ===============================
# AZURE POSTGRES CONFIG (PaaS)
# ===============================
DS_NAME = "finastra-azure-postgres"

POSTGRES_RESOURCE_ID = (
    "/subscriptions/<sub-id>"
    "/resourceGroups/<rg-name>"
    "/providers/Microsoft.DBforPostgreSQL/flexibleServers/<server-name>"
)

POSTGRES_ENDPOINT = "<server-name>.postgres.database.azure.com"

# ===============================
# ENDPOINTS
# ===============================
PURVIEW_ENDPOINT = f"https://{PURVIEW_ACCOUNT}.purview.azure.com"
SCAN_ENDPOINT = f"https://{PURVIEW_ACCOUNT}.scan.purview.azure.com"


def get_credentials():
    return ClientSecretCredential(
        tenant_id=TENANT_ID,
        client_id=CLIENT_ID,
        client_secret=CLIENT_SECRET
    )


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
            walk(child["name"], f"{friendly_path}/{child['friendlyName']}")

    for root in roots:
        walk(root["name"], root["friendlyName"])

    return mapping


def run_registration():
    try:
        creds = get_credentials()

        admin_client = PurviewAccountClient(PURVIEW_ENDPOINT, creds)
        scanning_client = PurviewScanningClient(SCAN_ENDPOINT, creds)

        print(f"Resolving collection path: {COLLECTION_PATH}")
        collection_map = build_collection_map(admin_client)
        internal_id = collection_map.get(normalize(COLLECTION_PATH))

        if not internal_id:
            print("ERROR: Collection path not found.")
            sys.exit(1)

        print(f"Collection resolved. Internal ID = {internal_id}")

        body_input = {
            "kind": "AzurePostgreSql",
            "properties": {
                "resourceId": POSTGRES_RESOURCE_ID,
                "serverEndpoint": POSTGRES_ENDPOINT,
                "collection": {
                    "type": "CollectionReference",
                    "referenceName": internal_id
                }
            }
        }

        print(f"Registering Azure PostgreSQL: {DS_NAME}")

        scanning_client.data_sources.create_or_update(
            DS_NAME,
            body=body_input
        )

        print("SUCCESS: Azure PostgreSQL registered")

    except HttpResponseError as e:
        print("FAILED to register Azure PostgreSQL")
        print(e)


if __name__ == "__main__":
    run_registration()