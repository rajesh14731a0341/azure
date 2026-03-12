import sys
from azure.purview.scanning import PurviewScanningClient
from azure.identity import ClientSecretCredential 
from azure.core.exceptions import HttpResponseError
from azure.purview.administration.account import PurviewAccountClient

CLIENT_ID = "2c98dd46-5ec9-4198-b265-058e18078125"
CLIENT_SECRET = "Ijh8Q~s8DdTg~PNkeiFR.Al~rjAPMBb5eovbqcbY" 
TENANT_ID = "5f9bacc0-ffe8-41f7-8d25-215d55cb0f96"
PURVIEW_ACCOUNT = "Finastrapurview"
COLLECTION_NAME = "Lending-POS"
STORAGE_NAME = "finastrastorage"
STORAGE_ID = "/subscriptions/430f4e84-391b-4eaf-a24d-9f33c28a8f3d/resourceGroups/Finastra/providers/Microsoft.Storage/storageAccounts/finastrastorage"
RG_NAME = "Finastra"
RG_LOCATION = "East US"
DS_NAME = "finastra-storage-datasource"

# Endpoints
PURVIEW_ENDPOINT = f"https://{PURVIEW_ACCOUNT}.purview.azure.com"
SCAN_ENDPOINT = f"https://{PURVIEW_ACCOUNT}.scan.purview.azure.com"

def get_credentials():
    return ClientSecretCredential(client_id=CLIENT_ID, client_secret=CLIENT_SECRET, tenant_id=TENANT_ID)

def run_registration():
    try:
        creds = get_credentials()
        admin_client = PurviewAccountClient(endpoint=PURVIEW_ENDPOINT, credential=creds)
        scanning_client = PurviewScanningClient(endpoint=SCAN_ENDPOINT, credential=creds)

        # Step 1: Resolve the Friendly Collection Name to the 6-character ID
        print(f"Resolving collection ID for: {COLLECTION_NAME}...")
        collections = admin_client.collections.list_collections()
        internal_id = next((c["name"] for c in collections if c["friendlyName"].lower() == COLLECTION_NAME.lower()), None)

        if not internal_id:
            print(f"Error: Could not find collection '{COLLECTION_NAME}'. Check your permissions.")
            return

        print(f"Success: Internal ID is {internal_id}")

        # Step 2: Define the Data Source Body
        body_input = {
            "kind": "AzureStorage",
            "properties": {
                "endpoint": f"https://{STORAGE_NAME}.blob.core.windows.net/",
                "resourceGroup": RG_NAME,
                "location": RG_LOCATION,
                "resourceName": STORAGE_NAME,
                "resourceId": STORAGE_ID,
                "collection": {
                    "type": "CollectionReference",
                    "referenceName": internal_id
                },
                "dataUseGovernance": "Disabled"
            }
        }

        # Step 3: Register the Source
        print(f"Registering {DS_NAME}...")
        response = scanning_client.data_sources.create_or_update(DS_NAME, body=body_input)
        print(f"Successfully registered data source: {DS_NAME}")

    except HttpResponseError as e:
        print(f"Registration failed. Check if your Service Principal has 'Data Source Admin' role on the collection.")
        print(f"Error details: {e}")

if __name__ == "__main__":
    run_registration()
	
	
	
