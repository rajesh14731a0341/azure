from azure.purview.scanning import PurviewScanningClient
from azure.identity import ClientSecretCredential
from azure.core.exceptions import HttpResponseError

# ===============================
# AUTH CONFIG
# ===============================
CLIENT_ID = "2c98dd46-5ec9-4198-b265-058e18078125"
CLIENT_SECRET = "Ijh8Q~s8DdTg~PNkeiFR.Al~rjAPMBb5eovbqcbY"
TENANT_ID = "5f9bacc0-ffe8-41f7-8d25-215d55cb0f96"

PURVIEW_ACCOUNT = "Finastrapurview"

# ===============================
# DATA SOURCE NAME (IMPORTANT)
# ===============================
# This must EXACTLY match the name used during registration
DS_NAME = "finastracosmos-mongo_db"

# ===============================
# ENDPOINT
# ===============================
SCAN_ENDPOINT = f"https://{PURVIEW_ACCOUNT}.scan.purview.azure.com"


# ===============================
# AUTH FUNCTION
# ===============================
def get_credentials():
    return ClientSecretCredential(
        tenant_id=TENANT_ID,
        client_id=CLIENT_ID,
        client_secret=CLIENT_SECRET
    )


# ===============================
# CREATE SCANNING CLIENT
# ===============================
def get_scanning_client():
    credentials = get_credentials()
    client = PurviewScanningClient(
        endpoint=SCAN_ENDPOINT,
        credential=credentials,
        logging_enable=True
    )
    return client


# ===============================
# MAIN
# ===============================
try:
    client_scanning = get_scanning_client()
    print("Scanning client created successfully.")
except ValueError as e:
    print("Error creating scanning client:")
    print(e)

try:
    print(f"Deleting data source: {DS_NAME}")

    response = client_scanning.data_sources.delete(DS_NAME)

    print(response)
    print(f"SUCCESS: Data source '{DS_NAME}' deleted successfully")

except HttpResponseError as e:
    print("FAILED to delete data source")
    print("Check:")
    print("- Data source name is correct")
    print("- Service Principal has Purview Data Source Admin role")
    print(e)