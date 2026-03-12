import requests
from msal import ConfidentialClientApplication

# ==============================
# CONFIGURATION
# ==============================

TENANT_ID = "5f9bacc0-ffe8-41f7-8d25-215d55cb0f96"
CLIENT_ID = "2c98dd46-5ec9-4198-b265-058e18078125"
CLIENT_SECRET = "Ijh8Q~s8DdTg~PNkeiFR.Al~rjAPMBb5eovbqcbY"
PURVIEW_ACCOUNT = "Finastrapurview"



MAP_FILE = "collection_map.txt"
API_VERSION = "2019-11-01-preview"
BASE = f"https://{PURVIEW_ACCOUNT}.purview.azure.com/account"

# ==============================
# AUTH
# ==============================
app = ConfidentialClientApplication(
    CLIENT_ID,
    authority=f"https://login.microsoftonline.com/{TENANT_ID}",
    client_credential=CLIENT_SECRET
)

token = app.acquire_token_for_client(
    scopes=["https://purview.azure.net/.default"]
)["access_token"]

headers = {
    "Authorization": f"Bearer {token}",
    "Content-Type": "application/json"
}

# ==============================
# API CALLS
# ==============================
def get_all_collections():
    url = f"{BASE}/collections?api-version={API_VERSION}"
    return requests.get(url, headers=headers).json().get("value", [])

def get_children(name):
    url = f"{BASE}/collections/{name}/getChildCollectionNames?api-version={API_VERSION}"
    r = requests.get(url, headers=headers)
    if r.status_code == 200:
        return r.json().get("value", [])
    return []

# ==============================
# BUILD TREE (EXACT PS LOGIC)
# ==============================
def build_full_tree_map():

    visited = set()
    child_names = set()
    mapping = {}

    all_cols = get_all_collections()

    # STEP 1: find all child collections
    for c in all_cols:
        children = get_children(c["name"])
        for child in children:
            child_names.add(child["name"])

    # STEP 2: true roots = collections that are NOT a child
    roots = [c for c in all_cols if c["name"] not in child_names]

    # STEP 3: recursive walk
    def walk(internal_name, friendly_path):

        if internal_name in visited:
            return

        visited.add(internal_name)

        # save mapping
        mapping[friendly_path] = internal_name

        children = get_children(internal_name)

        for child in children:
            child_path = f"{friendly_path}/{child['friendlyName']}"
            walk(child["name"], child_path)

    # STEP 4: walk every root independently
    for root in roots:
        walk(root["name"], root["friendlyName"])

    # STEP 5: write file
    with open(MAP_FILE, "w", encoding="utf-8") as f:
        for path, code in mapping.items():
            f.write(f"{path}|{code}\n")

    print("FULL TREE mapping created successfully")

# ==============================
# MAIN
# ==============================
if __name__ == "__main__":
    build_full_tree_map()
