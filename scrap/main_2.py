import requests
from msal import ConfidentialClientApplication

# =========================================================
# CONFIG
# =========================================================
TENANT_ID = "5f9bacc0-ffe8-41f7-8d25-215d55cb0f96"
CLIENT_ID = "2c98dd46-5ec9-4198-b265-058e18078125"
CLIENT_SECRET = "Ijh8Q~s8DdTg~PNkeiFR.Al~rjAPMBb5eovbqcbY"
PURVIEW_ACCOUNT = "Finastrapurview"

ACCESS_FILE = "access.txt"
COLLECTION_FILE = "collections.txt"
MAP_FILE = "collection_map.txt"

COLLECTION_API = "2019-11-01-preview"
POLICY_API = "2021-07-01"

BASE = f"https://{PURVIEW_ACCOUNT}.purview.azure.com/account"
POLICY_BASE = f"https://{PURVIEW_ACCOUNT}.purview.azure.com/policystore"

ROLE_MAP = {
    "data readers": "purview-reader",
    "data curator": "data-curator",
    "collection administrator": "collection-administrator",
    "data source administrator": "data-source-administrator",
    "policy author": "policy-author"
}

# =========================================================
# AUTH
# =========================================================
print("🔐 Authenticating using Service Principal...")

app = ConfidentialClientApplication(
    CLIENT_ID,
    authority=f"https://login.microsoftonline.com/{TENANT_ID}",
    client_credential=CLIENT_SECRET
)

purview_token = app.acquire_token_for_client(
    scopes=["https://purview.azure.net/.default"]
)["access_token"]

graph_token = app.acquire_token_for_client(
    scopes=["https://graph.microsoft.com/.default"]
)["access_token"]

headers = {
    "Authorization": f"Bearer {purview_token}",
    "Content-Type": "application/json"
}

graph_headers = {"Authorization": f"Bearer {graph_token}"}

print("✅ Authentication complete")


# =========================================================
# COLLECTION DISCOVERY
# =========================================================
def get_all_collections():
    print("📡 Fetching ALL collections from Purview...")
    url = f"{BASE}/collections?api-version={COLLECTION_API}"
    data = requests.get(url, headers=headers).json().get("value", [])
    print(f"   → Found {len(data)} collections")
    return data

def get_children(name):
    url = f"{BASE}/collections/{name}/getChildCollectionNames?api-version={COLLECTION_API}"
    r = requests.get(url, headers=headers)
    if r.status_code == 200:
        return r.json().get("value", [])
    print(f"⚠️ Failed to fetch children for {name}")
    return []


# =========================================================
# BUILD FULL TREE MAP
# =========================================================
def build_full_tree_map():
    print("\n🌳 Building FULL TREE mapping...")

    visited = set()
    child_names = set()
    mapping = {}

    all_cols = get_all_collections()

    # detect children
    for c in all_cols:
        for child in get_children(c["name"]):
            child_names.add(child["name"])

    roots = [c for c in all_cols if c["name"] not in child_names]

    print(f"   → Root collections detected: {len(roots)}")

    def walk(internal_name, friendly_path):
        if internal_name in visited:
            return

        visited.add(internal_name)
        mapping[friendly_path] = internal_name

        print(f"   MAP: {friendly_path}  -->  {internal_name}")

        for child in get_children(internal_name):
            child_path = f"{friendly_path}/{child['friendlyName']}"
            walk(child["name"], child_path)

    for root in roots:
        walk(root["name"], root["friendlyName"])

    with open(MAP_FILE, "w", encoding="utf-8") as f:
        for p, code in mapping.items():
            f.write(f"{p}|{code}\n")

    print("✅ Collection mapping file rebuilt")


def load_map():
    print("\n📂 Loading mapping file...")
    m = {}
    with open(MAP_FILE, encoding="utf-8") as f:
        for line in f:
            if "|" in line:
                path, code = line.strip().split("|", 1)
                m[path] = code

    print(f"   → Loaded {len(m)} mapped collections")
    return m


# =========================================================
# GRAPH USER LOOKUP
# =========================================================
def get_object_id(upn):
    print(f"🔎 Resolving user: {upn}")
    url = f"https://graph.microsoft.com/v1.0/users/{upn}"
    r = requests.get(url, headers=graph_headers)
    if r.status_code == 200:
        oid = r.json()["id"]
        print(f"   → ObjectID: {oid}")
        return oid
    print("❌ User not found in AAD")
    return None


# =========================================================
# COLLECTION OPS
# =========================================================
def create_collection(path):
    print(f"\n🆕 CREATE requested: {path}")

    parent_path = "/".join(path.split("/")[:-1])
    new_friendly = path.split("/")[-1]

    parent_internal = COLLECTION_MAP.get(parent_path)

    if not parent_internal:
        print(f"❌ Parent NOT FOUND in mapping: {parent_path}")
        return

    print(f"   Parent internal ID: {parent_internal}")

    new_name = new_friendly.replace(" ", "").replace("-", "").lower()

    url = f"{BASE}/collections/{new_name}?api-version={COLLECTION_API}"

    payload = {
        "friendlyName": new_friendly,
        "parentCollection": {
            "referenceName": parent_internal
        }
    }

    r = requests.put(url, headers=headers, json=payload)

    print(f"   → API STATUS: {r.status_code}")
    print(r.text)


def delete_collection(path):
    print(f"\n🗑 DELETE requested: {path}")

    internal = COLLECTION_MAP.get(path)

    if not internal:
        print("❌ Collection not found in mapping")
        return

    if "/" not in path:
        print("🚫 Root delete blocked for safety")
        return

    url = f"{BASE}/collections/{internal}?api-version={COLLECTION_API}"
    r = requests.delete(url, headers=headers)

    print(f"   → API STATUS: {r.status_code}")


# =========================================================
# ROLE OPS
# =========================================================
def modify_role(path, role, user, action):
    print(f"\n🔐 ROLE {action}: {user} | {role} | {path}")

    internal = COLLECTION_MAP.get(path)
    if not internal:
        print("❌ Collection path not in mapping")
        return

    oid = get_object_id(user)
    if not oid:
        return

    role_api = ROLE_MAP.get(role.lower(), role)

    url = f"{POLICY_BASE}/collections/{internal}/metadataPolicy?api-version={POLICY_API}"
    policy = requests.get(url, headers=headers).json()

    if "properties" not in policy:
        print("❌ Failed reading policy")
        return

    bindings = policy["properties"]["policy"]["bindings"]

    if action == "ADD":
        bindings.append({
            "role": role_api,
            "members": [oid]
        })
        print("   → Role binding added")

    if action == "REMOVE":
        for b in bindings:
            if b["role"] == role_api and oid in b["members"]:
                b["members"].remove(oid)
                print("   → Role binding removed")

    requests.put(url, headers=headers, json=policy)
    print("   → Policy updated")


# =========================================================
# PARSE COLLECTION FILE
# =========================================================
def process_collections():
    print("\n📄 Processing collections.txt")

    mode = None

    with open(COLLECTION_FILE) as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith("#"):
                continue

            if line.startswith("[CREATE]"):
                mode = "CREATE"
                continue

            if line.startswith("[DELETE]"):
                mode = "DELETE"
                continue

            if line.startswith("path="):
                path = line.split("=", 1)[1].strip()

                if mode == "CREATE":
                    create_collection(path)
                elif mode == "DELETE":
                    delete_collection(path)


# =========================================================
# PARSE ACCESS FILE
# =========================================================
def process_access():
    print("\n📄 Processing access.txt")

    mode = None
    block = {}

    with open(ACCESS_FILE) as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith("#"):
                continue

            if line.startswith("[ADD]"):
                mode = "ADD"; block = {}
                continue

            if line.startswith("[REMOVE]"):
                mode = "REMOVE"; block = {}
                continue

            if "=" in line:
                k, v = line.split("=", 1)
                block[k] = v

                if "role" in block and "collection" in block and "requester" in block:
                    modify_role(
                        block["collection"],
                        block["role"],
                        block["requester"],
                        mode
                    )
                    block = {}


# =========================================================
# MAIN
# =========================================================
if __name__ == "__main__":

    build_full_tree_map()
    COLLECTION_MAP = load_map()

    process_collections()
    process_access()

    print("\n🏁 ALL OPERATIONS COMPLETED")
