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

# UI role name variations → Purview role IDs
ROLE_MAP = {
    "data readers": "purview-reader",
    "data reader": "purview-reader",
    "collection admins": "collection-administrator",
    "collection administrator": "collection-administrator",
    "data curator": "data-curator",
    "data source administrator": "data-source-administrator",
    "policy author": "policy-author"
}

# =========================================================
# AUTH
# =========================================================
print("🔐 Authenticating...")

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

print("✅ Auth success")

# =========================================================
# HELPERS
# =========================================================
def norm(x: str) -> str:
    return x.strip().lower()

# =========================================================
# COLLECTION DISCOVERY
# =========================================================
def get_all_collections():
    print("📡 Fetching collections...")
    url = f"{BASE}/collections?api-version={COLLECTION_API}"
    return requests.get(url, headers=headers).json().get("value", [])

def get_children(name):
    url = f"{BASE}/collections/{name}/getChildCollectionNames?api-version={COLLECTION_API}"
    r = requests.get(url, headers=headers)
    if r.status_code == 200:
        return r.json().get("value", [])
    return []

# =========================================================
# BUILD FULL TREE MAP (AUTO EACH RUN)
# =========================================================
def build_full_tree_map():
    print("\n🌳 Building collection map dynamically...")

    visited = set()
    child_names = set()
    mapping = {}

    all_cols = get_all_collections()

    # find children
    for c in all_cols:
        for child in get_children(c["name"]):
            child_names.add(child["name"])

    # true roots
    roots = [c for c in all_cols if c["name"] not in child_names]

    def walk(internal, friendly):
        if internal in visited:
            return
        visited.add(internal)
        mapping[norm(friendly)] = internal
        print(f"MAP: {friendly} -> {internal}")

        for child in get_children(internal):
            walk(child["name"], f"{friendly}/{child['friendlyName']}")

    for r in roots:
        walk(r["name"], r["friendlyName"])

    # overwrite every run
    with open(MAP_FILE, "w", encoding="utf-8") as f:
        for p, code in mapping.items():
            f.write(f"{p}|{code}\n")

    print("✅ collection_map.txt rebuilt")
    return mapping

# =========================================================
# GRAPH USER LOOKUP
# =========================================================
def get_object_id(upn):
    print(f"🔎 Resolving {upn}")
    url = f"https://graph.microsoft.com/v1.0/users/{upn}"
    r = requests.get(url, headers=graph_headers)
    if r.status_code == 200:
        oid = r.json().get("id")
        print("   ObjectID:", oid)
        return oid
    print("❌ User not found")
    return None

# =========================================================
# COLLECTION OPS
# =========================================================
def create_collection(path):
    print(f"\n🆕 CREATE {path}")

    parent_path = norm("/".join(path.split("/")[:-1]))
    new_name = path.split("/")[-1]

    parent_internal = COLLECTION_MAP.get(parent_path)
    if not parent_internal:
        print("❌ Parent not found:", parent_path)
        return

    ref = new_name.replace(" ", "").replace("-", "").lower()
    url = f"{BASE}/collections/{ref}?api-version={COLLECTION_API}"
    payload = {
        "friendlyName": new_name,
        "parentCollection": {"referenceName": parent_internal}
    }

    r = requests.put(url, headers=headers, json=payload)
    print("STATUS:", r.status_code)
    print(r.text)

def delete_collection(path):
    print(f"\n🗑 DELETE {path}")

    internal = COLLECTION_MAP.get(norm(path))
    if not internal:
        print("❌ Not found in map")
        return

    if "/" not in path:
        print("🚫 Root delete blocked")
        return

    url = f"{BASE}/collections/{internal}?api-version={COLLECTION_API}"
    r = requests.delete(url, headers=headers)
    print("STATUS:", r.status_code)

# =========================================================
# ROLE OPS (SAFE POLICY UPDATE)
# =========================================================
def modify_role(path, role, user, action):
    print(f"\n🔐 {action} ROLE: {user} | {role} | {path}")

    internal = COLLECTION_MAP.get(norm(path))
    if not internal:
        print("❌ Collection not found in map")
        return

    oid = get_object_id(user)
    if not oid:
        return

    role_api = ROLE_MAP.get(norm(role))
    if not role_api:
        print("❌ Unknown role name:", role)
        return

    role_rule_id = f"purviewmetadatarole_builtin_{role_api}:{internal}"

    # read policy by collection
    url = f"{POLICY_BASE}/collections/{internal}/metadataPolicy?api-version={POLICY_API}"
    r = requests.get(url, headers=headers)
    if r.status_code != 200:
        print("❌ Failed to read policy")
        print(r.text)
        return

    policy = r.json()
    policy_id = policy.get("id")
    print("Policy ID:", policy_id)

    props = policy.get("properties", {})
    rules = props.get("attributeRules", [])

    found = False

    for rule in rules:
        if rule.get("id") == role_rule_id:
            found = True
            members = rule["dnfCondition"][0][0]["attributeValueIncludedIn"]

            if action == "ADD":
                if oid not in members:
                    members.append(oid)
                    print("✔ Added user")
                else:
                    print("Already exists")

            if action == "REMOVE":
                if oid in members:
                    members.remove(oid)
                    print("✔ Removed user")

    if not found:
        print("❌ Role block not present. Assign once manually in UI.")
        return

    # update via policyId
    update_url = f"{POLICY_BASE}/metadataPolicies/{policy_id}?api-version={POLICY_API}"
    r2 = requests.put(update_url, headers=headers, json=policy)
    print("UPDATE STATUS:", r2.status_code)
    print(r2.text)

# =========================================================
# PARSERS
# =========================================================
def process_collections():
    mode = None
    with open(COLLECTION_FILE) as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith("#"):
                continue

            if line.startswith("[CREATE]"):
                mode = "CREATE"; continue
            if line.startswith("[DELETE]"):
                mode = "DELETE"; continue

            if line.startswith("path="):
                path = line.split("=", 1)[1].strip()
                if mode == "CREATE":
                    create_collection(path)
                if mode == "DELETE":
                    delete_collection(path)

def process_access():
    mode = None
    block = {}
    with open(ACCESS_FILE) as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith("#"):
                continue

            if line.startswith("[ADD]"):
                mode = "ADD"; block = {}; continue
            if line.startswith("[REMOVE]"):
                mode = "REMOVE"; block = {}; continue

            if "=" in line:
                k, v = line.split("=", 1)
                block[k.strip()] = v.strip()

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
    COLLECTION_MAP = build_full_tree_map()  # dynamic rebuild every run
    process_collections()                   # create/delete collections
    process_access()                        # add/remove roles
    print("\n🏁 DONE")
