import requests
from urllib.parse import urlparse
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
# PAGINATED SEARCH
# ===============================
def fetch_all_assets(token):

    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json"
    }

    all_assets = []
    continuation = None

    while True:

        body = {"keywords": "*", "limit": 1000}

        if continuation:
            body["continuationToken"] = continuation

        url = f"{CATALOG_ENDPOINT}/datamap/api/search/query?api-version={API_VERSION}"
        response = requests.post(url, headers=headers, json=body)
        response.raise_for_status()

        data = response.json()
        all_assets.extend(data.get("value", []))

        continuation = data.get("continuationToken")

        if not continuation:
            break

    print(f"Total assets fetched: {len(all_assets)}")
    return all_assets


# ===============================
# ROOT DETECTION (NO FILTERING)
# ===============================
def detect_roots(assets):

    roots = {}

    for asset in assets:

        qn = asset.get("qualifiedName")
        if not qn:
            continue

        parsed = urlparse(qn)

        if parsed.scheme == "oracle":
            root = f"{parsed.scheme}://{parsed.netloc}"
            label = "Oracle Server"

        elif parsed.scheme in ["http", "https"]:
            account = parsed.netloc.split(".")[0]
            root = account
            label = "Storage Account"

        else:
            continue

        roots.setdefault(root, {"label": label, "assets": []})
        roots[root]["assets"].append(asset)

    return roots


# ===============================
# BUILD TREE (REAL NAMES ONLY)
# ===============================
def build_tree(assets):

    tree = {}

    for asset in assets:

        qn = asset.get("qualifiedName")
        name = asset.get("name")

        if not qn or not name:
            continue

        parsed = urlparse(qn)
        path = parsed.path.lstrip("/")

        if not path:
            continue

        parts = path.split("/")

        current = tree

        for i, part in enumerate(parts):

            if i == len(parts) - 1:
                part = name  # always use real asset name

            current = current.setdefault(part, {})

        current["_asset"] = asset

    return tree


# ===============================
# LABEL FROM ENTITY TYPE
# ===============================
def get_label(asset):

    if not asset:
        return "Folder"

    entity = asset.get("entityType", "")
    return entity.replace("_", " ").title()


# ===============================
# RECURSIVE COLLECT
# ===============================
def collect_recursive(node, collected):

    for key, value in node.items():
        if key.startswith("_"):
            continue

        asset = value.get("_asset")
        if asset:
            collected.append(asset)

        collect_recursive(value, collected)


# ===============================
# NAVIGATION
# ===============================
def navigate(tree):

    stack = []
    current = tree

    while True:

        keys = [k for k in current.keys() if not k.startswith("_")]

        if not keys:
            print("\nNo further levels.")
            return None

        print("\nAvailable Objects:\n")

        for i, key in enumerate(keys):
            asset = current[key].get("_asset")
            label = get_label(asset)
            print(f"{i+1}. {key} ({label})")

        print("0. Go Back")
        print("99. Quit")

        selection = input("Select number: ")

        if selection == "99":
            return None

        if selection == "0":
            if stack:
                current = stack.pop()
            continue

        try:
            idx = int(selection)
        except:
            continue

        if 1 <= idx <= len(keys):

            selected_key = keys[idx - 1]
            selected_node = current[selected_key]
            asset = selected_node.get("_asset")
            label = get_label(asset)

            print(f"\nSelected: {selected_key} ({label})")
            print(f"1. Enter {label}")
            print(f"2. Delete {label} (Recursive)")
            print("3. Go Back")
            print("4. Quit")

            action = input("Choose option: ")

            if action == "1":
                stack.append(current)
                current = selected_node

            elif action == "2":

                to_delete = []

                if asset:
                    to_delete.append(asset)

                collect_recursive(selected_node, to_delete)

                return to_delete

            elif action == "3":
                continue

            elif action == "4":
                return None


# ===============================
# PREVIEW
# ===============================
def preview(assets):

    print("\nRECURSIVE DELETE PREVIEW")
    print("=" * 60)

    for asset in assets:
        print(asset.get("qualifiedName"))

    print(f"\nTotal objects: {len(assets)}")


# ===============================
# DELETE
# ===============================
def delete_entities(token, assets):

    headers = {"Authorization": f"Bearer {token}"}

    guids = [a.get("id") for a in assets if a.get("id")]

    batch_size = 20

    for i in range(0, len(guids), batch_size):

        chunk = guids[i:i+batch_size]
        query = "&".join([f"guid={g}" for g in chunk])

        delete_url = (
            f"{CATALOG_ENDPOINT}/datamap/api/atlas/v2/entity/bulk?"
            f"{query}&api-version={API_VERSION}"
        )

        response = requests.delete(delete_url, headers=headers)

        if response.status_code == 200:
            print(f"Batch {i//batch_size + 1} deleted.")
        else:
            print("Delete failed:", response.text)


# ===============================
# MAIN
# ===============================
if __name__ == "__main__":

    print("Authenticating...")
    token = get_token()

    print("Fetching Unified Catalog...")
    assets = fetch_all_assets(token)

    roots = detect_roots(assets)

    print("\nAVAILABLE DATA SOURCES")
    print("=" * 60)

    root_list = list(roots.keys())

    for i, root in enumerate(root_list):
        print(f"{i+1}. {root} ({roots[root]['label']})")

    print("0. Exit")

    choice = int(input("Select source number: "))

    if choice == 0:
        exit()

    selected_root = root_list[choice - 1]
    root_assets = roots[selected_root]["assets"]

    tree = build_tree(root_assets)

    selected_assets = navigate(tree)

    if not selected_assets:
        print("Operation cancelled.")
        exit()

    preview(selected_assets)

    confirm = input("\nType YES to confirm deletion: ")

    if confirm.strip().upper() == "YES":
        delete_entities(token, selected_assets)
    else:
        print("Cancelled.")