import requests
import json
from azure.identity import ClientSecretCredential

# ================= CONFIG =================
CLIENT_ID = "2c98dd46-5ec9-4198-b265-058e18078125"
CLIENT_SECRET = "xxxxxxxxxxxxxxxxxxxxx"
TENANT_ID = "5f9bacc0-ffe8-41f7-8d25-215d55cb0f96"


PURVIEW_ENDPOINT = "https://api.purview-service.microsoft.com"
API_VERSION = "2025-09-15-preview"


# ================= AUTH =================
def get_access_token():
    credential = ClientSecretCredential(
        tenant_id=TENANT_ID,
        client_id=CLIENT_ID,
        client_secret=CLIENT_SECRET
    )
    token = credential.get_token("https://purview.azure.net/.default")
    return token.token


def headers(token):
    return {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json"
    }


# ================= FETCH DATA PRODUCTS =================
def list_data_products(token):
    url = f"{PURVIEW_ENDPOINT}/datagovernance/catalog/dataProducts"
    params = {"api-version": API_VERSION}

    res = requests.get(url, headers=headers(token), params=params)

    if res.status_code != 200:
        print("❌ Failed fetching data products:", res.text)
        return []

    return res.json().get("value", [])


# ================= FILTER =================
def filter_products(products, keyword):
    if not keyword:
        return products

    keyword = keyword.lower()

    return [p for p in products if keyword in p.get("name", "").lower()]


# ================= SELECT DATA PRODUCT =================
def select_product(products):

    print("\nFiltered Results:")
    print("=" * 60)

    for idx, p in enumerate(products, 1):
        print(f"{idx}. {p['name']} | Status: {p.get('status')} | Type: {p.get('type')}")

    print("0. Cancel")

    while True:
        choice = input("\nSelect Data Product number: ").strip()

        if choice == "0":
            return None

        try:
            index = int(choice) - 1
            return products[index]
        except:
            print("Invalid selection.")


# ================= EDIT MODE =================
def edit_data_product(token, product):

    dp_id = product["id"]

    while True:
        print("\n" + "=" * 60)
        print(f"Working on: {product['name']}")
        print("=" * 60)

        print("1. View Summary")
        print("2. View Full JSON")
        print("3. Edit Name")
        print("4. Edit Description")
        print("5. Edit Status")
        print("6. Save Changes")
        print("7. Exit Without Saving")

        choice = input("Choose option: ").strip()

        if choice == "1":
            print("\nSummary:")
            print(f"Name: {product.get('name')}")
            print(f"Status: {product.get('status')}")
            print(f"Type: {product.get('type')}")
            print(f"Description: {product.get('description')}")

        elif choice == "2":
            print("\nFull JSON:")
            print(json.dumps(product, indent=2, ensure_ascii=False))

        elif choice == "3":
            new_name = input("Enter new name: ").strip()
            if new_name:
                product["name"] = new_name

        elif choice == "4":
            new_desc = input("Enter new description: ").strip()
            if new_desc:
                product["description"] = new_desc

        elif choice == "5":
            new_status = input("Enter new status (DRAFT / PUBLISHED): ").strip()
            if new_status:
                product["status"] = new_status

        elif choice == "6":
            confirm = input("Type YES to confirm update: ")
            if confirm.strip().upper() == "YES":

                update_body = {
                    "name": product["name"],
                    "description": product.get("description"),
                    "status": product.get("status"),
                    "type": product.get("type")
                }

                url = f"{PURVIEW_ENDPOINT}/datagovernance/catalog/dataProducts/{dp_id}"
                params = {"api-version": API_VERSION}

                res = requests.patch(
                    url,
                    headers=headers(token),
                    params=params,
                    json=update_body
                )

                if res.status_code in [200, 204]:
                    print("✅ Data Product updated successfully")
                else:
                    print("❌ Update failed:", res.text)

                return

        elif choice == "7":
            print("Exiting without saving.")
            return

        else:
            print("Invalid option.")


# ================= MAIN =================
if __name__ == "__main__":

    print("Authenticating...")
    token = get_access_token()

    print("Fetching Data Products...")
    products = list_data_products(token)

    if not products:
        exit()

    keyword = input("Enter name filter (press Enter to show all): ").strip()

    filtered = filter_products(products, keyword)

    if not filtered:
        print("No matching data products.")
        exit()

    selected = select_product(filtered)

    if selected:
        edit_data_product(token, selected)

    print("\n✔ Session Completed")