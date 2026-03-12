import os
import sys
import json
import requests
import pandas as pd
# ------------------------------------------------
# CONFIG
# ------------------------------------------------

CLIENT_ID="2c98dd46-5ec9-4198-b265-058e18078125"
CLIENT_SECRET="Ijh8Q~s8DdTg~PNkeiFR.Al~rjAPMBb5eovbqcbY"
TENANT_ID="5f9bacc0-ffe8-41f7-8d25-215d55cb0f96"
PURVIEW_ACCOUNT="finastrapurview"


SEARCH_API = f"https://{PURVIEW_ACCOUNT}.purview.azure.com/datamap/api/search/query?api-version=2023-09-01"
ENTITY_API = f"https://{PURVIEW_ACCOUNT}.purview.azure.com/datamap/api/atlas/v2/entity/guid"

EXCEL_FILE = "purview_columns_to_classify.xlsx"

# ------------------------------------------------
# AUTHENTICATION
# ------------------------------------------------

def get_token():

    url = f"https://login.microsoftonline.com/{TENANT_ID}/oauth2/token"

    payload = {
        "grant_type": "client_credentials",
        "client_id": CLIENT_ID,
        "client_secret": CLIENT_SECRET,
        "resource": "https://purview.azure.net"
    }

    r = requests.post(url, data=payload)
    r.raise_for_status()

    return r.json()["access_token"]

token = get_token()

headers = {
    "Authorization": f"Bearer {token}",
    "Content-Type": "application/json"
}

# ------------------------------------------------
# FETCH CATALOG
# ------------------------------------------------

def fetch_catalog():

    print("\nScanning Purview catalog...\n")

    body = {
        "keywords": "*",
        "limit": 1000
    }

    r = requests.post(SEARCH_API, headers=headers, json=body)
    r.raise_for_status()

    data = r.json()

    assets = data.get("value", [])

    print("Total assets discovered →", len(assets))

    return assets

# ------------------------------------------------
# FETCH ENTITY
# ------------------------------------------------

def fetch_entity(guid):

    url = f"{ENTITY_API}/{guid}?minExtInfo=false"

    r = requests.get(url, headers=headers)

    if r.status_code != 200:
        return None

    return r.json()

# ------------------------------------------------
# EXTRACT COLUMNS (NO DUPLICATES)
# ------------------------------------------------

def extract_columns(entity_json):

    columns = {}

    entity = entity_json.get("entity", {})
    relationships = entity.get("relationshipAttributes", {})
    referred = entity_json.get("referredEntities", {})

    # SQL tables
    table_cols = relationships.get("table_columns")

    if table_cols:

        for ref in table_cols:

            guid = ref.get("guid")

            if guid in referred:
                columns[guid] = referred[guid]

    # Blob / CSV schema
    attached_schema = relationships.get("attachedSchema")

    if attached_schema:

        for schema in attached_schema:

            sguid = schema.get("guid")

            schema_json = fetch_entity(sguid)

            if not schema_json:
                continue

            schema_entities = schema_json.get("referredEntities", {})

            for g, obj in schema_entities.items():

                if "column" in obj.get("typeName", "").lower():
                    columns[g] = obj

    return list(columns.values())

# ------------------------------------------------
# SCAN MODE
# ------------------------------------------------

def scan_catalog():

    rows = []

    assets = fetch_catalog()

    for asset in assets:

        guid = asset.get("id")
        qualified_name = asset.get("qualifiedName", "")

        if not guid:
            continue

        entity_json = fetch_entity(guid)

        if not entity_json:
            continue

        columns = extract_columns(entity_json)

        if not columns:
            continue

        print("\n--------------------------------")
        print("ASSET:", qualified_name)

        for col in columns:

            attr = col.get("attributes", {})
            col_name = attr.get("name")

            if not col_name:
                continue

            classifications = col.get("classifications", [])
            class_list = [c.get("typeName") for c in classifications]

            print("COLUMN:", col_name, "| CLASSIFICATION:", ",".join(class_list))

            if not class_list:

                rows.append({
                    "FullQualifiedName": qualified_name,
                    "Column": col_name,
                    "ColumnGUID": col.get("guid"),
                    "Classification": "",
                    "Status": ""
                })

    df = pd.DataFrame(rows)

    if not df.empty:
        df = df.drop_duplicates(subset=["FullQualifiedName", "Column"])

    df.to_excel(EXCEL_FILE, index=False)

    print("\nExcel generated →", EXCEL_FILE)

# ------------------------------------------------
# APPLY CLASSIFICATION MODE
# ------------------------------------------------

def apply_classifications(file):

    print("\nApplying classifications from Excel...\n")

    df = pd.read_excel(file)

    statuses = []

    for _, row in df.iterrows():

        asset = row.get("FullQualifiedName")
        column = row.get("Column")
        guid = row.get("ColumnGUID")
        classification = row.get("Classification")

        if pd.isna(classification) or classification == "":
            statuses.append("Skipped")
            continue

        url = f"{ENTITY_API}/{guid}/classifications"

        body = [
            {
                "typeName": classification
            }
        ]

        r = requests.post(url, headers=headers, json=body)

        print("\n--------------------------------")
        print("ASSET :", asset)
        print("COLUMN:", column)
        print("GUID  :", guid)
        print("CLASS :", classification)

        if r.status_code in [200, 204]:

            print("STATUS: UPDATED")

            statuses.append("Updated")

        else:

            print("STATUS: FAILED")
            print("ERROR :", r.text)

            statuses.append("Failed")

    df["Status"] = statuses

    df.to_excel(file, index=False)

    print("\nExcel updated with status →", file)

# ------------------------------------------------
# MAIN
# ------------------------------------------------

def main():

    if len(sys.argv) == 1:

        scan_catalog()

    else:

        excel_file = sys.argv[1]

        if not os.path.exists(excel_file):

            print("Excel file not found.")
            return

        apply_classifications(excel_file)

# ------------------------------------------------

if __name__ == "__main__":
    main()