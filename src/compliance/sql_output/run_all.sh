#!/bin/bash
# Purview SQL loader — schema [compliance]  (5-table mode)
# Load order: collections → entities → glossary → search_assets → asset_registry
# export SQL_SERVER=... SQL_DATABASE=... SQL_USER=... SQL_PASSWORD=...
# Total files: 5

echo "Loading purview_collections\purview_collections_part1.sql..."
sqlcmd -S "$SQL_SERVER" -d "$SQL_DATABASE" -U "$SQL_USER" -P "$SQL_PASSWORD" -b -I -i "purview_collections\purview_collections_part1.sql"
if [ $? -ne 0 ]; then echo "FAILED: purview_collections\purview_collections_part1.sql"; exit 1; fi
echo "Loading purview_entities\purview_entities_part1.sql..."
sqlcmd -S "$SQL_SERVER" -d "$SQL_DATABASE" -U "$SQL_USER" -P "$SQL_PASSWORD" -b -I -i "purview_entities\purview_entities_part1.sql"
if [ $? -ne 0 ]; then echo "FAILED: purview_entities\purview_entities_part1.sql"; exit 1; fi
echo "Loading purview_glossary_terms\purview_glossary_terms_part1.sql..."
sqlcmd -S "$SQL_SERVER" -d "$SQL_DATABASE" -U "$SQL_USER" -P "$SQL_PASSWORD" -b -I -i "purview_glossary_terms\purview_glossary_terms_part1.sql"
if [ $? -ne 0 ]; then echo "FAILED: purview_glossary_terms\purview_glossary_terms_part1.sql"; exit 1; fi
echo "Loading purview_search_assets\purview_search_assets_part1.sql..."
sqlcmd -S "$SQL_SERVER" -d "$SQL_DATABASE" -U "$SQL_USER" -P "$SQL_PASSWORD" -b -I -i "purview_search_assets\purview_search_assets_part1.sql"
if [ $? -ne 0 ]; then echo "FAILED: purview_search_assets\purview_search_assets_part1.sql"; exit 1; fi
echo "Loading asset_registry\asset_registry_part1.sql..."
sqlcmd -S "$SQL_SERVER" -d "$SQL_DATABASE" -U "$SQL_USER" -P "$SQL_PASSWORD" -b -I -i "asset_registry\asset_registry_part1.sql"
if [ $? -ne 0 ]; then echo "FAILED: asset_registry\asset_registry_part1.sql"; exit 1; fi
echo 'All files loaded successfully.'