@echo off
REM Purview SQL loader — schema [compliance]  (5-table mode)
REM Load order: collections → entities → glossary → search_assets → asset_registry
REM env vars needed: SQL_SERVER  SQL_DATABASE  SQL_USER  SQL_PASSWORD
REM Total files: 5

echo Loading purview_collections\purview_collections_part1.sql...
sqlcmd -S %SQL_SERVER% -d %SQL_DATABASE% -U %SQL_USER% -P %SQL_PASSWORD% -b -I -i "purview_collections\purview_collections_part1.sql"
if %ERRORLEVEL% NEQ 0 (echo FAILED: purview_collections\purview_collections_part1.sql & exit /b 1)
echo Loading purview_entities\purview_entities_part1.sql...
sqlcmd -S %SQL_SERVER% -d %SQL_DATABASE% -U %SQL_USER% -P %SQL_PASSWORD% -b -I -i "purview_entities\purview_entities_part1.sql"
if %ERRORLEVEL% NEQ 0 (echo FAILED: purview_entities\purview_entities_part1.sql & exit /b 1)
echo Loading purview_glossary_terms\purview_glossary_terms_part1.sql...
sqlcmd -S %SQL_SERVER% -d %SQL_DATABASE% -U %SQL_USER% -P %SQL_PASSWORD% -b -I -i "purview_glossary_terms\purview_glossary_terms_part1.sql"
if %ERRORLEVEL% NEQ 0 (echo FAILED: purview_glossary_terms\purview_glossary_terms_part1.sql & exit /b 1)
echo Loading purview_search_assets\purview_search_assets_part1.sql...
sqlcmd -S %SQL_SERVER% -d %SQL_DATABASE% -U %SQL_USER% -P %SQL_PASSWORD% -b -I -i "purview_search_assets\purview_search_assets_part1.sql"
if %ERRORLEVEL% NEQ 0 (echo FAILED: purview_search_assets\purview_search_assets_part1.sql & exit /b 1)
echo Loading asset_registry\asset_registry_part1.sql...
sqlcmd -S %SQL_SERVER% -d %SQL_DATABASE% -U %SQL_USER% -P %SQL_PASSWORD% -b -I -i "asset_registry\asset_registry_part1.sql"
if %ERRORLEVEL% NEQ 0 (echo FAILED: asset_registry\asset_registry_part1.sql & exit /b 1)
echo All files loaded successfully.