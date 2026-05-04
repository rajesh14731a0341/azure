#!/bin/bash
# Purview SQL loader — [compliance].[change_audit_log]  +  [compliance].[unified_catalog]
# Load order: audit DDL first, then catalog parts
# export SQL_SERVER=... SQL_DATABASE=... SQL_USER=... SQL_PASSWORD=...
# Total files: 2

echo "Loading change_audit_log\change_audit_log_part1.sql..."
sqlcmd -S "$SQL_SERVER" -d "$SQL_DATABASE" -U "$SQL_USER" -P "$SQL_PASSWORD" -b -I -i "change_audit_log\change_audit_log_part1.sql"
if [ $? -ne 0 ]; then echo "FAILED: change_audit_log\change_audit_log_part1.sql"; exit 1; fi
echo "Loading unified_catalog\unified_catalog_part1.sql..."
sqlcmd -S "$SQL_SERVER" -d "$SQL_DATABASE" -U "$SQL_USER" -P "$SQL_PASSWORD" -b -I -i "unified_catalog\unified_catalog_part1.sql"
if [ $? -ne 0 ]; then echo "FAILED: unified_catalog\unified_catalog_part1.sql"; exit 1; fi
echo 'All files loaded successfully.'