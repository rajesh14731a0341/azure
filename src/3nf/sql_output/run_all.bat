@echo off
REM Purview SQL loader — [compliance].[change_audit_log]  +  [compliance].[unified_catalog]
REM Load order: audit DDL first, then catalog parts
REM Set env vars: SQL_SERVER  SQL_DATABASE  SQL_USER  SQL_PASSWORD
REM Total files: 2

echo Loading change_audit_log\change_audit_log_part1.sql...
sqlcmd -S %SQL_SERVER% -d %SQL_DATABASE% -U %SQL_USER% -P %SQL_PASSWORD% -b -I -i "change_audit_log\change_audit_log_part1.sql"
if %ERRORLEVEL% NEQ 0 (echo FAILED: change_audit_log\change_audit_log_part1.sql & exit /b 1)
echo Loading unified_catalog\unified_catalog_part1.sql...
sqlcmd -S %SQL_SERVER% -d %SQL_DATABASE% -U %SQL_USER% -P %SQL_PASSWORD% -b -I -i "unified_catalog\unified_catalog_part1.sql"
if %ERRORLEVEL% NEQ 0 (echo FAILED: unified_catalog\unified_catalog_part1.sql & exit /b 1)
echo All files loaded successfully.