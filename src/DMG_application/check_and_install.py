"""
check_and_install.py
====================
Dependency pre-flight for the Purview Scan & Compliance suite.

WHAT IT DOES
─────────────
  1. Checks every required Python package.
  2. Installs any that are missing (via pip — internet connection required).
  3. Re-checks after installation.
  4. Prints a clear driver-availability table for both DB options (pyodbc / pymssql).
  5. Launches purview_scan.py automatically (unless --check-only is passed).

USAGE
──────
  python check_and_install.py               # check, install missing, then run scan
  python check_and_install.py --check-only  # check and install only — do not run scan
  python check_and_install.py --no-install  # check only, do NOT install anything

HOW THE DB FALLBACK WORKS
──────────────────────────
  The compliance DB push tries drivers in this order:
    1. pyodbc  +  ODBC Driver 18 for SQL Server   (best performance)
    2. pyodbc  +  ODBC Driver 17 for SQL Server   (older but still supported)
    3. pymssql                                     (pure Python — no ODBC install needed)

  If ODBC drivers are not installed on the machine, pymssql handles the DB
  push automatically with no extra configuration.
"""

import sys
import subprocess
import importlib
import platform
import os
from pathlib import Path

# ── Colour helpers (no external dep) ──────────────────────────────────
_USE_COLOUR = sys.stdout.isatty() and platform.system() != "Windows"

def _c(code, text):
    return f"\033[{code}m{text}\033[0m" if _USE_COLOUR else text

OK   = lambda t: _c("32;1", t)
WARN = lambda t: _c("33;1", t)
ERR  = lambda t: _c("31;1", t)
HDR  = lambda t: _c("36;1", t)
DIM  = lambda t: _c("2",    t)


# ══════════════════════════════════════════════════════════════════════
#  PACKAGE REGISTRY
#  Each entry:
#    import_name  — the Python name used in "import X"
#    pip_name     — the package name passed to "pip install X"
#    required     — True = must succeed; False = optional (warn only)
#    description  — one-line purpose for the status table
# ══════════════════════════════════════════════════════════════════════
PACKAGES = [
    # ── Core ──────────────────────────────────────────────────────────
    {
        "import_name": "requests",
        "pip_name":    "requests",
        "required":    True,
        "description": "HTTP calls to Purview + GitHub APIs",
    },
    {
        "import_name": "yaml",
        "pip_name":    "PyYAML",
        "required":    True,
        "description": "Parse YAML compliance policy files",
    },
    {
        "import_name": "openpyxl",
        "pip_name":    "openpyxl",
        "required":    True,
        "description": "Write Excel compliance reports",
    },
    # ── Database — at least ONE must succeed ──────────────────────────
    {
        "import_name": "pyodbc",
        "pip_name":    "pyodbc",
        "required":    False,          # optional — pymssql can cover it
        "description": "SQL Server via system ODBC driver (primary — fastest)",
    },
    {
        "import_name": "pymssql",
        "pip_name":    "pymssql",
        "required":    False,          # optional — pyodbc can cover it
        "description": "SQL Server pure-Python fallback (no ODBC install needed)",
    },
    # ── Azure optional ────────────────────────────────────────────────
    {
        "import_name": "azure.storage.blob",
        "pip_name":    "azure-storage-blob",
        "required":    False,
        "description": "YAML from Azure Blob Storage (YAML_SOURCE_AZURE_BLOB mode)",
    },
]


def _check_import(import_name: str) -> bool:
    """Return True if the package can be imported."""
    try:
        importlib.import_module(import_name)
        return True
    except ImportError:
        return False


def _pip_install(pip_name: str) -> bool:
    """Run pip install and return True on success."""
    cmd = [sys.executable, "-m", "pip", "install", "--quiet", pip_name]
    result = subprocess.run(cmd, capture_output=True, text=True)
    return result.returncode == 0


def _check_odbc_drivers() -> list:
    """Return list of installed ODBC driver names (empty if pyodbc not available)."""
    try:
        import pyodbc
        return pyodbc.drivers()
    except Exception:
        return []


def run_checks(auto_install: bool = True) -> bool:
    """
    Check all packages, optionally install missing ones.
    Returns True when the environment is ready to run the scan.
    """
    print()
    print(HDR("=" * 68))
    print(HDR("  Purview Suite — Dependency Pre-flight Check"))
    print(HDR("=" * 68))
    print()

    col_w = max(len(p["import_name"]) for p in PACKAGES) + 2

    # ── Header row ────────────────────────────────────────────────────
    print(f"  {'Package':<{col_w}} {'Status':<14} Description")
    print(f"  {'─' * col_w} {'─' * 14} {'─' * 40}")

    results   = {}
    to_install = []

    for pkg in PACKAGES:
        imp  = pkg["import_name"]
        pip  = pkg["pip_name"]
        req  = pkg["required"]

        available = _check_import(imp)
        results[imp] = available

        if available:
            status = OK("✓ installed")
            print(f"  {imp:<{col_w}} {status:<23} {DIM(pkg['description'])}")
        else:
            if auto_install:
                status = WARN("✗ missing — will install")
                to_install.append(pkg)
            else:
                status = ERR("✗ missing")
            print(f"  {imp:<{col_w}} {status:<23} {DIM(pkg['description'])}")

    # ── Install missing packages ───────────────────────────────────────
    if to_install and auto_install:
        print()
        print(HDR(f"  Installing {len(to_install)} missing package(s)..."))
        print()
        for pkg in to_install:
            imp = pkg["import_name"]
            pip = pkg["pip_name"]
            print(f"  → pip install {pip} ...", end="", flush=True)
            ok = _pip_install(pip)
            if ok:
                # invalidate importlib cache so re-import works
                importlib.invalidate_caches()
                now_ok = _check_import(imp)
                results[imp] = now_ok
                print(f"  {OK('done') if now_ok else WARN('installed but import still failed')}")
            else:
                print(f"  {ERR('FAILED')}")
                print(f"    Run manually: pip install {pip}")

    # ── Database driver status summary ────────────────────────────────
    print()
    print(HDR("  Database Driver Status"))
    print(f"  {'─' * 64}")

    has_pyodbc  = results.get("pyodbc",  False)
    has_pymssql = results.get("pymssql", False)

    if has_pyodbc:
        odbc_drivers = _check_odbc_drivers()
        preferred = [d for d in odbc_drivers
                     if "SQL Server" in d and "ODBC Driver" in d]
        if preferred:
            print(f"  {OK('✓')} pyodbc     — available  |  "
                  f"ODBC drivers found: {', '.join(preferred)}")
            print(f"        {DIM('→ Will use: ' + preferred[0])}")
        else:
            print(f"  {WARN('!')} pyodbc     — installed  |  "
                  f"{WARN('NO ODBC system driver found on this machine')}")
            print(f"        {DIM('→ Download ODBC Driver 18: '
                                 'https://learn.microsoft.com/sql/connect/odbc/')}")
            print(f"        {DIM('→ Will fall back to pymssql automatically')}")
    else:
        print(f"  {ERR('✗')} pyodbc     — not available")

    if has_pymssql:
        print(f"  {OK('✓')} pymssql    — available  |  "
              f"pure-Python TDS (no ODBC install needed)")
        if has_pyodbc:
            odbc_ok = bool(_check_odbc_drivers())
            if odbc_ok:
                print(f"        {DIM('→ pymssql ready as fallback (pyodbc + ODBC will be tried first)')}")
            else:
                print(f"        {DIM('→ pymssql WILL BE USED (no ODBC driver found — automatic fallback)')}")
    else:
        print(f"  {WARN('!')} pymssql    — not available  "
              f"{DIM('(fallback unavailable)')}")

    # ── Final verdict ──────────────────────────────────────────────────
    print()
    core_ok = all(results.get(p["import_name"], False)
                  for p in PACKAGES if p["required"])
    db_ok   = has_pyodbc or has_pymssql

    if core_ok and db_ok:
        print(OK("  ✓ All required packages present.  Environment is ready."))
        ready = True
    elif core_ok and not db_ok:
        print(WARN("  ⚠ Core packages OK but NO SQL driver found."))
        print(WARN("    DB push will be skipped.  Install pymssql for DB support:"))
        print(WARN("      pip install pymssql"))
        ready = True   # scan + Excel still works without DB
    else:
        missing_core = [
            p["import_name"] for p in PACKAGES
            if p["required"] and not results.get(p["import_name"], False)
        ]
        print(ERR(f"  ✗ Required package(s) missing: {', '.join(missing_core)}"))
        print(ERR("    Run:  pip install -r requirements.txt"))
        ready = False

    print()
    return ready


def launch_scan():
    """Launch purview_scan.py from the same directory as this script."""
    scan_script = Path(__file__).parent / "purview_scan.py"
    if not scan_script.exists():
        print(ERR(f"  purview_scan.py not found at: {scan_script}"))
        print(ERR("  Place this script in the same folder as purview_scan.py."))
        sys.exit(1)

    print(HDR("=" * 68))
    print(HDR("  Launching purview_scan.py"))
    print(HDR("=" * 68))
    print()

    # Forward any extra CLI args (e.g. a custom config path)
    extra_args = sys.argv[2:]   # argv[0]=this script, argv[1]=flag (if any)
    cmd = [sys.executable, str(scan_script)] + extra_args
    result = subprocess.run(cmd)
    sys.exit(result.returncode)


def main():
    check_only  = "--check-only"  in sys.argv
    no_install  = "--no-install"  in sys.argv

    ready = run_checks(auto_install=not no_install)

    if check_only or no_install:
        sys.exit(0 if ready else 1)

    if not ready:
        print(ERR("  Cannot start scan — fix the errors above first."))
        sys.exit(1)

    launch_scan()


if __name__ == "__main__":
    main()