"""
WSGI entry point for deployment (Railway / Gunicorn).
Imports the monolith etsy_dashboard.py directly, bypassing the
etsy_dashboard/ package which shares the same name.
"""
import importlib.util
import os

# Load etsy_dashboard.py explicitly by file path (not package name)
_spec = importlib.util.spec_from_file_location(
    "etsy_dashboard_mono",
    os.path.join(os.path.dirname(__file__), "etsy_dashboard.py"),
)
_mod = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(_mod)

# Expose the Flask server for gunicorn
server = _mod.server
