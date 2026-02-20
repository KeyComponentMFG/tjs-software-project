"""
TJs Software Project — Etsy Financial Dashboard v3
Run:  python -m etsy_dashboard.app
Open: http://127.0.0.1:8070
"""

import os
import sys

# Ensure project root is on the path for supabase_loader etc.
PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

import dash
from dash import html, dcc
import dash_bootstrap_components as dbc

from etsy_dashboard.theme import BG, SIDEBAR_WIDTH, CONTENT_MARGIN

# ── Create the Dash app ──────────────────────────────────────────────────────
app = dash.Dash(
    __name__,
    suppress_callback_exceptions=True,
    external_stylesheets=[
        dbc.themes.DARKLY,
        "https://fonts.googleapis.com/css2?family=Inter:wght@300;400;500;600;700&display=swap",
    ],
    assets_folder=os.path.join(os.path.dirname(__file__), "assets"),
    title="TJs Software Project",
)
server = app.server  # For deployment (Railway / Gunicorn)

# ── Sidebar navigation ──────────────────────────────────────────────────────
NAV_ITEMS = [
    {"label": "Overview",    "icon": "\U0001f4ca", "value": "/"},
    {"label": "Deep Dive",   "icon": "\U0001f50d", "value": "/deep-dive"},
    {"label": "Financials",  "icon": "\U0001f4b0", "value": "/financials"},
    "---",
    {"label": "Inventory",   "icon": "\U0001f4e6", "value": "/inventory"},
    {"label": "Locations",   "icon": "\U0001f4cd", "value": "/locations"},
    "---",
    {"label": "Tax Forms",   "icon": "\U0001f4c4", "value": "/tax-forms"},
    {"label": "Valuation",   "icon": "\U0001f4c8", "value": "/valuation"},
    "---",
    {"label": "Data Hub",    "icon": "\u2b06\ufe0f",  "value": "/data-hub"},
]


def _build_sidebar():
    nav_links = []
    for item in NAV_ITEMS:
        if item == "---":
            nav_links.append(html.Hr(className="sidebar-divider"))
        else:
            nav_links.append(
                dbc.NavLink(
                    [html.Span(item["icon"], className="nav-icon"), item["label"]],
                    href=item["value"],
                    active="exact",
                )
            )

    return html.Div([
        # Brand
        html.Div([
            html.H4("TJs SOFTWARE"),
            html.Small("Etsy Financial Dashboard"),
        ], className="sidebar-brand"),

        # Nav
        dbc.Nav(nav_links, vertical=True, pills=True),
    ], className="sidebar")


# ── App layout ───────────────────────────────────────────────────────────────
def serve_layout():
    from etsy_dashboard import data_state as ds
    subtitle = (
        f"Oct 2025 — Feb 2026  |  {ds.order_count} orders  |  "
        f"{len(ds.DATA)} transactions  |  "
        f"Profit: {ds.money(ds.full_profit)} ({ds.full_profit_margin:.1f}%)  |  "
        f"COGS: {ds.money(ds.true_inventory_cost)}  |  "
        f"Bank: {ds.money(ds.bank_net_cash)}  |  Etsy: {ds.money(ds.etsy_balance)}"
    )
    return html.Div([
        dcc.Location(id="url", refresh=False),

        # Sidebar
        _build_sidebar(),

        # Main content area
        html.Div([
            # Header
            html.Div([
                html.H3("TJs SOFTWARE PROJECT"),
                html.Div(subtitle, className="header-subtitle", id="app-header-content"),
            ], className="app-header"),

            # Page content (rendered by routing callback)
            html.Div(id="page-content"),

            # Toast notification container
            html.Div(id="toast-container"),

            # Hidden stores
            dcc.Store(id="editor-save-trigger", data=0),
        ], className="main-content"),
    ])


app.layout = serve_layout


# ── Register callbacks ───────────────────────────────────────────────────────
# Import callback modules AFTER app is created so they can reference `app`
from etsy_dashboard.callbacks.navigation_cb import register_callbacks as _reg_nav
_reg_nav(app)

# Import remaining callback modules
from etsy_dashboard.callbacks import inventory_cb, upload_cb, location_cb
inventory_cb.register_callbacks(app)
upload_cb.register_callbacks(app)
location_cb.register_callbacks(app)

# ── Run ──────────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    port = int(os.environ.get("PORT", 8070))
    print(f"\n  TJs Software Dashboard v3")
    print(f"  http://127.0.0.1:{port}")
    print(f"  Sidebar navigation • dbc components • modular architecture\n")
    app.run(debug=False, host="0.0.0.0", port=port)
