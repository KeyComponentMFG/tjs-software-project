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

from dash.dependencies import Input, Output, State

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


# ── Audit Banner ─────────────────────────────────────────────────────────────
_SEVERITY_CONFIG = {
    "error":   {"color": "#e74c3c", "icon": "\u2718", "label": "ERROR"},
    "warning": {"color": "#f39c12", "icon": "\u26a0", "label": "WARNING"},
    "info":    {"color": "#00d4ff", "icon": "\u2139", "label": "INFO"},
}
_SEVERITY_ORDER = {"error": 0, "warning": 1, "info": 2}


def _build_audit_banner():
    from etsy_dashboard import data_state as ds
    results = ds.AUDIT_RESULTS
    if not results:
        return html.Div(id="audit-banner-container")

    # Determine worst severity
    worst = min(results, key=lambda r: _SEVERITY_ORDER.get(r["severity"], 9))
    cfg = _SEVERITY_CONFIG[worst["severity"]]
    border_color = cfg["color"]

    # Count by severity
    counts = {}
    for r in results:
        s = r["severity"]
        counts[s] = counts.get(s, 0) + 1
    count_parts = []
    for s in ["error", "warning", "info"]:
        if counts.get(s):
            sc = _SEVERITY_CONFIG[s]
            count_parts.append(
                html.Span(
                    f"{sc['icon']} {counts[s]} {sc['label']}",
                    style={"color": sc["color"], "marginRight": "12px", "fontSize": "11px"},
                )
            )

    # Build issue rows
    issue_rows = []
    for r in sorted(results, key=lambda x: _SEVERITY_ORDER.get(x["severity"], 9)):
        sc = _SEVERITY_CONFIG[r["severity"]]
        issue_rows.append(
            html.Div([
                dbc.Badge(sc["label"], style={
                    "backgroundColor": sc["color"],
                    "color": "#000" if r["severity"] != "error" else "#fff",
                    "fontSize": "9px",
                    "fontWeight": "700",
                    "minWidth": "60px",
                    "textAlign": "center",
                }),
                html.Span(r["title"], style={
                    "fontWeight": "600",
                    "fontSize": "12px",
                    "color": "#fff",
                    "marginLeft": "10px",
                }),
                html.Span(f"  [{r['metric']}]", style={
                    "color": "#555",
                    "fontSize": "10px",
                    "fontFamily": "'JetBrains Mono', monospace",
                }),
                html.Div(r["detail"], style={
                    "color": "#999",
                    "fontSize": "11px",
                    "marginTop": "2px",
                    "paddingLeft": "70px",
                }),
            ], style={"padding": "6px 0", "borderBottom": "1px solid rgba(255,255,255,0.04)"})
        )

    return html.Div([
        # Clickable header bar
        html.Div([
            html.Div([
                html.Span(cfg["icon"], style={"fontSize": "14px", "marginRight": "8px"}),
                html.Span("DATA AUDIT", style={
                    "fontWeight": "700",
                    "fontSize": "11px",
                    "letterSpacing": "1.5px",
                }),
                html.Span(f" \u2014 {len(results)} issue{'s' if len(results) != 1 else ''}", style={
                    "color": "#888",
                    "fontSize": "11px",
                    "marginLeft": "6px",
                }),
            ], style={"display": "flex", "alignItems": "center"}),
            html.Div(count_parts, style={"display": "flex", "alignItems": "center"}),
            html.Span("\u25bc", id="audit-arrow", style={
                "fontSize": "10px",
                "color": "#666",
                "transition": "transform 0.2s",
            }),
        ], id="audit-banner-header", style={
            "display": "flex",
            "justifyContent": "space-between",
            "alignItems": "center",
            "padding": "10px 16px",
            "cursor": "pointer",
            "borderRadius": "8px",
            "color": border_color,
        }),
        # Collapsible detail body
        dbc.Collapse(
            html.Div(issue_rows, style={
                "maxHeight": "300px",
                "overflowY": "auto",
                "padding": "8px 16px",
            }),
            id="audit-banner-collapse",
            is_open=False,
        ),
    ], id="audit-banner-container", style={
        "backgroundColor": "rgba(30, 34, 55, 0.98)",
        "border": f"1px solid {border_color}55",
        "borderLeft": f"4px solid {border_color}",
        "borderRadius": "8px",
        "marginBottom": "12px",
        "boxShadow": f"0 2px 12px rgba(0,0,0,0.4), inset 0 0 30px {border_color}08",
    })


# ── App layout ───────────────────────────────────────────────────────────────
def serve_layout():
    from etsy_dashboard import data_state as ds
    subtitle = (
        f"Oct 2025 -- Feb 2026  |  {ds.order_count} orders  |  "
        f"Profit: {ds.money(ds.real_profit)} ({ds.real_profit_margin:.1f}%)  |  "
        f"Cash: {ds.money(ds.bank_cash_on_hand)}"
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

            # Audit banner (rebuilt on every page load via serve_layout)
            _build_audit_banner(),

            # Page content (rendered by routing callback)
            html.Div(id="page-content"),

            # Toast notification container
            html.Div(id="toast-container"),

            # Hidden stores
            dcc.Store(id="editor-save-trigger", data=0),
        ], className="main-content"),
    ])


app.layout = serve_layout


# ── Audit banner expand/collapse callback ────────────────────────────────────
@app.callback(
    Output("audit-banner-collapse", "is_open"),
    Input("audit-banner-header", "n_clicks"),
    State("audit-banner-collapse", "is_open"),
    prevent_initial_call=True,
)
def _toggle_audit_banner(n_clicks, is_open):
    if n_clicks:
        return not is_open
    return is_open


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
