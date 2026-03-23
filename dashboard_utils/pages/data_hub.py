"""Data Hub tab — upload & auto-update everything."""

import dash_bootstrap_components as dbc
from dash import dcc, html
from dashboard_utils.theme import *


def build_tab7_data_hub(active_store_tab=None):
    """Build the Data Hub tab — upload & auto-update everything."""
    # Lazy import to avoid circular dependency during bridge phase
    import etsy_dashboard as ed

    # Pull globals from the monolith
    strict_mode = ed.strict_mode
    _sm = strict_mode if isinstance(strict_mode, bool) else False

    # Functions
    _strict_banner = ed._strict_banner
    _get_existing_files = ed._get_existing_files
    _build_datahub_summary = ed._build_datahub_summary
    _build_data_coverage = ed._build_data_coverage
    _build_upload_zone = ed._build_upload_zone
    _build_reconciliation_panel = ed._build_reconciliation_panel
    _build_audit_report = ed._build_audit_report
    _build_manage_data_content = ed._build_manage_data_content
    _render_file_list = ed._render_file_list

    etsy_files = _get_existing_files("etsy")
    receipt_files = _get_existing_files("receipt")
    bank_files = _get_existing_files("bank")

    # Build store sub-tabs for Etsy section
    _store_tab_style = {
        "backgroundColor": "transparent", "color": GRAY, "border": "none",
        "borderBottom": f"2px solid transparent", "padding": "10px 18px",
        "fontSize": "13px", "fontWeight": "bold", "letterSpacing": "1px",
    }
    _store_tab_selected_style = {
        "backgroundColor": f"{CYAN}15", "color": WHITE, "border": "none",
        "borderBottom": f"2px solid {CYAN}", "padding": "10px 18px",
        "fontSize": "13px", "fontWeight": "bold", "letterSpacing": "1px",
    }
    _store_subtabs = []
    for _sk, _sl in STORES.items():
        _sc = STORE_COLORS.get(_sk, TEAL)
        _sel_style = {**_store_tab_selected_style, "borderBottom": f"2px solid {_sc}", "backgroundColor": f"{_sc}15"}
        _store_subtabs.append(dcc.Tab(
            label=_sl, value=f"dh-{_sk}",
            style=_store_tab_style, selected_style=_sel_style,
        ))

    return html.Div([
        # Strict mode banner
        _strict_banner("Data quality gaps are highlighted. Upload missing data to improve accuracy.") if _sm else html.Div(),

        # Title
        html.Div([
            html.H2("DATA HUB", style={"color": CYAN, "margin": "0", "fontSize": "22px",
                                         "letterSpacing": "2px"}),
            html.P("Upload files and auto-update all dashboard data. No restart needed.",
                   style={"color": GRAY, "margin": "4px 0 0 0", "fontSize": "13px"}),
        ], style={"marginBottom": "16px"}),

        # Summary KPI strip
        html.Div(id="datahub-summary-strip", children=[_build_datahub_summary()]),

        html.Hr(style={"border": "none", "borderTop": f"1px solid {DARKGRAY}33", "margin": "16px 0"}),

        # Data coverage panel — what's uploaded and what's missing
        _build_data_coverage(),

        # Hidden dcc.Store to hold active store (replaces old dropdown)
        dcc.Store(id="datahub-etsy-store-picker", data="keycomponentmfg"),

        # Store Sub-Tabs for Etsy
        html.Div([
            html.Div([
                html.Span("\U0001f3ea", style={"fontSize": "18px", "marginRight": "8px"}),
                html.Span("ETSY STORES", style={"color": WHITE, "fontSize": "14px", "fontWeight": "bold",
                                                   "letterSpacing": "1.5px"}),
            ], style={"marginBottom": "12px"}),
            dcc.Tabs(
                id="datahub-store-tabs",
                value=active_store_tab or "dh-keycomponentmfg",
                children=_store_subtabs,
                style={"borderBottom": f"1px solid {DARKGRAY}33"},
            ),
            html.Div(id="datahub-store-tab-content", style={"marginTop": "16px"}),
        ], style={
            "backgroundColor": f"{CARD}cc", "borderRadius": "12px", "padding": "18px",
            "border": f"1px solid {DARKGRAY}33", "marginBottom": "20px",
            "boxShadow": "0 4px 15px rgba(0,0,0,0.3), inset 0 1px 0 rgba(255,255,255,0.05)",
        }),

        # Shared Upload Zones: Receipts + Bank
        html.Div([
            html.Div([
                html.Span("\U0001f4e6", style={"fontSize": "16px", "marginRight": "8px"}),
                html.Span("SHARED DATA", style={"color": WHITE, "fontSize": "13px", "fontWeight": "bold",
                                                    "letterSpacing": "1px"}),
                html.Span("  \u2014 Receipts and bank statements apply to all stores",
                           style={"color": GRAY, "fontSize": "11px", "marginLeft": "8px"}),
            ], style={"marginBottom": "14px"}),
            html.Div([
                _build_upload_zone("receipt", "\U0001f4e6", "Receipt PDFs", PURPLE, ".pdf",
                                   "Upload Amazon/supplier invoice PDFs. Parses items and updates inventory."),
                _build_upload_zone("bank", "\U0001f3e6", "Bank Statements", CYAN, ".pdf,.csv",
                                   "Upload Capital One bank PDFs or CSV transaction downloads. Deduplicates automatically."),
            ], style={"display": "flex", "gap": "16px", "flexWrap": "wrap"}),
        ], style={"marginBottom": "20px"}),

        # Reconciliation panel
        html.Hr(style={"border": "none", "borderTop": f"1px solid {DARKGRAY}33", "margin": "0 0 16px 0"}),
        _build_reconciliation_panel(),

        # Audit Reconciliation Report
        html.Hr(style={"border": "none", "borderTop": f"1px solid {DARKGRAY}33", "margin": "16px 0"}),
        _build_audit_report(),

        # Pre-populate existing file lists (static on load)
        dcc.Store(id="datahub-init-trigger", data="init"),

        # EXPORT DATA
        html.Div([
            html.Div([
                html.Span("\U0001f4e5", style={"fontSize": "20px", "marginRight": "8px"}),
                html.Span("EXPORT DATA", style={"fontSize": "16px", "fontWeight": "bold", "color": GREEN,
                                                   "letterSpacing": "1.5px"}),
                html.Span("  \u2014 Download CSVs to verify in Excel or share with your accountant",
                           style={"color": GRAY, "fontSize": "12px", "marginLeft": "8px"}),
            ], style={"marginBottom": "14px"}),
            html.Div([
                html.Button(["\U0001f4ca  Etsy Transactions"], id="btn-download-etsy", n_clicks=0,
                            style={"backgroundColor": f"{TEAL}22", "color": TEAL,
                                   "border": f"1px solid {TEAL}55", "borderRadius": "8px",
                                   "padding": "10px 18px", "fontSize": "13px", "cursor": "pointer",
                                   "fontWeight": "bold"}),
                html.Button(["\U0001f3e6  Bank Transactions"], id="btn-download-bank", n_clicks=0,
                            style={"backgroundColor": f"{CYAN}22", "color": CYAN,
                                   "border": f"1px solid {CYAN}55", "borderRadius": "8px",
                                   "padding": "10px 18px", "fontSize": "13px", "cursor": "pointer",
                                   "fontWeight": "bold"}),
                html.Button(["\U0001f4e6  All Inventory Items"], id="btn-download-inventory", n_clicks=0,
                            style={"backgroundColor": f"{PURPLE}22", "color": PURPLE,
                                   "border": f"1px solid {PURPLE}55", "borderRadius": "8px",
                                   "padding": "10px 18px", "fontSize": "13px", "cursor": "pointer",
                                   "fontWeight": "bold"}),
                html.Button(["\U0001f4ca  Stock Summary"], id="btn-download-stock", n_clicks=0,
                            style={"backgroundColor": f"{ORANGE}22", "color": ORANGE,
                                   "border": f"1px solid {ORANGE}55", "borderRadius": "8px",
                                   "padding": "10px 18px", "fontSize": "13px", "cursor": "pointer",
                                   "fontWeight": "bold"}),
                html.Button(["\U0001f4b0  Profit & Loss"], id="btn-download-pl", n_clicks=0,
                            style={"backgroundColor": f"{GREEN}22", "color": GREEN,
                                   "border": f"1px solid {GREEN}55", "borderRadius": "8px",
                                   "padding": "10px 18px", "fontSize": "13px", "cursor": "pointer",
                                   "fontWeight": "bold"}),
            ], style={"display": "flex", "gap": "10px", "flexWrap": "wrap"}),
        ], style={
            "backgroundColor": CARD2, "borderRadius": "12px", "padding": "18px",
            "borderLeft": f"4px solid {GREEN}", "marginBottom": "20px",
            "boxShadow": "0 4px 15px rgba(0,0,0,0.3), inset 0 1px 0 rgba(255,255,255,0.05)",
        }),

        # MANAGE DATA (Delete)
        html.Div([
            html.Div([
                html.Span("\U0001f5d1\ufe0f", style={"fontSize": "20px", "marginRight": "8px"}),
                html.Span("MANAGE DATA", style={"fontSize": "16px", "fontWeight": "bold", "color": RED,
                                                    "letterSpacing": "1.5px"}),
                html.Span("  \u2014 Remove uploaded data by month or source",
                           style={"color": GRAY, "fontSize": "12px", "marginLeft": "8px"}),
            ], style={"marginBottom": "14px"}),
            html.Div(id="manage-data-content", children=_build_manage_data_content()),
            # Delete result feedback
            html.Div(id="manage-data-status", style={"marginTop": "10px"}),
        ], style={
            "backgroundColor": CARD2, "borderRadius": "12px", "padding": "18px",
            "borderLeft": f"4px solid {RED}", "marginBottom": "20px",
            "boxShadow": "0 4px 15px rgba(0,0,0,0.3), inset 0 1px 0 rgba(255,255,255,0.05)",
        }),

        # Confirmation modal for deletions
        dbc.Modal([
            dbc.ModalHeader(dbc.ModalTitle("Confirm Deletion", style={"color": RED}),
                            style={"backgroundColor": CARD, "borderBottom": f"1px solid {RED}44"}),
            dbc.ModalBody(id="delete-confirm-body",
                          style={"backgroundColor": CARD, "color": WHITE}),
            dbc.ModalFooter([
                html.Button("Cancel", id="delete-cancel-btn", n_clicks=0,
                            style={"backgroundColor": CARD2, "color": GRAY, "border": f"1px solid {DARKGRAY}",
                                   "borderRadius": "6px", "padding": "8px 20px", "cursor": "pointer"}),
                html.Button("Delete", id="delete-confirm-btn", n_clicks=0,
                            style={"backgroundColor": f"{RED}33", "color": RED, "border": f"1px solid {RED}",
                                   "borderRadius": "6px", "padding": "8px 20px", "cursor": "pointer",
                                   "fontWeight": "bold"}),
            ], style={"backgroundColor": CARD, "borderTop": f"1px solid {RED}44"}),
        ], id="delete-confirm-modal", is_open=False, centered=True),
        dcc.Store(id="delete-pending-action", data=None),

        # Activity Log
        html.Div([
            html.Div([
                html.Span("\U0001f4dd", style={"marginRight": "8px"}),
                html.Span("Activity Log", style={"fontWeight": "bold", "color": WHITE}),
            ], style={"marginBottom": "8px"}),
            html.Div(id="datahub-activity-log", children=[
                html.Div("No uploads yet this session.", style={
                    "color": DARKGRAY, "fontSize": "12px", "fontStyle": "italic"}),
            ], style={"maxHeight": "200px", "overflowY": "auto"}),
        ], style={
            "backgroundColor": CARD2, "borderRadius": "12px", "padding": "16px",
            "boxShadow": "0 4px 15px rgba(0,0,0,0.3), inset 0 1px 0 rgba(255,255,255,0.05)",
        }),

        # Hidden initial file-list populator
        html.Div(id="datahub-etsy-files-init", children=[_render_file_list(etsy_files, TEAL)],
                  style={"display": "none"}),
        html.Div(id="datahub-receipt-files-init", children=[_render_file_list(receipt_files, PURPLE)],
                  style={"display": "none"}),
        html.Div(id="datahub-bank-files-init", children=[_render_file_list(bank_files, CYAN)],
                  style={"display": "none"}),
    ], style={"padding": TAB_PADDING})
