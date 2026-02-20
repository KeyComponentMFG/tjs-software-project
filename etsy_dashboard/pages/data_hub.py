"""Data Hub page — File uploads for Etsy CSV, bank PDF, receipt PDF."""
import os
from dash import html, dcc
import dash_bootstrap_components as dbc

from etsy_dashboard.theme import *
from etsy_dashboard.components.cards import section
from etsy_dashboard import data_state as ds


def _upload_zone(upload_id, title, icon, description, accept, color=CYAN):
    """Build a single upload dropzone."""
    return dbc.Card(dbc.CardBody([
        html.Div([
            html.Span(icon, style={"fontSize": "32px", "marginBottom": "8px", "display": "block"}),
            html.H5(title, style={"color": color, "fontWeight": "bold", "marginBottom": "4px"}),
            html.P(description, style={"color": GRAY, "fontSize": "12px", "marginBottom": "12px"}),
        ], style={"textAlign": "center"}),
        dcc.Upload(
            id=upload_id,
            children=html.Div([
                html.Span("Drag & Drop or "),
                html.A("Click to Browse", style={"color": CYAN, "textDecoration": "underline"}),
            ], style={"color": GRAY, "fontSize": "13px"}),
            style={
                "width": "100%", "borderWidth": "2px", "borderStyle": "dashed",
                "borderColor": f"{color}44", "borderRadius": "10px",
                "textAlign": "center", "padding": "20px",
                "cursor": "pointer",
            },
            accept=accept,
            className="upload-zone",
        ),
    ]), style={"borderTop": f"3px solid {color}"}, className="mb-3")


def _file_list(directory, extension, color=GRAY):
    """List existing files in a directory."""
    if not os.path.isdir(directory):
        return html.P("No files yet.", style={"color": DARKGRAY, "fontSize": "12px"})
    files = sorted([f for f in os.listdir(directory) if f.lower().endswith(extension)])
    if not files:
        return html.P("No files yet.", style={"color": DARKGRAY, "fontSize": "12px"})
    return html.Div([
        html.Div([
            html.Span("📄 " if extension == ".csv" else "📕 ", style={"fontSize": "12px"}),
            html.Span(f, style={"color": color, "fontSize": "12px"}),
        ], style={"padding": "2px 0"})
        for f in files
    ])


def layout():
    """Build the Data Hub page."""
    etsy_dir = os.path.join(ds.BASE_DIR, "data", "etsy_statements")
    bank_dir = os.path.join(ds.BASE_DIR, "data", "bank_statements")
    invoice_dir = os.path.join(ds.BASE_DIR, "data", "invoices")

    return html.Div([
        html.P("Upload your financial documents to keep the dashboard up to date.",
               style={"color": GRAY, "fontSize": "13px", "marginBottom": "16px"}),

        # Upload zones
        dbc.Row([
            dbc.Col([
                _upload_zone(
                    "upload-etsy-csv",
                    "Etsy Statement CSV",
                    "\U0001f4ca",
                    "Monthly CSV from Etsy Shop Manager → Finances → Payment Account → Monthly Statement",
                    ".csv",
                    GREEN,
                ),
                html.Div(id="upload-etsy-status"),
                section("Uploaded Etsy Statements", [
                    _file_list(etsy_dir, ".csv", GREEN),
                    html.P(f"{len(ds.DATA)} transactions loaded",
                           style={"color": DARKGRAY, "fontSize": "11px", "marginTop": "6px"}),
                ], GREEN),
            ], md=4),

            dbc.Col([
                _upload_zone(
                    "upload-bank-pdf",
                    "Bank Statement PDF",
                    "\U0001f3e6",
                    "Capital One checking account monthly PDF statements",
                    ".pdf",
                    BLUE,
                ),
                html.Div(id="upload-bank-status"),
                section("Uploaded Bank Statements", [
                    _file_list(bank_dir, ".pdf", BLUE),
                    html.P(f"{ds.bank_statement_count} statement(s), {len(ds.BANK_TXNS)} transactions",
                           style={"color": DARKGRAY, "fontSize": "11px", "marginTop": "6px"}),
                ], BLUE),
            ], md=4),

            dbc.Col([
                _upload_zone(
                    "upload-receipt-pdf",
                    "Inventory Receipt PDF",
                    "\U0001f9fe",
                    "Amazon/supplier order confirmations — PDF invoices or receipts",
                    ".pdf",
                    ORANGE,
                ),
                html.Div(id="upload-receipt-status"),
                section("Uploaded Receipts", [
                    _file_list(invoice_dir, ".pdf", ORANGE),
                    html.P(f"{len(ds.INVOICES)} orders loaded, {len(ds.INV_ITEMS)} items",
                           style={"color": DARKGRAY, "fontSize": "11px", "marginTop": "6px"}),
                ], ORANGE),
            ], md=4),
        ], className="g-3 mb-3"),

        # Data summary
        section("Data Summary", [
            dbc.Row([
                dbc.Col([
                    html.Div([
                        html.Span("Etsy: ", style={"color": GRAY, "fontSize": "13px"}),
                        html.Span(f"{len(ds.DATA)} transactions, {ds.order_count} orders",
                                  style={"color": GREEN, "fontSize": "13px"}),
                    ]),
                    html.Div([
                        html.Span("Bank: ", style={"color": GRAY, "fontSize": "13px"}),
                        html.Span(f"{len(ds.BANK_TXNS)} transactions from {ds.bank_statement_count} statements",
                                  style={"color": BLUE, "fontSize": "13px"}),
                    ]),
                    html.Div([
                        html.Span("Inventory: ", style={"color": GRAY, "fontSize": "13px"}),
                        html.Span(f"{len(ds.INVOICES)} orders, {len(ds.INV_ITEMS)} items, "
                                  f"{ds.money(ds.total_inventory_cost)} total",
                                  style={"color": ORANGE, "fontSize": "13px"}),
                    ]),
                ]),
            ]),
        ], CYAN),
    ])
