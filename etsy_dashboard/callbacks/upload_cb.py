"""Data Hub upload callbacks — Etsy CSV, bank PDF, receipt PDF."""
import base64
import os
import io

from dash import html, Input, Output, State, no_update
import dash_bootstrap_components as dbc
import pandas as pd

from etsy_dashboard import data_state as ds
from etsy_dashboard.theme import *


def register_callbacks(app):
    # ── Etsy CSV Upload ───────────────────────────────────────────────────
    @app.callback(
        Output("upload-etsy-status", "children"),
        Input("upload-etsy-csv", "contents"),
        State("upload-etsy-csv", "filename"),
        prevent_initial_call=True,
    )
    def upload_etsy_csv(contents, filename):
        if contents is None:
            return no_update
        content_type, content_string = contents.split(",")
        decoded = base64.b64decode(content_string)

        ok, msg, df = ds._validate_etsy_csv(decoded)
        if not ok:
            return dbc.Alert(f"Invalid CSV: {msg}", color="danger")

        # Check for overlap
        has_overlap, overlap_file, overlap_msg = ds._check_etsy_csv_overlap(df, filename)
        if has_overlap and overlap_file and "Replacing" not in overlap_msg:
            return dbc.Alert(f"Warning: {overlap_msg}", color="warning")

        # Save file
        etsy_dir = os.path.join(ds.BASE_DIR, "data", "etsy_statements")
        os.makedirs(etsy_dir, exist_ok=True)
        out_path = os.path.join(etsy_dir, filename)
        with open(out_path, "wb") as f:
            f.write(decoded)

        # Reload
        result = ds._reload_etsy_data()
        ds._cascade_reload("etsy")

        return dbc.Alert(
            f"Uploaded {filename} — {msg}. "
            f"Now {result['transactions']} transactions, {result['orders']} orders.",
            color="success",
        )

    # ── Bank PDF Upload ───────────────────────────────────────────────────
    @app.callback(
        Output("upload-bank-status", "children"),
        Input("upload-bank-pdf", "contents"),
        State("upload-bank-pdf", "filename"),
        prevent_initial_call=True,
    )
    def upload_bank_pdf(contents, filename):
        if contents is None:
            return no_update
        content_type, content_string = contents.split(",")
        decoded = base64.b64decode(content_string)

        is_dup, dup_file = ds._check_bank_pdf_duplicate(decoded, filename)
        if is_dup:
            return dbc.Alert(f"Duplicate of {dup_file}", color="warning")

        bank_dir = os.path.join(ds.BASE_DIR, "data", "bank_statements")
        os.makedirs(bank_dir, exist_ok=True)
        out_path = os.path.join(bank_dir, filename)
        with open(out_path, "wb") as f:
            f.write(decoded)

        result = ds._reload_bank_data()
        ds._cascade_reload("bank")

        return dbc.Alert(
            f"Uploaded {filename} — {result['transactions']} transactions from "
            f"{result['statements']} statement(s). Net cash: {ds.money(result['net_cash'])}",
            color="success",
        )

    # ── Receipt PDF Upload ────────────────────────────────────────────────
    @app.callback(
        Output("upload-receipt-status", "children"),
        Input("upload-receipt-pdf", "contents"),
        State("upload-receipt-pdf", "filename"),
        prevent_initial_call=True,
    )
    def upload_receipt_pdf(contents, filename):
        if contents is None:
            return no_update
        content_type, content_string = contents.split(",")
        decoded = base64.b64decode(content_string)

        import sys
        sys.path.insert(0, ds.BASE_DIR)
        from _parse_invoices import parse_pdf_file

        # Save temp file for parsing
        import tempfile
        with tempfile.NamedTemporaryFile(suffix=".pdf", delete=False) as tmp:
            tmp.write(decoded)
            tmp_path = tmp.name

        try:
            order = parse_pdf_file(tmp_path)
        except Exception as e:
            return dbc.Alert(f"Could not parse PDF: {e}", color="danger")
        finally:
            try:
                os.unlink(tmp_path)
            except Exception:
                pass

        if not order:
            return dbc.Alert("No orders found in PDF", color="warning")

        # Wrap single order dict into a list
        orders = [order] if isinstance(order, dict) else order

        # Save receipt to invoices dir
        receipts_dir = os.path.join(ds.BASE_DIR, "data", "invoices")
        os.makedirs(receipts_dir, exist_ok=True)
        dest = os.path.join(receipts_dir, filename)
        with open(dest, "wb") as f:
            f.write(decoded)

        results = []
        for o in orders:
            result = ds._reload_inventory_data(o)
            results.append(result)

        ds._cascade_reload("inventory")
        total_items = sum(r["item_count"] for r in results)

        return dbc.Alert(
            f"Uploaded {filename} — {len(results)} order(s), {total_items} items, "
            f"${sum(r['grand_total'] for r in results):,.2f} total",
            color="success",
        )
