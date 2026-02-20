"""Inventory page callbacks — batch save, stock filter, usage, image saves."""
from dash import html, Input, Output, State, callback_context, no_update, ALL, MATCH
import dash_bootstrap_components as dbc
import json

from etsy_dashboard import data_state as ds
from etsy_dashboard.theme import *


def register_callbacks(app):
    # ── Batch editor save ──────────────────────────────────────────────────
    @app.callback(
        Output("inv-save-toast", "children", allow_duplicate=True),
        Input("inv-batch-save-btn", "n_clicks"),
        State("inv-batch-table", "data"),
        prevent_initial_call=True,
    )
    def save_batch_edits(n_clicks, table_data):
        if not n_clicks or not table_data:
            return no_update
        saved = 0
        for row in table_data:
            order_num = row.get("order_num", "")
            item_name = row.get("orig_name", row.get("name", ""))
            display_name = row.get("name", item_name)
            category = row.get("category", "Other")
            true_qty = int(row.get("qty", 1))
            location = row.get("location", "")
            key = (order_num, item_name)
            details = [{
                "display_name": display_name,
                "category": category,
                "true_qty": true_qty,
                "location": location,
            }]
            ds._save_item_details(order_num, item_name, details)
            ds._ITEM_DETAILS[key] = details
            saved += 1
        ds._recompute_stock_summary()
        return dbc.Toast(
            f"Saved {saved} item(s)",
            header="Inventory Updated",
            icon="success",
            duration=3000,
            style={"position": "fixed", "top": 20, "right": 20, "zIndex": 9999},
        )

    # ── Image URL save ────────────────────────────────────────────────────
    @app.callback(
        Output("inv-save-toast", "children", allow_duplicate=True),
        Input("inv-image-save-btn", "n_clicks"),
        State("inv-image-name", "value"),
        State("inv-image-url", "value"),
        prevent_initial_call=True,
    )
    def save_image_url(n_clicks, name, url):
        if not n_clicks or not name or not url:
            return no_update
        ds.save_image_override(name, url)
        ds._IMAGE_URLS[name] = url
        return dbc.Toast(
            f"Image saved for {name}",
            header="Image Updated",
            icon="success",
            duration=3000,
            style={"position": "fixed", "top": 20, "right": 20, "zIndex": 9999},
        )

    # ── Usage log (mark item as used) ─────────────────────────────────────
    @app.callback(
        Output("inv-save-toast", "children", allow_duplicate=True),
        Input("inv-use-btn", "n_clicks"),
        State("inv-use-item", "value"),
        State("inv-use-qty", "value"),
        prevent_initial_call=True,
    )
    def log_usage(n_clicks, item_name, qty):
        if not n_clicks or not item_name:
            return no_update
        qty = int(qty or 1)
        ds._save_usage(item_name, qty, "Used in production")
        ds._usage_by_item[item_name] = ds._usage_by_item.get(item_name, 0) + qty
        ds._recompute_stock_summary()
        return dbc.Toast(
            f"Logged {qty}x {item_name} as used",
            header="Usage Recorded",
            icon="info",
            duration=3000,
            style={"position": "fixed", "top": 20, "right": 20, "zIndex": 9999},
        )
