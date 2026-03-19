"""Inventory page callbacks — batch save, quick add, usage, image saves.
Mirrors monolith callback IDs exactly."""
from dash import html, Input, Output, State, callback_context, no_update, ALL
import dash_bootstrap_components as dbc
import json

from etsy_dashboard import data_state as ds
from etsy_dashboard.theme import *


def register_callbacks(app):
    # ── Batch editor save (inv-batch-save-btn) ─────────────────────────────
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

    # ── Image URL save (inv-image-save-btn) ────────────────────────────────
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

    # ── Usage log (inv-use-btn — hidden, kept for compatibility) ───────────
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

    # ── Quick Add toggle (qa-toggle-btn → qa-panel) ───────────────────────
    @app.callback(
        Output("qa-panel", "style"),
        Input("qa-toggle-btn", "n_clicks"),
        State("qa-panel", "style"),
        prevent_initial_call=True,
    )
    def toggle_quick_add(n_clicks, current_style):
        if not n_clicks:
            return no_update
        style = dict(current_style) if current_style else {}
        if style.get("display") == "none":
            style["display"] = "block"
        else:
            style["display"] = "none"
        return style

    # ── Quick Add — add item (qa-add-btn) ──────────────────────────────────
    @app.callback(
        [Output("inv-save-toast", "children", allow_duplicate=True),
         Output("qa-status", "children")],
        Input("qa-add-btn", "n_clicks"),
        [State("qa-name", "value"),
         State("qa-category", "value"),
         State("qa-qty", "value"),
         State("qa-price", "value"),
         State("qa-location", "value")],
        prevent_initial_call=True,
    )
    def add_quick_item(n_clicks, name, category, qty, price, location):
        if not n_clicks or not name:
            return no_update, no_update
        qty = int(qty or 1)
        price = float(price or 0)
        qa_dict = {
            "item_name": name,
            "category": category or "Other",
            "qty": qty,
            "unit_price": price,
            "location": location or "Tulsa, OK",
            "source": "Manual",
        }
        try:
            ds._save_quick_add(qa_dict)
            qa_dict["id"] = len(ds._QUICK_ADDS) + 9000
            ds._QUICK_ADDS.insert(0, qa_dict)
            ds._recompute_stock_summary()
            toast = dbc.Toast(
                f"Added {qty}x {name} ({category})",
                header="Quick Add Saved",
                icon="success",
                duration=3000,
                style={"position": "fixed", "top": 20, "right": 20, "zIndex": 9999},
            )
            return toast, f"Added {name}"
        except Exception as e:
            toast = dbc.Toast(
                f"Error: {e}",
                header="Quick Add Failed",
                icon="danger",
                duration=4000,
                style={"position": "fixed", "top": 20, "right": 20, "zIndex": 9999},
            )
            return toast, ""

    # ── Quick Add — delete item (del-qa-btn pattern) ───────────────────────
    @app.callback(
        Output("inv-save-toast", "children", allow_duplicate=True),
        Input({"type": "del-qa-btn", "index": ALL}, "n_clicks"),
        prevent_initial_call=True,
    )
    def delete_quick_item(n_clicks_list):
        if not callback_context.triggered:
            return no_update
        triggered = callback_context.triggered[0]
        if triggered["value"] is None or triggered["value"] == 0:
            return no_update
        try:
            btn_info = json.loads(triggered["prop_id"].split(".")[0])
            qa_id = int(btn_info["index"])
        except (json.JSONDecodeError, KeyError, ValueError):
            return no_update
        try:
            ds._delete_quick_add(qa_id)
            ds._QUICK_ADDS[:] = [qa for qa in ds._QUICK_ADDS if qa.get("id") != qa_id]
            ds._recompute_stock_summary()
            return dbc.Toast(
                "Quick-add item deleted",
                header="Deleted",
                icon="warning",
                duration=3000,
                style={"position": "fixed", "top": 20, "right": 20, "zIndex": 9999},
            )
        except Exception as e:
            return dbc.Toast(
                f"Error: {e}",
                header="Delete Failed",
                icon="danger",
                duration=4000,
                style={"position": "fixed", "top": 20, "right": 20, "zIndex": 9999},
            )
