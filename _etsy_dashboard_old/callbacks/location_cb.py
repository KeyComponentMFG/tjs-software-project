"""Location page callbacks — move/split items between Tulsa and Texas."""
from dash import html, Input, Output, State, no_update
import dash_bootstrap_components as dbc

from etsy_dashboard import data_state as ds
from etsy_dashboard.theme import *


def register_callbacks(app):
    # ── Move item to other location ───────────────────────────────────────
    @app.callback(
        Output("loc-toast", "children"),
        Input("loc-move-btn", "n_clicks"),
        State("loc-move-item", "value"),
        State("loc-move-dest", "value"),
        State("loc-move-qty", "value"),
        prevent_initial_call=True,
    )
    def move_item(n_clicks, item_name, destination, qty):
        if not n_clicks or not item_name or not destination:
            return no_update
        qty = int(qty or 1)
        ds.save_location_override("", item_name, [{"location": destination, "qty": qty}])
        return dbc.Toast(
            f"Moved {qty}x {item_name} to {destination}",
            header="Location Updated",
            icon="success",
            duration=3000,
            style={"position": "fixed", "top": 20, "right": 20, "zIndex": 9999},
        )
