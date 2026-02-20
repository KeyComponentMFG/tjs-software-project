"""Reusable table builders."""
from dash import html, dcc
import dash_bootstrap_components as dbc
from etsy_dashboard.theme import *
from etsy_dashboard.components.thumbnail import item_thumbnail
from etsy_dashboard.data_state import _IMAGE_URLS, _usage_by_item, money


def stock_level_bar(in_stock, total_purchased):
    """Visual stock gauge bar — 8px height, gradient fill."""
    if total_purchased <= 0:
        return html.Div(style={"width": "80px", "display": "inline-block"})
    pct = max(0, min(100, (in_stock / total_purchased) * 100))
    color = GREEN if pct > 50 else (ORANGE if pct > 20 else RED)
    return html.Div([
        html.Div(style={"width": f"{max(pct, 4)}%", "height": "8px",
                         "background": f"linear-gradient(90deg, {color}88, {color})",
                         "borderRadius": "4px",
                         "transition": "width 0.3s ease"}),
    ], style={"width": "80px", "height": "8px", "backgroundColor": "#0d0d1a",
              "borderRadius": "4px", "display": "inline-block", "verticalAlign": "middle",
              "overflow": "hidden"})
