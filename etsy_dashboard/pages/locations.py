"""Locations page — Side-by-side Tulsa/Texas inventory boards."""
from dash import html, dcc
import dash_bootstrap_components as dbc
import plotly.graph_objects as go

from etsy_dashboard.theme import *
from etsy_dashboard.components.kpi import kpi_card
from etsy_dashboard.components.cards import section, make_chart
from etsy_dashboard.components.thumbnail import item_thumbnail
from etsy_dashboard import data_state as ds


def _build_item_card(name, category, qty, unit_cost, location, other_location):
    """Build a single item card with move button."""
    cat_color = CATEGORY_COLORS.get(category, GRAY)
    img_url = ds._IMAGE_URLS.get(name, "")
    stock_color = GREEN if qty > 2 else ORANGE if qty > 0 else RED

    return dbc.Card(dbc.CardBody([
        html.Div([
            item_thumbnail(name, img_url, size=60),
            html.Div([
                html.Div(name, style={"color": WHITE, "fontSize": "13px", "fontWeight": "600",
                                       "lineHeight": "1.3"}),
                html.Div([
                    html.Span("● ", style={"color": cat_color, "fontSize": "10px"}),
                    html.Span(category, style={"color": GRAY, "fontSize": "11px"}),
                    html.Span(f" · ${unit_cost:,.2f}/ea", style={"color": DARKGRAY, "fontSize": "11px"}),
                ]),
                html.Div([
                    html.Span(f"×{qty}", style={"color": stock_color, "fontWeight": "bold",
                                                   "fontFamily": "monospace", "fontSize": "16px"}),
                ], style={"marginTop": "4px"}),
            ], style={"marginLeft": "12px", "flex": "1", "minWidth": "0"}),
        ], style={"display": "flex", "alignItems": "flex-start"}),
    ], style={"padding": "10px"}),
    style={"borderLeft": f"3px solid {cat_color}"}, className="mb-2")


def _build_location_board(location_label, items_df, other_label):
    """Build a single location board column."""
    if len(items_df) == 0:
        return html.P("No items at this location.", style={"color": GRAY, "textAlign": "center", "padding": "20px"})

    # Aggregate by item name
    agg = items_df.groupby("name").agg(
        category=("category", "first"),
        qty=("qty", "sum"),
        total=("total", "sum"),
        price=("price", "first"),
    ).reset_index().sort_values("category")

    total_items = int(agg["qty"].sum())
    total_value = agg["total"].sum()

    cards = []
    for _, row in agg.iterrows():
        cards.append(_build_item_card(
            row["name"], row["category"], int(row["qty"]),
            row["price"], location_label, other_label,
        ))

    return html.Div([
        # Header
        html.Div([
            html.H5(location_label, style={"color": CYAN, "margin": "0", "fontWeight": "bold"}),
            html.Div([
                html.Span(f"{total_items} items", style={"color": GRAY, "fontSize": "12px"}),
                html.Span(" · ", style={"color": DARKGRAY}),
                html.Span(ds.money(total_value), style={"color": GREEN, "fontFamily": "monospace",
                                                          "fontSize": "13px", "fontWeight": "bold"}),
            ]),
        ], style={"marginBottom": "12px", "paddingBottom": "8px", "borderBottom": f"2px solid {CYAN}33"}),

        # Item cards
        html.Div(cards, style={"maxHeight": "600px", "overflowY": "auto"}),
    ])


def layout():
    """Build the Locations page."""
    # Get items for each location
    if len(ds.BIZ_INV_ITEMS) > 0:
        tulsa_df = ds.BIZ_INV_ITEMS[ds.BIZ_INV_ITEMS["location"] == "Tulsa, OK"]
        texas_df = ds.BIZ_INV_ITEMS[ds.BIZ_INV_ITEMS["location"] == "Texas"]
    else:
        import pandas as pd
        tulsa_df = pd.DataFrame()
        texas_df = pd.DataFrame()

    tulsa_count = int(tulsa_df["qty"].sum()) if len(tulsa_df) > 0 else 0
    texas_count = int(texas_df["qty"].sum()) if len(texas_df) > 0 else 0
    tulsa_value = tulsa_df["total"].sum() if len(tulsa_df) > 0 else 0
    texas_value = texas_df["total"].sum() if len(texas_df) > 0 else 0

    # Category breakdown comparison chart
    comp_fig = go.Figure()
    all_cats = sorted(set(
        list(ds.tulsa_by_cat.index) + list(ds.texas_by_cat.index)
    )) if len(ds.tulsa_by_cat) > 0 or len(ds.texas_by_cat) > 0 else []

    if all_cats:
        comp_fig.add_trace(go.Bar(
            x=all_cats,
            y=[ds.tulsa_by_cat.get(c, 0) for c in all_cats],
            name="Tulsa (TJ)", marker_color=CYAN,
        ))
        comp_fig.add_trace(go.Bar(
            x=all_cats,
            y=[ds.texas_by_cat.get(c, 0) for c in all_cats],
            name="Texas (Braden)", marker_color=ORANGE,
        ))
    make_chart(comp_fig, 300)
    comp_fig.update_layout(title="Category Comparison", barmode="group")

    # Monthly spend comparison
    spend_fig = go.Figure()
    all_months = sorted(set(
        list(ds.tulsa_monthly.index) + list(ds.texas_monthly.index)
    )) if len(ds.tulsa_monthly) > 0 or len(ds.texas_monthly) > 0 else []
    if all_months:
        spend_fig.add_trace(go.Bar(
            x=all_months,
            y=[ds.tulsa_monthly.get(m, 0) for m in all_months],
            name="Tulsa", marker_color=CYAN,
        ))
        spend_fig.add_trace(go.Bar(
            x=all_months,
            y=[ds.texas_monthly.get(m, 0) for m in all_months],
            name="Texas", marker_color=ORANGE,
        ))
    make_chart(spend_fig, 300)
    spend_fig.update_layout(title="Monthly Spending by Location", barmode="group")

    return html.Div([
        # KPI strip
        dbc.Row([
            dbc.Col(kpi_card("TULSA ITEMS", str(tulsa_count), CYAN,
                             f"Value: {ds.money(tulsa_value)}"), md=3),
            dbc.Col(kpi_card("TEXAS ITEMS", str(texas_count), ORANGE,
                             f"Value: {ds.money(texas_value)}"), md=3),
            dbc.Col(kpi_card("TULSA SPEND", ds.money(ds.tulsa_spend), CYAN,
                             f"{ds.tulsa_orders} orders"), md=3),
            dbc.Col(kpi_card("TEXAS SPEND", ds.money(ds.texas_spend), ORANGE,
                             f"{ds.texas_orders} orders"), md=3),
        ], className="g-2 mb-3"),

        # Side-by-side boards
        dbc.Row([
            dbc.Col([
                dbc.Card(dbc.CardBody([
                    _build_location_board("Tulsa, OK (TJ)", tulsa_df, "Texas"),
                ]), style={"borderTop": f"3px solid {CYAN}"}),
            ], md=6),
            dbc.Col([
                dbc.Card(dbc.CardBody([
                    _build_location_board("Texas (Braden)", texas_df, "Tulsa, OK"),
                ]), style={"borderTop": f"3px solid {ORANGE}"}),
            ], md=6),
        ], className="g-3 mb-3"),

        # Charts
        dbc.Row([
            dbc.Col(dbc.Card(dbc.CardBody(
                dcc.Graph(figure=comp_fig, config={"displayModeBar": False}))), md=6),
            dbc.Col(dbc.Card(dbc.CardBody(
                dcc.Graph(figure=spend_fig, config={"displayModeBar": False}))), md=6),
        ], className="g-3 mb-3"),

        # Move item controls
        section("Move Items Between Locations", [
            dbc.Row([
                dbc.Col([
                    dcc.Dropdown(
                        id="loc-move-item",
                        options=[{"label": row["display_name"], "value": row["display_name"]}
                                 for _, row in ds.STOCK_SUMMARY.iterrows()] if len(ds.STOCK_SUMMARY) > 0 else [],
                        placeholder="Select item to move...",
                        style={"backgroundColor": BG},
                    ),
                ], md=4),
                dbc.Col([
                    dcc.Dropdown(
                        id="loc-move-dest",
                        options=[
                            {"label": "Tulsa, OK", "value": "Tulsa, OK"},
                            {"label": "Texas", "value": "Texas"},
                        ],
                        placeholder="Destination...",
                        style={"backgroundColor": BG},
                    ),
                ], md=3),
                dbc.Col([
                    dbc.Input(id="loc-move-qty", type="number", value=1, min=1, placeholder="Qty"),
                ], md=2),
                dbc.Col([
                    dbc.Button("Move", id="loc-move-btn", color="primary", size="sm"),
                ], md=3),
            ], className="g-2"),
        ], CYAN),

        # Toast container for move actions
        html.Div(id="loc-toast"),
    ])
