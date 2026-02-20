"""Inventory page — Batch editor, stock table, image management."""
from dash import html, dcc, dash_table
import dash_bootstrap_components as dbc

from etsy_dashboard.theme import *
from etsy_dashboard.components.kpi import kpi_card
from etsy_dashboard.components.cards import section, make_chart
from etsy_dashboard.components.tables import stock_level_bar
from etsy_dashboard.components.thumbnail import item_thumbnail
from etsy_dashboard import data_state as ds
import plotly.graph_objects as go


def _build_stock_table():
    """Build the stock levels table with thumbnails and gauges."""
    if len(ds.STOCK_SUMMARY) == 0:
        return html.P("No inventory items yet. Upload receipts in Data Hub.",
                       style={"color": GRAY, "textAlign": "center", "padding": "40px"})

    rows = []
    for _, row in ds.STOCK_SUMMARY.iterrows():
        name = row["display_name"]
        cat = row["category"]
        cat_color = CATEGORY_COLORS.get(cat, GRAY)
        in_stock = int(row["in_stock"])
        total_purchased = int(row["total_purchased"])
        total_used = int(row["total_used"])
        unit_cost = row["unit_cost"]
        total_cost = row["total_cost"]
        img_url = ds._IMAGE_URLS.get(name, "")
        loc = row.get("location", "")

        stock_color = GREEN if in_stock > 2 else ORANGE if in_stock > 0 else RED
        stock_label = f"{in_stock}" if in_stock > 0 else "OUT"

        rows.append(html.Tr([
            html.Td(item_thumbnail(name, img_url, size=45), style={"width": "55px"}),
            html.Td([
                html.Div(name, style={"color": WHITE, "fontSize": "13px", "fontWeight": "600"}),
                html.Div([
                    html.Span("● ", style={"color": cat_color, "fontSize": "10px"}),
                    html.Span(cat, style={"color": GRAY, "fontSize": "11px"}),
                    html.Span(f" · {loc}", style={"color": DARKGRAY, "fontSize": "11px"}) if loc else None,
                ]),
            ]),
            html.Td([
                html.Span(stock_label, style={"color": stock_color, "fontWeight": "bold",
                                                "fontFamily": "monospace", "fontSize": "16px"}),
                html.Div(stock_level_bar(in_stock, total_purchased), style={"marginTop": "2px"}),
            ], style={"textAlign": "center", "width": "100px"}),
            html.Td(f"{total_purchased}", style={"color": GRAY, "textAlign": "center", "fontFamily": "monospace"}),
            html.Td(f"{total_used}", style={"color": GRAY, "textAlign": "center", "fontFamily": "monospace"}),
            html.Td(ds.money(unit_cost), style={"fontFamily": "monospace", "textAlign": "right", "fontSize": "12px"}),
            html.Td(ds.money(total_cost), style={"fontFamily": "monospace", "textAlign": "right",
                                                    "color": CYAN, "fontSize": "12px"}),
        ], className="stock-row"))

    return dbc.Table([
        html.Thead(html.Tr([
            html.Th("", style={"width": "55px"}),
            html.Th("Item"),
            html.Th("In Stock", style={"textAlign": "center"}),
            html.Th("Bought", style={"textAlign": "center"}),
            html.Th("Used", style={"textAlign": "center"}),
            html.Th("Unit $", style={"textAlign": "right"}),
            html.Th("Total $", style={"textAlign": "right"}),
        ])),
        html.Tbody(rows),
    ], striped=True, hover=True, size="sm", className="mb-0")


def _build_batch_editor():
    """Build the batch editor table for unreviewed inventory items."""
    unreviewed = []
    for inv in ds.INVOICES:
        for item in inv["items"]:
            key = (inv["order_num"], item["name"])
            if key not in ds._ITEM_DETAILS:
                item_name = item["name"]
                if item_name.startswith("Your package was left near the front door or porch."):
                    item_name = item_name.replace("Your package was left near the front door or porch.", "").strip()
                unreviewed.append({
                    "order_num": inv["order_num"],
                    "orig_name": item["name"],
                    "name": item_name[:60],
                    "category": ds.categorize_item(item_name),
                    "qty": item["qty"],
                    "price": f"${item['price']:.2f}",
                    "location": ds.classify_location(inv.get("ship_address", "")),
                })

    if not unreviewed:
        return dbc.Alert("All items have been reviewed and categorized!", color="success",
                         className="text-center")

    return html.Div([
        html.Div([
            html.Span(f"{len(unreviewed)} item(s) need review",
                       style={"color": ORANGE, "fontWeight": "bold", "fontSize": "14px"}),
            dbc.Button("Save All", id="inv-batch-save-btn", color="success", size="sm",
                       className="ms-auto"),
        ], style={"display": "flex", "alignItems": "center", "marginBottom": "10px"}),

        dash_table.DataTable(
            id="inv-batch-table",
            data=unreviewed,
            columns=[
                {"name": "Order", "id": "order_num", "editable": False},
                {"name": "Name", "id": "name", "editable": True},
                {"name": "Category", "id": "category", "editable": True,
                 "presentation": "dropdown"},
                {"name": "Qty", "id": "qty", "editable": True, "type": "numeric"},
                {"name": "Price", "id": "price", "editable": False},
                {"name": "Location", "id": "location", "editable": True,
                 "presentation": "dropdown"},
            ],
            dropdown={
                "category": {"options": [{"label": c, "value": c} for c in CATEGORY_OPTIONS]},
                "location": {"options": [
                    {"label": "Tulsa, OK", "value": "Tulsa, OK"},
                    {"label": "Texas", "value": "Texas"},
                    {"label": "Other", "value": "Other"},
                ]},
            },
            style_table={"overflowX": "auto"},
            style_cell={
                "backgroundColor": CARD,
                "color": WHITE,
                "border": f"1px solid #ffffff10",
                "fontSize": "12px",
                "padding": "8px",
            },
            style_header={
                "backgroundColor": BG,
                "color": GRAY,
                "fontWeight": "600",
                "textTransform": "uppercase",
                "fontSize": "11px",
            },
            style_data_conditional=[
                {"if": {"state": "active"}, "backgroundColor": f"{CYAN}15", "border": f"1px solid {CYAN}"},
            ],
            page_size=20,
            editable=True,
        ),
    ])


def layout():
    """Build the Inventory page."""
    skpi = ds._compute_stock_kpis()

    # Category breakdown chart
    cat_fig = go.Figure()
    if len(ds.biz_inv_by_category) > 0:
        cats = list(ds.biz_inv_by_category.index)
        vals = list(ds.biz_inv_by_category.values)
        colors = [CATEGORY_COLORS.get(c, GRAY) for c in cats]
        cat_fig.add_trace(go.Pie(
            labels=cats, values=vals,
            marker=dict(colors=colors),
            hole=0.45, textinfo="label+percent",
        ))
    make_chart(cat_fig, 300)
    cat_fig.update_layout(title="Inventory by Category")

    # Monthly inventory spend
    inv_spend_fig = go.Figure()
    if len(ds.monthly_inv_spend) > 0:
        inv_spend_fig.add_trace(go.Bar(
            x=ds.monthly_inv_spend.index,
            y=ds.monthly_inv_spend.values,
            marker_color=ORANGE,
        ))
    make_chart(inv_spend_fig, 250)
    inv_spend_fig.update_layout(title="Monthly Inventory Spending")

    return html.Div([
        # KPI strip
        dbc.Row([
            dbc.Col(kpi_card("IN STOCK", str(skpi["in_stock"]), GREEN, f"{skpi['unique']} unique items"), md=2),
            dbc.Col(kpi_card("TOTAL VALUE", ds.money(skpi["value"]), CYAN), md=2),
            dbc.Col(kpi_card("LOW STOCK", str(skpi["low"]), ORANGE, "1-2 remaining"), md=2),
            dbc.Col(kpi_card("OUT OF STOCK", str(skpi["oos"]), RED), md=2),
            dbc.Col(kpi_card("ORDERS", str(ds.inv_order_count), BLUE, ds.money(ds.total_inventory_cost)), md=2),
            dbc.Col(kpi_card("COGS", ds.money(ds.true_inventory_cost), TEAL,
                             f"excl personal & fees"), md=2),
        ], className="g-2 mb-3"),

        # Batch Editor
        section("Batch Item Editor", [_build_batch_editor()], ORANGE),

        # Stock Table + Charts
        dbc.Row([
            dbc.Col([
                section("Stock Levels", [_build_stock_table()], GREEN),
            ], md=8),
            dbc.Col([
                dbc.Card(dbc.CardBody(dcc.Graph(figure=cat_fig, config={"displayModeBar": False})), className="mb-3"),
                dbc.Card(dbc.CardBody(dcc.Graph(figure=inv_spend_fig, config={"displayModeBar": False}))),
            ], md=4),
        ], className="g-3 mb-3"),

        # Usage logger
        section("Log Usage", [
            dbc.Row([
                dbc.Col([
                    dcc.Dropdown(
                        id="inv-use-item",
                        options=[{"label": row["display_name"], "value": row["display_name"]}
                                 for _, row in ds.STOCK_SUMMARY.iterrows()] if len(ds.STOCK_SUMMARY) > 0 else [],
                        placeholder="Select item...",
                        style={"backgroundColor": BG},
                    ),
                ], md=6),
                dbc.Col([
                    dbc.Input(id="inv-use-qty", type="number", value=1, min=1, placeholder="Qty"),
                ], md=3),
                dbc.Col([
                    dbc.Button("Mark Used", id="inv-use-btn", color="warning", size="sm"),
                ], md=3),
            ], className="g-2"),
        ], ORANGE),

        # Image manager
        section("Image Manager", [
            dbc.Row([
                dbc.Col([
                    dbc.Input(id="inv-image-name", placeholder="Item name...", size="sm"),
                ], md=5),
                dbc.Col([
                    dbc.Input(id="inv-image-url", placeholder="Image URL...", size="sm"),
                ], md=5),
                dbc.Col([
                    dbc.Button("Save", id="inv-image-save-btn", color="primary", size="sm"),
                ], md=2),
            ], className="g-2"),
        ], CYAN),

        # Toast container
        html.Div(id="inv-save-toast"),
    ])
