"""Overview page — KPI strip + Health Checks + P&L + Monthly chart."""
import os
from dash import html, dcc
import dash_bootstrap_components as dbc
import plotly.graph_objects as go

from etsy_dashboard.theme import *
from etsy_dashboard.components.kpi import kpi_pill
from etsy_dashboard.components.cards import section, row_item, make_chart
from etsy_dashboard import data_state as ds


def _build_health_checks():
    """Scan all data sources and return a health panel with every issue found."""
    todos = []

    # 1. Missing Etsy CSVs
    import re as _re
    etsy_dir = os.path.join(ds.BASE_DIR, "data", "etsy_statements")
    etsy_months_found = set()
    if os.path.isdir(etsy_dir):
        for fn in os.listdir(etsy_dir):
            if fn.endswith(".csv"):
                m = _re.search(r"(\d{4})_(\d{2})", fn)
                if m:
                    etsy_months_found.add(f"{m.group(1)}-{m.group(2)}")
    if etsy_months_found:
        all_months = sorted(etsy_months_found)
        first_y, first_m = map(int, all_months[0].split("-"))
        last_y, last_m = map(int, all_months[-1].split("-"))
        expected = set()
        y, mo = first_y, first_m
        while (y, mo) <= (last_y, last_m):
            expected.add(f"{y}-{mo:02d}")
            mo += 1
            if mo > 12:
                mo = 1
                y += 1
        missing_etsy = sorted(expected - etsy_months_found)
        for mm in missing_etsy:
            todos.append((1, "\U0001f4c4", RED, f"Missing Etsy statement: {mm}",
                          f"Upload etsy_statement_{mm.replace('-', '_')}.csv in Data Hub"))
    if not etsy_months_found:
        todos.append((1, "\U0001f4c4", RED, "No Etsy statements uploaded",
                      "Go to Data Hub and upload your Etsy CSV statements"))

    # 2. Missing bank statements
    if ds.bank_statement_count == 0:
        todos.append((1, "\U0001f3e6", RED, "No bank statements uploaded",
                      "Go to Data Hub and upload your Capital One PDF statements"))
    elif ds.bank_statement_count < 3:
        todos.append((2, "\U0001f3e6", ORANGE, f"Only {ds.bank_statement_count} bank statement(s)",
                      "Upload more bank statements for better cash flow tracking"))

    # 3. No receipts
    if len(ds.INVOICES) == 0:
        todos.append((1, "\U0001f9fe", RED, "No inventory receipts uploaded",
                      "Upload Amazon/supplier invoices in Data Hub to track COGS"))

    # 4. Unreviewed inventory items
    unreviewed = []
    for inv in ds.INVOICES:
        for item in inv["items"]:
            key = (inv["order_num"], item["name"])
            if key not in ds._ITEM_DETAILS:
                unreviewed.append(item["name"][:40])
    if unreviewed:
        count = len(unreviewed)
        examples = ", ".join(unreviewed[:3])
        todos.append((2, "\u270f\ufe0f", ORANGE,
                      f"{count} inventory item(s) need naming/categorizing",
                      f"Go to Inventory tab editor. Examples: {examples}{'...' if count > 3 else ''}"))

    # 5. Items without images
    no_image = []
    if len(ds.STOCK_SUMMARY) > 0:
        for _, row in ds.STOCK_SUMMARY.iterrows():
            name = row["display_name"]
            if not ds._IMAGE_URLS.get(name, ""):
                no_image.append(name[:35])
    if no_image:
        count = len(no_image)
        todos.append((3, "\U0001f5bc\ufe0f", CYAN,
                      f"{count} product(s) missing images",
                      f"Add image URLs in Inventory tab. Examples: {', '.join(no_image[:3])}{'...' if count > 3 else ''}"))

    # 6. Out-of-stock items
    if len(ds.STOCK_SUMMARY) > 0:
        oos = ds.STOCK_SUMMARY[ds.STOCK_SUMMARY["in_stock"] <= 0]
        if len(oos) > 0:
            oos_names = list(oos["display_name"].values[:5])
            todos.append((2, "\U0001f6a8", RED,
                          f"{len(oos)} item(s) out of stock",
                          f"Reorder needed: {', '.join(oos_names)}{'...' if len(oos) > 5 else ''}"))

    # 7. Low stock
    if len(ds.STOCK_SUMMARY) > 0:
        low = ds.STOCK_SUMMARY[ds.STOCK_SUMMARY["in_stock"].between(1, 2)]
        if len(low) > 0:
            low_names = list(low["display_name"].values[:5])
            todos.append((3, "\u26a0", ORANGE,
                          f"{len(low)} item(s) low stock (1-2 left)",
                          f"Running low: {', '.join(low_names)}{'...' if len(low) > 5 else ''}"))

    # 8. Receipt vs Bank gap
    _bank_amz = ds.bank_by_cat.get("Amazon Inventory", 0)
    _cogs_gap = max(0, ds.true_inventory_cost - _bank_amz)
    if _cogs_gap > 50:
        todos.append((2, "\U0001f50d", ORANGE,
                      f"${_cogs_gap:,.0f} inventory spending not visible in bank",
                      f"Receipts: ${ds.true_inventory_cost:,.0f}. Bank: ${_bank_amz:,.0f}. Gap is likely Discover card."))

    # 9. Etsy balance gap
    if abs(ds.etsy_csv_gap) > 5:
        todos.append((2, "\U0001f4b1", ORANGE,
                      f"Etsy balance gap: ${abs(ds.etsy_csv_gap):,.2f}",
                      f"Reported: ${ds.etsy_balance:,.2f} vs Calculated: ${ds.etsy_balance_calculated:,.2f}."))

    # 10. Draw imbalance
    if ds.draw_diff > 50:
        todos.append((3, "\U0001f91d", ORANGE,
                      f"Owner draw imbalance: ${ds.draw_diff:,.0f} ({ds.draw_owed_to} is owed)",
                      f"TJ: ${ds.tulsa_draw_total:,.0f} vs Braden: ${ds.texas_draw_total:,.0f}."))

    todos.sort(key=lambda x: x[0])

    if not todos:
        return dbc.Card(dbc.CardBody([
            html.Div([
                html.Span("\u2705", style={"fontSize": "20px", "marginRight": "8px"}),
                html.Span("ALL CLEAR", style={"color": GREEN, "fontWeight": "bold",
                                                "fontSize": "15px", "letterSpacing": "1.5px"}),
            ], style={"marginBottom": "6px"}),
            html.P("All data sources are connected, up to date, and balanced.",
                   style={"color": GRAY, "fontSize": "13px", "margin": "0"}),
        ]), style={"borderLeft": f"4px solid {GREEN}"}, className="mb-3")

    # Build todo rows
    todo_rows = []
    for pri, icon, color, title, detail in todos:
        pri_badge = {1: ("CRITICAL", RED), 2: ("ACTION", ORANGE), 3: ("INFO", CYAN)}[pri]
        todo_rows.append(html.Div([
            html.Div([
                html.Span(icon, style={"fontSize": "16px", "width": "24px", "textAlign": "center",
                                        "flexShrink": "0"}),
                dbc.Badge(pri_badge[0], color="", style={
                    "fontSize": "9px", "backgroundColor": f"{pri_badge[1]}22",
                    "color": pri_badge[1], "letterSpacing": "0.5px"}),
                html.Span(title, style={"color": WHITE, "fontSize": "13px", "fontWeight": "600",
                                         "flex": "1"}),
            ], style={"display": "flex", "alignItems": "center", "gap": "8px"}),
            html.P(detail, style={"color": GRAY, "fontSize": "11px", "margin": "3px 0 0 32px",
                                   "lineHeight": "1.4"}),
        ], style={"padding": "8px 10px", "borderBottom": "1px solid #ffffff08"}))

    critical_count = sum(1 for t in todos if t[0] == 1)

    return dbc.Card(dbc.CardBody([
        html.Div([
            html.Span("\U0001f4cb", style={"fontSize": "18px", "marginRight": "8px"}),
            html.Span("DASHBOARD HEALTH", style={"fontSize": "14px", "fontWeight": "bold",
                                                    "color": RED if critical_count else ORANGE,
                                                    "letterSpacing": "1.5px"}),
            html.Span(f"  {len(todos)} issue{'s' if len(todos) != 1 else ''}",
                       style={"color": GRAY, "fontSize": "12px", "marginLeft": "8px"}),
        ], style={"marginBottom": "8px", "display": "flex", "alignItems": "center"}),
        html.Div(todo_rows, style={"maxHeight": "300px", "overflowY": "auto",
                                     "borderRadius": "8px"}),
    ]), style={"borderLeft": f"4px solid {RED if critical_count else ORANGE}"}, className="mb-3")


def _build_pl_row(label, amount_str, color, bold=False, border=False):
    """Return a single P&L waterfall row."""
    row_style = {
        "display": "flex", "justifyContent": "space-between",
        "padding": "4px 0",
        "fontWeight": "bold" if bold else "normal",
    }
    if border:
        row_style["borderTop"] = f"1px solid {DARKGRAY}44"
        row_style["marginTop"] = "2px"
        row_style["paddingTop"] = "6px"
    return html.Div([
        html.Span(label, style={"color": color if bold else GRAY, "flex": "1", "fontSize": "13px"}),
        html.Span(amount_str, style={"color": color, "fontFamily": "monospace",
                                      "fontWeight": "bold", "fontSize": "13px"}),
    ], style=row_style)


def layout():
    """Build the Overview page."""
    _total_deductions = ds.total_fees + ds.total_shipping_cost + ds.total_marketing + ds.total_refunds + ds.total_taxes + ds.total_buyer_fees

    # Draw text
    if ds.draw_diff > 0:
        _draw_text = f"${ds.bank_owner_draw_total:,.2f}"
    else:
        _draw_text = f"${ds.bank_owner_draw_total:,.2f}"

    # Monthly chart
    monthly_fig = go.Figure()
    monthly_fig.add_trace(go.Bar(
        x=ds.months_sorted,
        y=[ds.monthly_sales.get(m, 0) for m in ds.months_sorted],
        name="Gross Sales", marker_color=GREEN,
    ))
    monthly_fig.add_trace(go.Bar(
        x=ds.months_sorted,
        y=[-ds.monthly_fees.get(m, 0) for m in ds.months_sorted],
        name="Fees", marker_color=RED,
    ))
    monthly_fig.add_trace(go.Bar(
        x=ds.months_sorted,
        y=[-ds.monthly_shipping.get(m, 0) for m in ds.months_sorted],
        name="Shipping", marker_color=ORANGE,
    ))
    monthly_fig.add_trace(go.Bar(
        x=ds.months_sorted,
        y=[-ds.monthly_marketing.get(m, 0) for m in ds.months_sorted],
        name="Marketing", marker_color=PURPLE,
    ))
    monthly_fig.add_trace(go.Scatter(
        x=ds.months_sorted,
        y=[ds.monthly_net_revenue.get(m, 0) for m in ds.months_sorted],
        name="Net Revenue", line=dict(color=CYAN, width=3),
        mode="lines+markers",
    ))
    make_chart(monthly_fig, 380)
    monthly_fig.update_layout(
        title="Monthly Revenue vs Costs",
        barmode="relative",
        xaxis_title="Month",
    )

    return html.Div([
        # KPI Strip
        dbc.Row([
            dbc.Col(kpi_pill("\U0001f4b0", "PROFIT", f"${ds.full_profit:,.2f}", GREEN,
                             f"{ds.full_profit_margin:.1f}% margin",
                             f"Cash {ds.money(ds.bank_cash_on_hand)} + Draws {ds.money(ds.bank_owner_draw_total)} - Outside-bank COGS {ds.money(ds.receipt_cogs_outside_bank)}"), width="auto"),
            dbc.Col(kpi_pill("\U0001f3e2", "CASH ON HAND", f"${ds.bank_cash_on_hand:,.2f}", CYAN,
                             f"Bank {ds.money(ds.bank_net_cash)} + Etsy {ds.money(ds.etsy_balance)}"), width="auto"),
            dbc.Col(kpi_pill("\U0001f4b3", "DEBT", f"${ds.bb_cc_balance:,.2f}", RED,
                             f"Best Buy CC ({ds.money(ds.bb_cc_available)} avail)",
                             f"Limit {ds.money(ds.bb_cc_limit)}. Charged {ds.money(ds.bb_cc_total_charged)}. Paid {ds.money(ds.bb_cc_total_paid)}."), width="auto"),
            dbc.Col(kpi_pill("\U0001f91d", "DRAWS", _draw_text, ORANGE,
                             f"TJ {ds.money(ds.tulsa_draw_total)} / Braden {ds.money(ds.texas_draw_total)}",
                             f"{ds.draw_owed_to} owed {ds.money(ds.draw_diff)}"), width="auto"),
            dbc.Col(kpi_pill("\U0001f4ca", "GROSS SALES", f"${ds.gross_sales:,.2f}", TEAL,
                             f"{ds.order_count} orders @ {ds.money(ds.avg_order)} avg"), width="auto"),
            dbc.Col(kpi_pill("\u2702\ufe0f", "DEDUCTIONS", f"${_total_deductions:,.2f}", RED,
                             f"Fees {ds.money(ds.total_fees)} + Ship {ds.money(ds.total_shipping_cost)} + Ads {ds.money(ds.total_marketing)}"), width="auto"),
        ], className="g-2 mb-3", style={"flexWrap": "wrap"}),

        # Subtitle
        html.P("Your business at a glance — all data sources reconciled.",
               style={"color": GRAY, "fontSize": "12px", "marginBottom": "12px"}),

        # Health checks
        _build_health_checks(),

        # Side-by-side: P&L + Monthly chart
        dbc.Row([
            dbc.Col([
                dbc.Card(dbc.CardBody([
                    html.H5("P&L (all 3 sources)", style={"color": CYAN, "marginBottom": "12px",
                                                            "fontSize": "15px", "fontWeight": "bold"}),
                    _build_pl_row("Gross Sales", f"${ds.gross_sales:,.0f}", GREEN, bold=True),
                    _build_pl_row("  Etsy Fees", f"-${ds.total_fees:,.0f}", RED),
                    _build_pl_row("  Shipping Labels", f"-${ds.total_shipping_cost:,.0f}", RED),
                    _build_pl_row("  Marketing/Ads", f"-${ds.total_marketing:,.0f}", RED),
                    _build_pl_row("  Refunds", f"-${ds.total_refunds:,.0f}", RED),
                    _build_pl_row("  Sales Tax", f"-${ds.total_taxes:,.0f}", RED),
                    _build_pl_row("= Etsy Net", f"${ds.net_profit:,.0f}", WHITE, bold=True, border=True),
                    _build_pl_row("  Inventory COGS", f"-${ds.true_inventory_cost:,.0f}", RED),
                    _build_pl_row("  Bank Expenses", f"-${ds.bank_biz_expense_total:,.0f}", RED),
                    _build_pl_row("  Owner Draws", f"-${ds.bank_owner_draw_total:,.0f}", ORANGE),
                    html.Div([
                        html.Span("= CASH ON HAND", style={"color": CYAN, "fontWeight": "bold", "fontSize": "14px"}),
                        html.Span(f"${ds.bank_cash_on_hand:,.0f}",
                                  style={"color": CYAN, "fontWeight": "bold", "fontSize": "20px",
                                         "fontFamily": "monospace", "float": "right"}),
                    ], style={"borderTop": f"2px solid {CYAN}44", "padding": "10px 0", "marginTop": "8px"}),
                ])),
            ], md=4),
            dbc.Col([
                dbc.Card(dbc.CardBody([
                    dcc.Graph(figure=monthly_fig, config={"displayModeBar": False},
                              style={"height": "380px"}),
                ])),
            ], md=8),
        ], className="g-3"),
    ])
