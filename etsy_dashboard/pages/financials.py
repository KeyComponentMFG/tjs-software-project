"""Financials page — Full P&L, waterfall, bank reconciliation."""
from dash import html, dcc
import dash_bootstrap_components as dbc
import plotly.graph_objects as go

from etsy_dashboard.theme import *
from etsy_dashboard.components.kpi import kpi_card
from etsy_dashboard.components.cards import section, row_item, make_chart, chart_context
from etsy_dashboard import data_state as ds


def _pl_row(label, amount, indent=0, bold=False, color=WHITE, border=False):
    """P&L line item."""
    neg = amount < 0
    display_color = RED if neg else color
    style = {
        "display": "flex", "justifyContent": "space-between",
        "padding": "5px 0", "marginLeft": f"{indent * 20}px",
    }
    if bold:
        style["fontWeight"] = "bold"
    if border:
        style["borderTop"] = f"1px solid {DARKGRAY}44"
        style["marginTop"] = "4px"
        style["paddingTop"] = "8px"
    return html.Div([
        html.Span(label, style={"color": display_color if bold else GRAY, "fontSize": "13px", "flex": "1"}),
        html.Span(ds.money(amount), style={"color": display_color, "fontFamily": "monospace",
                                             "fontWeight": "bold" if bold else "normal", "fontSize": "13px"}),
    ], style=style)


def layout():
    """Build the Financials page."""
    _total_deductions = ds.total_fees + ds.total_shipping_cost + ds.total_marketing + ds.total_refunds + ds.total_taxes + ds.total_buyer_fees

    # ── P&L Waterfall Chart ───────────────────────────────────────────────
    wf_labels = ["Gross Sales", "Fees", "Shipping", "Marketing", "Refunds", "Tax", "Net Revenue",
                 "Inventory", "Bank Exp", "Draws", "Cash on Hand"]
    wf_values = [ds.gross_sales, -ds.total_fees, -ds.total_shipping_cost, -ds.total_marketing,
                 -ds.total_refunds, -ds.total_taxes, ds.net_profit,
                 -ds.true_inventory_cost, -ds.bank_biz_expense_total,
                 -ds.bank_owner_draw_total, ds.bank_cash_on_hand]
    wf_measures = ["absolute", "relative", "relative", "relative", "relative", "relative", "total",
                   "relative", "relative", "relative", "total"]
    wf_colors = [GREEN, RED, RED, RED, RED, RED, CYAN, RED, RED, ORANGE, CYAN]

    wf_fig = go.Figure(go.Waterfall(
        x=wf_labels, y=wf_values, measure=wf_measures,
        connector={"line": {"color": "rgba(255,255,255,0.13)"}},
        decreasing={"marker": {"color": RED}},
        increasing={"marker": {"color": GREEN}},
        totals={"marker": {"color": CYAN}},
    ))
    make_chart(wf_fig, 400)
    wf_fig.update_layout(title="P&L Waterfall — All Sources", showlegend=False)

    # ── Fee Breakdown Pie ─────────────────────────────────────────────────
    fee_labels = ["Listing Fees", "Transaction Fees (Product)", "Transaction Fees (Shipping)",
                  "Processing Fees", "Credits/Discounts"]
    fee_values = [ds.listing_fees, ds.transaction_fees_product, ds.transaction_fees_shipping,
                  ds.processing_fees, abs(ds.total_credits)]
    fee_colors = [RED, ORANGE, PURPLE, PINK, GREEN]

    fee_fig = go.Figure(go.Pie(
        labels=fee_labels, values=fee_values,
        marker=dict(colors=fee_colors),
        hole=0.4, textinfo="label+percent",
    ))
    make_chart(fee_fig, 350)
    fee_fig.update_layout(title=f"Fee Breakdown (${ds.total_fees:,.0f} total)")

    # ── Shipping Breakdown Pie ────────────────────────────────────────────
    ship_labels = ["USPS Outbound", "USPS Returns", "Asendia", "Adjustments", "Insurance", "Credits"]
    ship_values = [ds.usps_outbound, ds.usps_return, ds.asendia_labels,
                   ds.ship_adjustments, ds.ship_insurance, abs(ds.ship_credits)]
    ship_fig = go.Figure(go.Pie(
        labels=ship_labels, values=ship_values,
        marker=dict(colors=[BLUE, RED, PURPLE, ORANGE, TEAL, GREEN]),
        hole=0.4, textinfo="label+percent",
    ))
    make_chart(ship_fig, 350)
    ship_fig.update_layout(title=f"Shipping Breakdown (${ds.total_shipping_cost:,.0f} total)")

    # ── Bank Category Bar ─────────────────────────────────────────────────
    bank_fig = go.Figure()
    if ds.bank_by_cat:
        cats = list(ds.bank_by_cat.keys())[:12]
        vals = [ds.bank_by_cat[c] for c in cats]
        bank_fig.add_trace(go.Bar(
            x=vals, y=cats, orientation="h",
            marker_color=[ORANGE if "Draw" in c else RED if c != "Amazon Inventory" else BLUE for c in cats],
        ))
    make_chart(bank_fig, 380, legend_h=False)
    bank_fig.update_layout(title="Bank Spending by Category", yaxis=dict(autorange="reversed"))

    # ── Monthly Bank Deposits vs Debits ───────────────────────────────────
    bank_month_fig = go.Figure()
    if ds.bank_monthly:
        bm_sorted = sorted(ds.bank_monthly.keys())
        bank_month_fig.add_trace(go.Bar(
            x=bm_sorted, y=[ds.bank_monthly[m]["deposits"] for m in bm_sorted],
            name="Deposits", marker_color=GREEN,
        ))
        bank_month_fig.add_trace(go.Bar(
            x=bm_sorted, y=[-ds.bank_monthly[m]["debits"] for m in bm_sorted],
            name="Debits", marker_color=RED,
        ))
    make_chart(bank_month_fig, 350)
    bank_month_fig.update_layout(title="Monthly Bank Flow", barmode="relative")

    return html.Div([
        # KPI strip
        dbc.Row([
            dbc.Col(kpi_card("GROSS SALES", ds.money(ds.gross_sales), GREEN), md=2),
            dbc.Col(kpi_card("TOTAL FEES", ds.money(ds.total_fees), RED), md=2),
            dbc.Col(kpi_card("NET REVENUE", ds.money(ds.net_profit), CYAN), md=2),
            dbc.Col(kpi_card("COGS", ds.money(ds.true_inventory_cost), ORANGE), md=2),
            dbc.Col(kpi_card("BANK NET", ds.money(ds.bank_net_cash), BLUE), md=2),
            dbc.Col(kpi_card("PROFIT", ds.money(ds.full_profit), GREEN,
                             f"{ds.full_profit_margin:.1f}% margin"), md=2),
        ], className="g-2 mb-3"),

        # Waterfall
        dbc.Card(dbc.CardBody(dcc.Graph(figure=wf_fig, config={"displayModeBar": False})), className="mb-3"),

        # Detailed P&L
        dbc.Row([
            dbc.Col([
                section("Full P&L Statement", [
                    _pl_row("Gross Sales", ds.gross_sales, bold=True, color=GREEN),
                    html.Hr(style={"borderColor": "#ffffff10", "margin": "6px 0"}),
                    html.Div("Etsy Deductions", style={"color": GRAY, "fontSize": "11px",
                                                         "letterSpacing": "1px", "marginBottom": "4px"}),
                    _pl_row("Listing Fees", -ds.listing_fees, indent=1),
                    _pl_row("Transaction Fees (Product)", -ds.transaction_fees_product, indent=1),
                    _pl_row("Transaction Fees (Shipping)", -ds.transaction_fees_shipping, indent=1),
                    _pl_row("Processing Fees", -ds.processing_fees, indent=1),
                    _pl_row("Credits & Discounts", ds.total_credits, indent=1, color=GREEN),
                    _pl_row("Shipping Labels", -ds.total_shipping_cost, indent=1),
                    _pl_row("Marketing/Ads", -ds.total_marketing, indent=1),
                    _pl_row("Refunds", -ds.total_refunds, indent=1),
                    _pl_row("Sales Tax Collected", -ds.total_taxes, indent=1),
                    _pl_row("Buyer Fees", -ds.total_buyer_fees, indent=1),
                    _pl_row("= Etsy Net Revenue", ds.net_profit, bold=True, color=CYAN, border=True),
                    html.Hr(style={"borderColor": "#ffffff10", "margin": "6px 0"}),
                    html.Div("Below-the-line Costs", style={"color": GRAY, "fontSize": "11px",
                                                              "letterSpacing": "1px", "marginBottom": "4px"}),
                    _pl_row("Inventory COGS", -ds.true_inventory_cost, indent=1),
                    _pl_row("Bank Business Expenses", -ds.bank_biz_expense_total, indent=1),
                    _pl_row("Owner Draws", -ds.bank_owner_draw_total, indent=1, color=ORANGE),
                    _pl_row("= Cash on Hand", ds.bank_cash_on_hand, bold=True, color=CYAN, border=True),
                ], CYAN),
            ], md=5),

            dbc.Col([
                # Fee breakdown
                dbc.Card(dbc.CardBody(dcc.Graph(figure=fee_fig, config={"displayModeBar": False})), className="mb-3"),
                # Shipping breakdown
                dbc.Card(dbc.CardBody(dcc.Graph(figure=ship_fig, config={"displayModeBar": False}))),
            ], md=7),
        ], className="g-3 mb-3"),

        # Bank section
        dbc.Row([
            dbc.Col(dbc.Card(dbc.CardBody(dcc.Graph(figure=bank_fig, config={"displayModeBar": False}))), md=6),
            dbc.Col(dbc.Card(dbc.CardBody(dcc.Graph(figure=bank_month_fig, config={"displayModeBar": False}))), md=6),
        ], className="g-3 mb-3"),

        # Bank Ledger
        section("Bank Ledger (Recent)", [
            _build_bank_ledger(),
        ], BLUE),
    ])


def _build_bank_ledger():
    """Build a compact bank transaction ledger table."""
    if not ds.bank_running:
        return html.P("No bank transactions loaded.", style={"color": GRAY})

    rows = []
    for t in ds.bank_running[-30:]:  # Last 30 transactions
        is_deposit = t["type"] == "deposit"
        rows.append(html.Tr([
            html.Td(t["date"], style={"fontSize": "12px", "color": GRAY}),
            html.Td(t["desc"][:50], style={"fontSize": "12px"}),
            html.Td(t["category"], style={"fontSize": "11px", "color": GRAY}),
            html.Td(
                f"+${t['amount']:,.2f}" if is_deposit else f"-${t['amount']:,.2f}",
                style={"color": GREEN if is_deposit else RED, "fontFamily": "monospace", "fontSize": "12px",
                        "textAlign": "right"},
            ),
            html.Td(
                f"${t['_balance']:,.2f}",
                style={"color": CYAN, "fontFamily": "monospace", "fontSize": "12px", "textAlign": "right"},
            ),
        ]))

    return dbc.Table([
        html.Thead(html.Tr([
            html.Th("Date"), html.Th("Description"), html.Th("Category"),
            html.Th("Amount", style={"textAlign": "right"}),
            html.Th("Balance", style={"textAlign": "right"}),
        ])),
        html.Tbody(rows),
    ], striped=True, hover=True, size="sm", className="mb-0")
