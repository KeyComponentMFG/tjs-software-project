"""Tax Forms page — Balance Sheet, Form 1065, K-1s."""
from dash import html, dcc
import dash_bootstrap_components as dbc

from etsy_dashboard.theme import *
from etsy_dashboard.components.kpi import kpi_card
from etsy_dashboard.components.cards import section, row_item
from etsy_dashboard import data_state as ds


def _form_row(label, value_str, color=WHITE, bold=False, indent=0, border=False):
    """A single row in a tax form."""
    style = {
        "display": "flex", "justifyContent": "space-between",
        "padding": "4px 0", "marginLeft": f"{indent * 20}px",
    }
    if bold:
        style["fontWeight"] = "bold"
    if border:
        style["borderTop"] = f"1px solid {DARKGRAY}44"
        style["marginTop"] = "4px"
        style["paddingTop"] = "8px"
    return html.Div([
        html.Span(label, style={"color": color if bold else GRAY, "fontSize": "13px", "flex": "1"}),
        html.Span(value_str, style={"color": color, "fontFamily": "monospace",
                                      "fontWeight": "bold" if bold else "normal", "fontSize": "13px"}),
    ], style=style)


def layout():
    """Build the Tax Forms page."""
    # ── Balance Sheet ────────────────────────────────────────────────────
    # Assets
    bank_cash = ds.bank_net_cash
    etsy_bal = ds.etsy_balance
    inventory_value = ds.true_inventory_cost
    cc_asset = ds.bb_cc_asset_value
    total_assets = bank_cash + etsy_bal + inventory_value + cc_asset

    # Liabilities
    cc_balance = ds.bb_cc_balance
    total_liabilities = cc_balance

    # Equity
    equity = total_assets - total_liabilities

    # ── Schedule C approximation ─────────────────────────────────────────
    gross_income = ds.gross_sales
    cogs = ds.true_inventory_cost
    gross_profit = gross_income - cogs
    expenses = ds.total_fees + ds.total_shipping_cost + ds.total_marketing + ds.bank_biz_expense_total
    net_income = gross_profit - expenses

    # K-1 splits (50/50 partnership)
    k1_each = net_income / 2

    return html.Div([
        # KPI strip
        dbc.Row([
            dbc.Col(kpi_card("TOTAL ASSETS", ds.money(total_assets), GREEN), md=3),
            dbc.Col(kpi_card("LIABILITIES", ds.money(total_liabilities), RED), md=3),
            dbc.Col(kpi_card("EQUITY", ds.money(equity), CYAN), md=3),
            dbc.Col(kpi_card("NET INCOME", ds.money(net_income), GREEN,
                             f"K-1 each: {ds.money(k1_each)}"), md=3),
        ], className="g-2 mb-3"),

        dbc.Row([
            # Balance Sheet
            dbc.Col([
                section("Balance Sheet", [
                    html.Div("ASSETS", style={"color": GREEN, "fontSize": "12px", "fontWeight": "bold",
                                                "letterSpacing": "1.5px", "marginBottom": "6px"}),
                    _form_row("Bank Account (Capital One)", ds.money(bank_cash), indent=1),
                    _form_row("Etsy Balance", ds.money(etsy_bal), indent=1),
                    _form_row("Inventory (at cost)", ds.money(inventory_value), indent=1),
                    _form_row("Best Buy CC (purchased assets)", ds.money(cc_asset), indent=1),
                    _form_row("TOTAL ASSETS", ds.money(total_assets), color=GREEN, bold=True, border=True),

                    html.Hr(style={"borderColor": "#ffffff10", "margin": "10px 0"}),

                    html.Div("LIABILITIES", style={"color": RED, "fontSize": "12px", "fontWeight": "bold",
                                                      "letterSpacing": "1.5px", "marginBottom": "6px"}),
                    _form_row("Best Buy Citi CC Balance", ds.money(cc_balance), indent=1),
                    _form_row("TOTAL LIABILITIES", ds.money(total_liabilities), color=RED, bold=True, border=True),

                    html.Hr(style={"borderColor": "#ffffff10", "margin": "10px 0"}),

                    html.Div("EQUITY", style={"color": CYAN, "fontSize": "12px", "fontWeight": "bold",
                                                "letterSpacing": "1.5px", "marginBottom": "6px"}),
                    _form_row("Owner's Equity", ds.money(equity), color=CYAN, bold=True),
                ], GREEN),
            ], md=6),

            # Schedule C / Form 1065
            dbc.Col([
                section("Form 1065 (Partnership Return)", [
                    _form_row("Line 1a  Gross receipts", ds.money(gross_income)),
                    _form_row("Line 2   Cost of goods sold", ds.money(cogs)),
                    _form_row("Line 3   Gross profit", ds.money(gross_profit), color=GREEN, bold=True, border=True),

                    html.Hr(style={"borderColor": "#ffffff10", "margin": "8px 0"}),

                    html.Div("DEDUCTIONS", style={"color": RED, "fontSize": "11px", "fontWeight": "bold",
                                                     "letterSpacing": "1px", "marginBottom": "4px"}),
                    _form_row("Etsy Fees", ds.money(ds.total_fees), indent=1),
                    _form_row("Shipping Labels", ds.money(ds.total_shipping_cost), indent=1),
                    _form_row("Marketing / Ads", ds.money(ds.total_marketing), indent=1),
                    _form_row("Other Bank Expenses", ds.money(ds.bank_biz_expense_total), indent=1),
                    _form_row("Total Deductions", ds.money(expenses), color=RED, bold=True, border=True),

                    html.Hr(style={"borderColor": "#ffffff10", "margin": "8px 0"}),

                    _form_row("Line 22  Ordinary business income", ds.money(net_income),
                              color=GREEN, bold=True, border=True),
                ], ORANGE),

                section("Schedule K-1 (Partner Shares)", [
                    html.Div("50/50 Partnership Split", style={"color": GRAY, "fontSize": "11px",
                                                                  "marginBottom": "8px"}),
                    _form_row("TJ (Tulsa) — Ordinary income", ds.money(k1_each)),
                    _form_row("TJ — Draws taken", ds.money(ds.tulsa_draw_total)),
                    _form_row("TJ — Net owed", ds.money(k1_each - ds.tulsa_draw_total),
                              color=GREEN if k1_each > ds.tulsa_draw_total else RED),
                    html.Hr(style={"borderColor": "#ffffff10", "margin": "6px 0"}),
                    _form_row("Braden (Texas) — Ordinary income", ds.money(k1_each)),
                    _form_row("Braden — Draws taken", ds.money(ds.texas_draw_total)),
                    _form_row("Braden — Net owed", ds.money(k1_each - ds.texas_draw_total),
                              color=GREEN if k1_each > ds.texas_draw_total else RED),
                ], PURPLE),
            ], md=6),
        ], className="g-3 mb-3"),

        # Tax-deductible bank expenses
        section("Tax-Deductible Bank Expenses (Schedule C)", [
            html.P(f"Total deductible: {ds.money(ds.bank_tax_deductible)}",
                   style={"color": GREEN, "fontWeight": "bold", "marginBottom": "8px"}),
            html.Div([
                _form_row(cat, ds.money(amt))
                for cat, amt in ds.bank_by_cat.items()
                if cat in ds.BANK_TAX_DEDUCTIBLE
            ]) if ds.bank_by_cat else html.P("No bank data loaded.", style={"color": GRAY}),
        ], TEAL),
    ])
