"""Overview tab — Business health at a glance."""

from dash import dcc, html
from dashboard_utils.theme import *


def _build_pl_row(label, amount_str, color, bold=False, border=False):
    """Return a list of Dash components for one P&L waterfall row.
    Use * splat to unpack into a parent's children list."""
    row_style = {
        "display": "flex", "justifyContent": "space-between",
        "padding": "4px 0",
        "fontWeight": "bold" if bold else "normal",
    }
    if border:
        row_style["borderTop"] = f"1px solid {DARKGRAY}44"
        row_style["marginTop"] = "2px"
        row_style["paddingTop"] = "6px"
    return [html.Div([
        html.Span(label, style={"color": color if bold else GRAY, "flex": "1",
                                 "fontSize": "14px" if bold else "13px"}),
        html.Span(amount_str, style={"color": color, "fontFamily": "monospace",
                                      "fontSize": "14px" if bold else "13px",
                                      "fontWeight": "bold" if bold else "normal"}),
    ], style=row_style)]


def build_tab1_overview():
    """Tab 1 - Overview: Business health at a glance. Single screen, no scrolling."""
    import etsy_dashboard as ed

    _sm = ed.strict_mode if isinstance(ed.strict_mode, bool) else False

    # Strict mode banner
    _strict_banner_ov = ed._strict_banner() if _sm else html.Div()

    # Missing receipts warning
    _missing_receipt_banner = html.Div([
        html.Span(f"{len(ed.expense_missing_receipts)} bank debits missing receipts", style={"color": ORANGE, "fontWeight": "bold", "fontSize": "13px"}),
        html.Span(f" — ${ed.expense_gap:,.2f} unverified expenses", style={"color": GRAY, "fontSize": "12px"}),
        html.Span(" (see Financials tab)", style={"color": DARKGRAY, "fontSize": "11px"}),
    ], style={"backgroundColor": "#1a1000", "border": f"1px solid {ORANGE}44", "borderRadius": "6px",
              "padding": "8px 14px", "marginBottom": "10px"}) if len(ed.expense_missing_receipts) > 0 else html.Div()

    return html.Div([
        _strict_banner_ov,
        _missing_receipt_banner,
        # KPI Strip (4 pills)
        html.Div([
            ed._build_kpi_pill("\U0001f4ca", "REVENUE", money(ed.gross_sales), TEAL,
                            f"{ed.order_count} orders, avg {money(ed.avg_order)}",
                            f"Total from {ed.order_count} orders, avg {money(ed.avg_order)} each.", status="verified"),
            ed._build_kpi_pill("\U0001f4b0", "PROFIT", money(ed.profit), GREEN,
                            f"{_fmt(ed.profit_margin, prefix='', fmt='.1f', unknown='?')}% margin",
                            f"Revenue minus all costs -- {_fmt(ed.profit_margin, prefix='', fmt='.1f', unknown='?')}% margin.", status="verified"),
            ed._build_kpi_pill("\U0001f3e6", "CASH", money(ed.bank_cash_on_hand), CYAN,
                            f"Bank {money(ed.bank_net_cash)} + Etsy {money(ed.etsy_balance)}",
                            f"Bank {money(ed.bank_net_cash)} + Etsy pending {money(ed.etsy_balance)}.", status="verified"),
            ed._build_kpi_pill("\U0001f4b3", "DEBT", money(ed.bb_cc_balance), RED,
                            f"Best Buy CC ({money(ed.bb_cc_available)} avail)",
                            f"Best Buy Citi CC -- {money(ed.bb_cc_available)} available of {money(ed.bb_cc_limit)} limit.", status="verified"),
        ], style={"display": "flex", "gap": "8px", "marginBottom": "14px", "flexWrap": "wrap"}),

        # Dashboard Health / To-Do panel (hidden in strict mode — composite estimate)
        ed._build_health_checks() if not _sm else html.Div(),

        # Simplified data sources one-liner
        html.Div([
            html.Span(f"{ed.order_count} orders", style={"color": TEAL, "fontWeight": "bold", "fontSize": "13px"}),
            html.Span("  |  ", style={"color": DARKGRAY}),
            html.Span(f"{len(ed.BANK_TXNS)} bank transactions", style={"color": CYAN, "fontWeight": "bold", "fontSize": "13px"}),
            html.Span("  |  ", style={"color": DARKGRAY}),
            html.Span(f"{len(ed.INVOICES)} receipts", style={"color": PURPLE, "fontWeight": "bold", "fontSize": "13px"}),
        ], style={"backgroundColor": CARD, "padding": "10px 16px", "borderRadius": "10px",
                  "borderLeft": f"4px solid {CYAN}", "marginBottom": "12px", "textAlign": "center"}),

        # Quick P&L + Monthly chart side by side
        html.Div([
            # Quick P&L
            html.Div([
                html.H3("Profit & Loss", style={"color": CYAN, "margin": "0 0 10px 0", "fontSize": "15px"}),
                *_build_pl_row("Gross Sales", f"${ed.gross_sales:,.0f}", GREEN, bold=True),
                *_build_pl_row("  Etsy Deductions (fees, ship, ads, refunds, tax)",
                               f"-${ed.gross_sales - ed.etsy_net:,.0f}", RED),
                *_build_pl_row("= After Etsy Fees", f"${ed.etsy_net:,.0f}", WHITE, bold=True, border=True),
                *_build_pl_row("  Supplies & Materials",
                               f"-${ed.true_inventory_cost:,.0f}", RED),
                *_build_pl_row("  Business Expenses",
                               f"-${max(0, ed.bank_total_debits - ed.bank_amazon_inv - ed.bank_owner_draw_total):,.0f}", RED),
                *_build_pl_row("  Owner Draws (TJ + Braden)",
                               f"-${ed.bank_owner_draw_total:,.0f}", ORANGE),
                html.Div([
                    html.Span("= CASH ON HAND", style={"color": GREEN, "flex": "1", "fontSize": "16px",
                              "fontWeight": "bold"}),
                    html.Span(f"${ed.bank_cash_on_hand:,.0f}", style={"color": GREEN, "fontFamily": "monospace",
                              "fontSize": "18px", "fontWeight": "bold"}),
                ], style={"display": "flex", "justifyContent": "space-between", "padding": "8px 0",
                          "borderTop": f"2px solid {GREEN}44", "marginTop": "4px"}),
            ], style={"backgroundColor": CARD, "padding": "16px", "borderRadius": "10px",
                      "flex": "1", "minWidth": "280px"}),

            # Monthly Performance chart (actuals only, no projections in strict mode)
            html.Div([
                dcc.Graph(figure=ed.monthly_fig, config={"displayModeBar": False}),
            ], style={"flex": "2"}),
        ], style={"display": "flex", "gap": "12px"}),
    ], style={"padding": TAB_PADDING})
