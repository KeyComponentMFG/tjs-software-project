"""Financials page — Full P&L, Cash Flow, Shipping, Monthly, Fees, Ledger."""
from dash import html, dcc
import dash_bootstrap_components as dbc
import plotly.graph_objects as go

from etsy_dashboard.theme import *
from etsy_dashboard.components.kpi import kpi_card, kpi_pill
from etsy_dashboard.components.cards import section, row_item, make_chart
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


def _detail_card(title, color, children, total=None):
    """Compact detail card for financial breakdowns."""
    header = [html.Span(title, style={"fontSize": "13px", "fontWeight": "bold", "color": color})]
    total_el = html.Div(ds.money(total), style={
        "fontSize": "18px", "fontWeight": "bold", "color": color, "fontFamily": "monospace",
    }) if total is not None else html.Div()
    return html.Div([
        html.Div([html.Div(header), total_el], style={"marginBottom": "8px"}),
        html.Div(children, style={"maxHeight": "250px", "overflowY": "auto"}),
    ], style={
        "backgroundColor": CARD2, "borderRadius": "10px", "padding": "14px",
        "borderLeft": f"3px solid {color}", "flex": "1", "minWidth": "240px",
    })


def _txn_row(txn, color):
    """Single transaction row."""
    return html.Div([
        html.Span(txn.get("date", ""), style={"color": GRAY, "fontSize": "10px", "width": "70px", "display": "inline-block"}),
        html.Span(f"${txn['amount']:,.2f}", style={"color": color, "fontFamily": "monospace", "fontSize": "11px", "width": "60px", "display": "inline-block"}),
        html.Span(txn.get("desc", "")[:35], style={"color": WHITE, "fontSize": "11px"}),
    ], style={"padding": "2px 0", "borderBottom": "1px solid #ffffff08"})


# ── Shared style for all collapsible section headers ─────────────────────────
_DETAILS_STYLE = {
    "color": CYAN, "fontSize": "14px", "fontWeight": "bold",
    "cursor": "pointer", "padding": "10px 14px", "listStyle": "none",
    "backgroundColor": "#ffffff08", "borderRadius": "6px",
    "border": f"1px solid {CYAN}33",
}


def _section_header(title, subtitle):
    """Bold title + gray subtitle for collapsible headers."""
    return html.Summary([
        html.Span("\u25b6 ", style={"fontSize": "12px"}),
        html.Span(title, style={"fontWeight": "bold"}),
        html.Span(f" \u2014 {subtitle}", style={"color": GRAY, "fontWeight": "normal", "fontSize": "12px"}),
    ], style=_DETAILS_STYLE)


def _build_charts():
    """Build all charts used on the page. Returns dict of figures."""
    figs = {}

    # 1. Expense donut — where every dollar goes
    labels = ["Fees", "Shipping", "Marketing", "Refunds", "Taxes"]
    values = [ds.total_fees, ds.total_shipping_cost, ds.total_marketing, ds.total_refunds, ds.total_taxes]
    colors = [RED, BLUE, PURPLE, ORANGE, GRAY]
    if ds.true_inventory_cost > 0:
        labels.append("Inventory"); values.append(ds.true_inventory_cost); colors.append(TEAL)
    if ds.bank_biz_expense_total > 0:
        labels.append("Bank Expenses"); values.append(ds.bank_biz_expense_total); colors.append(PINK)
    if ds.bank_owner_draw_total > 0:
        labels.append("Owner Draws"); values.append(ds.bank_owner_draw_total); colors.append("#ff9800")
    remaining = ds.gross_sales - sum(values)
    if remaining > 0:
        labels.append("Cash Remaining"); values.append(remaining); colors.append(GREEN)
    fig = go.Figure(go.Pie(labels=labels, values=values, marker=dict(colors=colors),
                           hole=0.45, textinfo="label+percent", textposition="outside", textfont=dict(size=11)))
    make_chart(fig, 400)
    fig.update_layout(title=f"WHERE EVERY DOLLAR GOES (${ds.gross_sales:,.0f} gross)", showlegend=False)
    figs["expense"] = fig

    # 2. Reconciliation waterfall
    wl, wv, wm = ["Gross Sales"], [ds.gross_sales], ["absolute"]
    for lbl, val in [("Fees", ds.total_fees), ("Ship Labels", ds.total_shipping_cost),
                     ("Ads", ds.total_marketing), ("Refunds", ds.total_refunds), ("Taxes", ds.total_taxes)]:
        wl.append(lbl); wv.append(-val); wm.append("relative")
    if ds.total_buyer_fees > 0:
        wl.append("Buyer Fee"); wv.append(-ds.total_buyer_fees); wm.append("relative")
    wl.append("AFTER ETSY"); wv.append(0); wm.append("total")
    for lbl, cat in [("Inventory", "Amazon Inventory"), ("AliExpress", "AliExpress Supplies"),
                     ("Craft", "Craft Supplies"), ("Subscriptions", "Subscriptions"),
                     ("Ship Supplies", "Shipping"), ("Best Buy CC", "Business Credit Card")]:
        v = ds.bank_by_cat.get(cat, 0)
        if v > 0: wl.append(lbl); wv.append(-v); wm.append("relative")
    if ds.bank_owner_draw_total > 0:
        wl.append("Draws"); wv.append(-ds.bank_owner_draw_total); wm.append("relative")
    wl.append("CASH ON HAND"); wv.append(0); wm.append("total")
    fig = go.Figure(go.Waterfall(
        orientation="v", measure=wm, x=wl, y=wv,
        connector={"line": {"color": "#555"}},
        decreasing={"marker": {"color": RED}}, increasing={"marker": {"color": GREEN}},
        totals={"marker": {"color": CYAN}}, textposition="outside",
        text=[f"${abs(v):,.0f}" if v != 0 else "" for v in wv],
    ))
    make_chart(fig, 400)
    fig.update_layout(title="HOW EVERY DOLLAR BALANCES", yaxis_title="$", xaxis_tickangle=-35, xaxis_tickfont=dict(size=10))
    figs["waterfall"] = fig

    # 3. Fee pie
    fig = go.Figure(go.Pie(
        labels=["Listing", "Txn (Product)", "Txn (Shipping)", "Processing", "Credits"],
        values=[ds.listing_fees, ds.transaction_fees_product, ds.transaction_fees_shipping, ds.processing_fees, abs(ds.total_credits)],
        marker=dict(colors=[RED, ORANGE, PURPLE, PINK, GREEN]), hole=0.4, textinfo="label+percent"))
    make_chart(fig, 340); fig.update_layout(title=f"Fee Breakdown (${ds.total_fees:,.0f})")
    figs["fee_pie"] = fig

    # 4. Shipping pie
    fig = go.Figure(go.Pie(
        labels=["USPS Out", "USPS Return", "Asendia", "Adjust", "Insurance", "Credits"],
        values=[ds.usps_outbound, ds.usps_return, ds.asendia_labels, ds.ship_adjustments, ds.ship_insurance, abs(ds.ship_credits)],
        marker=dict(colors=[BLUE, RED, PURPLE, ORANGE, TEAL, GREEN]), hole=0.4, textinfo="label+percent"))
    make_chart(fig, 340); fig.update_layout(title=f"Shipping Breakdown (${ds.total_shipping_cost:,.0f})")
    figs["ship_pie"] = fig

    # 5. Shipping comparison
    fig = go.Figure()
    fig.add_trace(go.Bar(x=["Buyer Paid", "Label Cost", "Net P&L"],
        y=[ds.buyer_paid_shipping, ds.total_shipping_cost, abs(ds.shipping_profit)],
        marker_color=[GREEN, RED, GREEN if ds.shipping_profit >= 0 else RED],
        text=[f"${v:,.0f}" for v in [ds.buyer_paid_shipping, ds.total_shipping_cost, abs(ds.shipping_profit)]],
        textposition="outside"))
    make_chart(fig, 340); fig.update_layout(title="Shipping Revenue vs Cost")
    figs["ship_compare"] = fig

    # 6. Bank category bar
    fig = go.Figure()
    if ds.bank_by_cat:
        cats = [c for c in ds.bank_by_cat if ds.bank_by_cat[c] > 0][:12]
        vals = [ds.bank_by_cat[c] for c in cats]
        fig.add_trace(go.Bar(x=vals, y=cats, orientation="h",
            marker_color=[ORANGE if "Draw" in c else RED if c != "Amazon Inventory" else BLUE for c in cats],
            text=[f"${v:,.0f}" for v in vals], textposition="outside"))
    make_chart(fig, 380, legend_h=False)
    fig.update_layout(title="Bank Spending by Category", yaxis=dict(autorange="reversed"))
    figs["bank_cat"] = fig

    # 7. Monthly bank flow
    fig = go.Figure()
    if ds.bank_monthly:
        bm = sorted(ds.bank_monthly.keys())
        fig.add_trace(go.Bar(x=bm, y=[ds.bank_monthly[m]["deposits"] for m in bm], name="Deposits", marker_color=GREEN))
        fig.add_trace(go.Bar(x=bm, y=[-ds.bank_monthly[m]["debits"] for m in bm], name="Debits", marker_color=RED))
    make_chart(fig, 350); fig.update_layout(title="Monthly Bank Flow", barmode="relative")
    figs["bank_monthly"] = fig

    # 8. Product revenue
    fig = go.Figure()
    if ds.product_revenue_est is not None and len(ds.product_revenue_est) > 0:
        top = ds.product_revenue_est.head(15)
        fig.add_trace(go.Bar(x=top.values, y=[str(n)[:40] for n in top.index], orientation="h",
            marker_color=CYAN, text=[f"${v:,.0f}" for v in top.values], textposition="outside"))
    make_chart(fig, 450, legend_h=False)
    fig.update_layout(title="Top Products by Est. Revenue", yaxis=dict(autorange="reversed"))
    figs["products"] = fig

    return figs


def layout():
    """Build the Financials page."""
    figs = _build_charts()
    total_fees_gross = ds.listing_fees + ds.transaction_fees_product + ds.transaction_fees_shipping + ds.processing_fees
    net_fees_after_credits = total_fees_gross - abs(ds.total_credits)
    total_label_count = ds.usps_outbound_count + ds.usps_return_count + ds.asendia_count

    return html.Div([
        # ── KPI Pills ────────────────────────────────────────────────────
        html.Div([
            kpi_pill("\U0001f4b3", "DEBT", ds.money(ds.bb_cc_balance), RED,
                     f"Best Buy CC (${ds.bb_cc_available:,.0f} avail)",
                     f"Charged: {ds.money(ds.bb_cc_total_charged)}. Paid: {ds.money(ds.bb_cc_total_paid)}. Limit: {ds.money(ds.bb_cc_limit)}."),
            kpi_pill("\U0001f4e5", "AFTER ETSY FEES", ds.money(ds.net_profit), ORANGE,
                     "What Etsy deposits to your bank",
                     f"Gross ({ds.money(ds.gross_sales)}) minus fees, shipping, ads, refunds, taxes."),
            kpi_pill("\U0001f4c9", "TOTAL FEES", ds.money(net_fees_after_credits), RED,
                     f"{net_fees_after_credits / ds.gross_sales * 100:.1f}% of sales" if ds.gross_sales else "",
                     f"Listing: {ds.money(ds.listing_fees)}. Processing: {ds.money(ds.processing_fees)}. Credits: {ds.money(abs(ds.total_credits))}."),
            kpi_pill("\u21a9\ufe0f", "REFUNDS", ds.money(ds.total_refunds), PINK,
                     f"{len(ds.refund_df)} orders ({len(ds.refund_df) / max(ds.order_count, 1) * 100:.1f}%)",
                     f"{len(ds.refund_df)} refunded of {ds.order_count}. Return labels: {ds.money(ds.usps_return)}."),
            kpi_pill("\U0001f4b0", "PROFIT", ds.money(ds.full_profit), GREEN,
                     f"{ds.full_profit_margin:.1f}% margin",
                     f"Cash ({ds.money(ds.bank_cash_on_hand)}) + draws ({ds.money(ds.bank_owner_draw_total)})."),
        ], style={"display": "flex", "gap": "8px", "marginBottom": "14px", "flexWrap": "wrap"}),

        # ── Business Snapshot ────────────────────────────────────────────
        html.Div([
            html.Div(f"You've earned {ds.money(ds.gross_sales)} in gross sales across {ds.order_count} orders. "
                     f"After all Etsy fees, shipping, and expenses, your profit is {ds.money(ds.full_profit)} ({ds.full_profit_margin:.1f}% margin). "
                     f"You have {ds.money(ds.bank_cash_on_hand)} cash on hand.",
                     style={"color": WHITE, "fontSize": "14px", "lineHeight": "1.6"}),
        ], style={"backgroundColor": CARD, "borderRadius": "8px", "marginBottom": "14px",
                   "borderLeft": f"4px solid {CYAN}", "padding": "16px 20px",
                   "boxShadow": "0 2px 8px rgba(0,0,0,0.3)"}),

        # ═══ 1. THE BIG PICTURE ══════════════════════════════════════════
        html.Details([
            _section_header("THE BIG PICTURE", "see where every dollar goes"),
            html.Div([
                dbc.Row([
                    dbc.Col(dbc.Card(dbc.CardBody(dcc.Graph(figure=figs["expense"], config={"displayModeBar": False}))), md=6),
                    dbc.Col(dbc.Card(dbc.CardBody(dcc.Graph(figure=figs["waterfall"], config={"displayModeBar": False}))), md=6),
                ], className="g-3"),
            ], style={"paddingTop": "10px"}),
        ], style={"marginBottom": "8px"}),

        # ═══ 2. PROFIT & LOSS ════════════════════════════════════════════
        html.Details([
            _section_header("PROFIT & LOSS", "line-by-line from gross sales to final profit"),
            html.Div([
                dbc.Row([
                    dbc.Col([
                        section("Full P&L Statement", [
                            _pl_row("Gross Sales", ds.gross_sales, bold=True, color=GREEN),
                            html.Hr(style={"borderColor": "#ffffff10", "margin": "6px 0"}),
                            html.Div("ETSY DEDUCTIONS", style={"color": RED, "fontWeight": "bold", "fontSize": "11px",
                                                                 "letterSpacing": "1px", "marginBottom": "4px"}),
                            _pl_row("Listing Fees", -ds.listing_fees, indent=1),
                            _pl_row("Transaction Fees (Product)", -ds.transaction_fees_product, indent=1),
                            _pl_row("Transaction Fees (Shipping)", -ds.transaction_fees_shipping, indent=1),
                            _pl_row("Processing Fees", -ds.processing_fees, indent=1),
                            _pl_row("Fee Credits", ds.total_credits, indent=1, color=GREEN),
                            _pl_row("Shipping Labels", -ds.total_shipping_cost, indent=1),
                            _pl_row("Marketing/Ads", -ds.total_marketing, indent=1),
                            _pl_row("Refunds", -ds.total_refunds, indent=1),
                            _pl_row("Sales Tax", -ds.total_taxes, indent=1),
                            *([_pl_row("Buyer Fees", -ds.total_buyer_fees, indent=1)] if ds.total_buyer_fees > 0 else []),
                            _pl_row("= AFTER ETSY FEES", ds.net_profit, bold=True, color=ORANGE, border=True),
                            html.Hr(style={"borderColor": "#ffffff10", "margin": "6px 0"}),
                            html.Div("BANK EXPENSES (Cap One)", style={"color": RED, "fontWeight": "bold", "fontSize": "11px",
                                                                        "letterSpacing": "1px", "marginBottom": "4px"}),
                            _pl_row("Amazon Inventory", -ds.bank_by_cat.get("Amazon Inventory", 0), indent=1),
                            _pl_row("AliExpress Supplies", -ds.bank_by_cat.get("AliExpress Supplies", 0), indent=1),
                            _pl_row("Craft Supplies", -ds.bank_by_cat.get("Craft Supplies", 0), indent=1),
                            _pl_row("Subscriptions", -ds.bank_by_cat.get("Subscriptions", 0), indent=1),
                            _pl_row("Shipping (UPS/USPS)", -ds.bank_by_cat.get("Shipping", 0), indent=1),
                            _pl_row("Best Buy CC Payment", -ds.bank_by_cat.get("Business Credit Card", 0), indent=1),
                            _pl_row("  CC Balance Owed", -ds.bb_cc_balance, indent=2, color=BLUE),
                            _pl_row("  Equipment (asset)", ds.bb_cc_asset_value, indent=2, color=TEAL),
                            _pl_row("Owner Draws", -ds.bank_owner_draw_total, indent=1, color=ORANGE),
                            html.Div(style={"borderTop": f"3px solid {GREEN}", "marginTop": "10px"}),
                            html.Div([
                                html.Span("PROFIT", style={"color": GREEN, "fontWeight": "bold", "fontSize": "22px"}),
                                html.Span(ds.money(ds.full_profit), style={"color": GREEN, "fontWeight": "bold", "fontSize": "22px", "fontFamily": "monospace"}),
                            ], style={"display": "flex", "justifyContent": "space-between", "padding": "12px 0"}),
                            html.Div(f"= Cash {ds.money(ds.bank_cash_on_hand)} + Draws {ds.money(ds.bank_owner_draw_total)}",
                                     style={"color": GRAY, "fontSize": "12px", "textAlign": "center"}),
                        ], CYAN),
                    ], md=6),
                    dbc.Col([
                        # Profit metric boxes
                        html.Div([
                            html.Div([
                                html.Div("Profit/Day", style={"color": GRAY, "fontSize": "11px"}),
                                html.Div(f"${ds.full_profit / max(ds.days_active, 1):,.2f}",
                                         style={"color": GREEN, "fontSize": "20px", "fontWeight": "bold", "fontFamily": "monospace"}),
                            ], style={"textAlign": "center", "flex": "1", "padding": "12px", "backgroundColor": "#ffffff06", "borderRadius": "8px"}),
                            html.Div([
                                html.Div("Profit/Order", style={"color": GRAY, "fontSize": "11px"}),
                                html.Div(f"${ds.full_profit / max(ds.order_count, 1):,.2f}",
                                         style={"color": GREEN, "fontSize": "20px", "fontWeight": "bold", "fontFamily": "monospace"}),
                            ], style={"textAlign": "center", "flex": "1", "padding": "12px", "backgroundColor": "#ffffff06", "borderRadius": "8px"}),
                        ], style={"display": "flex", "gap": "8px", "marginBottom": "10px"}),
                        html.Div([
                            html.Div([
                                html.Div("Avg Order", style={"color": GRAY, "fontSize": "11px"}),
                                html.Div(f"${ds.avg_order:,.2f}",
                                         style={"color": TEAL, "fontSize": "20px", "fontWeight": "bold", "fontFamily": "monospace"}),
                            ], style={"textAlign": "center", "flex": "1", "padding": "12px", "backgroundColor": "#ffffff06", "borderRadius": "8px"}),
                            html.Div([
                                html.Div("Revenue/Day", style={"color": GRAY, "fontSize": "11px"}),
                                html.Div(f"${ds.gross_sales / max(ds.days_active, 1):,.2f}",
                                         style={"color": TEAL, "fontSize": "20px", "fontWeight": "bold", "fontFamily": "monospace"}),
                            ], style={"textAlign": "center", "flex": "1", "padding": "12px", "backgroundColor": "#ffffff06", "borderRadius": "8px"}),
                        ], style={"display": "flex", "gap": "8px", "marginBottom": "10px"}),
                        dbc.Card(dbc.CardBody(dcc.Graph(figure=figs["fee_pie"], config={"displayModeBar": False})), className="mb-3"),
                        dbc.Card(dbc.CardBody(dcc.Graph(figure=figs["ship_pie"], config={"displayModeBar": False}))),
                    ], md=6),
                ], className="g-3"),
            ], style={"paddingTop": "10px"}),
        ], style={"marginBottom": "8px"}),

        # ═══ 3. CASH & BALANCE SHEET ═════════════════════════════════════
        html.Details([
            _section_header("CASH & BALANCE SHEET", "bank balances, assets, debts, and owner draws"),
            html.Div([
                # Cash KPI row
                html.Div([
                    html.Div([
                        html.Div(ds.money(ds.full_profit), style={"color": GREEN, "fontSize": "22px", "fontWeight": "bold", "fontFamily": "monospace"}),
                        html.Div("PROFIT", style={"color": GRAY, "fontSize": "10px"}),
                    ], style={"textAlign": "center", "flex": "1"}),
                    html.Div([
                        html.Div(ds.money(ds.bank_cash_on_hand), style={"color": CYAN, "fontSize": "18px", "fontWeight": "bold", "fontFamily": "monospace"}),
                        html.Div(f"Cash (Bank {ds.money(ds.bank_net_cash)} + Etsy {ds.money(ds.etsy_balance)})", style={"color": GRAY, "fontSize": "10px"}),
                    ], style={"textAlign": "center", "flex": "1"}),
                    html.Div([
                        html.Div(ds.money(ds.bank_owner_draw_total), style={"color": ORANGE, "fontSize": "18px", "fontWeight": "bold", "fontFamily": "monospace"}),
                        html.Div("Owner Draws", style={"color": GRAY, "fontSize": "10px"}),
                    ], style={"textAlign": "center", "flex": "1"}),
                    html.Div([
                        html.Div(ds.money(ds.bank_all_expenses), style={"color": RED, "fontSize": "18px", "fontWeight": "bold", "fontFamily": "monospace"}),
                        html.Div("Expenses", style={"color": GRAY, "fontSize": "10px"}),
                    ], style={"textAlign": "center", "flex": "1"}),
                ], style={"display": "flex", "gap": "6px", "marginBottom": "12px",
                           "padding": "12px", "backgroundColor": "#ffffff06", "borderRadius": "8px"}),

                # Balance Sheet + Bank chart
                dbc.Row([
                    dbc.Col([
                        section("BALANCE SHEET", [
                            html.Div("ASSETS", style={"color": GREEN, "fontWeight": "bold", "fontSize": "12px", "marginBottom": "4px"}),
                            row_item("Cash On Hand (Bank + Etsy)", ds.bank_cash_on_hand, indent=1, color=GREEN),
                            row_item("Equipment (3D Printers)", ds.bb_cc_asset_value, indent=1, color=TEAL),
                            row_item("TOTAL ASSETS", ds.bank_cash_on_hand + ds.bb_cc_asset_value, bold=True, color=GREEN),
                            html.Div(style={"borderTop": f"1px solid {DARKGRAY}", "margin": "8px 0"}),
                            html.Div("LIABILITIES", style={"color": RED, "fontWeight": "bold", "fontSize": "12px", "marginBottom": "4px"}),
                            row_item("Best Buy CC Balance", -ds.bb_cc_balance, indent=1, color=RED),
                            row_item("TOTAL LIABILITIES", -ds.bb_cc_balance, bold=True, color=RED),
                            html.Div(style={"borderTop": f"3px solid {CYAN}", "marginTop": "10px"}),
                            html.Div([
                                html.Span("NET WORTH", style={"color": CYAN, "fontWeight": "bold", "fontSize": "20px"}),
                                html.Span(ds.money(ds.bank_cash_on_hand + ds.bb_cc_asset_value - ds.bb_cc_balance),
                                           style={"color": CYAN, "fontWeight": "bold", "fontSize": "20px", "fontFamily": "monospace"}),
                            ], style={"display": "flex", "justifyContent": "space-between", "padding": "10px 0"}),
                        ], CYAN),
                    ], md=5),
                    dbc.Col(dbc.Card(dbc.CardBody(dcc.Graph(figure=figs["bank_cat"], config={"displayModeBar": False}))), md=7),
                ], className="g-3 mb-3"),

                # Draw settlement banner
                html.Div([
                    html.Span(f"TJ: {ds.money(ds.tulsa_draw_total)}", style={"color": "#ffb74d", "fontWeight": "bold", "fontSize": "14px"}),
                    html.Span("  |  ", style={"color": DARKGRAY}),
                    html.Span(f"Braden: {ds.money(ds.texas_draw_total)}", style={"color": "#ff9800", "fontWeight": "bold", "fontSize": "14px"}),
                    html.Span("  |  ", style={"color": DARKGRAY}),
                    html.Span(
                        f"Company owes {ds.draw_owed_to} {ds.money(ds.draw_diff)}" if ds.draw_diff >= 0.01 else "Even!",
                        style={"color": CYAN, "fontWeight": "bold", "fontSize": "14px"}),
                ], style={"padding": "10px", "marginBottom": "10px", "backgroundColor": "#ffffff06",
                           "borderRadius": "8px", "border": f"1px solid {CYAN}33", "textAlign": "center"}),

                # Detail cards
                html.Div([
                    _detail_card(f"TJ DRAWS ({len(ds.tulsa_draws)})", "#ffb74d",
                        [_txn_row(t, ORANGE) for t in ds.tulsa_draws], ds.tulsa_draw_total),
                    _detail_card(f"BRADEN DRAWS ({len(ds.texas_draws)})", "#ff9800",
                        [_txn_row(t, ORANGE) for t in ds.texas_draws], ds.texas_draw_total),
                    _detail_card("CASH ON HAND", GREEN, [
                        html.Div([
                            html.Span("Capital One", style={"color": WHITE, "fontSize": "12px", "width": "120px", "display": "inline-block"}),
                            html.Span(ds.money(ds.bank_net_cash), style={"color": GREEN, "fontFamily": "monospace", "fontWeight": "bold"}),
                        ], style={"padding": "4px 0"}),
                        html.Div([
                            html.Span("Etsy Account", style={"color": WHITE, "fontSize": "12px", "width": "120px", "display": "inline-block"}),
                            html.Span(ds.money(ds.etsy_balance), style={"color": TEAL, "fontFamily": "monospace", "fontWeight": "bold"}),
                        ], style={"padding": "4px 0"}),
                    ], ds.bank_cash_on_hand),
                    _detail_card("BEST BUY CC", BLUE, [
                        *[html.Div([
                            html.Span(lbl, style={"color": WHITE, "fontSize": "12px", "width": "100px", "display": "inline-block"}),
                            html.Span(ds.money(val), style={"color": clr, "fontFamily": "monospace", "fontWeight": "bold"}),
                        ], style={"padding": "3px 0"}) for lbl, val, clr in [
                            ("Limit", ds.bb_cc_limit, BLUE), ("Charged", ds.bb_cc_total_charged, RED),
                            ("Paid", ds.bb_cc_total_paid, GREEN), ("Balance", ds.bb_cc_balance, RED),
                            ("Available", ds.bb_cc_available, TEAL)]],
                        html.Div("Purchases:", style={"color": GRAY, "fontSize": "11px", "fontWeight": "bold",
                                  "padding": "6px 0 2px 0", "borderTop": f"1px solid {BLUE}44"}),
                        *[html.Div([
                            html.Span(p["date"], style={"color": GRAY, "fontSize": "10px", "width": "65px", "display": "inline-block"}),
                            html.Span(f"${p['amount']:,.2f}", style={"color": RED, "fontFamily": "monospace", "fontSize": "10px", "width": "55px", "display": "inline-block"}),
                            html.Span(p["desc"][:28], style={"color": WHITE, "fontSize": "10px"}),
                        ], style={"padding": "2px 0", "borderBottom": "1px solid #ffffff08"}) for p in ds.bb_cc_purchases],
                    ], ds.bb_cc_balance),
                ], style={"display": "flex", "gap": "8px", "flexWrap": "wrap"}),
            ], style={"paddingTop": "10px"}),
        ], style={"marginBottom": "8px"}),

        # ═══ 4. SHIPPING ═════════════════════════════════════════════════
        html.Details([
            _section_header("SHIPPING", "are you making or losing money on shipping?"),
            html.Div([
                html.Div([
                    kpi_card("NET SHIPPING P&L", ds.money(ds.shipping_profit),
                        GREEN if ds.shipping_profit >= 0 else RED,
                        f"{'Profitable' if ds.shipping_profit >= 0 else 'Losing'} ({ds.shipping_margin:.1f}%)"),
                    kpi_card("LABEL COST", ds.money(-ds.total_shipping_cost), RED, f"{total_label_count} labels"),
                    kpi_card("BUYER PAID", ds.money(ds.buyer_paid_shipping), GREEN, f"{ds.paid_ship_count} orders"),
                    kpi_card("FREE ORDERS", str(ds.free_ship_count), ORANGE, f"~{ds.money(-ds.est_label_cost_free_orders)} cost"),
                    kpi_card("RETURNS", str(ds.usps_return_count), PINK, f"{ds.money(-ds.usps_return)} labels"),
                    kpi_card("AVG LABEL", f"${ds.avg_outbound_label:.2f}", BLUE, f"{ds.usps_outbound_count} USPS"),
                ], style={"display": "flex", "gap": "8px", "marginBottom": "14px", "flexWrap": "wrap"}),
                dbc.Row([
                    dbc.Col(dbc.Card(dbc.CardBody(dcc.Graph(figure=figs["ship_compare"], config={"displayModeBar": False}))), md=6),
                    dbc.Col(dbc.Card(dbc.CardBody(dcc.Graph(figure=figs["ship_pie"], config={"displayModeBar": False}))), md=6),
                ], className="g-3 mb-3"),
                section("SHIPPING P&L", [
                    row_item(f"Buyers Paid ({ds.paid_ship_count} orders)", ds.buyer_paid_shipping, color=GREEN),
                    html.Div(style={"borderTop": f"1px solid {DARKGRAY}", "margin": "8px 0"}),
                    row_item(f"USPS Outbound ({ds.usps_outbound_count})", -ds.usps_outbound, indent=1),
                    row_item(f"USPS Return ({ds.usps_return_count})", -ds.usps_return, indent=1),
                    row_item(f"Asendia Intl ({ds.asendia_count})", -ds.asendia_labels, indent=1),
                    row_item(f"Adjustments ({ds.ship_adjust_count})", -ds.ship_adjustments, indent=1),
                    *([row_item(f"Insurance ({ds.ship_insurance_count})", -ds.ship_insurance, indent=1)] if ds.ship_insurance > 0 else []),
                    row_item(f"Credits ({ds.ship_credit_count})", ds.ship_credits, indent=1, color=GREEN),
                    row_item("TOTAL SHIPPING COST", -ds.total_shipping_cost, bold=True),
                    html.Div(style={"borderTop": f"3px solid {ORANGE}", "marginTop": "8px"}),
                    html.Div([
                        html.Span("NET SHIPPING P&L", style={"color": GREEN if ds.shipping_profit >= 0 else RED, "fontWeight": "bold", "fontSize": "20px"}),
                        html.Span(ds.money(ds.shipping_profit), style={"color": GREEN if ds.shipping_profit >= 0 else RED, "fontWeight": "bold", "fontSize": "20px", "fontFamily": "monospace"}),
                    ], style={"display": "flex", "justifyContent": "space-between", "padding": "10px 0"}),
                ], ORANGE),
                section("PAID vs FREE BREAKDOWN", [
                    html.P("PAID SHIPPING", style={"color": TEAL, "fontWeight": "bold", "fontSize": "13px", "margin": "0 0 4px 0"}),
                    row_item(f"Buyers Paid ({ds.paid_ship_count} orders)", ds.buyer_paid_shipping, color=GREEN, indent=1),
                    row_item(f"Est. Label Cost ({ds.paid_ship_count} x ${ds.avg_outbound_label:.2f})", -ds.est_label_cost_paid_orders, indent=1),
                    row_item("Profit on Paid Shipping", ds.paid_shipping_profit, bold=True, color=GREEN if ds.paid_shipping_profit >= 0 else RED),
                    html.Div(style={"borderTop": f"1px solid {DARKGRAY}", "margin": "8px 0"}),
                    html.P("FREE SHIPPING", style={"color": ORANGE, "fontWeight": "bold", "fontSize": "13px", "margin": "0 0 4px 0"}),
                    row_item(f"Free Shipping Orders ({ds.free_ship_count})", 0, color=GRAY, indent=1),
                    row_item(f"Est. Label Cost ({ds.free_ship_count} x ${ds.avg_outbound_label:.2f})", -ds.est_label_cost_free_orders, indent=1),
                    row_item("Loss on Free Shipping", -ds.est_label_cost_free_orders, bold=True, color=RED),
                ], TEAL),
                section("RETURNS & REFUNDS", [
                    row_item(f"Return Labels ({ds.usps_return_count})", -ds.usps_return, indent=1),
                ] + [html.Div([
                    html.Span(m["date"], style={"color": GRAY, "width": "130px", "display": "inline-block", "fontSize": "12px"}),
                    html.Span(f"{m['product'][:40]}", style={"color": WHITE, "flex": "1", "fontSize": "12px"}),
                    html.Span(f"Label: ${m['cost']:.2f}", style={"color": RED, "fontFamily": "monospace", "width": "100px", "textAlign": "right", "fontSize": "12px"}),
                    html.Span(f"Refund: ${m['refund_amt']:.2f}", style={"color": ORANGE, "fontFamily": "monospace", "width": "110px", "textAlign": "right", "fontSize": "12px"}),
                ], style={"display": "flex", "padding": "3px 0", "borderBottom": "1px solid #ffffff08", "gap": "6px"})
                for m in ds.return_label_matches], PINK),
            ], style={"paddingTop": "10px"}),
        ], style={"marginBottom": "8px"}),

        # ═══ 5. MONTHLY TRENDS ═══════════════════════════════════════════
        html.Details([
            _section_header("MONTHLY TRENDS", "month-by-month performance + top products"),
            html.Div([
                section("MONTHLY BREAKDOWN", [
                    html.Div([
                        html.Table([
                            html.Thead(html.Tr([
                                html.Th("Month", style={"textAlign": "left", "padding": "6px 8px"}),
                                html.Th("Sales", style={"textAlign": "right", "padding": "6px 8px"}),
                                html.Th("Fees", style={"textAlign": "right", "padding": "6px 8px"}),
                                html.Th("Shipping", style={"textAlign": "right", "padding": "6px 8px"}),
                                html.Th("Marketing", style={"textAlign": "right", "padding": "6px 8px"}),
                                html.Th("Refunds", style={"textAlign": "right", "padding": "6px 8px"}),
                                html.Th("Net", style={"textAlign": "right", "padding": "6px 8px", "fontWeight": "bold"}),
                                html.Th("Margin", style={"textAlign": "right", "padding": "6px 8px"}),
                            ], style={"borderBottom": f"2px solid {BLUE}"})),
                            html.Tbody(
                                [html.Tr([
                                    html.Td(m, style={"color": WHITE, "padding": "4px 8px", "fontSize": "13px"}),
                                    html.Td(f"${ds.monthly_sales.get(m, 0):,.2f}", style={"textAlign": "right", "color": GREEN, "padding": "4px 8px", "fontSize": "13px"}),
                                    html.Td(f"${ds.monthly_fees.get(m, 0):,.2f}", style={"textAlign": "right", "color": RED, "padding": "4px 8px", "fontSize": "13px"}),
                                    html.Td(f"${ds.monthly_shipping.get(m, 0):,.2f}", style={"textAlign": "right", "color": BLUE, "padding": "4px 8px", "fontSize": "13px"}),
                                    html.Td(f"${ds.monthly_marketing.get(m, 0):,.2f}", style={"textAlign": "right", "color": PURPLE, "padding": "4px 8px", "fontSize": "13px"}),
                                    html.Td(f"${ds.monthly_refunds.get(m, 0):,.2f}", style={"textAlign": "right", "color": ORANGE, "padding": "4px 8px", "fontSize": "13px"}),
                                    html.Td(f"${ds.monthly_net_revenue.get(m, 0):,.2f}", style={"textAlign": "right", "color": ORANGE, "fontWeight": "bold", "padding": "4px 8px", "fontSize": "13px"}),
                                    html.Td(
                                        f"{(ds.monthly_net_revenue.get(m, 0) / ds.monthly_sales.get(m, 1) * 100):.1f}%" if ds.monthly_sales.get(m, 0) > 0 else "--",
                                        style={"textAlign": "right", "color": GRAY, "padding": "4px 8px", "fontSize": "13px"}),
                                ], style={"borderBottom": "1px solid #ffffff10"}) for m in ds.months_sorted]
                                + [html.Tr([
                                    html.Td("TOTAL", style={"color": ORANGE, "fontWeight": "bold", "padding": "6px 8px"}),
                                    html.Td(f"${sum(ds.monthly_sales.get(m, 0) for m in ds.months_sorted):,.2f}", style={"textAlign": "right", "color": GREEN, "fontWeight": "bold", "padding": "6px 8px"}),
                                    html.Td(f"${sum(ds.monthly_fees.get(m, 0) for m in ds.months_sorted):,.2f}", style={"textAlign": "right", "color": RED, "fontWeight": "bold", "padding": "6px 8px"}),
                                    html.Td(f"${sum(ds.monthly_shipping.get(m, 0) for m in ds.months_sorted):,.2f}", style={"textAlign": "right", "color": BLUE, "fontWeight": "bold", "padding": "6px 8px"}),
                                    html.Td(f"${sum(ds.monthly_marketing.get(m, 0) for m in ds.months_sorted):,.2f}", style={"textAlign": "right", "color": PURPLE, "fontWeight": "bold", "padding": "6px 8px"}),
                                    html.Td(f"${sum(ds.monthly_refunds.get(m, 0) for m in ds.months_sorted):,.2f}", style={"textAlign": "right", "color": ORANGE, "fontWeight": "bold", "padding": "6px 8px"}),
                                    html.Td(f"${sum(ds.monthly_net_revenue.get(m, 0) for m in ds.months_sorted):,.2f}", style={"textAlign": "right", "color": ORANGE, "fontWeight": "bold", "fontSize": "15px", "padding": "6px 8px"}),
                                    html.Td(f"{(sum(ds.monthly_net_revenue.get(m, 0) for m in ds.months_sorted) / ds.gross_sales * 100):.1f}%" if ds.gross_sales else "--",
                                        style={"textAlign": "right", "color": GRAY, "fontWeight": "bold", "padding": "6px 8px"}),
                                ], style={"borderTop": f"3px solid {ORANGE}"})]
                            ),
                        ], style={"width": "100%", "borderCollapse": "collapse", "color": WHITE}),
                    ]),
                ], BLUE),
                dbc.Card(dbc.CardBody(dcc.Graph(figure=figs["products"], config={"displayModeBar": False})), className="mt-3"),
            ], style={"paddingTop": "10px"}),
        ], style={"marginBottom": "8px"}),

        # ═══ 6. FEES & REFUNDS ═══════════════════════════════════════════
        html.Details([
            _section_header("FEES & REFUNDS", "detailed fee breakdown + refund history"),
            html.Div([
                dbc.Row([
                    dbc.Col([
                        section("FEE & MARKETING DETAIL", [
                            html.Div("FEES CHARGED", style={"color": RED, "fontWeight": "bold", "fontSize": "13px", "marginBottom": "4px"}),
                            row_item("Listing Fees", -ds.listing_fees, indent=1, color=GRAY),
                            row_item("Transaction Fees (product)", -ds.transaction_fees_product, indent=1, color=GRAY),
                            row_item("Transaction Fees (shipping)", -ds.transaction_fees_shipping, indent=1, color=GRAY),
                            row_item("Processing Fees", -ds.processing_fees, indent=1, color=GRAY),
                            row_item("Total Fees (gross)", -total_fees_gross, bold=True),
                            html.Div(style={"height": "8px"}),
                            html.Div("CREDITS RECEIVED", style={"color": GREEN, "fontWeight": "bold", "fontSize": "13px", "marginBottom": "4px"}),
                            row_item("Transaction Credits", ds.credit_transaction, indent=1, color=GREEN),
                            row_item("Listing Credits", ds.credit_listing, indent=1, color=GREEN),
                            row_item("Processing Credits", ds.credit_processing, indent=1, color=GREEN),
                            row_item("Share & Save", ds.share_save, indent=1, color=GREEN),
                            row_item("Total Credits", ds.total_credits, bold=True, color=GREEN),
                            html.Div(style={"borderTop": f"1px solid {ORANGE}44", "margin": "8px 0"}),
                            row_item("Net Fees (after credits)", -net_fees_after_credits, bold=True, color=ORANGE),
                            html.Div(style={"height": "12px"}),
                            html.Div("MARKETING", style={"color": PURPLE, "fontWeight": "bold", "fontSize": "13px", "marginBottom": "4px"}),
                            row_item("Etsy Ads", -ds.etsy_ads, indent=1, color=GRAY),
                            row_item("Offsite Ads", -ds.offsite_ads_fees, indent=1, color=GRAY),
                            *([row_item("Offsite Credits", ds.offsite_ads_credits, indent=1, color=GREEN)] if ds.offsite_ads_credits != 0 else []),
                            row_item("Total Marketing", -ds.total_marketing, bold=True),
                        ], RED),
                    ], md=6),
                    dbc.Col([
                        section("REFUNDS", [
                            html.Div([
                                html.Div([
                                    html.Span("Total Refunded", style={"color": GRAY, "fontSize": "11px"}),
                                    html.Div(ds.money(ds.total_refunds), style={"color": RED, "fontSize": "20px", "fontWeight": "bold", "fontFamily": "monospace"}),
                                ], style={"textAlign": "center", "flex": "1"}),
                                html.Div([
                                    html.Span("Count", style={"color": GRAY, "fontSize": "11px"}),
                                    html.Div(f"{len(ds.refund_df)}", style={"color": ORANGE, "fontSize": "20px", "fontWeight": "bold", "fontFamily": "monospace"}),
                                ], style={"textAlign": "center", "flex": "1"}),
                                html.Div([
                                    html.Span("Avg Refund", style={"color": GRAY, "fontSize": "11px"}),
                                    html.Div(f"${ds.total_refunds / max(len(ds.refund_df), 1):,.2f}", style={"color": ORANGE, "fontSize": "20px", "fontWeight": "bold", "fontFamily": "monospace"}),
                                ], style={"textAlign": "center", "flex": "1"}),
                            ], style={"display": "flex", "gap": "8px", "padding": "10px",
                                       "backgroundColor": "#ffffff06", "borderRadius": "8px", "marginBottom": "10px"}),
                        ] + [html.Div([
                            html.Span(f"{r['Date']}", style={"color": GRAY, "width": "110px", "display": "inline-block", "fontSize": "12px"}),
                            html.Span(f"{r['Title'][:50]}", style={"color": WHITE, "flex": "1", "fontSize": "12px"}),
                            html.Span(f"${abs(r['Net_Clean']):,.2f}", style={"color": RED, "fontFamily": "monospace", "width": "80px", "textAlign": "right", "fontSize": "12px"}),
                        ], style={"display": "flex", "padding": "3px 0", "borderBottom": "1px solid #ffffff08"})
                        for _, r in ds.refund_df.sort_values("Date_Parsed", ascending=False).iterrows()], ORANGE),
                    ], md=6),
                ], className="g-3"),
            ], style={"paddingTop": "10px"}),
        ], style={"marginBottom": "8px"}),

        # ═══ 7. BANK LEDGER ══════════════════════════════════════════════
        html.Details([
            _section_header("BANK LEDGER", "full transaction history with running balance"),
            html.Div([
                dbc.Card(dbc.CardBody(dcc.Graph(figure=figs["bank_monthly"], config={"displayModeBar": False})), className="mb-3"),
                section(f"FULL LEDGER ({len(ds.BANK_TXNS)} Transactions)", [
                    html.Div([
                        html.Table([
                            html.Thead(html.Tr([
                                html.Th("Date", style={"textAlign": "left", "padding": "6px 8px"}),
                                html.Th("Description", style={"textAlign": "left", "padding": "6px 8px"}),
                                html.Th("Category", style={"textAlign": "left", "padding": "6px 8px"}),
                                html.Th("Deposit", style={"textAlign": "right", "padding": "6px 8px"}),
                                html.Th("Debit", style={"textAlign": "right", "padding": "6px 8px"}),
                                html.Th("Balance", style={"textAlign": "right", "padding": "6px 8px"}),
                            ], style={"borderBottom": f"2px solid {CYAN}"})),
                            html.Tbody([
                                html.Tr([
                                    html.Td(t["date"], style={"color": GRAY, "padding": "4px 8px", "fontSize": "12px"}),
                                    html.Td(t["desc"][:45], style={"color": WHITE, "padding": "4px 8px", "fontSize": "12px"}),
                                    html.Td(t["category"], style={"color": GRAY, "padding": "4px 8px", "fontSize": "11px"}),
                                    html.Td(
                                        f"+${t['amount']:,.2f}" if t["type"] == "deposit" else "",
                                        style={"textAlign": "right", "color": GREEN, "fontWeight": "bold",
                                               "padding": "4px 8px", "fontSize": "12px", "fontFamily": "monospace"}),
                                    html.Td(
                                        f"-${t['amount']:,.2f}" if t["type"] != "deposit" else "",
                                        style={"textAlign": "right", "color": RED, "fontWeight": "bold",
                                               "padding": "4px 8px", "fontSize": "12px", "fontFamily": "monospace"}),
                                    html.Td(
                                        f"${t['_balance']:,.2f}",
                                        style={"textAlign": "right",
                                               "color": GREEN if t["_balance"] >= 0 else RED,
                                               "fontWeight": "bold",
                                               "padding": "4px 8px", "fontSize": "12px", "fontFamily": "monospace"}),
                                ], style={"borderBottom": "1px solid #ffffff10",
                                           "backgroundColor": f"{GREEN}08" if t["type"] == "deposit" else "#ffffff04"})
                                for t in ds.bank_running
                            ] + [html.Tr([
                                html.Td("TOTAL", colSpan="3", style={"color": CYAN, "fontWeight": "bold", "padding": "8px"}),
                                html.Td(f"${ds.bank_total_deposits:,.2f}", style={"textAlign": "right", "color": GREEN, "fontWeight": "bold", "padding": "8px", "fontFamily": "monospace"}),
                                html.Td(f"${ds.bank_total_debits:,.2f}", style={"textAlign": "right", "color": RED, "fontWeight": "bold", "padding": "8px", "fontFamily": "monospace"}),
                                html.Td(f"${ds.bank_net_cash:,.2f}", style={"textAlign": "right", "color": CYAN, "fontWeight": "bold", "padding": "8px", "fontFamily": "monospace"}),
                            ], style={"borderTop": f"3px solid {CYAN}"})]),
                        ], style={"width": "100%", "borderCollapse": "collapse", "color": WHITE}),
                    ], style={"maxHeight": "700px", "overflowY": "auto"}),
                ], CYAN),
            ], style={"paddingTop": "10px"}),
        ], style={"marginBottom": "8px"}),
    ])
