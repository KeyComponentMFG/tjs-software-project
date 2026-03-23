"""Tab 3 — Financials: Full P&L, Cash Flow, Shipping, Monthly Trends, Fees, Bank Ledger."""

from dash import html, dcc
import dash_bootstrap_components as dbc
import plotly.graph_objects as go
from dashboard_utils.theme import *


def build_tab3_financials():
    """Tab 3 - Financials: Full P&L + Cash Flow + Shipping + Monthly + Fees + Ledger"""
    import etsy_dashboard as ed

    # Pull globals from the monolith
    total_fees_gross = ed.total_fees_gross
    total_credits = ed.total_credits
    _DATA_ALL = ed._DATA_ALL
    DATA = ed.DATA
    refund_df = ed.refund_df
    _extract_order_num = ed._extract_order_num
    _refund_assignments = ed._refund_assignments
    ship_df = ed.ship_df
    usps_outbound_count = ed.usps_outbound_count
    usps_return_count = ed.usps_return_count
    asendia_count = ed.asendia_count
    gross_sales = ed.gross_sales
    total_fees = ed.total_fees
    total_shipping_cost = ed.total_shipping_cost
    total_marketing = ed.total_marketing
    total_refunds = ed.total_refunds
    total_taxes = ed.total_taxes
    total_buyer_fees = ed.total_buyer_fees
    bank_by_cat = ed.bank_by_cat
    bank_owner_draw_total = ed.bank_owner_draw_total
    old_bank_receipted = ed.old_bank_receipted
    bank_unaccounted = ed.bank_unaccounted
    etsy_csv_gap = ed.etsy_csv_gap
    strict_mode = ed.strict_mode
    _strict_banner = ed._strict_banner
    bb_cc_balance = ed.bb_cc_balance
    bb_cc_available = ed.bb_cc_available
    bb_cc_limit = ed.bb_cc_limit
    bb_cc_total_charged = ed.bb_cc_total_charged
    bb_cc_total_paid = ed.bb_cc_total_paid
    bb_cc_asset_value = ed.bb_cc_asset_value
    _etsy_deposit_total = ed._etsy_deposit_total
    _deposit_rows = ed._deposit_rows
    etsy_balance = ed.etsy_balance
    order_count = ed.order_count
    profit = ed.profit
    profit_margin = ed.profit_margin
    bank_cash_on_hand = ed.bank_cash_on_hand
    _build_kpi_pill = ed._build_kpi_pill
    expense_pie = ed.expense_pie
    sankey_fig = ed.sankey_fig
    product_fig = ed.product_fig
    bank_monthly_fig = ed.bank_monthly_fig
    tulsa_draw_total = ed.tulsa_draw_total
    texas_draw_total = ed.texas_draw_total
    draw_owed_to = ed.draw_owed_to
    draw_diff = ed.draw_diff
    tulsa_draws = ed.tulsa_draws
    texas_draws = ed.texas_draws
    get_draw_reason = ed.get_draw_reason
    personal_inv_items = ed.personal_inv_items
    bank_net_cash = ed.bank_net_cash
    bank_all_expenses = ed.bank_all_expenses
    bank_debits = ed.bank_debits
    _biz_expense_cats = ed._biz_expense_cats
    _bank_amazon_txns = ed._bank_amazon_txns
    expense_receipt_verified = ed.expense_receipt_verified
    expense_bank_recorded = ed.expense_bank_recorded
    expense_gap = ed.expense_gap
    expense_by_category = ed.expense_by_category
    expense_missing_receipts = ed.expense_missing_receipts
    expense_matched_count = ed.expense_matched_count
    listing_fees = ed.listing_fees
    transaction_fees_product = ed.transaction_fees_product
    transaction_fees_shipping = ed.transaction_fees_shipping
    processing_fees = ed.processing_fees
    credit_transaction = ed.credit_transaction
    credit_listing = ed.credit_listing
    credit_processing = ed.credit_processing
    share_save = ed.share_save
    etsy_ads = ed.etsy_ads
    offsite_ads_fees = ed.offsite_ads_fees
    offsite_ads_credits = ed.offsite_ads_credits
    etsy_net = ed.etsy_net
    days_active = ed.days_active
    avg_order = ed.avg_order
    usps_outbound = ed.usps_outbound
    usps_return = ed.usps_return
    asendia_labels = ed.asendia_labels
    ship_adjustments = ed.ship_adjustments
    ship_credits = ed.ship_credits
    ship_adjust_count = ed.ship_adjust_count
    ship_insurance_count = ed.ship_insurance_count
    ship_insurance = ed.ship_insurance
    ship_credit_count = ed.ship_credit_count
    avg_outbound_label = ed.avg_outbound_label
    paid_ship_count = ed.paid_ship_count
    free_ship_count = ed.free_ship_count
    return_label_matches = ed.return_label_matches
    monthly_sales = ed.monthly_sales
    monthly_fees = ed.monthly_fees
    monthly_shipping = ed.monthly_shipping
    monthly_marketing = ed.monthly_marketing
    monthly_refunds = ed.monthly_refunds
    monthly_net_revenue = ed.monthly_net_revenue
    months_sorted = ed.months_sorted
    BANK_TXNS = ed.BANK_TXNS
    bank_running = ed.bank_running
    bank_total_deposits = ed.bank_total_deposits
    bank_total_debits = ed.bank_total_debits
    _bank_cat_color_map = ed._bank_cat_color_map
    _build_shipping_compare = ed._build_shipping_compare
    _build_ship_type = ed._build_ship_type
    _build_per_order_profit_section = ed._build_per_order_profit_section
    bb_cc_purchases = ed.bb_cc_purchases

    net_fees_after_credits = total_fees_gross - abs(total_credits)

    # Build order->product map for refund display
    _refund_product_map = {}
    _all_data = _DATA_ALL if _DATA_ALL is not None else DATA
    _all_fees = _all_data[_all_data["Type"] == "Fee"]
    for _, _fr in _all_fees.iterrows():
        _ftitle = str(_fr.get("Title", ""))
        _finfo = str(_fr.get("Info", ""))
        if _ftitle.startswith("Transaction fee:") and "Shipping" not in _ftitle:
            _prod = _ftitle.replace("Transaction fee: ", "").strip()
            if _finfo and _finfo != "nan":
                _refund_product_map[_finfo] = _prod

    # Refund accountability totals
    _rtj_n = _rtj_amt = _rbr_n = _rbr_amt = _rca_n = _rca_amt = 0
    for _, _rr in refund_df.iterrows():
        _rkey = _extract_order_num(_rr.get("Title", ""))
        _rassign = _refund_assignments.get(_rkey, "") if _rkey else ""
        _rval = abs(_rr["Net_Clean"])
        if _rassign == "TJ":
            _rtj_n += 1; _rtj_amt += _rval
        elif _rassign == "Braden":
            _rbr_n += 1; _rbr_amt += _rval
        elif _rassign == "Cancelled":
            _rca_n += 1; _rca_amt += _rval

    # Build refund section rows (before return statement)
    _refund_section_rows = [
        html.Div([
            html.Div([
                html.Span("Total Refunded", style={"color": GRAY, "fontSize": "11px"}),
                html.Div(f"${total_refunds:,.2f}", style={"color": RED, "fontSize": "20px", "fontWeight": "bold", "fontFamily": "monospace"}),
            ], style={"textAlign": "center", "flex": "1"}),
            html.Div([
                html.Span("Count", style={"color": GRAY, "fontSize": "11px"}),
                html.Div(f"{len(refund_df)}", style={"color": ORANGE, "fontSize": "20px", "fontWeight": "bold", "fontFamily": "monospace"}),
            ], style={"textAlign": "center", "flex": "1"}),
            html.Div([
                html.Span("Avg Refund", style={"color": GRAY, "fontSize": "11px"}),
                html.Div(f"${total_refunds / len(refund_df) if len(refund_df) else 0:,.2f}", style={"color": ORANGE, "fontSize": "20px", "fontWeight": "bold", "fontFamily": "monospace"}),
            ], style={"textAlign": "center", "flex": "1"}),
        ], style={"display": "flex", "gap": "8px", "padding": "10px", "backgroundColor": "#ffffff06", "borderRadius": "8px", "marginBottom": "4px"}),
    ]
    if len(refund_df) > 0:
        _refund_section_rows.append(html.Div([
            html.Span(f"TJ: {_rtj_n} (${_rtj_amt:,.2f})", style={"color": CYAN, "fontSize": "12px", "fontWeight": "bold"}),
            html.Span("  |  ", style={"color": DARKGRAY}),
            html.Span(f"Braden: {_rbr_n} (${_rbr_amt:,.2f})", style={"color": GREEN, "fontSize": "12px", "fontWeight": "bold"}),
            html.Span("  |  ", style={"color": DARKGRAY}),
            html.Span(f"Cancelled: {_rca_n} (${_rca_amt:,.2f})", style={"color": ORANGE, "fontSize": "12px"}),
        ], style={"textAlign": "center", "padding": "6px", "marginBottom": "10px"}))
    for i, (_, r) in enumerate(refund_df.sort_values("Date_Parsed", ascending=False).iterrows()):
        _rkey = _extract_order_num(r['Title']) or f"row-{i}"
        _rprod = _refund_product_map.get(_extract_order_num(r['Title']), "")
        _rassignee = _refund_assignments.get(_rkey, "")
        _rstore = r.get('Store', 'keycomponentmfg')
        _rborder = (f"1px solid {CYAN}" if _rassignee == "TJ"
                    else (f"1px solid {GREEN}" if _rassignee == "Braden"
                          else (f"1px solid {ORANGE}" if _rassignee == "Cancelled"
                                else "1px solid #ffffff20")))
        _store_badge = ([html.Span(f" | {_rstore}", style={"color": STORE_COLORS.get(_rstore, GRAY),
                         "fontSize": "10px", "fontWeight": "bold"})]
                        if _rstore != "keycomponentmfg" else [])
        _refund_section_rows.append(html.Div([
            html.Span(f"{r['Date']}", style={"color": GRAY, "width": "90px", "display": "inline-block", "fontSize": "11px"}),
            html.Div([
                html.Div(_rprod[:45] if _rprod else r['Title'][:45],
                         style={"color": WHITE, "fontSize": "12px", "lineHeight": "1.2"}),
                html.Div([
                    html.Span(_extract_order_num(r['Title']) or "", style={"color": DARKGRAY, "fontSize": "10px"}),
                ] + _store_badge, style={"display": "flex", "gap": "4px"}),
            ], style={"flex": "1"}),
            html.Span(f"${abs(r['Net_Clean']):,.2f}", style={"color": RED, "fontFamily": "monospace",
                       "width": "70px", "textAlign": "right", "fontSize": "12px"}),
            dbc.Select(
                id={"type": "refund-assignee", "index": _rkey},
                options=[{"label": "\u2014", "value": ""}, {"label": "TJ", "value": "TJ"},
                         {"label": "Braden", "value": "Braden"}, {"label": "Cancelled", "value": "Cancelled"}],
                value=_rassignee,
                style={"width": "90px", "height": "26px", "fontSize": "11px", "padding": "2px 4px",
                       "backgroundColor": "#1a1a2e", "color": WHITE, "border": _rborder,
                       "borderRadius": "4px", "marginLeft": "8px"},
            ),
        ], style={"display": "flex", "alignItems": "center", "padding": "4px 0",
                   "borderBottom": "1px solid #ffffff08"}))

    # Shipping data needed
    usps_labels = ship_df[ship_df["Title"] == "USPS shipping label"]["Net_Clean"].abs()
    usps_min = usps_labels.min() if len(usps_labels) else 0
    usps_max = usps_labels.max() if len(usps_labels) else 0
    total_label_count = usps_outbound_count + usps_return_count + asendia_count

    # Reconciliation waterfall
    _wf_labels = [
        "Gross Sales",
        "Fees", "Ship Labels", "Ads", "Refunds", "Taxes",
    ]
    _wf_values = [
        _safe(gross_sales),
        -_safe(total_fees), -_safe(total_shipping_cost), -_safe(total_marketing), -_safe(total_refunds), -_safe(total_taxes),
    ]
    _wf_measures = [
        "absolute",
        "relative", "relative", "relative", "relative", "relative",
    ]
    if _safe(total_buyer_fees) > 0:
        _wf_labels.append("CO Buyer Fee")
        _wf_values.append(-_safe(total_buyer_fees))
        _wf_measures.append("relative")
    _wf_labels += ["AFTER ETSY FEES"]
    _wf_values += [0]
    _wf_measures += ["total"]
    # Bank expenses — only include non-zero categories
    for _lbl, _val in [
        ("Amazon Inv.", bank_by_cat.get("Amazon Inventory", 0)),
        ("AliExpress", bank_by_cat.get("AliExpress Supplies", 0)),
        ("Craft", bank_by_cat.get("Craft Supplies", 0)),
        ("Etsy Fees", bank_by_cat.get("Etsy Fees", 0)),
        ("Subscriptions", bank_by_cat.get("Subscriptions", 0)),
        ("Ship Supplies", bank_by_cat.get("Shipping", 0)),
        ("Best Buy CC", bank_by_cat.get("Business Credit Card", 0)),
    ]:
        if _val > 0:
            _wf_labels.append(_lbl)
            _wf_values.append(-_val)
            _wf_measures.append("relative")
    if _safe(bank_owner_draw_total) > 0:
        _wf_labels.append("Owner Draws")
        _wf_values.append(-_safe(bank_owner_draw_total))
        _wf_measures.append("relative")
    # Reconciliation items — only include non-zero
    for _lbl, _val in [
        ("Prior Bank Inv.", _safe(old_bank_receipted)),
        ("Unmatched Bank", _safe(bank_unaccounted)),
        ("Untracked Etsy", _safe(etsy_csv_gap)),
    ]:
        if abs(_val) > 0.01:
            _wf_labels.append(_lbl)
            _wf_values.append(-_val)
            _wf_measures.append("relative")
    _wf_labels.append("CASH ON HAND")
    _wf_values.append(0)
    _wf_measures.append("total")
    recon_fig = go.Figure(go.Waterfall(
        orientation="v", measure=_wf_measures, x=_wf_labels, y=_wf_values,
        connector={"line": {"color": "#555"}},
        decreasing={"marker": {"color": RED}},
        increasing={"marker": {"color": GREEN}},
        totals={"marker": {"color": CYAN}},
        textposition="outside",
        text=[f"${abs(v):,.0f}" if v != 0 else "" for v in _wf_values],
    ))
    make_chart(recon_fig, 420)
    recon_fig.update_layout(
        title="HOW EVERY DOLLAR BALANCES",
        yaxis_title="Amount ($)",
        xaxis_tickangle=-35,
        xaxis_tickfont=dict(size=10),
    )

    _sm = strict_mode if isinstance(strict_mode, bool) else False

    # Strict mode banner (financials)
    _strict_banner_fin = _strict_banner("Only VERIFIED metrics shown. Income tax estimates and derived ratios are hidden.") if _sm else html.Div()

    _fee_pct = f"{net_fees_after_credits / gross_sales * 100:.1f}% of sales" if gross_sales and net_fees_after_credits is not None else ""
    _refund_pct = f"{len(refund_df)} orders ({len(refund_df) / order_count * 100:.1f}%)" if order_count else ""

    return html.Div([
        _strict_banner_fin,
        # KPI pills (always visible at top)
        html.Div([
            _build_kpi_pill("\U0001f4b3", "DEBT", money(bb_cc_balance), RED,
                            f"Best Buy CC ({money(bb_cc_available)} avail)",
                            f"Best Buy Citi CC for equipment. Charged: {money(bb_cc_total_charged)}. Paid: {money(bb_cc_total_paid)}. Balance: {money(bb_cc_balance)}. Limit: {money(bb_cc_limit)}. Asset value: {money(bb_cc_asset_value)}.", status="verified"),
            _build_kpi_pill("\U0001f4e5", "ETSY DEPOSITS", money(_etsy_deposit_total), ORANGE,
                            f"Sent to bank ({len(_deposit_rows)} deposits)" if _DATA_ALL is None or len(DATA) == len(_DATA_ALL) else f"Sent to bank \u2014 {STORES.get(DATA['Store'].iloc[0], 'this store') if len(DATA) > 0 and 'Store' in DATA.columns else 'filtered'}",
                            f"Sum of Etsy deposit transactions to your bank account ({len(_deposit_rows)} deposits). Remaining Etsy balance: {money(etsy_balance)}.", status="verified"),
            _build_kpi_pill("\U0001f4c9", "TOTAL FEES", money(net_fees_after_credits), RED,
                            _fee_pct,
                            f"Listing: {money(listing_fees)}. Transaction (product): {money(transaction_fees_product)}. Transaction (shipping): {money(transaction_fees_shipping)}. Processing: {money(processing_fees)}. Credits: {money(abs(total_credits) if total_credits is not None else None)}. Net: {money(net_fees_after_credits)}.", status="verified"),
            _build_kpi_pill("\u21a9\ufe0f", "REFUNDS", money(total_refunds), PINK,
                            _refund_pct,
                            f"{len(refund_df)} refunded of {order_count} total. Avg refund: {money(total_refunds / max(len(refund_df), 1) if total_refunds is not None else None)}. Return labels: {money(usps_return)} ({usps_return_count} labels).", status="verified"),
            _build_kpi_pill("\U0001f4b0", "PROFIT", money(profit), GREEN,
                            f"{_fmt(profit_margin, prefix='', fmt='.1f', unknown='?')}% margin",
                            f"Revenue minus all costs. Cash ({money(bank_cash_on_hand)}) + draws ({money(bank_owner_draw_total)}).", status="verified"),
        ], style={"display": "flex", "gap": "8px", "marginBottom": "14px", "flexWrap": "wrap"}),

        # Business Snapshot banner
        html.Div([
            html.Div(f"You've earned {money(gross_sales)} in gross sales across {order_count} orders. "
                     f"After all Etsy fees, shipping, and expenses, your profit is {money(profit)} ({_fmt(profit_margin, prefix='', fmt='.1f', unknown='?')}% margin). "
                     f"You have {money(bank_cash_on_hand)} cash on hand.",
                     style={"color": WHITE, "fontSize": "14px", "lineHeight": "1.6"}),
        ], style={"backgroundColor": CARD, "borderRadius": "8px", "marginBottom": "14px",
                   "borderLeft": f"4px solid {CYAN}", "padding": "16px 20px",
                   "boxShadow": "0 2px 8px rgba(0,0,0,0.3)"}),

        # 1. THE BIG PICTURE
        html.Details([
            html.Summary([
                html.Span("\u25b6 ", style={"fontSize": "12px"}),
                html.Span("THE BIG PICTURE", style={"fontWeight": "bold"}),
                html.Span(" \u2014 see where every dollar goes", style={"color": GRAY, "fontWeight": "normal", "fontSize": "12px"}),
            ], style={"color": CYAN, "fontSize": "14px", "fontWeight": "bold",
                "cursor": "pointer", "padding": "10px 14px", "listStyle": "none",
                "backgroundColor": "#ffffff08", "borderRadius": "6px",
                "border": f"1px solid {CYAN}33"}),
            html.Div([
        # Expense donut + Reconciliation waterfall side by side
        html.Div([
            html.Div([dcc.Graph(figure=expense_pie, config={"displayModeBar": False})], style={"flex": "1"}),
            html.Div([dcc.Graph(figure=recon_fig, config={"displayModeBar": False})], style={"flex": "1"}),
        ], style={"display": "flex", "gap": "8px", "marginBottom": "10px"}),
            ], style={"paddingTop": "10px"}),
        ], style={"marginBottom": "8px"}),

        # 2. PROFIT & LOSS
        html.Details([
            html.Summary([
                html.Span("\u25b6 ", style={"fontSize": "12px"}),
                html.Span("PROFIT & LOSS", style={"fontWeight": "bold"}),
                html.Span(" \u2014 line-by-line from gross sales to final profit", style={"color": GRAY, "fontWeight": "normal", "fontSize": "12px"}),
            ], style={"color": CYAN, "fontSize": "14px", "fontWeight": "bold",
                "cursor": "pointer", "padding": "10px 14px", "listStyle": "none",
                "backgroundColor": "#ffffff08", "borderRadius": "6px",
                "border": f"1px solid {CYAN}33"}),
            html.Div([

        # Full P&L
        section("PROFIT & LOSS", [
            row_item("Gross Sales", gross_sales, color=GREEN, metric_name="gross_sales"),
            html.Div("ETSY DEDUCTIONS:", style={"color": RED, "fontWeight": "bold", "fontSize": "12px",
                      "marginTop": "8px", "marginBottom": "4px"}),
            row_item("  Fees (listing + transaction + processing)", -total_fees_gross, indent=1, color=GRAY, metric_name="total_fees_gross"),
            row_item("  Fee Credits", abs(total_credits), indent=1, color=GREEN, metric_name="total_credits"),
            row_item("  Shipping Labels", -total_shipping_cost, indent=1, color=GRAY, metric_name="total_shipping_cost"),
            row_item("  Ads & Marketing", -total_marketing, indent=1, color=GRAY, metric_name="total_marketing"),
            row_item("  Refunds to Customers", -total_refunds, indent=1, color=GRAY, metric_name="total_refunds"),
            row_item("  Sales Tax Collected & Remitted", -total_taxes, indent=1, color=GRAY, metric_name="total_taxes"),
            row_item("  Buyer Fees", -total_buyer_fees, indent=1, color=GRAY, metric_name="total_buyer_fees") if total_buyer_fees > 0 else html.Div(),
            html.Div(style={"borderTop": f"2px solid {ORANGE}44", "margin": "6px 0"}),
            row_item("AFTER ETSY FEES (what Etsy pays you)", etsy_net, bold=True, color=ORANGE, metric_name="etsy_net_earned"),
            html.Div("BANK EXPENSES (from Cap One statement):", style={"color": RED, "fontWeight": "bold", "fontSize": "12px",
                      "marginTop": "10px", "marginBottom": "4px"}),
            row_item("  Amazon Inventory", -bank_by_cat.get("Amazon Inventory", 0), indent=1, color=GRAY),
            row_item("  AliExpress Supplies", -bank_by_cat.get("AliExpress Supplies", 0), indent=1, color=GRAY),
            row_item("  Craft Supplies", -bank_by_cat.get("Craft Supplies", 0), indent=1, color=GRAY),
            row_item("  Etsy Bank Fees", -bank_by_cat.get("Etsy Fees", 0), indent=1, color=GRAY),
            row_item("  Subscriptions", -bank_by_cat.get("Subscriptions", 0), indent=1, color=GRAY),
            row_item("  Shipping (UPS/USPS)", -bank_by_cat.get("Shipping", 0), indent=1, color=GRAY),
            row_item("  Best Buy CC Payment (toward equipment)", -bank_by_cat.get("Business Credit Card", 0), indent=1, color=GRAY),
            row_item("    CC Balance Still Owed (liability)", -bb_cc_balance, indent=2, color=BLUE),
            row_item("    Equipment Purchased (asset)", bb_cc_asset_value, indent=2, color=TEAL),
            row_item("  Owner Draws", -bank_owner_draw_total, indent=1, color=ORANGE),
            row_item("  Old bank inventory (receipted)", -old_bank_receipted, indent=1, color=ORANGE),
            row_item("  Untracked (prior bank + Etsy gap)", -(_safe(bank_unaccounted) + _safe(etsy_csv_gap)), indent=1, color=RED),
            html.Div(style={"borderTop": f"3px solid {GREEN}", "marginTop": "10px"}),
            html.Div([
                html.Span("PROFIT", style={"color": GREEN, "fontWeight": "bold", "fontSize": "22px"}),
                html.Span(money(profit), style={"color": GREEN, "fontWeight": "bold", "fontSize": "22px", "fontFamily": "monospace"}),
            ], style={"display": "flex", "justifyContent": "space-between", "padding": "12px 0"}),
            html.Div(f"= Cash On Hand {money(bank_cash_on_hand)} + Owner Draws {money(bank_owner_draw_total)}",
                     style={"color": GRAY, "fontSize": "12px", "textAlign": "center"}),
            html.Div([
                html.Div([
                    html.Span("Profit/Day", style={"color": GRAY, "fontSize": "11px"}),
                    html.Div(f"${profit / days_active:,.2f}",
                             style={"color": GREEN, "fontSize": "18px", "fontWeight": "bold", "fontFamily": "monospace"}),
                ], style={"textAlign": "center", "flex": "1"}),
                html.Div([
                    html.Span("Profit/Order", style={"color": GRAY, "fontSize": "11px"}),
                    html.Div(f"${profit / order_count if order_count else 0:,.2f}",
                             style={"color": GREEN, "fontSize": "18px", "fontWeight": "bold", "fontFamily": "monospace"}),
                ], style={"textAlign": "center", "flex": "1"}),
                html.Div([
                    html.Span("Avg Order Value", style={"color": GRAY, "fontSize": "11px"}),
                    html.Div(f"${avg_order:,.2f}",
                             style={"color": TEAL, "fontSize": "18px", "fontWeight": "bold", "fontFamily": "monospace"}),
                ], style={"textAlign": "center", "flex": "1"}),
                html.Div([
                    html.Span("Revenue/Day", style={"color": GRAY, "fontSize": "11px"}),
                    html.Div(f"${gross_sales / days_active:,.2f}",
                             style={"color": TEAL, "fontSize": "18px", "fontWeight": "bold", "fontFamily": "monospace"}),
                ], style={"textAlign": "center", "flex": "1"}),
            ], style={"display": "flex", "gap": "8px", "padding": "10px",
                       "backgroundColor": "#ffffff06", "borderRadius": "8px"}),
        ], GREEN),
            ], style={"paddingTop": "10px"}),
        ], style={"marginBottom": "8px"}),

        # 3. CASH & BALANCE SHEET
        html.Details([
            html.Summary([
                html.Span("\u25b6 ", style={"fontSize": "12px"}),
                html.Span("CASH & BALANCE SHEET", style={"fontWeight": "bold"}),
                html.Span(" \u2014 bank balances, assets, debts, and owner draws", style={"color": GRAY, "fontWeight": "normal", "fontSize": "12px"}),
            ], style={"color": CYAN, "fontSize": "14px", "fontWeight": "bold",
                "cursor": "pointer", "padding": "10px 14px", "listStyle": "none",
                "backgroundColor": "#ffffff08", "borderRadius": "6px",
                "border": f"1px solid {CYAN}33"}),
            html.Div([

        # Cash KPIs
        html.Div([
            html.Div([
                html.Div(f"${profit:,.2f}", style={"color": GREEN, "fontSize": "22px", "fontWeight": "bold", "fontFamily": "monospace"}),
                html.Div("PROFIT", style={"color": GRAY, "fontSize": "10px"}),
            ], style={"textAlign": "center", "flex": "1"}),
            html.Div([
                html.Div(f"${bank_cash_on_hand:,.2f}", style={"color": CYAN, "fontSize": "18px", "fontWeight": "bold", "fontFamily": "monospace"}),
                html.Div(f"Cash (Bank ${bank_net_cash:,.0f} + Etsy ${etsy_balance:,.0f})", style={"color": GRAY, "fontSize": "10px"}),
            ], style={"textAlign": "center", "flex": "1"}),
            html.Div([
                html.Div(f"${bank_owner_draw_total:,.2f}", style={"color": ORANGE, "fontSize": "18px", "fontWeight": "bold", "fontFamily": "monospace"}),
                html.Div("Owner Draws", style={"color": GRAY, "fontSize": "10px"}),
            ], style={"textAlign": "center", "flex": "1"}),
            html.Div([
                html.Div(f"${bank_all_expenses:,.2f}", style={"color": RED, "fontSize": "18px", "fontWeight": "bold", "fontFamily": "monospace"}),
                html.Div("Expenses", style={"color": GRAY, "fontSize": "10px"}),
            ], style={"textAlign": "center", "flex": "1"}),
        ], style={"display": "flex", "gap": "6px", "marginBottom": "10px",
                   "padding": "10px", "backgroundColor": "#ffffff06", "borderRadius": "8px"}),

        # Mini Balance Sheet
        section("BALANCE SHEET (snapshot)", [
            html.Div("ASSETS", style={"color": GREEN, "fontWeight": "bold", "fontSize": "12px", "marginBottom": "4px"}),
            row_item("  Cash On Hand (Bank + Etsy)", bank_cash_on_hand, indent=1, color=GREEN),
            row_item("  Equipment (3D Printers)", bb_cc_asset_value, indent=1, color=TEAL),
            row_item("TOTAL ASSETS", bank_cash_on_hand + bb_cc_asset_value, bold=True, color=GREEN),
            html.Div(style={"borderTop": f"1px solid {DARKGRAY}", "margin": "8px 0"}),
            html.Div("LIABILITIES", style={"color": RED, "fontWeight": "bold", "fontSize": "12px", "marginBottom": "4px"}),
            row_item("  Best Buy CC Balance", -bb_cc_balance, indent=1, color=RED),
            row_item("TOTAL LIABILITIES", -bb_cc_balance, bold=True, color=RED),
            html.Div(style={"borderTop": f"3px solid {CYAN}", "marginTop": "10px"}),
            html.Div([
                html.Span("NET WORTH", style={"color": CYAN, "fontWeight": "bold", "fontSize": "20px"}),
                html.Span(f"${bank_cash_on_hand + bb_cc_asset_value - bb_cc_balance:,.2f}",
                           style={"color": CYAN, "fontWeight": "bold", "fontSize": "20px", "fontFamily": "monospace"}),
            ], style={"display": "flex", "justifyContent": "space-between", "padding": "10px 0"}),
            html.Div(f"= Cash ${bank_cash_on_hand:,.2f} + Equipment ${bb_cc_asset_value:,.2f} - CC Debt ${bb_cc_balance:,.2f}",
                     style={"color": GRAY, "fontSize": "12px", "textAlign": "center"}),
        ], CYAN),

        # Sankey diagram
        dcc.Graph(figure=sankey_fig, config={"displayModeBar": False}),

        # Draw settlement banner
        html.Div([
            html.Div([
                html.Span(f"TJ: ${tulsa_draw_total:,.2f}", style={"color": "#ffb74d", "fontWeight": "bold", "fontSize": "14px"}),
                html.Span("  |  ", style={"color": DARKGRAY}),
                html.Span(f"Braden: ${texas_draw_total:,.2f}", style={"color": "#ff9800", "fontWeight": "bold", "fontSize": "14px"}),
                html.Span("  |  ", style={"color": DARKGRAY}),
                html.Span(
                    f"Company owes {draw_owed_to} ${draw_diff:,.2f}" if draw_diff >= 0.01
                    else "Even!",
                    style={"color": CYAN, "fontWeight": "bold", "fontSize": "14px"}
                ),
            ], style={"textAlign": "center"}),
        ], style={"padding": "10px", "marginBottom": "10px", "backgroundColor": "#ffffff06",
                   "borderRadius": "8px", "border": f"1px solid {CYAN}33"}),

        # Detail cards: Draws + Cash
        html.Div([
            ed.cat_card(f"TJ DRAWS ({len(tulsa_draws)} txns)", "#ffb74d",
                [ed.txn_row(t, ORANGE, get_draw_reason(t["desc"]))
                 for t in tulsa_draws] + (
                [html.Div("Personal / Gift Items", style={"color": PINK, "fontWeight": "bold",
                          "fontSize": "12px", "padding": "6px 0 2px 0", "borderTop": f"1px solid {PINK}44"})] +
                [html.Div([
                    html.Span(f"${r['total']:,.2f}", style={"color": PINK, "fontFamily": "monospace",
                              "fontSize": "11px", "fontWeight": "bold", "width": "60px", "display": "inline-block"}),
                    html.Span(r["name"][:50], style={"color": WHITE, "fontSize": "11px"}),
                ], style={"padding": "2px 0", "borderBottom": "1px solid #ffffff08"})
                 for _, r in personal_inv_items.iterrows()] if len(personal_inv_items) > 0 else []),
                tulsa_draw_total),
            ed.cat_card(f"BRADEN DRAWS ({len(texas_draws)} txns)", "#ff9800",
                [ed.txn_row(t, ORANGE, get_draw_reason(t["desc"]))
                 for t in texas_draws],
                texas_draw_total),
            ed.cat_card("CASH ON HAND", GREEN, [
                html.Div([
                    html.Span("Capital One Bank", style={"color": WHITE, "fontSize": "12px", "width": "180px", "display": "inline-block"}),
                    html.Span(f"${bank_net_cash:,.2f}", style={"color": GREEN, "fontFamily": "monospace", "fontWeight": "bold"}),
                ], style={"padding": "4px 0"}),
                html.Div([
                    html.Span("Etsy Account", style={"color": WHITE, "fontSize": "12px", "width": "180px", "display": "inline-block"}),
                    html.Span(f"${etsy_balance:,.2f}", style={"color": TEAL, "fontFamily": "monospace", "fontWeight": "bold"}),
                ], style={"padding": "4px 0"}),
            ], bank_cash_on_hand, f"Bank ~${bank_net_cash:,.0f} / Etsy ~${etsy_balance:,.0f}"),
            ed.cat_card("BEST BUY CITI CC", BLUE, [
                html.Div([
                    html.Span("Credit Limit", style={"color": WHITE, "fontSize": "12px", "width": "160px", "display": "inline-block"}),
                    html.Span(f"${bb_cc_limit:,.2f}", style={"color": BLUE, "fontFamily": "monospace", "fontWeight": "bold"}),
                ], style={"padding": "4px 0"}),
                html.Div([
                    html.Span("Total Charged", style={"color": WHITE, "fontSize": "12px", "width": "160px", "display": "inline-block"}),
                    html.Span(f"${bb_cc_total_charged:,.2f}", style={"color": RED, "fontFamily": "monospace", "fontWeight": "bold"}),
                ], style={"padding": "4px 0"}),
                html.Div([
                    html.Span("Total Paid", style={"color": WHITE, "fontSize": "12px", "width": "160px", "display": "inline-block"}),
                    html.Span(f"${bb_cc_total_paid:,.2f}", style={"color": GREEN, "fontFamily": "monospace", "fontWeight": "bold"}),
                ], style={"padding": "4px 0"}),
                html.Div([
                    html.Span("Balance Owed", style={"color": WHITE, "fontSize": "12px", "width": "160px", "display": "inline-block"}),
                    html.Span(f"${bb_cc_balance:,.2f}", style={"color": RED, "fontFamily": "monospace", "fontWeight": "bold"}),
                ], style={"padding": "4px 0"}),
                html.Div([
                    html.Span("Available Credit", style={"color": WHITE, "fontSize": "12px", "width": "160px", "display": "inline-block"}),
                    html.Span(f"${bb_cc_available:,.2f}", style={"color": TEAL, "fontFamily": "monospace", "fontWeight": "bold"}),
                ], style={"padding": "4px 0"}),
                html.Div("Purchases:", style={"color": GRAY, "fontSize": "11px", "fontWeight": "bold",
                          "padding": "6px 0 2px 0", "borderTop": f"1px solid {BLUE}44"}),
            ] + [html.Div([
                    html.Span(p["date"], style={"color": GRAY, "fontSize": "11px", "width": "65px", "display": "inline-block"}),
                    html.Span(f"${p['amount']:,.2f}", style={"color": RED, "fontFamily": "monospace", "fontSize": "11px", "width": "70px", "display": "inline-block"}),
                    html.Span(p["desc"][:40], style={"color": WHITE, "fontSize": "11px"}),
                ], style={"padding": "2px 0", "borderBottom": "1px solid #ffffff08"})
                for p in bb_cc_purchases],
                bb_cc_balance, f"Limit ${bb_cc_limit:,.0f} | Paid ${bb_cc_total_paid:,.0f} | Avail ${bb_cc_available:,.0f}"),
        ], style={"display": "flex", "gap": "8px", "flexWrap": "wrap", "marginBottom": "8px"}),

        # Expenses + Receipts cards
        html.Div([
            ed.cat_card(f"EXPENSES ({len([t for t in bank_debits if t['category'] in ['Amazon Inventory'] + _biz_expense_cats])} txns)", RED, [
                *[html.Div([
                    html.Span(c, style={"color": WHITE, "fontSize": "11px", "width": "160px", "display": "inline-block"}),
                    html.Span(f"${bank_by_cat.get(c, 0):,.2f}", style={"color": RED, "fontFamily": "monospace", "fontSize": "11px", "width": "65px", "display": "inline-block"}),
                    html.Span({"Amazon Inventory": f"{len(_bank_amazon_txns)} charges (debit card)",
                               "Shipping": "UPS/USPS/Walmart", "Craft Supplies": "Hobby Lobby/Westlake",
                               "Etsy Fees": "ETSY COM US", "Subscriptions": "Thangs 3D x2",
                               "AliExpress Supplies": "PayPal/AliExpress LEDs",
                               "Business Credit Card": "Best Buy CC payment"}.get(c, ""),
                              style={"color": GRAY, "fontSize": "10px"}),
                ], style={"padding": "2px 0", "borderBottom": "1px solid #ffffff08"})
                  for c in ["Amazon Inventory"] + _biz_expense_cats if bank_by_cat.get(c, 0) > 0],
            ], bank_all_expenses),
            # ── Expense Completeness: 3-pill KPI strip ──
            html.Div([
                html.Div([
                    html.Div("RECEIPT-VERIFIED", style={"fontSize": "9px", "color": GRAY, "textTransform": "uppercase"}),
                    html.Div(f"${expense_receipt_verified:,.2f}", style={"fontSize": "22px", "fontWeight": "bold", "color": GREEN, "fontFamily": "monospace"}),
                ], style={"backgroundColor": "#ffffff08", "padding": "8px 14px", "borderRadius": "6px", "border": f"1px solid {GREEN}44", "flex": "1", "textAlign": "center"}),
                html.Div([
                    html.Div("BANK-RECORDED", style={"fontSize": "9px", "color": GRAY, "textTransform": "uppercase"}),
                    html.Div(f"${expense_bank_recorded:,.2f}", style={"fontSize": "22px", "fontWeight": "bold", "color": WHITE, "fontFamily": "monospace"}),
                ], style={"backgroundColor": "#ffffff08", "padding": "8px 14px", "borderRadius": "6px", "border": "1px solid #ffffff22", "flex": "1", "textAlign": "center"}),
                html.Div([
                    html.Div("UNVERIFIED GAP", style={"fontSize": "9px", "color": GRAY, "textTransform": "uppercase"}),
                    html.Div(f"${expense_gap:,.2f}", style={"fontSize": "22px", "fontWeight": "bold", "color": ORANGE, "fontFamily": "monospace"}),
                ], style={"backgroundColor": "#ffffff08", "padding": "8px 14px", "borderRadius": "6px", "border": f"1px solid {ORANGE}44", "flex": "1", "textAlign": "center"}),
            ], style={"display": "flex", "gap": "8px", "marginBottom": "8px"}),
            # ── Per-category breakdown table ──
            html.Div([
                html.Table([
                    html.Thead(html.Tr([
                        html.Th("Category", style={"color": CYAN, "fontSize": "10px", "textAlign": "left", "padding": "4px 8px"}),
                        html.Th("Verified", style={"color": GREEN, "fontSize": "10px", "textAlign": "right", "padding": "4px 8px"}),
                        html.Th("Bank Total", style={"color": WHITE, "fontSize": "10px", "textAlign": "right", "padding": "4px 8px"}),
                        html.Th("Gap", style={"color": ORANGE, "fontSize": "10px", "textAlign": "right", "padding": "4px 8px"}),
                        html.Th("Missing", style={"color": RED, "fontSize": "10px", "textAlign": "right", "padding": "4px 8px"}),
                    ])),
                    html.Tbody([
                        html.Tr([
                            html.Td(cat, style={"color": WHITE, "fontSize": "10px", "padding": "3px 8px"}),
                            html.Td(f"${vals.get('verified', 0):,.2f}", style={"color": GREEN, "fontSize": "10px", "textAlign": "right", "padding": "3px 8px", "fontFamily": "monospace"}),
                            html.Td(f"${vals.get('bank_recorded', 0):,.2f}", style={"color": WHITE, "fontSize": "10px", "textAlign": "right", "padding": "3px 8px", "fontFamily": "monospace"}),
                            html.Td(f"${vals.get('gap', 0):,.2f}", style={"color": ORANGE if vals.get('gap', 0) > 0 else GREEN, "fontSize": "10px", "textAlign": "right", "padding": "3px 8px", "fontFamily": "monospace"}),
                            html.Td(str(vals.get('missing_count', 0)), style={"color": RED if vals.get('missing_count', 0) > 0 else GREEN, "fontSize": "10px", "textAlign": "right", "padding": "3px 8px"}),
                        ]) for cat, vals in expense_by_category.items()
                    ]),
                ], style={"width": "100%", "borderCollapse": "collapse"}),
            ], style={"backgroundColor": "#ffffff06", "borderRadius": "6px", "padding": "6px", "marginBottom": "8px"}),
            # ── Missing Receipt Queue (open by default) — with progress + actions ──
            html.Details([
                html.Summary([
                    html.Span("\u25b6 ", style={"fontSize": "12px"}),
                    html.Span(f"MISSING RECEIPT QUEUE ({len(expense_missing_receipts)})", style={"fontWeight": "bold"}),
                    html.Span(f" \u2014 ${expense_gap:,.2f} unverified", style={"color": ORANGE, "fontWeight": "normal", "fontSize": "12px"}),
                ], style={"color": RED, "fontSize": "13px", "fontWeight": "bold",
                    "cursor": "pointer", "padding": "8px 12px", "listStyle": "none",
                    "backgroundColor": "#b71c1c18", "borderRadius": "6px",
                    "border": f"1px solid {RED}33"}),
                html.Div([
                    # Progress bar
                    html.Div([
                        html.Div([
                            html.Span(f"{expense_matched_count}/{expense_matched_count + len(expense_missing_receipts)} expenses verified ",
                                      style={"color": WHITE, "fontSize": "12px", "fontWeight": "bold"}),
                            html.Span(f"({int(expense_matched_count / max(expense_matched_count + len(expense_missing_receipts), 1) * 100)}%)",
                                      style={"color": GREEN, "fontSize": "12px"}),
                        ]),
                        html.Div([
                            html.Div(style={"width": f"{int(expense_matched_count / max(expense_matched_count + len(expense_missing_receipts), 1) * 100)}%",
                                            "height": "8px", "backgroundColor": GREEN, "borderRadius": "4px",
                                            "transition": "width 0.3s"}),
                        ], style={"width": "100%", "height": "8px", "backgroundColor": "#ffffff15",
                                  "borderRadius": "4px", "marginTop": "4px"}),
                    ], style={"marginBottom": "12px", "padding": "8px 0"}),

                    # Sort hint
                    html.P("Sorted by amount (largest gaps first). Mark items as verified if confirmed.",
                           style={"color": GRAY, "fontSize": "10px", "margin": "0 0 8px 0"}),

                    # Receipt rows with action buttons — sorted by amount descending
                    *[html.Div([
                        html.Span(f"${t['amount']:,.2f}", style={"color": RED, "fontFamily": "monospace",
                                  "fontSize": "11px", "width": "70px", "display": "inline-block", "fontWeight": "bold"}),
                        html.Span(t.get("vendor", ""), style={"color": WHITE, "fontSize": "11px",
                                  "width": "140px", "display": "inline-block"}),
                        html.Span(t.get("date", ""), style={"color": GRAY, "fontSize": "10px",
                                  "width": "80px", "display": "inline-block"}),
                        html.Span(t.get("category", ""), style={"color": CYAN, "fontSize": "10px",
                                  "width": "110px", "display": "inline-block"}),
                        html.Span("TAX" if t.get("tax_deductible") else "",
                                  style={"color": ORANGE, "fontSize": "9px", "fontWeight": "bold",
                                         "width": "30px", "display": "inline-block"}),
                        html.Button("Mark Verified", id={"type": "receipt-verify-btn", "index": i},
                                    n_clicks=0,
                                    style={"fontSize": "9px", "padding": "2px 8px",
                                           "backgroundColor": "transparent", "color": GREEN,
                                           "border": f"1px solid {GREEN}44", "borderRadius": "4px",
                                           "cursor": "pointer", "marginLeft": "4px"}),
                    ], style={"padding": "4px 0", "borderBottom": "1px solid #ffffff08",
                              "display": "flex", "alignItems": "center"})
                      for i, t in enumerate(sorted(expense_missing_receipts,
                                                    key=lambda x: x.get("amount", 0), reverse=True)[:50])],

                    *([html.Div(f"... and {len(expense_missing_receipts) - 50} more",
                         style={"color": GRAY, "fontSize": "10px", "padding": "4px 0"})]
                       if len(expense_missing_receipts) > 50 else []),
                ], style={"padding": "8px 12px", "maxHeight": "400px", "overflowY": "auto"}),
            ], open=True, style={"marginBottom": "8px"}),
        ], style={"display": "flex", "gap": "8px", "flexWrap": "wrap", "marginBottom": "10px"}),
            ], style={"paddingTop": "10px"}),
        ], style={"marginBottom": "8px"}),

        html.Details([
            html.Summary([
                html.Span("\u25b6 ", style={"fontSize": "12px"}),
                html.Span("SHIPPING", style={"fontWeight": "bold"}),
                html.Span(f" \u2014 {total_label_count} labels, {money(total_shipping_cost)} total cost", style={"color": GRAY, "fontWeight": "normal", "fontSize": "12px"}),
            ], style={"color": CYAN, "fontSize": "14px", "fontWeight": "bold",
                "cursor": "pointer", "padding": "10px 14px", "listStyle": "none",
                "backgroundColor": "#ffffff08", "borderRadius": "6px",
                "border": f"1px solid {CYAN}33"}),
            html.Div([

        # Shipping KPIs — only real data, no UNKNOWN cards
        html.Div([
            kpi_card("TOTAL LABEL COST", money(-total_shipping_cost if total_shipping_cost is not None else None), RED,
                f"{total_label_count} labels purchased",
                f"USPS outbound: {usps_outbound_count} labels ({money(usps_outbound)}). USPS return: {usps_return_count} labels ({money(usps_return)}). Asendia intl: {asendia_count} labels ({money(asendia_labels)}). Adjustments: {money(ship_adjustments)}. Credits back: {money(abs(ship_credits) if ship_credits is not None else None)}.", status="verified", metric_name="total_shipping_cost"),
            kpi_card("AVG LABEL", money(avg_outbound_label), BLUE,
                f"USPS {money(usps_min)}-{money(usps_max)}" if usps_outbound_count else "No USPS labels",
                f"Average cost of a USPS outbound label. Range: {money(usps_min)} (lightest) to {money(usps_max)} (heaviest). {usps_outbound_count} labels total. Heavier/larger items cost more to ship.", status="verified", metric_name="avg_outbound_label"),
            kpi_card("PAID / FREE ORDERS", f"{paid_ship_count} / {free_ship_count}", TEAL,
                f"{paid_ship_count} paid + {free_ship_count} free shipping",
                f"{paid_ship_count} orders where buyer paid for shipping, {free_ship_count} orders shipped free (you absorbed the label cost). Avg label cost: {money(avg_outbound_label)}.", status="verified"),
            kpi_card("RETURN LABELS", str(usps_return_count), PINK,
                money(-usps_return if usps_return is not None else None) + " in return label cost",
                f"{usps_return_count} return shipping labels purchased for refunded orders. Total return label cost: {money(usps_return)}. This is on top of the original outbound label cost and the refund amount -- returns are triple losses.", status="verified"),
        ], style={"display": "flex", "gap": "8px", "marginBottom": "14px", "flexWrap": "wrap"}),

        # Shipping charts — built inline so they always reflect current data
        html.Div([
            html.Div([dcc.Graph(figure=_build_shipping_compare(), config={"displayModeBar": False})], style={"flex": "1"}),
            html.Div([dcc.Graph(figure=_build_ship_type(), config={"displayModeBar": False})], style={"flex": "1"}),
        ], style={"display": "flex", "gap": "8px", "marginBottom": "10px"}),

        # Shipping Cost Breakdown
        section("SHIPPING COST BREAKDOWN", [
            row_item(f"USPS Outbound Labels ({usps_outbound_count})", -usps_outbound, indent=1, metric_name="usps_outbound"),
            row_item(f"USPS Return Labels ({usps_return_count})", -usps_return, indent=1, metric_name="usps_return"),
            row_item(f"Asendia / International ({asendia_count})", -asendia_labels, indent=1, metric_name="asendia_labels"),
            row_item(f"Label Adjustments ({ship_adjust_count})", -ship_adjustments, indent=1, metric_name="ship_adjustments"),
            row_item(f"Insurance ({ship_insurance_count})", -ship_insurance, indent=1, metric_name="ship_insurance") if ship_insurance > 0 else html.Div(),
            row_item(f"Label Credits ({ship_credit_count})", ship_credits, indent=1, color=GREEN, metric_name="ship_credits"),
            html.Div(style={"borderTop": f"2px solid {RED}", "marginTop": "8px"}),
            row_item("TOTAL LABEL COST", -total_shipping_cost, bold=True, color=RED, metric_name="total_shipping_cost"),
            html.P("Upload Etsy order-level CSV with 'Shipping charged to buyer' column for full shipping P&L analysis.",
                   style={"color": DARKGRAY, "fontSize": "11px", "margin": "10px 0 0 0", "fontStyle": "italic"}),
        ], RED),

        # Paid vs Free order counts
        section("PAID vs FREE SHIPPING ORDERS", [
            html.P("PAID SHIPPING", style={"color": TEAL, "fontWeight": "bold", "fontSize": "13px", "margin": "0 0 4px 0"}),
            row_item(f"Orders with paid shipping", paid_ship_count, color=TEAL, indent=1),
            html.Div(style={"borderTop": f"1px solid {DARKGRAY}", "margin": "8px 0"}),
            html.P("FREE SHIPPING", style={"color": ORANGE, "fontWeight": "bold", "fontSize": "13px", "margin": "0 0 4px 0"}),
            row_item(f"Orders with free shipping", free_ship_count, color=ORANGE, indent=1),
            html.P(f"Avg label cost: ${avg_outbound_label:.2f} (absorbed by seller)", style={"color": GRAY, "fontSize": "11px", "margin": "2px 0 0 16px"}),
        ], TEAL),

        # Returns section
        section("RETURNS & REFUNDS", [
            row_item(f"Return Labels Purchased ({usps_return_count})", -usps_return, indent=1),
        ] + [
            html.Div([
                html.Span(m["date"], style={"color": GRAY, "width": "140px", "display": "inline-block", "fontSize": "12px"}),
                html.Span(f"{m['product'][:40]}", style={"color": WHITE, "flex": "1", "fontSize": "12px"}),
                html.Span(f"Label: ${m['cost']:.2f}", style={"color": RED, "fontFamily": "monospace", "width": "100px", "textAlign": "right", "fontSize": "12px"}),
                html.Span(f"Refund: ${m['refund_amt']:.2f}", style={"color": ORANGE, "fontFamily": "monospace", "width": "110px", "textAlign": "right", "fontSize": "12px"}),
            ], style={"display": "flex", "padding": "3px 0", "borderBottom": "1px solid #ffffff08", "gap": "6px"})
            for m in return_label_matches
        ], PINK),
            ], style={"paddingTop": "10px"}),
        ], style={"marginBottom": "8px"}),

        html.Details([
            html.Summary([
                html.Span("\u25b6 ", style={"fontSize": "12px"}),
                html.Span("MONTHLY TRENDS", style={"fontWeight": "bold"}),
                html.Span(" \u2014 month-by-month performance + top products", style={"color": GRAY, "fontWeight": "normal", "fontSize": "12px"}),
            ], style={"color": CYAN, "fontSize": "14px", "fontWeight": "bold",
                "cursor": "pointer", "padding": "10px 14px", "listStyle": "none",
                "backgroundColor": "#ffffff08", "borderRadius": "6px",
                "border": f"1px solid {CYAN}33"}),
            html.Div([

        # Monthly Breakdown table
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
                            html.Td(f"${monthly_sales.get(m, 0):,.2f}", style={"textAlign": "right", "color": GREEN, "padding": "4px 8px", "fontSize": "13px"}),
                            html.Td(f"${monthly_fees.get(m, 0):,.2f}", style={"textAlign": "right", "color": RED, "padding": "4px 8px", "fontSize": "13px"}),
                            html.Td(f"${monthly_shipping.get(m, 0):,.2f}", style={"textAlign": "right", "color": BLUE, "padding": "4px 8px", "fontSize": "13px"}),
                            html.Td(f"${monthly_marketing.get(m, 0):,.2f}", style={"textAlign": "right", "color": PURPLE, "padding": "4px 8px", "fontSize": "13px"}),
                            html.Td(f"${monthly_refunds.get(m, 0):,.2f}", style={"textAlign": "right", "color": ORANGE, "padding": "4px 8px", "fontSize": "13px"}),
                            html.Td(f"${monthly_net_revenue.get(m, 0):,.2f}", style={"textAlign": "right", "color": ORANGE, "fontWeight": "bold", "padding": "4px 8px", "fontSize": "13px"}),
                            html.Td(
                                f"{(monthly_net_revenue.get(m, 0) / monthly_sales.get(m, 1) * 100):.1f}%"
                                if monthly_sales.get(m, 0) > 0 else "--",
                                style={"textAlign": "right", "color": GRAY, "padding": "4px 8px", "fontSize": "13px"}),
                        ], style={"borderBottom": "1px solid #ffffff10"}) for m in months_sorted]
                        + [html.Tr([
                            html.Td("TOTAL", style={"color": ORANGE, "fontWeight": "bold", "padding": "6px 8px"}),
                            html.Td(f"${sum(monthly_sales.get(m, 0) for m in months_sorted):,.2f}",
                                style={"textAlign": "right", "color": GREEN, "fontWeight": "bold", "padding": "6px 8px"}),
                            html.Td(f"${sum(monthly_fees.get(m, 0) for m in months_sorted):,.2f}",
                                style={"textAlign": "right", "color": RED, "fontWeight": "bold", "padding": "6px 8px"}),
                            html.Td(f"${sum(monthly_shipping.get(m, 0) for m in months_sorted):,.2f}",
                                style={"textAlign": "right", "color": BLUE, "fontWeight": "bold", "padding": "6px 8px"}),
                            html.Td(f"${sum(monthly_marketing.get(m, 0) for m in months_sorted):,.2f}",
                                style={"textAlign": "right", "color": PURPLE, "fontWeight": "bold", "padding": "6px 8px"}),
                            html.Td(f"${sum(monthly_refunds.get(m, 0) for m in months_sorted):,.2f}",
                                style={"textAlign": "right", "color": ORANGE, "fontWeight": "bold", "padding": "6px 8px"}),
                            html.Td(f"${sum(monthly_net_revenue.get(m, 0) for m in months_sorted):,.2f}",
                                style={"textAlign": "right", "color": ORANGE, "fontWeight": "bold", "fontSize": "15px", "padding": "6px 8px"}),
                            html.Td(
                                f"{(sum(monthly_net_revenue.get(m, 0) for m in months_sorted) / gross_sales * 100):.1f}%" if gross_sales else "--",
                                style={"textAlign": "right", "color": GRAY, "fontWeight": "bold", "padding": "6px 8px"}),
                        ], style={"borderTop": f"3px solid {ORANGE}"})]
                    ),
                ], style={"width": "100%", "borderCollapse": "collapse", "color": WHITE}),
            ]),
        ], BLUE),

        # Top Products chart
        dcc.Graph(figure=product_fig, config={"displayModeBar": False}),
            ], style={"paddingTop": "10px"}),
        ], style={"marginBottom": "8px"}),

        html.Details([
            html.Summary([
                html.Span("\u25b6 ", style={"fontSize": "12px"}),
                html.Span("FEES & REFUNDS", style={"fontWeight": "bold"}),
                html.Span(" \u2014 detailed fee breakdown + refund history", style={"color": GRAY, "fontWeight": "normal", "fontSize": "12px"}),
            ], style={"color": CYAN, "fontSize": "14px", "fontWeight": "bold",
                "cursor": "pointer", "padding": "10px 14px", "listStyle": "none",
                "backgroundColor": "#ffffff08", "borderRadius": "6px",
                "border": f"1px solid {CYAN}33"}),
            html.Div([

        # Fee breakdown
        section("FEE & MARKETING DETAIL", [
            html.Div("FEES CHARGED", style={"color": RED, "fontWeight": "bold", "fontSize": "13px", "marginBottom": "4px"}),
            row_item("  Listing Fees", -listing_fees, indent=1, color=GRAY, metric_name="listing_fees"),
            row_item("  Transaction Fees (product)", -transaction_fees_product, indent=1, color=GRAY, metric_name="transaction_fees_product"),
            row_item("  Transaction Fees (shipping)", -transaction_fees_shipping, indent=1, color=GRAY, metric_name="transaction_fees_shipping"),
            row_item("  Processing Fees", -processing_fees, indent=1, color=GRAY, metric_name="processing_fees"),
            row_item("Total Fees (gross)", -total_fees_gross, bold=True, metric_name="total_fees_gross"),
            html.Div(style={"height": "8px"}),
            html.Div("CREDITS RECEIVED", style={"color": GREEN, "fontWeight": "bold", "fontSize": "13px", "marginBottom": "4px"}),
            row_item("  Transaction Credits", credit_transaction, indent=1, color=GREEN, metric_name="credit_transaction"),
            row_item("  Listing Credits", credit_listing, indent=1, color=GREEN, metric_name="credit_listing"),
            row_item("  Processing Credits", credit_processing, indent=1, color=GREEN, metric_name="credit_processing"),
            row_item("  Share & Save", share_save, indent=1, color=GREEN, metric_name="share_save"),
            row_item("Total Credits", total_credits, bold=True, color=GREEN, metric_name="total_credits"),
            html.Div(style={"height": "2px", "borderTop": f"1px solid {ORANGE}44", "margin": "8px 0"}),
            row_item("Net Fees (after credits)", -net_fees_after_credits, bold=True, color=ORANGE),
            html.Div(style={"height": "12px"}),
            html.Div("MARKETING", style={"color": PURPLE, "fontWeight": "bold", "fontSize": "13px", "marginBottom": "4px"}),
            row_item("  Etsy Ads", -etsy_ads, indent=1, color=GRAY, metric_name="etsy_ads"),
            row_item("  Offsite Ads", -offsite_ads_fees, indent=1, color=GRAY, metric_name="offsite_ads_fees"),
            *([] if offsite_ads_credits == 0 else [row_item("  Offsite Credits", offsite_ads_credits, indent=1, color=GREEN, metric_name="offsite_ads_credits")]),
            row_item("Total Marketing", -total_marketing, bold=True, metric_name="total_marketing"),
        ], RED),

        # Refunds list
        section("REFUNDS", _refund_section_rows, ORANGE),

        # Missing statements
        section("MISSING STATEMENTS & RECEIPTS", [
            html.Div([
                html.Div("BEFORE CAPITAL ONE (Oct - early Dec 2025)", style={
                    "color": GREEN, "fontWeight": "bold", "fontSize": "14px", "marginBottom": "6px"}),
                html.P(f"Etsy deposited $941.99 to your old bank. "
                       f"${old_bank_receipted:,.2f} matched to inventory receipts (non-Discover cards).",
                       style={"color": GRAY, "fontSize": "12px", "margin": "0 0 8px 0"}),
                html.Div([
                    html.Span("STATUS: ", style={"color": GREEN, "fontWeight": "bold", "fontSize": "12px"}),
                    html.Span(f"Nearly fully accounted -- only ${bank_unaccounted:,.2f} unmatched",
                              style={"color": GRAY, "fontSize": "12px"}),
                ], style={"padding": "6px", "backgroundColor": "#4caf5008", "borderRadius": "6px"}),
            ], style={"padding": "12px", "backgroundColor": "#ffffff04", "borderRadius": "8px",
                       "borderLeft": f"4px solid {GREEN}", "marginBottom": "10px"}),
            html.Div([
                html.Div("ETSY CSV GAP (recent activity)", style={
                    "color": ORANGE, "fontWeight": "bold", "fontSize": "14px", "marginBottom": "6px"}),
                html.Div([
                    html.Span(f"${etsy_csv_gap:,.2f} ", style={"color": ORANGE, "fontWeight": "bold", "fontFamily": "monospace", "fontSize": "13px"}),
                    html.Span("in Etsy fees/activity since last CSV export (~39 sales not in CSVs)", style={"color": GRAY, "fontSize": "12px"}),
                ]),
            ], style={"padding": "12px", "backgroundColor": "#ffffff04", "borderRadius": "8px",
                       "borderLeft": f"4px solid {ORANGE}", "marginBottom": "10px"}),
        ], RED),
            ], style={"paddingTop": "10px"}),
        ], style={"marginBottom": "8px"}),

        # 7. BANK LEDGER
        html.Details([
            html.Summary([
                html.Span("\u25b6 ", style={"fontSize": "12px"}),
                html.Span("BANK LEDGER", style={"fontWeight": "bold"}),
                html.Span(" \u2014 full transaction history with running balance", style={"color": GRAY, "fontWeight": "normal", "fontSize": "12px"}),
            ], style={"color": CYAN, "fontSize": "14px", "fontWeight": "bold",
                "cursor": "pointer", "padding": "10px 14px", "listStyle": "none",
                "backgroundColor": "#ffffff08", "borderRadius": "6px",
                "border": f"1px solid {CYAN}33"}),
            html.Div([

        # Monthly cash flow chart
        dcc.Graph(figure=bank_monthly_fig, config={"displayModeBar": False}),

        # Full running balance ledger
        section(f"FULL LEDGER ({len(BANK_TXNS)} Transactions)", [
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
                            html.Td(t["category"], style={
                                "color": _bank_cat_color_map.get(t["category"], WHITE),
                                "padding": "4px 8px", "fontSize": "11px", "fontWeight": "600"}),
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
                                   "backgroundColor": f"{GREEN}08" if t["type"] == "deposit" else f"{_bank_cat_color_map.get(t['category'], WHITE)}08"})
                        for t in bank_running
                    ] + [
                        html.Tr([
                            html.Td("TOTAL", colSpan="3", style={"color": CYAN, "fontWeight": "bold",
                                                                    "padding": "8px 8px", "fontSize": "13px"}),
                            html.Td(f"${bank_total_deposits:,.2f}", style={
                                "textAlign": "right", "color": GREEN, "fontWeight": "bold",
                                "padding": "8px 8px", "fontSize": "13px", "fontFamily": "monospace"}),
                            html.Td(f"${bank_total_debits:,.2f}", style={
                                "textAlign": "right", "color": RED, "fontWeight": "bold",
                                "padding": "8px 8px", "fontSize": "13px", "fontFamily": "monospace"}),
                            html.Td(f"${bank_net_cash:,.2f}", style={
                                "textAlign": "right", "color": CYAN, "fontWeight": "bold",
                                "padding": "8px 8px", "fontSize": "13px", "fontFamily": "monospace"}),
                        ], style={"borderTop": f"3px solid {CYAN}"}),
                    ]),
                ], style={"width": "100%", "borderCollapse": "collapse", "color": WHITE}),
            ], style={"maxHeight": "700px", "overflowY": "auto"}),
        ], CYAN),
            ], style={"paddingTop": "10px"}),
        ], style={"marginBottom": "8px"}),

        # ══ PER-ORDER PROFIT ══
        _build_per_order_profit_section(),

    ], style={"padding": TAB_PADDING})
