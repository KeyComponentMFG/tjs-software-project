"""Valuation tab — Comprehensive business value analysis from every angle."""

from dash import dcc, html
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import pandas as pd
from dashboard_utils.theme import *


def build_tab6_valuation():
    """Tab 6 - Business Valuation: Comprehensive business value analysis from every angle."""
    import etsy_dashboard as ed

    _sm = ed.strict_mode if isinstance(ed.strict_mode, bool) else False
    if _sm:
        return html.Div([
            html.Div([
                html.Span("STRICT MODE", style={"color": RED, "fontWeight": "bold", "fontSize": "16px", "letterSpacing": "1px"}),
            ], style={"marginBottom": "10px"}),
            html.P("Business valuations are estimates based on industry multiples and annualized projections. "
                   "None of these numbers are verified facts.",
                   style={"color": GRAY, "fontSize": "14px", "lineHeight": "1.6"}),
            html.P("Disable strict mode to view valuation estimates.",
                   style={"color": ORANGE, "fontSize": "13px", "marginTop": "10px"}),
        ], style={"backgroundColor": "#1a0000", "border": f"1px solid {RED}44", "borderRadius": "10px",
                  "padding": "30px", "marginTop": "20px", "textAlign": "center"})

    children = []

    # ── HERO KPI STRIP (6 bubbles) ──
    children.append(html.Div([
        kpi_card("BUSINESS VALUE", money(ed.val_blended_mid), CYAN, f"Range: {money(ed.val_blended_low)} — {money(ed.val_blended_high)}",
                 f"Blended estimate using 3 methods: SDE Multiple (50% weight, {money(ed.val_sde_mid)}), Revenue Multiple (25%, {money(ed.val_rev_mid)}), Asset-Based (25%, {money(ed.val_asset_val)}). Low estimate: {money(ed.val_blended_low)}, high: {money(ed.val_blended_high)}. This is what the business would likely sell for."),
        kpi_card("ANNUAL SDE", money(ed.val_annual_sde), GREEN, f"Monthly: {money(ed.val_sde / ed._val_months_operating)}",
                 f"Seller's Discretionary Earnings = Profit ({money(ed.profit)}) + Owner Draws ({money(ed.bank_owner_draw_total)}), annualized from {ed._val_months_operating} months. SDE represents what a single owner-operator could earn. It's the most common metric for valuing small businesses."),
        kpi_card("HEALTH SCORE", f"{ed.val_health_score}/100 ({ed.val_health_grade})", ed.val_health_color,
                 "Profitability + Growth + Cash + Diversity",
                 f"Composite score: Profitability {ed._hs_profit:.0f}/25 + Growth {ed._hs_growth:.0f}/25 + Product Diversity {ed._hs_diversity:.0f}/15 + Cash Position {ed._hs_cash:.0f}/15 + Debt {ed._hs_debt:.0f}/10 + Shipping {ed._hs_shipping:.0f}/10. Grade: A (80+), B (60-79), C (40-59), D (below 40)."),
        kpi_card("ANNUAL REVENUE", money(ed.val_annual_revenue), TEAL, f"{ed._val_months_operating}mo annualized",
                 f"Gross sales ({money(ed.gross_sales)}) over {ed._val_months_operating} months, projected to 12 months ({money(ed.gross_sales)} x 12/{ed._val_months_operating}). Assumes current sales pace continues. Seasonal variation could make this higher or lower."),
        kpi_card("EQUITY", money(ed.val_equity), GREEN, f"Assets {money(ed.val_total_assets)} - Liabilities {money(ed.val_total_liabilities)}",
                 f"ASSETS: Bank cash {money(ed.bank_cash_on_hand)}, Equipment (Best Buy CC purchases) {money(ed.bb_cc_asset_value)}, Inventory on hand {money(ed.true_inventory_cost)}. Total: {money(ed.val_total_assets)}. LIABILITIES: CC debt {money(ed.bb_cc_balance)}. Equity = Assets minus Liabilities."),
        kpi_card("GROWTH RATE", f"{ed._val_growth_pct:+.1f}%/mo",
                 GREEN if ed._val_growth_pct > 5 else ORANGE if ed._val_growth_pct > 0 else RED,
                 f"R² = {ed._val_r2:.0%} confidence",
                 f"Monthly revenue growth rate from linear regression on {ed._val_months_operating} months of data. Trend: {money(abs(ed._val_sales_trend))}/month {'increase' if ed._val_sales_trend > 0 else 'decrease'}. R² of {ed._val_r2:.0%} means the trend explains {ed._val_r2 * 100:.0f}% of the variation (higher = more predictable)."),
    ], style={"display": "flex", "gap": "10px", "flexWrap": "wrap", "marginBottom": "14px"}))

    # ── SECTION A: VALUATION SUMMARY ──
    # Gauge chart
    val_gauge = go.Figure(go.Indicator(
        mode="gauge+number+delta",
        value=ed.val_blended_mid,
        number={"prefix": "$", "valueformat": ",.0f"},
        delta={"reference": ed.val_blended_low, "valueformat": ",.0f", "prefix": "$"},
        title={"text": "Blended Business Valuation"},
        gauge={
            "axis": {"range": [0, ed.val_blended_high * 1.2], "tickprefix": "$", "tickformat": ",.0f"},
            "bar": {"color": CYAN},
            "steps": [
                {"range": [0, ed.val_blended_low], "color": "rgba(231,76,60,0.19)"},
                {"range": [ed.val_blended_low, ed.val_blended_mid], "color": "rgba(243,156,18,0.19)"},
                {"range": [ed.val_blended_mid, ed.val_blended_high], "color": "rgba(46,204,113,0.19)"},
            ],
            "threshold": {"line": {"color": WHITE, "width": 2}, "thickness": 0.75, "value": ed.val_blended_mid},
        },
    ))
    make_chart(val_gauge, 280, False)
    val_gauge.update_layout(title="")

    # Method cards
    def method_card(title, weight, low, mid, high, color, detail):
        return html.Div([
            html.Div(f"{title} ({weight}% weight)", style={"color": color, "fontWeight": "bold", "fontSize": "13px", "marginBottom": "6px"}),
            html.Div([
                html.Span("Low: ", style={"color": GRAY, "fontSize": "11px"}),
                html.Span(money(low), style={"color": ORANGE, "fontFamily": "monospace", "fontSize": "12px"}),
            ], style={"marginBottom": "2px"}),
            html.Div([
                html.Span("Mid: ", style={"color": GRAY, "fontSize": "11px"}),
                html.Span(money(mid), style={"color": GREEN, "fontFamily": "monospace", "fontSize": "14px", "fontWeight": "bold"}),
            ], style={"marginBottom": "2px"}),
            html.Div([
                html.Span("High: ", style={"color": GRAY, "fontSize": "11px"}),
                html.Span(money(high), style={"color": CYAN, "fontFamily": "monospace", "fontSize": "12px"}),
            ], style={"marginBottom": "6px"}),
            html.P(detail, style={"color": GRAY, "fontSize": "11px", "margin": "0", "lineHeight": "1.3"}),
        ], style={
            "flex": "1", "minWidth": "200px", "padding": "12px",
            "backgroundColor": f"{color}10", "borderLeft": f"3px solid {color}",
            "borderRadius": "4px",
        })

    children.append(section("A. VALUATION SUMMARY", [
        dcc.Graph(figure=val_gauge, config={"displayModeBar": False}),
        html.Div([
            method_card("SDE Multiple", 50, ed.val_sde_low, ed.val_sde_mid, ed.val_sde_high, GREEN,
                        f"Annual SDE {money(ed.val_annual_sde)} × 1.0/1.5/2.5x multiples"),
            method_card("Revenue Multiple", 25, ed.val_rev_low, ed.val_rev_mid, ed.val_rev_high, TEAL,
                        f"Annual Revenue {money(ed.val_annual_revenue)} × 0.3/0.5/1.0x multiples"),
            method_card("Asset-Based", 25, ed.val_asset_val, ed.val_asset_val, ed.val_asset_val, PURPLE,
                        f"Assets {money(ed.val_total_assets)} − Liabilities {money(ed.val_total_liabilities)}"),
        ], style={"display": "flex", "gap": "10px", "flexWrap": "wrap", "marginTop": "10px"}),
        html.Div([
            html.Span("BLENDED ESTIMATE: ", style={"color": GRAY, "fontSize": "13px"}),
            html.Span(money(ed.val_blended_mid), style={"color": CYAN, "fontSize": "20px", "fontWeight": "bold", "fontFamily": "monospace"}),
            html.Span(f"  (range {money(ed.val_blended_low)} — {money(ed.val_blended_high)})", style={"color": GRAY, "fontSize": "12px"}),
        ], style={"textAlign": "center", "padding": "12px", "marginTop": "10px",
                  "backgroundColor": f"{CYAN}10", "borderRadius": "6px", "border": f"1px solid {CYAN}33"}),
    ], CYAN))

    # ── SECTION B: VALUATION COMPARISON CHART ──
    _comp_methods = ["SDE Multiple", "Revenue Multiple", "Asset-Based", "Blended"]
    _comp_lows = [ed.val_sde_low, ed.val_rev_low, ed.val_asset_val, ed.val_blended_low]
    _comp_mids = [ed.val_sde_mid, ed.val_rev_mid, ed.val_asset_val, ed.val_blended_mid]
    _comp_highs = [ed.val_sde_high, ed.val_rev_high, ed.val_asset_val, ed.val_blended_high]
    comp_fig = go.Figure()
    comp_fig.add_trace(go.Bar(name="Low", y=_comp_methods, x=_comp_lows, orientation="h",
                              marker_color=ORANGE, text=[f"${v:,.0f}" for v in _comp_lows], textposition="outside"))
    comp_fig.add_trace(go.Bar(name="Mid", y=_comp_methods, x=_comp_mids, orientation="h",
                              marker_color=GREEN, text=[f"${v:,.0f}" for v in _comp_mids], textposition="outside"))
    comp_fig.add_trace(go.Bar(name="High", y=_comp_methods, x=_comp_highs, orientation="h",
                              marker_color=CYAN, text=[f"${v:,.0f}" for v in _comp_highs], textposition="outside"))
    make_chart(comp_fig, 280)
    comp_fig.update_layout(title="Valuation Method Comparison (Low / Mid / High)", barmode="group",
                           yaxis={"categoryorder": "array", "categoryarray": _comp_methods[::-1]})

    children.append(section("B. VALUATION COMPARISON", [
        ed.chart_context("Side-by-side comparison of all valuation methods. Stacked bars show low → mid → high ranges.",
                      metrics=[("Blended Mid", money(ed.val_blended_mid), CYAN), ("SDE Weight", "50%", GREEN), ("Rev Weight", "25%", TEAL)],
                      simple="Each colored bar shows a different way to estimate what the business is worth. Longer bars = higher value. The 'Blended' row at top is the combined best estimate using all three methods."),
        dcc.Graph(figure=comp_fig, config={"displayModeBar": False}),
    ], TEAL))

    # ── SECTION C: BUSINESS HEALTH ASSESSMENT ──
    health_gauge = go.Figure(go.Indicator(
        mode="gauge+number",
        value=ed.val_health_score,
        title={"text": f"Health Grade: {ed.val_health_grade}"},
        gauge={
            "axis": {"range": [0, 100]},
            "bar": {"color": ed.val_health_color},
            "steps": [
                {"range": [0, 40], "color": "rgba(231,76,60,0.15)"},
                {"range": [40, 60], "color": "rgba(243,156,18,0.15)"},
                {"range": [60, 80], "color": "rgba(26,188,156,0.15)"},
                {"range": [80, 100], "color": "rgba(46,204,113,0.15)"},
            ],
        },
    ))
    make_chart(health_gauge, 220, False)

    def severity_badge(sev):
        c = RED if sev == "HIGH" else ORANGE if sev == "MED" else GREEN
        return html.Span(sev, style={
            "backgroundColor": f"{c}25", "color": c, "padding": "2px 8px",
            "borderRadius": "4px", "fontSize": "10px", "fontWeight": "bold", "marginRight": "8px",
        })

    risk_items = [html.Div([
        severity_badge(sev),
        html.Span(name, style={"color": WHITE, "fontSize": "12px", "fontWeight": "bold", "marginRight": "8px"}),
        html.Span(f"— {desc}", style={"color": GRAY, "fontSize": "11px"}),
    ], style={"padding": "4px 0", "borderBottom": "1px solid #ffffff08"}) for name, desc, sev in ed.val_risks]

    strength_items = [html.Div([
        html.Span("✓ ", style={"color": GREEN, "fontSize": "13px", "marginRight": "4px"}),
        html.Span(name, style={"color": GREEN, "fontSize": "12px", "fontWeight": "bold", "marginRight": "8px"}),
        html.Span(f"— {desc}", style={"color": GRAY, "fontSize": "11px"}),
    ], style={"padding": "4px 0", "borderBottom": "1px solid #ffffff08"}) for name, desc in ed.val_strengths]

    children.append(section("C. BUSINESS HEALTH ASSESSMENT", [
        html.Div([
            html.Div([dcc.Graph(figure=health_gauge, config={"displayModeBar": False})],
                     style={"flex": "1", "minWidth": "250px"}),
            html.Div([
                html.Div([
                    html.Div(f"Profitability: {ed._hs_profit:.0f}/25", style={"color": GREEN if ed._hs_profit > 15 else ORANGE, "fontSize": "11px"}),
                    html.Div(f"Growth: {ed._hs_growth:.0f}/25", style={"color": GREEN if ed._hs_growth > 15 else ORANGE, "fontSize": "11px"}),
                    html.Div(f"Diversity: {ed._hs_diversity:.0f}/15", style={"color": GREEN if ed._hs_diversity > 8 else ORANGE, "fontSize": "11px"}),
                    html.Div(f"Cash Position: {ed._hs_cash:.0f}/15", style={"color": GREEN if ed._hs_cash > 8 else ORANGE, "fontSize": "11px"}),
                    html.Div(f"Debt: {ed._hs_debt:.0f}/10", style={"color": GREEN if ed._hs_debt > 5 else ORANGE, "fontSize": "11px"}),
                    html.Div(f"Shipping: {ed._hs_shipping:.0f}/10", style={"color": GREEN if ed._hs_shipping > 5 else ORANGE, "fontSize": "11px"}),
                ], style={"padding": "10px", "backgroundColor": f"{ed.val_health_color}10", "borderRadius": "6px"}),
            ], style={"flex": "1", "minWidth": "200px"}),
        ], style={"display": "flex", "gap": "10px", "flexWrap": "wrap"}),
        html.Div([
            html.H4("Risk Factors", style={"color": RED, "fontSize": "13px", "margin": "12px 0 6px 0"}),
            *risk_items,
            html.H4("Strengths", style={"color": GREEN, "fontSize": "13px", "margin": "12px 0 6px 0"}),
            *strength_items,
        ]),
    ], ORANGE))

    # ── SECTION D: GROWTH TRAJECTORY & PROJECTIONS ──
    proj_fig = go.Figure()
    # Actual revenue line
    rev_vals = [ed.monthly_sales.get(m, 0) for m in ed.months_sorted]
    net_vals = [ed.monthly_net_revenue.get(m, 0) for m in ed.months_sorted]
    proj_fig.add_trace(go.Scatter(
        x=ed.months_sorted, y=rev_vals, mode="lines+markers",
        name="Actual Revenue", line=dict(color=GREEN, width=3), marker=dict(size=8),
    ))
    proj_fig.add_trace(go.Scatter(
        x=ed.months_sorted, y=net_vals, mode="lines+markers",
        name="Actual Net Profit", line=dict(color=ORANGE, width=2, dash="dot"), marker=dict(size=6),
    ))
    # Projected revenue with confidence bands
    if "proj_sales" in ed.analytics_projections and len(ed.months_sorted) >= 3:
        # Start projections from last COMPLETE month
        from datetime import datetime
        _cur_m_val = datetime.now().strftime("%Y-%m")
        _last_complete_val = ed.months_sorted[-1]
        if _last_complete_val == _cur_m_val and datetime.now().day < 25 and len(ed.months_sorted) >= 2:
            _last_complete_val = ed.months_sorted[-2]
        _last_complete_rev = ed.monthly_sales.get(_last_complete_val, 0)

        _last_period = pd.Period(_last_complete_val, freq="M")
        future_months = [str(_last_period + i) for i in range(1, 13)]
        _proj_x = [_last_complete_val] + future_months
        _proj_sales = ed.analytics_projections["proj_sales"]
        _residual_std = ed.analytics_projections.get("residual_std", 0)

        # Extend projections to 12 months using linear trend
        _lr_sales_trend = ed.analytics_projections.get("sales_trend", 0)
        _complete_months = [m for m in ed.months_sorted if m != _cur_m_val or datetime.now().day >= 25]
        _base_idx = len(_complete_months)
        _proj_12 = [max(0, ed.val_monthly_run_rate + _lr_sales_trend * (i - _base_idx + 1)) for i in range(_base_idx, _base_idx + 12)]

        proj_fig.add_trace(go.Scatter(
            x=_proj_x, y=[_last_complete_rev] + _proj_12,
            mode="lines+markers", name="Projected Revenue",
            line=dict(color=CYAN, width=2, dash="dash"), marker=dict(size=5),
        ))
        # Confidence bands (widening)
        _upper = [_last_complete_rev] + [max(0, _proj_12[i] + _residual_std * (i + 1) * 0.5) for i in range(12)]
        _lower = [_last_complete_rev] + [max(0, _proj_12[i] - _residual_std * (i + 1) * 0.5) for i in range(12)]
        proj_fig.add_trace(go.Scatter(
            x=_proj_x + _proj_x[::-1], y=_upper + _lower[::-1],
            fill="toself", fillcolor="rgba(0,212,255,0.09)", line=dict(color="rgba(0,0,0,0)"),
            name="Confidence Band", showlegend=True,
        ))
        # Milestone annotations
        for milestone_val, milestone_label in [(5000, "$5K/mo"), (10000, "$10K/mo")]:
            for i, v in enumerate(_proj_12):
                if v >= milestone_val:
                    proj_fig.add_annotation(
                        x=future_months[i], y=v, text=milestone_label,
                        showarrow=True, arrowhead=2, arrowcolor=CYAN, font=dict(color=CYAN, size=10),
                    )
                    break
    make_chart(proj_fig, 360)
    proj_fig.update_layout(title="12-Month Growth Trajectory")

    children.append(section("D. GROWTH TRAJECTORY & PROJECTIONS", [
        ed.chart_context(
            "Actual performance with 12-month linear projection and widening confidence bands.",
            metrics=[
                ("Monthly Run Rate", money(ed.val_monthly_run_rate), GREEN),
                ("Proj 12mo Revenue", money(ed.val_proj_12mo_revenue), CYAN),
                ("Growth", f"{ed._val_growth_pct:+.1f}%/mo", GREEN if ed._val_growth_pct > 0 else RED),
                ("R²", f"{ed._val_r2:.0%}", TEAL),
            ],
            simple="The solid lines show your real sales so far. The dashed line is where sales are headed if the current trend continues. The shaded area is the 'maybe' zone -- wider means less certain."
        ),
        dcc.Graph(figure=proj_fig, config={"displayModeBar": False}),
    ], GREEN))

    # ── SECTION E: REVENUE TO VALUE BRIDGE (Waterfall) ──
    wf_labels = ["Gross Sales", "Fees", "Shipping", "Ads", "Refunds", "Taxes"]
    wf_values = [ed.gross_sales, -ed.total_fees, -ed.total_shipping_cost, -ed.total_marketing, -ed.total_refunds, -ed.total_taxes]
    wf_measures = ["absolute", "relative", "relative", "relative", "relative", "relative"]
    wf_colors = [GREEN, RED, RED, RED, RED, RED]
    if ed.total_buyer_fees > 0:
        wf_labels.append("CO Buyer Fee")
        wf_values.append(-ed.total_buyer_fees)
        wf_measures.append("relative")
        wf_colors.append(RED)
    wf_labels += ["= After Etsy Fees", "Bank Expenses", "= PROFIT", "Owner Draws", "= SDE"]
    wf_values += [0, -ed.bank_all_expenses, 0, ed.bank_owner_draw_total, 0]
    wf_measures += ["total", "relative", "total", "relative", "total"]
    wf_colors += [TEAL, RED, GREEN, ORANGE, CYAN]

    waterfall_fig = go.Figure(go.Waterfall(
        orientation="v", measure=wf_measures,
        x=wf_labels, y=wf_values,
        connector={"line": {"color": GRAY, "width": 1, "dash": "dot"}},
        increasing={"marker": {"color": GREEN}},
        decreasing={"marker": {"color": RED}},
        totals={"marker": {"color": CYAN}},
        text=[money(abs(v)) if v != 0 else "" for v in wf_values],
        textposition="outside",
    ))
    make_chart(waterfall_fig, 380, False)
    waterfall_fig.update_layout(title="Revenue to SDE Bridge", showlegend=False)

    # Efficiency KPIs
    _profit_per_order = ed.profit / ed.order_count if ed.order_count else 0
    _revenue_per_day = ed.gross_sales / ed.days_active if ed.days_active else 0
    _etsy_take_rate = (ed.total_fees + ed.total_shipping_cost + ed.total_marketing + ed.total_taxes + ed.total_buyer_fees) / ed.gross_sales * 100 if ed.gross_sales else 0

    children.append(section("E. REVENUE TO VALUE BRIDGE", [
        ed.chart_context("Waterfall chart tracing every dollar from gross sales to SDE (Seller's Discretionary Earnings).",
                      simple="Start at the left with total sales. Each red bar takes money away (fees, shipping, etc). The blue bar at the end is what's left. Taller red bars = bigger expenses to investigate."),
        dcc.Graph(figure=waterfall_fig, config={"displayModeBar": False}),
        html.Div([
            kpi_card("Profit / Order", money(_profit_per_order), GREEN, f"{ed.order_count} orders",
                     f"Profit ({money(ed.profit)}) divided by {ed.order_count} orders. This is how much profit you actually make per sale after ALL costs. Higher is better -- raise this by increasing prices, reducing shipping costs, or cutting refunds."),
            kpi_card("Monthly Run Rate", money(ed.val_monthly_run_rate), TEAL, f"{ed._val_months_operating} months",
                     f"Average monthly gross sales: {money(ed.gross_sales)} over {ed._val_months_operating} months. This is what you'd expect to earn in a typical month at current pace. Annualized: {money(ed.val_annual_revenue)}."),
            kpi_card("Revenue / Day", money(_revenue_per_day), BLUE, f"{ed.days_active} days active",
                     f"Gross sales ({money(ed.gross_sales)}) divided by {ed.days_active} days of operation. This is your daily earning rate. At this pace, you'd earn {money(_revenue_per_day * 365)} per year."),
            kpi_card("Etsy Take Rate", f"{_etsy_take_rate:.1f}%", ORANGE if _etsy_take_rate < 25 else RED, "All Etsy deductions",
                     f"What percentage Etsy takes from each dollar of sales: Fees {money(ed.total_fees)} + Shipping {money(ed.total_shipping_cost)} + Ads {money(ed.total_marketing)} + Tax {money(ed.total_taxes)} + Buyer Fees {money(ed.total_buyer_fees)} = {money(ed.total_fees + ed.total_shipping_cost + ed.total_marketing + ed.total_taxes + ed.total_buyer_fees)}. Industry typical: 20-30%."),
        ], style={"display": "flex", "gap": "10px", "flexWrap": "wrap", "marginTop": "10px"}),
    ], ORANGE))

    # ── SECTION F: CASH POSITION & BALANCE SHEET ──
    bs_fig = go.Figure()
    bs_fig.add_trace(go.Bar(
        name="Assets", x=["Bank Cash", "Etsy Balance", "Equipment", "Inventory"],
        y=[ed.bank_net_cash, ed.etsy_balance, ed.bb_cc_asset_value, ed.true_inventory_cost],
        marker_color=[GREEN, TEAL, BLUE, PURPLE],
        text=[money(v) for v in [ed.bank_net_cash, ed.etsy_balance, ed.bb_cc_asset_value, ed.true_inventory_cost]],
        textposition="outside",
    ))
    bs_fig.add_trace(go.Bar(
        name="Liabilities", x=["CC Debt", "", "", ""],
        y=[ed.bb_cc_balance, 0, 0, 0],
        marker_color=[RED, "rgba(0,0,0,0)", "rgba(0,0,0,0)", "rgba(0,0,0,0)"],
        text=[money(ed.bb_cc_balance) if ed.bb_cc_balance > 0 else "", "", "", ""],
        textposition="outside",
    ))
    make_chart(bs_fig, 300)
    bs_fig.update_layout(title="Assets vs Liabilities", barmode="group")

    _settlement_text = f"Company owes {ed.draw_owed_to} {money(ed.draw_diff)}" if ed.draw_diff > 0 else "Draws are balanced"

    children.append(section("F. CASH POSITION & BALANCE SHEET", [
        dcc.Graph(figure=bs_fig, config={"displayModeBar": False}),
        html.Div([
            html.Div([
                row_item("Bank Cash (Capital One)", ed.bank_net_cash, bold=True, color=GREEN),
                row_item("Etsy Balance (pending)", ed.etsy_balance, indent=1),
                row_item("Total Cash", ed.bank_cash_on_hand, bold=True, color=CYAN),
            ], style={"flex": "1", "minWidth": "250px"}),
            html.Div([
                row_item("Monthly Burn Rate", ed.val_monthly_expenses, color=ORANGE),
                row_item("Runway (months)", ed.val_runway_months, color=TEAL if ed.val_runway_months > 3 else RED),
                row_item("Owner Draws Taken", ed.bank_owner_draw_total, color=ORANGE),
                row_item("Draw Settlement", ed.draw_diff, color=GRAY),
                html.P(_settlement_text, style={"color": GRAY, "fontSize": "11px", "marginTop": "4px"}),
            ], style={"flex": "1", "minWidth": "250px"}),
        ], style={"display": "flex", "gap": "16px", "flexWrap": "wrap", "marginTop": "10px"}),
    ], BLUE))

    # ── SECTION G: PRODUCT PORTFOLIO VALUE ──
    _top_products = ed.product_revenue_est.head(10)
    _other_rev = ed.product_revenue_est.iloc[10:].sum() if len(ed.product_revenue_est) > 10 else 0
    _donut_labels = [p[:30] for p in _top_products.index.tolist()]
    _donut_values = _top_products.values.tolist()
    if _other_rev > 0:
        _donut_labels.append("Other Products")
        _donut_values.append(_other_rev)

    product_donut = go.Figure(go.Pie(
        labels=_donut_labels, values=_donut_values, hole=0.5,
        textinfo="label+percent", textposition="outside",
    ))
    make_chart(product_donut, 340, False)
    product_donut.update_layout(title="Revenue by Product (Top 10)", showlegend=False)

    _total_prod_rev = ed.product_revenue_est.sum()
    _top1_rev = ed.product_revenue_est.values[0] if len(ed.product_revenue_est) > 0 else 0
    _top1_name = ed.product_revenue_est.index[0][:25] if len(ed.product_revenue_est) > 0 else "N/A"

    children.append(section("G. PRODUCT PORTFOLIO VALUE", [
        dcc.Graph(figure=product_donut, config={"displayModeBar": False}),
        html.Div([
            kpi_card("Active Products", str(len(ed.product_revenue_est)), TEAL, "unique product types",
                     f"{len(ed.product_revenue_est)} unique products that generated sales. More products = more diversified revenue. Revenue sourced directly from Etsy CSV sale transactions."),
            kpi_card("Top-3 Concentration", f"{ed._top3_conc:.0f}%", ORANGE if ed._top3_conc > 60 else GREEN,
                     "of total product revenue",
                     f"How much revenue your top 3 products account for. {ed._top3_conc:.0f}% means {'most of your revenue depends on just 3 products -- risky if any stop selling' if ed._top3_conc > 60 else 'your revenue is reasonably spread across products -- good diversification'}. Below 50% is considered well-diversified."),
            kpi_card("Top Product", money(_top1_rev), GREEN, _top1_name,
                     f"Your best-selling product by revenue. This single product accounts for {_top1_rev / _total_prod_rev * 100:.1f}% of total product revenue. Consider creating variations or bundles to capitalize on its popularity."),
            kpi_card("Avg Order Value", money(ed.avg_order), BLUE, f"{ed.order_count} total orders",
                     f"Total gross sales ({money(ed.gross_sales)}) divided by {ed.order_count} orders. Higher AOV means customers spend more per purchase. Increase AOV by bundling products, offering upsells, or raising prices on popular items."),
        ], style={"display": "flex", "gap": "10px", "flexWrap": "wrap", "marginTop": "10px"}),
    ], PURPLE))

    # ── SECTION H: SENSITIVITY ANALYSIS ──
    sde_multiples = [1.0, 1.5, 2.0, 2.5, 3.0]
    growth_scenarios = [("-20%", 0.80), ("-10%", 0.90), ("Current", 1.00), ("+10%", 1.10), ("+20%", 1.20), ("+30%", 1.30)]

    table_rows = []
    # Header row
    table_rows.append(html.Tr([
        html.Th("SDE Multiple", style={"color": CYAN, "padding": "8px", "borderBottom": f"2px solid {CYAN}44", "textAlign": "left"}),
    ] + [
        html.Th(label, style={"color": GRAY, "padding": "8px", "borderBottom": f"2px solid {CYAN}44", "textAlign": "right"})
        for label, _ in growth_scenarios
    ]))

    for mult in sde_multiples:
        cells = [html.Td(f"{mult:.1f}x", style={"color": WHITE, "padding": "6px 8px", "fontWeight": "bold"})]
        for label, factor in growth_scenarios:
            val = ed.val_annual_sde * factor * mult
            is_current = mult == 1.5 and label == "Current"
            cell_style = {
                "color": CYAN if is_current else WHITE,
                "padding": "6px 8px", "textAlign": "right", "fontFamily": "monospace", "fontSize": "12px",
                "backgroundColor": f"{CYAN}20" if is_current else "transparent",
                "fontWeight": "bold" if is_current else "normal",
                "borderRadius": "4px" if is_current else "0",
            }
            cells.append(html.Td(f"${val:,.0f}", style=cell_style))
        table_rows.append(html.Tr(cells, style={"borderBottom": "1px solid #ffffff08"}))

    children.append(section("H. SENSITIVITY ANALYSIS", [
        ed.chart_context("How valuation changes with different SDE multiples and growth scenarios. "
                      "Current estimate highlighted in cyan.",
                      metrics=[("Current SDE", money(ed.val_annual_sde), GREEN), ("Current Multiple", "1.5x", CYAN)],
                      simple="This table shows 'what if' scenarios. Each cell is a possible business value. The highlighted cell is the current estimate. Moving right = if the business grows more. Moving down = if a buyer pays a higher price multiple."),
        html.Table(table_rows, style={
            "width": "100%", "borderCollapse": "collapse",
            "backgroundColor": f"{CARD2}", "borderRadius": "6px",
        }),
    ], CYAN))

    # ── SECTION I: INDUSTRY BENCHMARKS ──
    _fee_rate = ed.total_fees / ed.gross_sales * 100 if ed.gross_sales else 0
    _refund_rate = ed.total_refunds / ed.gross_sales * 100 if ed.gross_sales else 0
    _cogs_ratio = ed.true_inventory_cost / ed.gross_sales * 100 if ed.gross_sales else 0

    bench_categories = ["Profit Margin", "Fee Rate", "Refund Rate", "Supply Cost Ratio", "Monthly Growth"]
    bench_actual = [ed.profit_margin, _fee_rate, _refund_rate, _cogs_ratio, ed._val_growth_pct]
    bench_avg = [15, 13, 3.0, 25, 5]  # Industry averages for small Etsy businesses
    bench_good = [25, 10, 1.5, 15, 10]  # "Good" benchmarks

    bench_fig = go.Figure()
    bench_fig.add_trace(go.Bar(
        name="Your Business", x=bench_categories, y=bench_actual,
        marker_color=CYAN, text=[f"{v:.1f}%" for v in bench_actual], textposition="outside",
    ))
    bench_fig.add_trace(go.Scatter(
        name="Industry Average", x=bench_categories, y=bench_avg,
        mode="markers+lines", marker=dict(color=ORANGE, size=10, symbol="diamond"),
        line=dict(color=ORANGE, dash="dash"),
    ))
    bench_fig.add_trace(go.Scatter(
        name="Good Benchmark", x=bench_categories, y=bench_good,
        mode="markers+lines", marker=dict(color=GREEN, size=10, symbol="star"),
        line=dict(color=GREEN, dash="dot"),
    ))
    make_chart(bench_fig, 320)
    bench_fig.update_layout(title="Your Business vs Industry Benchmarks (%)")

    children.append(section("I. INDUSTRY BENCHMARKS", [
        ed.chart_context("Compare your key metrics against typical Etsy small business averages and 'good' benchmarks.",
                      legend=[
                          (CYAN, "Your Business", "actual performance"),
                          (ORANGE, "Industry Average", "typical small Etsy shop"),
                          (GREEN, "Good Benchmark", "top-performing shops"),
                      ],
                      simple="Blue bars are YOUR numbers. Orange diamonds are average Etsy shops. Green stars are top performers. You want your bars to beat the orange diamonds and get close to the green stars."),
        dcc.Graph(figure=bench_fig, config={"displayModeBar": False}),
    ], TEAL))

    # ── SECTION J: KEY METRICS TIMELINE ──
    timeline_fig = make_subplots(specs=[[{"secondary_y": True}]])
    timeline_fig.add_trace(go.Bar(
        name="Monthly Revenue", x=ed.months_sorted,
        y=[ed.monthly_sales.get(m, 0) for m in ed.months_sorted],
        marker_color=GREEN, opacity=0.7,
    ))
    timeline_fig.add_trace(go.Scatter(
        name="Profit Margin %", x=ed.months_sorted,
        y=[ed.monthly_net_revenue.get(m, 0) / ed.monthly_sales.get(m, 1) * 100 for m in ed.months_sorted],
        mode="lines+markers+text",
        text=[f"{ed.monthly_net_revenue.get(m, 0) / ed.monthly_sales.get(m, 1) * 100:.0f}%" for m in ed.months_sorted],
        textposition="top center", textfont=dict(color=ORANGE),
        line=dict(color=ORANGE, width=2), marker=dict(size=8),
    ), secondary_y=True)
    timeline_fig.add_trace(go.Scatter(
        name="AOV", x=ed.months_sorted,
        y=[ed.monthly_aov.get(m, 0) for m in ed.months_sorted],
        mode="lines+markers",
        line=dict(color=PURPLE, width=2, dash="dot"), marker=dict(size=6),
    ), secondary_y=True)
    make_chart(timeline_fig, 340)
    timeline_fig.update_layout(title="Key Metrics Over Time")
    timeline_fig.update_yaxes(title_text="Revenue ($)", secondary_y=False)
    timeline_fig.update_yaxes(title_text="Margin % / AOV ($)", secondary_y=True)

    children.append(section("J. KEY METRICS TIMELINE", [
        ed.chart_context("Monthly revenue (bars) with profit margin % and AOV overlaid.",
                      metrics=[
                          ("Avg Margin", f"{ed.profit_margin:.1f}%", ORANGE),
                          ("Avg AOV", money(ed.avg_order), PURPLE),
                          ("Months", str(ed._val_months_operating), GRAY),
                      ],
                      simple="Green bars show how much you sold each month (taller = more sales). The orange line shows what percentage you kept as profit. Both going up together = healthy growth."),
        dcc.Graph(figure=timeline_fig, config={"displayModeBar": False}),
    ], ORANGE))

    # ── SECTION K: VALUATION NOTES & METHODOLOGY ──
    children.append(section("K. VALUATION NOTES & METHODOLOGY", [
        html.Div([
            html.H4("Methodology", style={"color": CYAN, "fontSize": "13px", "margin": "0 0 8px 0"}),
            html.Ul([
                html.Li([html.B("SDE Multiple (50%): "), "Seller's Discretionary Earnings = Profit + Owner Draws. "
                         "Annualized from 5 months, multiplied by 1.0x (floor), 1.5x (typical), 2.5x (optimistic) for small e-commerce businesses."],
                        style={"color": GRAY, "fontSize": "12px", "marginBottom": "4px"}),
                html.Li([html.B("Revenue Multiple (25%): "), "Gross sales annualized, multiplied by 0.3x-1.0x. "
                         "Etsy shops typically sell for 0.3-0.8x annual revenue depending on growth and margins."],
                        style={"color": GRAY, "fontSize": "12px", "marginBottom": "4px"}),
                html.Li([html.B("Asset-Based (25%): "), "Tangible assets (cash + equipment + inventory) minus liabilities (CC debt). "
                         "Floor valuation — what the business is worth if liquidated today."],
                        style={"color": GRAY, "fontSize": "12px", "marginBottom": "4px"}),
                html.Li([html.B("Blended: "), "Weighted average: SDE (50%) + Revenue (25%) + Asset (25%). "
                         "Gives heaviest weight to earnings power while accounting for revenue scale and hard assets."],
                        style={"color": GRAY, "fontSize": "12px", "marginBottom": "4px"}),
            ]),
            html.H4("Data Period & Caveats", style={"color": ORANGE, "fontSize": "13px", "margin": "12px 0 8px 0"}),
            html.Ul([
                html.Li(f"Data covers {ed._val_months_operating} months (Oct 2025 — Feb 2026). "
                        "Annualization assumes current performance is representative. Seasonal businesses may see significant variance.",
                        style={"color": GRAY, "fontSize": "12px", "marginBottom": "4px"}),
                html.Li("Growth projections use linear regression on monthly data. "
                        f"R² = {ed._val_r2:.0%} — {'high confidence' if ed._val_r2 > 0.7 else 'moderate confidence, take projections as estimates'}.",
                        style={"color": GRAY, "fontSize": "12px", "marginBottom": "4px"}),
                html.Li("Product revenue estimates are derived from 6.5% transaction fee reverse-engineering, not exact sale prices.",
                        style={"color": GRAY, "fontSize": "12px", "marginBottom": "4px"}),
            ]),
            html.Div([
                html.Span("DISCLAIMER: ", style={"color": RED, "fontWeight": "bold", "fontSize": "11px"}),
                html.Span("This valuation is for internal planning purposes only. Actual business sale price depends on "
                          "buyer negotiations, market conditions, verified financials, and due diligence. "
                          "Consult a business broker or CPA for formal valuation.",
                          style={"color": GRAY, "fontSize": "11px"}),
            ], style={"padding": "10px", "backgroundColor": f"{RED}10", "borderRadius": "6px",
                      "border": f"1px solid {RED}33", "marginTop": "8px"}),
        ]),
    ], GRAY))

    return html.Div(children, style={"padding": TAB_PADDING})
