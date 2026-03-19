"""Valuation page — Business valuation estimates."""
from dash import html, dcc
import dash_bootstrap_components as dbc
import plotly.graph_objects as go

from etsy_dashboard.theme import *
from etsy_dashboard.components.kpi import kpi_card
from etsy_dashboard.components.cards import section, make_chart
from etsy_dashboard import data_state as ds


def _val_row(label, value_str, color=WHITE, bold=False, indent=0, border=False):
    """A single valuation row."""
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
    """Build the Valuation page."""
    # Annualize from months of data
    months_of_data = len(ds.months_sorted)
    annual_factor = 12 / months_of_data if months_of_data > 0 else 1

    annual_revenue = ds.gross_sales * annual_factor
    annual_net = ds.net_profit * annual_factor
    annual_profit = ds.full_profit * annual_factor

    # Valuation methods
    # 1. Revenue multiple (1-3x for small Etsy shops)
    rev_low = annual_revenue * 1.0
    rev_mid = annual_revenue * 2.0
    rev_high = annual_revenue * 3.0

    # 2. Earnings multiple (SDE method, 2-4x)
    sde = annual_profit + (ds.bank_owner_draw_total * annual_factor)  # Add back draws
    earn_low = sde * 2.0
    earn_mid = sde * 3.0
    earn_high = sde * 4.0

    # 3. Asset-based (inventory + cash)
    asset_value = ds.bank_cash_on_hand + ds.true_inventory_cost + ds.bb_cc_asset_value - ds.bb_cc_balance

    # Blended estimate
    blended = (rev_mid + earn_mid + asset_value) / 3

    # Growth metrics
    if months_of_data >= 2:
        first_month_rev = ds.monthly_sales.get(ds.months_sorted[0], 0)
        last_month_rev = ds.monthly_sales.get(ds.months_sorted[-1], 0)
        if first_month_rev > 0:
            monthly_growth = ((last_month_rev / first_month_rev) ** (1 / max(months_of_data - 1, 1)) - 1) * 100
        else:
            monthly_growth = 0
    else:
        monthly_growth = 0

    # Valuation comparison chart
    val_fig = go.Figure()
    methods = ["Revenue\n(1x)", "Revenue\n(2x)", "Revenue\n(3x)",
               "Earnings\n(2x)", "Earnings\n(3x)", "Earnings\n(4x)",
               "Asset\nBased", "Blended\nEstimate"]
    values = [rev_low, rev_mid, rev_high, earn_low, earn_mid, earn_high, asset_value, blended]
    colors = [GREEN, GREEN, GREEN, CYAN, CYAN, CYAN, ORANGE, PURPLE]

    val_fig.add_trace(go.Bar(
        x=methods, y=values,
        marker_color=colors,
        text=[f"${v:,.0f}" for v in values],
        textposition="outside",
    ))
    make_chart(val_fig, 380)
    val_fig.update_layout(title="Valuation Estimates", showlegend=False,
                          yaxis=dict(title="Value ($)"))

    # Revenue projection chart (12 months)
    proj_fig = go.Figure()
    if months_of_data >= 2:
        import numpy as np
        from sklearn.linear_model import LinearRegression
        x = np.arange(months_of_data).reshape(-1, 1)
        y = np.array([ds.monthly_sales.get(m, 0) for m in ds.months_sorted])
        model = LinearRegression().fit(x, y)
        future_x = np.arange(months_of_data + 6).reshape(-1, 1)
        future_y = model.predict(future_x)

        proj_fig.add_trace(go.Bar(
            x=ds.months_sorted, y=y,
            name="Actual", marker_color=GREEN,
        ))
        all_months = list(ds.months_sorted) + [f"Proj {i+1}" for i in range(6)]
        proj_fig.add_trace(go.Scatter(
            x=all_months, y=future_y,
            name="Projected", line=dict(color=CYAN, width=2, dash="dash"),
            mode="lines+markers",
        ))
    make_chart(proj_fig, 300)
    proj_fig.update_layout(title="Revenue Projection (6-month)")

    return html.Div([
        # KPI strip
        dbc.Row([
            dbc.Col(kpi_card("BLENDED VALUE", ds.money(blended), PURPLE,
                             "Avg of 3 methods"), md=3),
            dbc.Col(kpi_card("ANNUAL REVENUE", ds.money(annual_revenue), GREEN,
                             f"{months_of_data}mo annualized"), md=3),
            dbc.Col(kpi_card("ANNUAL PROFIT", ds.money(annual_profit), CYAN), md=3),
            dbc.Col(kpi_card("GROWTH RATE", f"{monthly_growth:.1f}%/mo", ORANGE,
                             "Month-over-month"), md=3),
        ], className="g-2 mb-3"),

        # Valuation chart
        dbc.Card(dbc.CardBody(dcc.Graph(figure=val_fig, config={"displayModeBar": False})), className="mb-3"),

        dbc.Row([
            # Valuation methods
            dbc.Col([
                section("Revenue Multiple Method", [
                    html.P("Based on annualized gross revenue", style={"color": GRAY, "fontSize": "12px"}),
                    _val_row("Annual Revenue", ds.money(annual_revenue), color=GREEN),
                    html.Hr(style={"borderColor": "#ffffff10", "margin": "6px 0"}),
                    _val_row("Conservative (1x)", ds.money(rev_low)),
                    _val_row("Moderate (2x)", ds.money(rev_mid), color=GREEN, bold=True),
                    _val_row("Optimistic (3x)", ds.money(rev_high)),
                ], GREEN),

                section("Earnings / SDE Method", [
                    html.P("Seller's Discretionary Earnings = Profit + Owner Draws",
                           style={"color": GRAY, "fontSize": "12px"}),
                    _val_row("Annual Profit", ds.money(annual_profit)),
                    _val_row("+ Annual Draws", ds.money(ds.bank_owner_draw_total * annual_factor)),
                    _val_row("= SDE", ds.money(sde), color=CYAN, bold=True, border=True),
                    html.Hr(style={"borderColor": "#ffffff10", "margin": "6px 0"}),
                    _val_row("Conservative (2x SDE)", ds.money(earn_low)),
                    _val_row("Moderate (3x SDE)", ds.money(earn_mid), color=CYAN, bold=True),
                    _val_row("Optimistic (4x SDE)", ds.money(earn_high)),
                ], CYAN),
            ], md=6),

            dbc.Col([
                section("Asset-Based Valuation", [
                    _val_row("Cash on Hand", ds.money(ds.bank_cash_on_hand), indent=1),
                    _val_row("Inventory (at cost)", ds.money(ds.true_inventory_cost), indent=1),
                    _val_row("CC Purchased Assets", ds.money(ds.bb_cc_asset_value), indent=1),
                    _val_row("Less: CC Balance", f"-{ds.money(ds.bb_cc_balance)}", indent=1, color=RED),
                    _val_row("= Net Asset Value", ds.money(asset_value), color=ORANGE, bold=True, border=True),
                ], ORANGE),

                dbc.Card(dbc.CardBody(dcc.Graph(figure=proj_fig, config={"displayModeBar": False})), className="mb-3"),
            ], md=6),
        ], className="g-3"),
    ])
