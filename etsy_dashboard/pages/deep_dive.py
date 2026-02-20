"""Deep Dive page — AI analytics, trends, projections."""
from dash import html, dcc
import dash_bootstrap_components as dbc
import plotly.graph_objects as go
import numpy as np
import pandas as pd

from etsy_dashboard.theme import *
from etsy_dashboard.components.kpi import kpi_pill, kpi_card
from etsy_dashboard.components.cards import section, row_item, make_chart, chart_context, severity_color
from etsy_dashboard import data_state as ds


def _run_analytics():
    """Run the analytics engine and return findings."""
    findings = []

    # Revenue trend
    if len(ds.months_sorted) >= 2:
        vals = [ds.monthly_sales.get(m, 0) for m in ds.months_sorted]
        if vals[0] > 0:
            growth = ((vals[-1] - vals[0]) / vals[0]) * 100
            sev = "good" if growth > 0 else "bad"
            findings.append({
                "title": f"Revenue {'grew' if growth > 0 else 'declined'} {abs(growth):.0f}% over {len(ds.months_sorted)} months",
                "detail": f"From ${vals[0]:,.0f} to ${vals[-1]:,.0f}",
                "severity": sev,
                "category": "Revenue",
            })

    # Average order value trend
    if len(ds.months_sorted) >= 2:
        aov_vals = [ds.monthly_aov.get(m, 0) for m in ds.months_sorted]
        first_aov = aov_vals[0] if aov_vals[0] > 0 else 1
        aov_change = ((aov_vals[-1] - first_aov) / first_aov) * 100
        sev = "good" if aov_change > 0 else "warning" if aov_change > -10 else "bad"
        findings.append({
            "title": f"AOV {'increased' if aov_change > 0 else 'decreased'} {abs(aov_change):.0f}%",
            "detail": f"From ${first_aov:,.2f} to ${aov_vals[-1]:,.2f}",
            "severity": sev,
            "category": "Orders",
        })

    # Fee efficiency
    if ds.gross_sales > 0:
        fee_rate = (ds.total_fees / ds.gross_sales) * 100
        sev = "good" if fee_rate < 15 else "warning" if fee_rate < 20 else "bad"
        findings.append({
            "title": f"Fee rate: {fee_rate:.1f}% of gross sales",
            "detail": f"${ds.total_fees:,.0f} in fees on ${ds.gross_sales:,.0f} sales",
            "severity": sev,
            "category": "Costs",
        })

    # Shipping efficiency
    if ds.total_shipping_cost > 0 and ds.gross_sales > 0:
        ship_rate = (ds.total_shipping_cost / ds.gross_sales) * 100
        sev = "good" if ship_rate < 5 else "warning" if ship_rate < 8 else "bad"
        findings.append({
            "title": f"Shipping costs: {ship_rate:.1f}% of revenue",
            "detail": f"${ds.total_shipping_cost:,.0f} on labels",
            "severity": sev,
            "category": "Costs",
        })

    # Revenue projection (linear regression)
    if len(ds.months_sorted) >= 3:
        from sklearn.linear_model import LinearRegression
        x = np.arange(len(ds.months_sorted)).reshape(-1, 1)
        y = np.array([ds.monthly_sales.get(m, 0) for m in ds.months_sorted])
        model = LinearRegression().fit(x, y)
        next_month_rev = model.predict([[len(ds.months_sorted)]])[0]
        findings.append({
            "title": f"Projected next month revenue: ${next_month_rev:,.0f}",
            "detail": f"Linear trend based on {len(ds.months_sorted)} months (R²={model.score(x, y):.2f})",
            "severity": "info",
            "category": "Projections",
        })

    # Profit margin health
    if ds.gross_sales > 0:
        margin = ds.full_profit_margin
        sev = "good" if margin > 30 else "warning" if margin > 15 else "bad"
        findings.append({
            "title": f"Full profit margin: {margin:.1f}%",
            "detail": f"Profit ${ds.full_profit:,.0f} on ${ds.gross_sales:,.0f} gross",
            "severity": sev,
            "category": "Profitability",
        })

    # Best selling products
    if len(ds.product_revenue_est) > 0:
        top = ds.product_revenue_est.head(3)
        top_list = ", ".join([f"{name[:30]} (${rev:,.0f})" for name, rev in top.items()])
        findings.append({
            "title": "Top products by estimated revenue",
            "detail": top_list,
            "severity": "info",
            "category": "Products",
        })

    # Marketing ROI
    if ds.total_marketing > 0 and ds.gross_sales > 0:
        roi = ds.gross_sales / ds.total_marketing
        sev = "good" if roi > 10 else "warning" if roi > 5 else "bad"
        findings.append({
            "title": f"Marketing ROI: {roi:.1f}x return",
            "detail": f"${ds.total_marketing:,.0f} spent → ${ds.gross_sales:,.0f} gross sales",
            "severity": sev,
            "category": "Marketing",
        })

    return findings


def layout():
    """Build the Deep Dive analytics page."""
    findings = _run_analytics()

    # KPI strip
    days = ds.days_active
    daily_rev = ds.gross_sales / days if days else 0
    daily_profit = ds.full_profit / days if days else 0
    daily_orders = ds.order_count / days if days else 0

    # Cumulative chart
    cum_fig = go.Figure()
    if len(ds.daily_df) > 0:
        cum_fig.add_trace(go.Scatter(
            x=ds.daily_df.index, y=ds.daily_df["cum_revenue"],
            name="Cumulative Revenue", line=dict(color=GREEN, width=2),
            fill="tozeroy", fillcolor="rgba(46,204,113,0.08)",
        ))
        cum_fig.add_trace(go.Scatter(
            x=ds.daily_df.index, y=ds.daily_df["cum_profit"],
            name="Cumulative Profit", line=dict(color=CYAN, width=2),
            fill="tozeroy", fillcolor="rgba(0,212,255,0.08)",
        ))
    make_chart(cum_fig, 350)
    cum_fig.update_layout(title="Cumulative Revenue & Profit", xaxis_title="Date")

    # Weekly AOV chart
    aov_fig = go.Figure()
    if len(ds.weekly_aov) > 0:
        aov_fig.add_trace(go.Scatter(
            x=ds.weekly_aov.index, y=ds.weekly_aov["aov"],
            name="Weekly AOV", line=dict(color=ORANGE, width=2),
            mode="lines+markers",
        ))
        aov_fig.add_trace(go.Scatter(
            x=ds.weekly_aov.index, y=ds.weekly_aov["count"],
            name="Orders", line=dict(color=CYAN, width=1, dash="dot"),
            yaxis="y2",
        ))
    make_chart(aov_fig, 300)
    aov_fig.update_layout(
        title="Weekly AOV & Order Count",
        yaxis=dict(title="AOV ($)"),
        yaxis2=dict(title="Orders", overlaying="y", side="right"),
    )

    # Daily profit chart
    daily_fig = go.Figure()
    if len(ds.daily_df) > 0:
        colors = [GREEN if v >= 0 else RED for v in ds.daily_df["profit"]]
        daily_fig.add_trace(go.Bar(
            x=ds.daily_df.index, y=ds.daily_df["profit"],
            name="Daily Profit", marker_color=colors,
        ))
        # 7-day moving average
        if len(ds.daily_df) >= 7:
            ma7 = ds.daily_df["profit"].rolling(7).mean()
            daily_fig.add_trace(go.Scatter(
                x=ds.daily_df.index, y=ma7,
                name="7-day MA", line=dict(color=CYAN, width=2),
            ))
    make_chart(daily_fig, 300)
    daily_fig.update_layout(title="Daily Profit", xaxis_title="Date")

    # Product revenue chart
    prod_fig = go.Figure()
    if len(ds.product_revenue_est) > 0:
        top_prods = ds.product_revenue_est.head(10)
        prod_fig.add_trace(go.Bar(
            x=top_prods.values,
            y=[n[:35] for n in top_prods.index],
            orientation="h",
            marker_color=CYAN,
        ))
    make_chart(prod_fig, 350, legend_h=False)
    prod_fig.update_layout(title="Top Products by Est. Revenue", yaxis=dict(autorange="reversed"))

    # Build findings cards
    finding_cards = []
    for f in findings:
        color = severity_color(f["severity"])
        finding_cards.append(
            dbc.Card(dbc.CardBody([
                html.Div([
                    html.Span("● ", style={"color": color, "fontSize": "14px"}),
                    html.Span(f["category"], style={"color": GRAY, "fontSize": "10px",
                                                      "letterSpacing": "1px", "textTransform": "uppercase"}),
                ], style={"marginBottom": "4px"}),
                html.Div(f["title"], style={"color": WHITE, "fontSize": "14px", "fontWeight": "600"}),
                html.P(f["detail"], style={"color": GRAY, "fontSize": "12px", "margin": "4px 0 0 0"}),
            ], style={"padding": "12px"}),
            style={"borderLeft": f"3px solid {color}"}, className="mb-2")
        )

    return html.Div([
        # KPI strip
        dbc.Row([
            dbc.Col(kpi_card("DAILY REVENUE", f"${daily_rev:,.0f}", GREEN, f"{days} days active"), md=3),
            dbc.Col(kpi_card("DAILY PROFIT", f"${daily_profit:,.0f}", CYAN), md=3),
            dbc.Col(kpi_card("DAILY ORDERS", f"{daily_orders:.1f}", ORANGE), md=3),
            dbc.Col(kpi_card("AVG ORDER", ds.money(ds.avg_order), TEAL), md=3),
        ], className="g-2 mb-3"),

        # AI Findings
        section("AI Analytics Findings", finding_cards, CYAN),

        # Charts row
        dbc.Row([
            dbc.Col(dbc.Card(dbc.CardBody(dcc.Graph(figure=cum_fig, config={"displayModeBar": False}))), md=6),
            dbc.Col(dbc.Card(dbc.CardBody(dcc.Graph(figure=aov_fig, config={"displayModeBar": False}))), md=6),
        ], className="g-3 mb-3"),

        dbc.Row([
            dbc.Col(dbc.Card(dbc.CardBody(dcc.Graph(figure=daily_fig, config={"displayModeBar": False}))), md=6),
            dbc.Col(dbc.Card(dbc.CardBody(dcc.Graph(figure=prod_fig, config={"displayModeBar": False}))), md=6),
        ], className="g-3"),
    ])
