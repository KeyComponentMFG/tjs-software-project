"""KPI pill/card builders using dash-bootstrap-components."""
from dash import html
import dash_bootstrap_components as dbc
from etsy_dashboard.theme import *


def icon_badge(text, color):
    """Colored 36px icon circle with gradient bg for KPI pills."""
    return html.Div(text, style={
        "width": "36px", "height": "36px", "borderRadius": "50%",
        "background": f"linear-gradient(135deg, {color}, {color}88)",
        "color": "#ffffff",
        "display": "inline-flex", "alignItems": "center", "justifyContent": "center",
        "fontSize": "15px", "fontWeight": "bold", "flexShrink": "0",
        "boxShadow": f"0 3px 10px {color}44",
    })


def kpi_pill(icon, label, value, color, subtitle="", detail=""):
    """Premium KPI pill with gradient icon, bold value, and optional expandable detail."""
    text_children = [
        html.Div(label, style={"color": GRAY, "fontSize": "11px", "fontWeight": "600",
                                "letterSpacing": "1.2px", "textTransform": "uppercase",
                                "lineHeight": "1"}),
        html.Div(value, style={"color": WHITE, "fontSize": "28px", "fontWeight": "bold",
                                "fontFamily": "monospace", "lineHeight": "1.1",
                                "marginTop": "3px",
                                "textShadow": f"0 0 12px {color}33"}),
    ]
    if subtitle:
        text_children.append(html.Div(subtitle, style={"color": DARKGRAY, "fontSize": "11px",
                                                         "marginTop": "2px"}))
    if detail:
        text_children.append(dbc.Accordion([
            dbc.AccordionItem(
                html.P(detail, style={"color": GRAY, "fontSize": "11px", "margin": "0",
                                       "lineHeight": "1.4"}),
                title="details",
            ),
        ], start_collapsed=True, flush=True, className="kpi-detail-accordion"))
    children = [
        icon_badge(icon, color),
        html.Div(text_children, style={"marginLeft": "12px", "minWidth": "0"}),
    ]
    return dbc.Card(
        dbc.CardBody(children, style={"display": "flex", "alignItems": "center", "padding": "14px 18px"}),
        style={"borderLeft": f"4px solid {color}", "flex": "1", "minWidth": "130px"},
        className="kpi-pill",
    )


def kpi_card(label, value, color, subtitle="", detail=""):
    """Simpler KPI card (used in valuation and other sections)."""
    body_children = [
        html.Div(label, className="kpi-label"),
        html.Div(value, className="kpi-value", style={"color": color}),
    ]
    if subtitle:
        body_children.append(html.Div(subtitle, className="kpi-subtitle"))
    if detail:
        body_children.append(dbc.Accordion([
            dbc.AccordionItem(
                html.P(detail, style={"color": GRAY, "fontSize": "11px", "margin": "0",
                                       "lineHeight": "1.4"}),
                title="details",
            ),
        ], start_collapsed=True, flush=True, className="kpi-detail-accordion"))
    return dbc.Card(
        dbc.CardBody(body_children, style={"padding": "14px", "textAlign": "center"}),
        style={"borderTop": f"3px solid {color}", "flex": "1", "minWidth": "130px"},
        className="kpi-card-top",
    )
