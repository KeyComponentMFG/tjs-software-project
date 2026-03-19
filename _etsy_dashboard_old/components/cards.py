"""Reusable card/section builders."""
from dash import html, dcc
import dash_bootstrap_components as dbc
from etsy_dashboard.theme import *
from etsy_dashboard.data_state import money


def section(title, children, color=ORANGE):
    """Titled section card with colored top border."""
    return dbc.Card([
        dbc.CardHeader(title, style={"color": color, "fontWeight": "bold", "fontSize": "16px",
                                      "borderBottom": f"2px solid {color}",
                                      "backgroundColor": "transparent", "padding": "12px 16px"}),
        dbc.CardBody(children, style={"padding": "16px"}),
    ], className="mb-3")


def row_item(label, amount, indent=0, bold=False, color=WHITE, neg_color=RED):
    """Single P&L / ledger row."""
    display_color = neg_color if amount < 0 else color
    style = {
        "display": "flex", "justifyContent": "space-between",
        "padding": "4px 0", "borderBottom": "1px solid #ffffff10",
        "marginLeft": f"{indent * 24}px",
    }
    if bold:
        style["fontWeight"] = "bold"
        style["borderBottom"] = "2px solid #ffffff30"
        style["padding"] = "8px 0"
    return html.Div([
        html.Span(label, style={"color": color if not bold else display_color, "fontSize": "13px"}),
        html.Span(money(amount), style={"color": display_color, "fontFamily": "monospace", "fontSize": "13px"}),
    ], style=style)


def chart_context(description, metrics=None, legend=None, look_for=None, simple=None):
    """Compact context block displayed above a chart."""
    children = [
        html.P(description, style={"color": GRAY, "margin": "0 0 6px 0", "fontSize": "12px"}),
    ]
    if metrics:
        metric_spans = []
        for label, value, color in metrics:
            metric_spans.append(html.Span([
                html.Span(f"{label}: ", style={"color": GRAY, "fontSize": "11px"}),
                html.Span(value, style={"color": color, "fontFamily": "monospace", "fontWeight": "bold"}),
            ], style={"marginRight": "16px", "whiteSpace": "nowrap"}))
        children.append(html.Div(metric_spans, style={"display": "flex", "flexWrap": "wrap"}))
    if legend:
        legend_items = []
        for lcolor, llabel, ldesc in legend:
            legend_items.append(html.Div([
                html.Span("● ", style={"color": lcolor, "fontSize": "13px"}),
                html.Span(f"{llabel} — {ldesc}", style={"color": WHITE, "fontSize": "11px"}),
            ], style={"marginBottom": "2px"}))
        children.append(html.Div(legend_items, style={"marginTop": "4px"}))
    if look_for:
        children.append(html.P(f"→ Look for: {look_for}", style={"color": "#888888", "fontSize": "11px",
                                                                     "margin": "4px 0 0 0"}))
    if simple:
        children.append(dbc.Accordion([
            dbc.AccordionItem(
                html.P(simple, style={"color": "#cccccc", "fontSize": "11px", "margin": "0"}),
                title="how to read this chart",
            ),
        ], start_collapsed=True, flush=True, className="chart-explainer"))
    return dbc.Card(
        dbc.CardBody(children, style={"padding": "10px 14px"}),
        style={"borderLeft": f"3px solid {CYAN}"},
        className="mb-2",
    )


def make_chart(fig, height=360, legend_h=True):
    """Apply consistent styling to a Plotly figure."""
    layout = {**CHART_LAYOUT, "height": height}
    if legend_h:
        layout["legend"] = dict(orientation="h", y=1.12, x=0.5, xanchor="center")
    fig.update_layout(**layout)
    return fig


def severity_color(sev):
    return GREEN if sev == "good" else RED if sev == "bad" else ORANGE if sev == "warning" else BLUE
