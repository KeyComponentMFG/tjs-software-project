"""
Theme constants, styling helpers, and stateless UI builder functions.
Extracted from etsy_dashboard.py for reuse across the dashboard.
"""

import plotly.graph_objects as go
import dash_bootstrap_components as dbc
from dash import html

__all__ = [
    # Colors
    "BG", "CARD", "CARD2", "GREEN", "RED", "BLUE", "ORANGE", "PURPLE",
    "TEAL", "PINK", "WHITE", "GRAY", "DARKGRAY", "CYAN",
    # Layout
    "TAB_PADDING", "CHART_LAYOUT", "tab_style", "tab_selected_style",
    # Store constants
    "STORES", "STORE_COLORS",
    # Formatting helpers
    "_fmt", "_safe", "money", "severity_color",
    # Chart helpers
    "make_chart", "_no_data_fig",
    # UI component helpers
    "_verification_badge", "_provenance_icon", "kpi_card", "section", "row_item",
    # Provenance hook
    "set_provenance_hook",
]

# ── Color Constants ──────────────────────────────────────────────────────────

BG = "#0f0f1a"
CARD = "#16213e"
CARD2 = "#1a1a2e"
GREEN = "#2ecc71"
RED = "#e74c3c"
BLUE = "#3498db"
ORANGE = "#f39c12"
PURPLE = "#9b59b6"
TEAL = "#1abc9c"
PINK = "#e91e8f"
WHITE = "#ffffff"
GRAY = "#aaaaaa"
DARKGRAY = "#666666"
CYAN = "#00d4ff"

TAB_PADDING = "14px 16px"

CHART_LAYOUT = dict(
    template="plotly_dark",
    paper_bgcolor="rgba(0,0,0,0)",
    plot_bgcolor="rgba(0,0,0,0)",
    font={"color": WHITE},
    margin=dict(t=50, b=30, l=60, r=20),
)

# ── Multi-Store Support ──────────────────────────────────────────────────────

STORES = {
    "all": "All Stores",
    "keycomponentmfg": "KeyComponentMFG",
    "aurvio": "Aurvio",
    "lunalinks": "Luna&Links",
}
STORE_COLORS = {
    "keycomponentmfg": "#00d4ff",   # CYAN
    "aurvio": "#9b59b6",            # Purple
    "lunalinks": "#e91e63",         # Pink
}

# ── Tab Styling ──────────────────────────────────────────────────────────────

tab_style = {
    "backgroundColor": CARD2, "color": GRAY, "border": "none",
    "padding": "10px 20px", "fontSize": "14px", "fontWeight": "600",
}
tab_selected_style = {
    **tab_style, "backgroundColor": CARD, "color": CYAN,
    "borderBottom": f"3px solid {CYAN}",
}

# ── Provenance hook ─────────────────────────────────────────────────────────
# Set from etsy_dashboard.py after accounting pipeline loads.
# Must be a callable(metric_name) -> dict or None.
_get_metric_provenance = None


def set_provenance_hook(fn):
    """Set the provenance lookup function (called from main module)."""
    global _get_metric_provenance
    _get_metric_provenance = fn


# ── Formatting Helpers ───────────────────────────────────────────────────────

def _fmt(val, prefix="$", fmt=",.2f", unknown="UNKNOWN"):
    """Format a number for display, or show UNKNOWN if None."""
    if val is None:
        return unknown
    return f"{prefix}{val:{fmt}}"


def _safe(val, default=0.0):
    """Return val if not None, else default. Use in arithmetic to avoid TypeError on None globals."""
    return val if val is not None else default


def money(val, sign=True):
    if val is None:
        return "UNKNOWN"
    if val < 0:
        return f"-${abs(val):,.2f}"
    return f"${val:,.2f}"


def severity_color(sev):
    return GREEN if sev == "good" else RED if sev == "bad" else ORANGE if sev == "warning" else BLUE


# ── Chart Helpers ────────────────────────────────────────────────────────────

def make_chart(fig, height=360, legend_h=True):
    layout = {**CHART_LAYOUT, "height": height}
    if legend_h:
        layout["legend"] = dict(orientation="h", y=1.12, x=0.5, xanchor="center")
    fig.update_layout(**layout)
    return fig


def _no_data_fig(title="No Data Available", message="Upload data to populate this chart.", height=300):
    """Styled placeholder figure shown when chart has no data."""
    fig = go.Figure()
    fig.add_annotation(
        text=f"<b>{title}</b><br><span style='font-size:12px;color:#888'>{message}</span>",
        xref="paper", yref="paper", x=0.5, y=0.5,
        showarrow=False, font=dict(size=16, color="#666"),
        align="center",
    )
    fig.update_layout(
        **CHART_LAYOUT, height=height,
        xaxis=dict(visible=False), yaxis=dict(visible=False),
    )
    return fig


# ── UI Component Helpers ─────────────────────────────────────────────────────

def _verification_badge(status):
    """Return a small colored badge: VERIFIED (green), EST. (orange), N/A (red)."""
    badge_map = {
        "verified": (GREEN, "VERIFIED"),
        "estimated": (ORANGE, "EST."),
        "na": (RED, "N/A"),
    }
    clr, label = badge_map.get(status, (GRAY, ""))
    if not label:
        return html.Span()
    return html.Span(label, style={
        "color": clr, "fontSize": "8px", "fontWeight": "bold",
        "border": f"1px solid {clr}88", "borderRadius": "3px",
        "padding": "1px 4px", "marginLeft": "4px", "verticalAlign": "middle",
        "letterSpacing": "0.5px",
    })


def _provenance_icon(metric_name):
    """Build an info icon with popover showing metric provenance."""
    if not _get_metric_provenance or not metric_name:
        return html.Span()
    prov = _get_metric_provenance(metric_name)
    if not prov:
        return html.Span()

    conf = prov["confidence"].upper()
    conf_color = prov["confidence_color"]

    # Build tooltip text
    lines = [f"[{conf}]"]
    if prov["formula"]:
        lines.append(f"Formula: {prov['formula']}")
    if prov["source_entries"]:
        lines.append(f"Source entries: {prov['source_entries']}")
    if prov["source_types"]:
        lines.append(f"Types: {', '.join(prov['source_types'])}")
    if prov["notes"]:
        lines.append(f"Note: {prov['notes']}")
    if prov["missing_inputs"]:
        lines.append(f"Missing: {', '.join(prov['missing_inputs'])}")

    _id = f"prov-{metric_name}"
    return html.Span([
        html.Span("\u24d8", id=_id, style={
            "color": conf_color, "fontSize": "11px", "cursor": "pointer",
            "marginLeft": "5px", "opacity": "0.7", "verticalAlign": "middle",
        }),
        dbc.Tooltip(
            html.Div([
                html.Div([
                    html.Span(conf, style={"color": conf_color, "fontWeight": "bold", "fontSize": "11px"}),
                ], style={"marginBottom": "4px"}),
                *[html.Div(line, style={"fontSize": "11px", "color": "#ccc", "lineHeight": "1.4"})
                  for line in lines[1:]],
            ], style={"textAlign": "left", "padding": "4px"}),
            target=_id, placement="left", style={"maxWidth": "350px"},
        ),
    ])


def kpi_card(title, value, color, subtitle="", detail="", status=None, metric_name=None):
    """KPI card with optional verification badge. status: 'verified', 'estimated', 'na'"""
    # Auto-detect status from provenance if not provided
    if not status and metric_name and _get_metric_provenance:
        prov = _get_metric_provenance(metric_name)
        if prov:
            _conf = prov["confidence"]
            if _conf in ("verified", "derived"):
                status = "verified"
            elif _conf in ("estimated", "projection", "heuristic"):
                status = "estimated"
            elif _conf in ("unknown", "quarantined"):
                status = "na"
    title_children = [html.Span(title)]
    if status:
        title_children.append(_verification_badge(status))
    children = [
        html.P(title_children, style={"color": GRAY, "margin": "0", "fontSize": "12px", "fontWeight": "600", "letterSpacing": "0.5px"}),
        html.H2(value, style={"color": color, "margin": "4px 0", "fontSize": "26px"}),
        html.P(subtitle, style={"color": DARKGRAY, "margin": "0", "fontSize": "11px"}),
    ]
    if detail:
        children.append(html.Details([
            html.Summary("details", style={
                "color": f"{CYAN}88", "fontSize": "10px", "cursor": "pointer",
                "marginTop": "6px", "listStyle": "none", "textAlign": "center",
                "userSelect": "none",
            }),
            html.P(detail, style={
                "color": GRAY, "fontSize": "11px", "margin": "6px 0 0 0",
                "textAlign": "left", "lineHeight": "1.4", "padding": "6px",
                "backgroundColor": f"{CARD}dd", "borderRadius": "4px",
                "borderTop": f"1px solid {color}33",
            }),
        ]))
    return html.Div(children, style={
        "backgroundColor": CARD2, "padding": "14px 12px", "borderRadius": "8px",
        "textAlign": "center", "border": f"1px solid {color}33", "flex": "1", "minWidth": "130px",
    })


def section(title, children, color=ORANGE):
    return html.Div([
        html.H3(title, style={"color": color, "borderBottom": f"2px solid {color}", "paddingBottom": "6px", "marginTop": "0", "fontSize": "16px"}),
        *children,
    ], style={"backgroundColor": CARD, "padding": "16px", "borderRadius": "10px", "marginBottom": "14px"})


def row_item(label, amount, indent=0, bold=False, color=WHITE, neg_color=RED, metric_name=None):
    if amount is None:
        # Show UNKNOWN for metrics that can't be computed
        style = {"display": "flex", "justifyContent": "space-between",
                 "padding": "4px 0", "borderBottom": "1px solid #ffffff10",
                 "marginLeft": f"{indent * 24}px"}
        if bold:
            style["fontWeight"] = "bold"
            style["borderBottom"] = "2px solid #ffffff30"
            style["padding"] = "8px 0"
        prov_icon = _provenance_icon(metric_name) if metric_name else html.Span()
        return html.Div([
            html.Span([label, prov_icon], style={"color": color, "fontSize": "13px"}),
            html.Span("UNKNOWN", style={"color": ORANGE, "fontFamily": "monospace", "fontSize": "13px"}),
        ], style=style)
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
    prov_icon = _provenance_icon(metric_name) if metric_name else html.Span()
    return html.Div([
        html.Span([label, prov_icon], style={"color": color if not bold else display_color, "fontSize": "13px"}),
        html.Span(money(amount), style={"color": display_color, "fontFamily": "monospace", "fontSize": "13px"}),
    ], style=style)
