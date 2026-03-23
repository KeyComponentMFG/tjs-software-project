"""Agreement tab — renders the Operating Agreement markdown."""

import os
from dash import html, dcc
from dashboard_utils.theme import *

# Compute BASE_DIR relative to the project root (etsy_dashboard.py lives there)
_PROJECT_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))


def build_tab_agreement():
    """Build the Operating Agreement tab — renders the markdown agreement as a clean, professional page."""

    # Load the agreement markdown
    _agreement_path = os.path.join(_PROJECT_DIR, "OPERATING_AGREEMENT_DRAFT.md")
    _agreement_text = ""
    try:
        with open(_agreement_path, "r", encoding="utf-8") as f:
            _agreement_text = f.read()
    except Exception:
        return html.Div([
            html.H2("Operating Agreement", style={"color": CYAN, "textAlign": "center", "marginTop": "40px"}),
            html.P("Agreement file not found. Upload OPERATING_AGREEMENT_DRAFT.md to the project directory.",
                   style={"color": GRAY, "textAlign": "center", "fontSize": "14px"}),
        ])

    # Split into main agreement and appendix
    _parts = _agreement_text.split("# APPENDIX A")
    _main_text = _parts[0]
    _appendix_text = "# APPENDIX A" + _parts[1] if len(_parts) > 1 else ""

    return html.Div([
        # Header
        html.Div([
            html.Div([
                html.H1("KEY COMPONENT MANUFACTURING LLC", style={
                    "color": CYAN, "margin": "0", "fontSize": "24px",
                    "letterSpacing": "3px", "textAlign": "center",
                }),
                html.H2("Operating Agreement", style={
                    "color": WHITE, "margin": "8px 0 4px 0", "fontSize": "18px",
                    "fontWeight": "normal", "textAlign": "center",
                }),
                html.P("This document defines the rights, duties, and obligations of all Members.",
                       style={"color": GRAY, "textAlign": "center", "fontSize": "13px", "margin": "8px 0 0 0"}),
            ], style={
                "backgroundColor": f"{CYAN}08", "border": f"1px solid {CYAN}22",
                "borderRadius": "12px", "padding": "30px 40px",
                "marginBottom": "24px",
            }),
        ]),

        # Table of Contents
        html.Div([
            html.H3("TABLE OF CONTENTS", style={
                "color": CYAN, "fontSize": "14px", "letterSpacing": "1.5px", "marginBottom": "12px",
            }),
            html.Div([
                html.Div([
                    html.Span(f"Article {i}", style={"color": CYAN, "fontFamily": "monospace", "marginRight": "12px", "fontSize": "12px"}),
                    html.Span(title, style={"color": WHITE, "fontSize": "13px"}),
                ], style={"padding": "6px 0", "borderBottom": "1px solid #ffffff06", "cursor": "pointer"},
                   className="toc-item", **{"data-article": article_key})
                for i, title, article_key in [
                    ("I", "Definitions", "ARTICLE I"),
                    ("II", "Members", "ARTICLE II"),
                    ("III", "Purpose", "ARTICLE III"),
                    ("IV", "Capital Contributions & Equipment", "ARTICLE IV"),
                    ("V", "Equal Contribution Requirement", "ARTICLE V"),
                    ("VI", "Communication Requirements", "ARTICLE VI"),
                    ("VII", "Strike System & Quality Accountability", "ARTICLE VII"),
                    ("VIII", "Management", "ARTICLE VIII"),
                    ("IX", "Profits, Losses & Distributions", "ARTICLE IX"),
                    ("X", "Intellectual Property & Software", "ARTICLE X"),
                    ("XI", "Failure to Contribute / Remedies", "ARTICLE XI"),
                    ("XII", "Operations", "ARTICLE XII"),
                    ("XIII", "Social Media & Marketing", "ARTICLE XIII"),
                    ("XIV", "Dispute Resolution", "ARTICLE XIV"),
                    ("XV", "Non-Compete & Confidentiality", "ARTICLE XV"),
                    ("XVI", "Legal Liability & Indemnification", "ARTICLE XVI"),
                    ("XVII", "Books, Records & Transparency", "ARTICLE XVII"),
                    ("XVIII", "Transfer of Membership Interest", "ARTICLE XVIII"),
                    ("XIX", "Dissolution", "ARTICLE XIX"),
                    ("XX", "General Provisions", "ARTICLE XX"),
                ]
            ]),
        ], style={
            "backgroundColor": CARD, "borderRadius": "12px", "padding": "20px 24px",
            "marginBottom": "24px", "border": f"1px solid {DARKGRAY}33",
        }),

        # Main Agreement Body
        html.Div([
            dcc.Markdown(
                _main_text,
                style={
                    "color": WHITE, "fontSize": "14px", "lineHeight": "1.8",
                    "fontFamily": "'Georgia', 'Times New Roman', serif",
                },
                className="agreement-content",
            ),
        ], style={
            "backgroundColor": f"{CARD}cc", "borderRadius": "12px", "padding": "30px 36px",
            "marginBottom": "24px", "border": f"1px solid {DARKGRAY}33",
            "boxShadow": "0 4px 20px rgba(0,0,0,0.3)",
        }),

        # Appendix (collapsible)
        html.Details([
            html.Summary("APPENDIX A — Drafting Questions & Agreed Answers", style={
                "color": ORANGE, "fontSize": "16px", "fontWeight": "bold",
                "cursor": "pointer", "padding": "12px 0", "letterSpacing": "1px",
            }),
            html.Div([
                dcc.Markdown(
                    _appendix_text,
                    style={
                        "color": WHITE, "fontSize": "13px", "lineHeight": "1.7",
                        "fontFamily": "'Georgia', 'Times New Roman', serif",
                    },
                ),
            ], style={
                "backgroundColor": f"{CARD}cc", "borderRadius": "12px", "padding": "24px 30px",
                "marginTop": "12px", "border": f"1px solid {DARKGRAY}33",
            }),
        ], style={"marginBottom": "24px"}) if _appendix_text else html.Div(),

        # Signature reminder
        html.Div([
            html.Div([
                html.Span("\u270D\uFE0F", style={"fontSize": "24px", "marginRight": "12px"}),
                html.Span("SIGNATURES REQUIRED", style={
                    "color": ORANGE, "fontSize": "16px", "fontWeight": "bold", "letterSpacing": "1px",
                }),
            ], style={"marginBottom": "12px"}),
            html.P("Both Members must sign this Agreement for it to take effect. "
                   "Review all articles carefully. Once signed, this document supersedes all prior agreements.",
                   style={"color": GRAY, "fontSize": "13px", "lineHeight": "1.6"}),
            html.Div([
                html.Div([
                    html.Div("Member 1 — Managing Member", style={"color": CYAN, "fontSize": "12px", "fontWeight": "bold", "marginBottom": "8px"}),
                    html.Div("Thomas Joseph McNulty", style={"color": WHITE, "fontSize": "14px"}),
                    html.Div("Signature: ________________________________", style={"color": GRAY, "fontSize": "13px", "marginTop": "16px"}),
                    html.Div("Date: _______________", style={"color": GRAY, "fontSize": "13px", "marginTop": "8px"}),
                ], style={
                    "flex": "1", "padding": "20px", "backgroundColor": f"{CYAN}08",
                    "borderRadius": "8px", "border": f"1px solid {CYAN}22",
                }),
                html.Div([
                    html.Div("Member 2", style={"color": ORANGE, "fontSize": "12px", "fontWeight": "bold", "marginBottom": "8px"}),
                    html.Div("Braden Michael Walker", style={"color": WHITE, "fontSize": "14px"}),
                    html.Div("Signature: ________________________________", style={"color": GRAY, "fontSize": "13px", "marginTop": "16px"}),
                    html.Div("Date: _______________", style={"color": GRAY, "fontSize": "13px", "marginTop": "8px"}),
                ], style={
                    "flex": "1", "padding": "20px", "backgroundColor": f"{ORANGE}08",
                    "borderRadius": "8px", "border": f"1px solid {ORANGE}22",
                }),
            ], style={"display": "flex", "gap": "16px", "marginTop": "16px"}),
        ], style={
            "backgroundColor": CARD, "borderRadius": "12px", "padding": "24px",
            "border": f"1px solid {ORANGE}33", "marginBottom": "24px",
        }),

    ], style={"padding": TAB_PADDING, "maxWidth": "900px", "margin": "0 auto"})
