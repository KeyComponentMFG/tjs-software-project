"""Page routing callback — renders the correct page based on URL."""
from dash import html, Input, Output


def register_callbacks(app):
    @app.callback(
        Output("page-content", "children"),
        Input("url", "pathname"),
    )
    def route_page(pathname):
        if pathname == "/" or pathname is None:
            from etsy_dashboard.pages.overview import layout
            return layout()
        elif pathname == "/deep-dive":
            from etsy_dashboard.pages.deep_dive import layout
            return layout()
        elif pathname == "/financials":
            from etsy_dashboard.pages.financials import layout
            return layout()
        elif pathname == "/inventory":
            from etsy_dashboard.pages.inventory import layout
            return layout()
        elif pathname == "/locations":
            from etsy_dashboard.pages.locations import layout
            return layout()
        elif pathname == "/tax-forms":
            from etsy_dashboard.pages.tax_forms import layout
            return layout()
        elif pathname == "/valuation":
            from etsy_dashboard.pages.valuation import layout
            return layout()
        elif pathname == "/data-hub":
            from etsy_dashboard.pages.data_hub import layout
            return layout()
        else:
            return html.Div([
                html.H3("404 — Page Not Found", style={"color": "#e74c3c"}),
                html.P(f"No page at '{pathname}'"),
            ], style={"padding": "40px"})
