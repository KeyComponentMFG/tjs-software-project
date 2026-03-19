"""Item thumbnail component."""
from dash import html
from etsy_dashboard.theme import DARKGRAY


def item_thumbnail(name, image_url="", size=40):
    """Return a thumbnail img element or gray placeholder."""
    if image_url:
        return html.Img(
            src=image_url, referrerPolicy="no-referrer",
            style={"width": f"{size}px", "height": f"{size}px", "objectFit": "cover",
                   "borderRadius": "4px", "verticalAlign": "middle"})
    return html.Div(
        "?", style={
            "width": f"{size}px", "height": f"{size}px", "display": "inline-flex",
            "alignItems": "center", "justifyContent": "center",
            "backgroundColor": "#ffffff10", "borderRadius": "4px",
            "color": DARKGRAY, "fontSize": f"{size // 3}px", "fontWeight": "bold",
            "verticalAlign": "middle"})
