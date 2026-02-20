"""
Theme constants — colors, chart layout, Bootstrap overrides.
Import from here instead of hardcoding colors anywhere.
"""

# ── Color Palette ────────────────────────────────────────────────────────────
BG = "#0a0a14"
CARD = "#141828"
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

# ── Category Colors (for inventory) ──────────────────────────────────────────
CATEGORY_COLORS = {
    "Filament": BLUE,
    "Lighting": ORANGE,
    "Crafts": PURPLE,
    "Packaging": TEAL,
    "Hardware": GRAY,
    "Tools": PINK,
    "Printer Parts": CYAN,
    "Jewelry": "#e91e8f",
    "Personal/Gift": RED,
    "Business Fees": DARKGRAY,
    "Other": "#555555",
}

CATEGORY_OPTIONS = [
    "Filament", "Lighting", "Crafts", "Packaging", "Hardware",
    "Tools", "Printer Parts", "Jewelry", "Personal/Gift", "Business Fees", "Other",
]

# ── Plotly Chart Layout ──────────────────────────────────────────────────────
CHART_LAYOUT = dict(
    template="plotly_dark",
    paper_bgcolor="rgba(0,0,0,0)",
    plot_bgcolor="rgba(0,0,0,0)",
    font={"color": WHITE},
    margin=dict(t=50, b=30, l=60, r=20),
)

# ── Sidebar Dimensions ───────────────────────────────────────────────────────
SIDEBAR_WIDTH = "250px"
CONTENT_MARGIN = "266px"  # sidebar + gap

# ── Bootstrap class helpers ──────────────────────────────────────────────────
# These map to the DARKLY theme + our custom.css overrides
CARD_CLASS = "shadow-sm"

# ── Tax-deductible bank categories (Schedule C) ─────────────────────────────
BANK_TAX_DEDUCTIBLE = {
    "Amazon Inventory", "Shipping", "Craft Supplies", "Etsy Fees",
    "Subscriptions", "AliExpress Supplies", "Business Credit Card",
}
