"""
Tests for dashboard_utils.theme — formatting helpers, chart builders, color constants.

All tests are fast and have no external dependencies.
"""

import re
import pytest
import plotly.graph_objects as go

from dashboard_utils.theme import (
    money, _fmt, _safe, severity_color,
    make_chart, _no_data_fig,
    BG, CARD, CARD2, GREEN, RED, BLUE, ORANGE, PURPLE,
    TEAL, PINK, WHITE, GRAY, DARKGRAY, CYAN,
    STORES, STORE_COLORS, CHART_LAYOUT,
    kpi_card, section, row_item,
)


# ── money() ──

class TestMoney:
    def test_positive(self):
        assert money(1234.56) == "$1,234.56"

    def test_negative(self):
        assert money(-42.10) == "-$42.10"

    def test_zero(self):
        assert money(0) == "$0.00"

    def test_none(self):
        assert money(None) == "UNKNOWN"

    def test_large_value(self):
        assert money(1_000_000) == "$1,000,000.00"

    def test_small_cents(self):
        assert money(0.01) == "$0.01"


# ── _fmt() ──

class TestFmt:
    def test_default_dollar(self):
        assert _fmt(42.5) == "$42.50"

    def test_none_returns_unknown(self):
        assert _fmt(None) == "UNKNOWN"

    def test_custom_prefix(self):
        assert _fmt(99.9, prefix="") == "99.90"

    def test_custom_format(self):
        assert _fmt(0.5, prefix="", fmt=".0%") == "50%"

    def test_zero(self):
        assert _fmt(0) == "$0.00"

    def test_negative(self):
        assert _fmt(-10) == "$-10.00"


# ── _safe() ──

class TestSafe:
    def test_value_passes_through(self):
        assert _safe(42.5) == 42.5

    def test_none_returns_default(self):
        assert _safe(None) == 0.0

    def test_custom_default(self):
        assert _safe(None, default=-1) == -1

    def test_zero_is_not_none(self):
        assert _safe(0) == 0


# ── severity_color() ──

class TestSeverityColor:
    def test_good(self):
        assert severity_color("good") == GREEN

    def test_bad(self):
        assert severity_color("bad") == RED

    def test_warning(self):
        assert severity_color("warning") == ORANGE

    def test_unknown(self):
        assert severity_color("other") == BLUE


# ── Chart helpers ──

class TestChartHelpers:
    def test_make_chart_returns_figure(self):
        fig = go.Figure()
        result = make_chart(fig)
        assert isinstance(result, go.Figure)

    def test_make_chart_sets_height(self):
        fig = go.Figure()
        result = make_chart(fig, height=500)
        assert result.layout.height == 500

    def test_no_data_fig_returns_figure(self):
        fig = _no_data_fig()
        assert isinstance(fig, go.Figure)

    def test_no_data_fig_custom_title(self):
        fig = _no_data_fig(title="Custom Title")
        assert isinstance(fig, go.Figure)
        # The annotation should contain the title
        assert len(fig.layout.annotations) > 0
        assert "Custom Title" in fig.layout.annotations[0].text


# ── Color constants ──

_HEX_PATTERN = re.compile(r"^#[0-9a-fA-F]{6}$")

class TestColorConstants:
    @pytest.mark.parametrize("color_name,color_val", [
        ("BG", BG), ("CARD", CARD), ("CARD2", CARD2),
        ("GREEN", GREEN), ("RED", RED), ("BLUE", BLUE),
        ("ORANGE", ORANGE), ("PURPLE", PURPLE), ("TEAL", TEAL),
        ("PINK", PINK), ("WHITE", WHITE), ("GRAY", GRAY),
        ("DARKGRAY", DARKGRAY), ("CYAN", CYAN),
    ])
    def test_valid_hex(self, color_name, color_val):
        assert _HEX_PATTERN.match(color_val), f"{color_name}={color_val!r} is not valid hex"


# ── STORES dict ──

class TestStores:
    def test_has_all(self):
        assert "all" in STORES

    def test_has_keycomp(self):
        assert "keycomponentmfg" in STORES

    def test_has_aurvio(self):
        assert "aurvio" in STORES

    def test_has_lunalinks(self):
        assert "lunalinks" in STORES

    def test_store_colors_match_stores(self):
        # Every store color key should be a valid store (except "all")
        for key in STORE_COLORS:
            assert key in STORES, f"STORE_COLORS has key '{key}' not in STORES"


# ── CHART_LAYOUT ──

class TestChartLayout:
    def test_is_dict(self):
        assert isinstance(CHART_LAYOUT, dict)

    def test_has_template(self):
        assert CHART_LAYOUT["template"] == "plotly_dark"

    def test_has_font_color(self):
        assert CHART_LAYOUT["font"]["color"] == WHITE


# ── UI component helpers ──

class TestUIHelpers:
    def test_kpi_card_returns_div(self):
        from dash import html
        card = kpi_card("Revenue", "$100", GREEN)
        assert isinstance(card, html.Div)

    def test_section_returns_div(self):
        from dash import html
        s = section("Test Section", [html.P("content")])
        assert isinstance(s, html.Div)

    def test_row_item_with_value(self):
        from dash import html
        row = row_item("Sales", 100.0)
        assert isinstance(row, html.Div)

    def test_row_item_with_none(self):
        from dash import html
        row = row_item("Shipping Profit", None)
        assert isinstance(row, html.Div)
