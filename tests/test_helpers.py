"""
Tests for dashboard_utils.helpers — parse_money, product name normalization.

All tests are fast and have no external dependencies.
"""

import pytest
import pandas as pd

from dashboard_utils.helpers import (
    parse_money,
    _normalize_product_name,
    _merge_product_prefixes,
)


# ── parse_money() ──

class TestParseMoney:
    def test_dollar_string(self):
        assert parse_money("$1,234.56") == 1234.56

    def test_simple_number(self):
        assert parse_money("42.00") == 42.0

    def test_dash_returns_zero(self):
        assert parse_money("--") == 0.0

    def test_empty_string_returns_zero(self):
        assert parse_money("") == 0.0

    def test_none_returns_zero(self):
        assert parse_money(None) == 0.0

    def test_zero_string(self):
        assert parse_money("0.00") == 0.0

    def test_negative(self):
        assert parse_money("-$5.00") == -5.0

    def test_negative_without_dollar(self):
        assert parse_money("-5.00") == -5.0

    def test_float_passthrough(self):
        assert parse_money(42.5) == 42.5

    def test_quoted_dollar(self):
        """Values sometimes have stray quotes from CSV parsing."""
        assert parse_money('"$10.00"') == 10.0

    def test_unparseable_with_warnings(self):
        warnings = []
        result = parse_money("not_a_number", warnings=warnings)
        assert result == 0.0
        assert len(warnings) == 1

    def test_unparseable_without_warnings(self):
        result = parse_money("not_a_number")
        assert result == 0.0

    def test_nan_returns_zero(self):
        assert parse_money(float("nan")) == 0.0


# ── _normalize_product_name() ──

class TestNormalizeProductName:
    def test_strips_trailing_dots(self):
        assert _normalize_product_name("Ski Gondola Table Lamp...") == "Ski Gondola Table Lamp"

    def test_strips_after_pipe(self):
        name = "Vintage Ski Table Lamp | Rustic Decor"
        assert _normalize_product_name(name) == "Vintage Ski Table Lamp"

    def test_alias_mapping(self):
        aliases = {"Ski Lamp": ["Ski Gondola Table Lamp", "Ski Lamp Rustic"]}
        assert _normalize_product_name("Ski Gondola Table Lamp", aliases=aliases) == "Ski Lamp"

    def test_canonical_returns_self(self):
        aliases = {"Ski Lamp": ["Variant A"]}
        assert _normalize_product_name("Ski Lamp", aliases=aliases) == "Ski Lamp"

    def test_non_string_returns_as_is(self):
        assert _normalize_product_name(42) == 42
        assert _normalize_product_name(None) is None

    def test_no_alias_no_pipe(self):
        assert _normalize_product_name("Simple Name") == "Simple Name"


# ── _merge_product_prefixes() ──

class TestMergeProductPrefixes:
    def test_merges_prefix_match(self):
        series = pd.Series([
            "Ski Gondola Table Lamp",
            "Ski Gondola Table Lamp Rustic Cabin Decor",
        ])
        result = _merge_product_prefixes(series)
        # The longer name should merge to the shorter canonical name
        assert result.iloc[1] == "Ski Gondola Table Lamp"

    def test_no_merge_for_short_names(self):
        """Names shorter than 10 chars should not be used as merge targets."""
        series = pd.Series(["Short", "Short Extra"])
        result = _merge_product_prefixes(series)
        # "Short" is < 10 chars, so no merge should happen
        assert result.iloc[1] == "Short Extra"

    def test_with_aliases(self):
        aliases = {"Lamp": ["Lamp Deluxe", "Lamp Standard"]}
        series = pd.Series(["Lamp Deluxe", "Lamp Standard", "Other Item"])
        result = _merge_product_prefixes(series, aliases=aliases)
        assert result.iloc[0] == "Lamp"
        assert result.iloc[1] == "Lamp"
        assert result.iloc[2] == "Other Item"

    def test_empty_series(self):
        series = pd.Series(dtype=str)
        result = _merge_product_prefixes(series)
        assert len(result) == 0
