"""
Smoke tests for tab builders — verify each tab returns a Dash component.

These tests require the full etsy_dashboard monolith to be importable
(which requires Supabase connectivity). Tests skip gracefully if the
module cannot be loaded.
"""

import pytest
from dash import html

# Try importing the monolith. If it fails (no Supabase, missing deps, etc.),
# mark all tests in this module as skipped.
try:
    import etsy_dashboard as ed
    _ED_AVAILABLE = True
except Exception as _err:
    _ED_AVAILABLE = False
    _ED_IMPORT_ERROR = str(_err)

pytestmark = pytest.mark.skipif(
    not _ED_AVAILABLE,
    reason=f"etsy_dashboard not importable: {_ED_IMPORT_ERROR if not _ED_AVAILABLE else ''}",
)


def _is_dash_component(obj):
    """Check if obj is a Dash component (html.Div, dcc.Graph, etc.)."""
    return hasattr(obj, "to_plotly_json")


class TestTabBuilders:
    """Smoke tests: each builder should return a Dash component without crashing."""

    def test_overview(self):
        from dashboard_utils.pages.overview import build_tab1_overview
        result = build_tab1_overview()
        assert _is_dash_component(result), f"Expected Dash component, got {type(result)}"

    def test_deep_dive(self):
        from dashboard_utils.pages.deep_dive import build_tab2_deep_dive
        result = build_tab2_deep_dive()
        assert _is_dash_component(result), f"Expected Dash component, got {type(result)}"

    def test_financials(self):
        from dashboard_utils.pages.financials import build_tab3_financials
        result = build_tab3_financials()
        assert _is_dash_component(result), f"Expected Dash component, got {type(result)}"

    def test_inventory(self):
        from dashboard_utils.pages.inventory import build_tab4_inventory
        result = build_tab4_inventory()
        assert _is_dash_component(result), f"Expected Dash component, got {type(result)}"

    def test_tax_forms(self):
        from dashboard_utils.pages.tax_forms import build_tab5_tax_forms
        result = build_tab5_tax_forms()
        assert _is_dash_component(result), f"Expected Dash component, got {type(result)}"

    def test_valuation(self):
        from dashboard_utils.pages.valuation import build_tab6_valuation
        result = build_tab6_valuation()
        assert _is_dash_component(result), f"Expected Dash component, got {type(result)}"

    def test_data_hub(self):
        from dashboard_utils.pages.data_hub import build_tab7_data_hub
        result = build_tab7_data_hub()
        assert _is_dash_component(result), f"Expected Dash component, got {type(result)}"

    def test_agreement(self):
        from dashboard_utils.pages.agreement import build_tab_agreement
        result = build_tab_agreement()
        assert _is_dash_component(result), f"Expected Dash component, got {type(result)}"


class TestTabBuildersWithStoreFilter:
    """Verify tabs still render after switching to a sparse store (aurvio)."""

    @pytest.fixture(autouse=True)
    def switch_to_aurvio(self):
        """Switch the monolith's store filter to aurvio, restore after test."""
        original_store = getattr(ed, "_current_store", "all")
        # Apply aurvio filter if the function exists
        if hasattr(ed, "_apply_store_filter"):
            try:
                ed._apply_store_filter("aurvio")
            except Exception:
                pytest.skip("Cannot switch store filter")
        else:
            pytest.skip("_apply_store_filter not available")
        yield
        # Restore original store filter
        try:
            ed._apply_store_filter(original_store)
        except Exception:
            pass

    def test_overview_aurvio(self):
        from dashboard_utils.pages.overview import build_tab1_overview
        result = build_tab1_overview()
        assert _is_dash_component(result)

    def test_financials_aurvio(self):
        from dashboard_utils.pages.financials import build_tab3_financials
        result = build_tab3_financials()
        assert _is_dash_component(result)
