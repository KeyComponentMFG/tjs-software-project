"""
Tests for dashboard_utils.state — StateManager and EtsyState.

Validates:
- build_etsy_state produces correct metrics from real Supabase data
- StateManager round-trips between store filters
- Empty DataFrames do not crash
"""

import pytest
import pandas as pd

from dashboard_utils.state import build_etsy_state, StateManager, EtsyState


class TestBuildEtsyState:
    def test_all_stores(self, real_data):
        data, config = real_data
        state = build_etsy_state(data, "all", config)
        assert state.order_count > 0
        assert state.gross_sales > 0
        assert state.net_sales == pytest.approx(
            state.gross_sales - state.total_refunds, abs=0.01
        )
        assert len(state.months_sorted) > 0

    def test_single_store(self, real_data):
        data, config = real_data
        aurvio = data[data["Store"] == "aurvio"].copy()
        state = build_etsy_state(aurvio, "aurvio", config)
        assert state.store == "aurvio"
        assert state.order_count >= 0
        # Aurvio orders should be less than or equal to all stores
        all_state = build_etsy_state(data, "all", config)
        assert state.order_count <= all_state.order_count

    def test_empty_data(self, real_data):
        """Empty DataFrame should not crash."""
        _, config = real_data
        empty = pd.DataFrame(columns=[
            "Date", "Type", "Title", "Info", "Currency",
            "Amount", "Fees & Taxes", "Net", "Store",
            "Net_Clean", "Month", "Date_Parsed",
        ])
        state = build_etsy_state(empty, "nonexistent", config)
        assert state.order_count == 0
        assert state.gross_sales == 0
        assert len(state.months_sorted) == 0

    def test_net_sales_identity(self, real_data):
        """net_sales must always equal gross_sales - total_refunds."""
        data, config = real_data
        state = build_etsy_state(data, "all", config)
        assert state.net_sales == pytest.approx(
            state.gross_sales - state.total_refunds, abs=0.01
        )

    def test_buyer_paid_shipping_is_unknown(self, real_data):
        """buyer_paid_shipping was removed (back-solve), must be None."""
        data, config = real_data
        state = build_etsy_state(data, "all", config)
        assert state.buyer_paid_shipping is None
        assert state.shipping_profit is None
        assert state.shipping_margin is None

    def test_months_sorted_order(self, real_data):
        """months_sorted must be in chronological order."""
        data, config = real_data
        state = build_etsy_state(data, "all", config)
        assert state.months_sorted == sorted(state.months_sorted)

    def test_days_active_positive(self, real_data):
        data, config = real_data
        state = build_etsy_state(data, "all", config)
        assert state.days_active >= 1

    def test_fee_breakdown_sums(self, real_data):
        """Gross fee components should roughly match total_fees_gross."""
        data, config = real_data
        state = build_etsy_state(data, "all", config)
        component_sum = (
            state.listing_fees
            + state.transaction_fees_product
            + state.transaction_fees_shipping
            + state.processing_fees
        )
        assert state.total_fees_gross == pytest.approx(component_sum, abs=0.01)


class TestStateManager:
    def test_round_trip(self, real_data):
        """all -> aurvio -> all must produce identical results."""
        data, config = real_data
        mgr = StateManager()
        mgr.initialize(data, config)

        s_all = mgr.get_state()
        original_count = s_all.order_count
        original_gross = s_all.gross_sales

        mgr.set_store_filter("aurvio")
        s_aurvio = mgr.get_state()
        # Aurvio might have 0 or more orders, but should be <= all
        assert s_aurvio.order_count <= original_count

        mgr.set_store_filter("all")
        s_back = mgr.get_state()
        assert s_back.order_count == original_count
        assert s_back.gross_sales == pytest.approx(original_gross, abs=0.01)

    def test_update_data(self, real_data):
        data, config = real_data
        mgr = StateManager()
        mgr.initialize(data, config)
        count1 = mgr.get_state().order_count
        # Update with same data should produce same counts
        mgr.update_data(data, config)
        count2 = mgr.get_state().order_count
        assert count1 == count2

    def test_get_full_data(self, real_data):
        data, config = real_data
        mgr = StateManager()
        mgr.initialize(data, config)
        full = mgr.get_full_data()
        assert len(full) == len(data)

    def test_uninitialized_state_is_none(self):
        """Before initialize(), get_state() returns None."""
        mgr = StateManager()
        assert mgr.get_state() is None
        assert mgr.get_full_data() is None
