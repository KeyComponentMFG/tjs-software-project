"""
Tests for dashboard_utils.upload_validator — CSV validation, store detection.

All tests are fast and use synthetic data (no Supabase needed).
"""

import pytest
import pandas as pd

from dashboard_utils.upload_validator import (
    validate_etsy_csv,
    detect_store_from_csv,
    UploadPreview,
    REQUIRED_COLUMNS,
)


def _make_csv_bytes(rows=None, columns=None):
    """Build valid Etsy CSV bytes from a list of row dicts."""
    if columns is None:
        columns = list(REQUIRED_COLUMNS)
    if rows is None:
        # Minimal valid CSV: one sale row
        rows = [
            {
                "Date": "January 15, 2025",
                "Type": "Sale",
                "Title": "Payment for Order #1234567890",
                "Info": "Order #1234567890",
                "Currency": "USD",
                "Amount": "$25.00",
                "Fees & Taxes": "--",
                "Net": "$25.00",
            },
            {
                "Date": "January 15, 2025",
                "Type": "Fee",
                "Title": "Transaction fee: Test Product Name",
                "Info": "Order #1234567890",
                "Currency": "USD",
                "Amount": "--",
                "Fees & Taxes": "$1.63",
                "Net": "$-1.63",
            },
        ]
    df = pd.DataFrame(rows, columns=columns)
    return df.to_csv(index=False).encode("utf-8")


def _dummy_config_loader(key):
    """Stub config loader that returns nothing."""
    return None


class TestValidateEtsyCsv:
    def test_valid_csv(self):
        raw = _make_csv_bytes()
        preview = validate_etsy_csv(raw, "keycomponentmfg", _dummy_config_loader)
        assert preview.is_valid is True
        assert preview.row_count == 2
        assert len(preview.errors) == 0

    def test_date_range_extracted(self):
        raw = _make_csv_bytes()
        preview = validate_etsy_csv(raw, "keycomponentmfg", _dummy_config_loader)
        assert preview.date_range[0] != ""
        assert preview.month_count >= 1

    def test_transaction_types_counted(self):
        raw = _make_csv_bytes()
        preview = validate_etsy_csv(raw, "keycomponentmfg", _dummy_config_loader)
        assert "Sale" in preview.transaction_count_by_type
        assert "Fee" in preview.transaction_count_by_type

    def test_missing_columns_fails(self):
        # CSV with only Date and Type columns
        bad_rows = [{"Date": "January 1, 2025", "Type": "Sale"}]
        raw = pd.DataFrame(bad_rows).to_csv(index=False).encode("utf-8")
        preview = validate_etsy_csv(raw, "keycomponentmfg", _dummy_config_loader)
        assert preview.is_valid is False
        assert any("Missing required columns" in e for e in preview.errors)

    def test_non_utf8_fails(self):
        # Invalid UTF-8 bytes
        raw = b"\x80\x81\x82\x83\xff\xfe"
        preview = validate_etsy_csv(raw, "keycomponentmfg", _dummy_config_loader)
        assert preview.is_valid is False
        assert any("UTF-8" in e for e in preview.errors)

    def test_empty_csv_fails(self):
        # CSV with headers but no data rows
        header_only = ",".join(sorted(REQUIRED_COLUMNS)) + "\n"
        raw = header_only.encode("utf-8")
        preview = validate_etsy_csv(raw, "keycomponentmfg", _dummy_config_loader)
        assert preview.is_valid is False
        assert any("empty" in e.lower() for e in preview.errors)

    def test_non_usd_currency_warns(self):
        rows = [
            {
                "Date": "January 15, 2025",
                "Type": "Sale",
                "Title": "Payment for Order #123",
                "Info": "Order #123",
                "Currency": "GBP",
                "Amount": "25.00",
                "Fees & Taxes": "--",
                "Net": "25.00",
            },
        ]
        raw = _make_csv_bytes(rows)
        preview = validate_etsy_csv(raw, "keycomponentmfg", _dummy_config_loader)
        assert preview.is_valid is True
        assert any("Non-USD" in w for w in preview.warnings)

    def test_very_small_file_warns(self):
        rows = [
            {
                "Date": "January 15, 2025",
                "Type": "Sale",
                "Title": "Payment for Order #123",
                "Info": "Order #123",
                "Currency": "USD",
                "Amount": "$25.00",
                "Fees & Taxes": "--",
                "Net": "$25.00",
            },
        ]
        raw = _make_csv_bytes(rows)
        preview = validate_etsy_csv(raw, "keycomponentmfg", _dummy_config_loader)
        assert preview.is_valid is True
        assert any("Very small" in w for w in preview.warnings)

    def test_selected_store_defaults(self):
        raw = _make_csv_bytes()
        preview = validate_etsy_csv(raw, None, _dummy_config_loader)
        assert preview.selected_store == "keycomponentmfg"


class TestDetectStoreFromCsv:
    def test_no_fees_returns_none(self):
        df = pd.DataFrame({
            "Title": ["Payment for Order #123", "Listing fee"],
            "Type": ["Sale", "Fee"],
        })
        store, confidence = detect_store_from_csv(df, _dummy_config_loader)
        assert store is None
        assert confidence == 0.0

    def test_with_matching_listings(self):
        """When config_loader returns listings that match fee products, should detect store."""
        import json

        df = pd.DataFrame({
            "Title": [
                "Transaction fee: Cool Widget Gadget",
                "Transaction fee: Another Product Name",
                "Transaction fee: Shipping",  # Should be excluded
            ],
        })

        listings = [
            {"TITLE": "Cool Widget Gadget Deluxe Edition"},
            {"TITLE": "Another Product Name With Extra Words"},
        ]

        def config_loader(key):
            if key == "listings_csv_keycomponentmfg":
                return json.dumps(listings)
            return None

        store, confidence = detect_store_from_csv(df, config_loader)
        assert store == "keycomponentmfg"
        assert confidence > 0.0


class TestUploadPreview:
    def test_repr(self):
        p = UploadPreview()
        r = repr(p)
        assert "UploadPreview" in r
        assert "valid=False" in r
