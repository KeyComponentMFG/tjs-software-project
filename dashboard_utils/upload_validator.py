"""
Upload validation and preview for Etsy CSV statements.

Analyzes an uploaded CSV before committing it to storage/Supabase,
providing date range, row count, store detection, overlap checking,
and transaction breakdown.

Usage:
    from dashboard_utils.upload_validator import validate_etsy_csv
    preview = validate_etsy_csv(decoded_bytes, selected_store, config_loader)
"""

import io
import json
import logging
import pandas as pd
from collections import Counter

_logger = logging.getLogger("dashboard.upload_validator")

# Required columns for a valid Etsy statement CSV
REQUIRED_COLUMNS = {"Date", "Type", "Title", "Info", "Currency", "Amount", "Fees & Taxes", "Net"}

# Known stores and their display names
KNOWN_STORES = {
    "keycomponentmfg": "KeyComponentMFG",
    "aurvio": "Aurvio",
    "lunalinks": "Luna&Links",
}


class UploadPreview:
    """Result of analyzing an uploaded CSV before committing."""

    def __init__(self):
        self.is_valid = False
        self.errors = []          # Blocking errors (won't upload)
        self.warnings = []        # Non-blocking warnings (user should review)
        self.row_count = 0
        self.date_range = ("", "")  # (min_date_str, max_date_str)
        self.month_count = 0
        self.months = []
        self.detected_store = None   # Auto-detected store slug
        self.selected_store = None   # What tab user is on
        self.store_mismatch = False
        self.store_confidence = 0.0  # 0-1, how confident the detection is
        self.overlap_info = None     # Info about existing data overlap
        self.transaction_count_by_type = {}  # {"Sale": 45, "Fee": 120, ...}
        self.df = None               # The parsed DataFrame (for downstream use)
        self.filename = None         # Auto-generated or original filename

    def __repr__(self):
        return (
            f"<UploadPreview valid={self.is_valid} rows={self.row_count} "
            f"detected={self.detected_store}({self.store_confidence:.0%}) "
            f"selected={self.selected_store} mismatch={self.store_mismatch}>"
        )


def detect_store_from_csv(df, config_loader):
    """Match transaction fee product names against known listings to detect store.

    Args:
        df: DataFrame with a 'Title' column containing Etsy transaction titles.
        config_loader: callable(key) -> value, e.g. supabase get_config_value.

    Returns:
        (store_slug, confidence) where confidence is 0.0-1.0.
        Returns (None, 0.0) if no match found.
    """
    # Extract product names from "Transaction fee: <product>" rows
    fee_mask = df["Title"].str.startswith("Transaction fee:", na=False)
    fee_titles = df.loc[fee_mask, "Title"]

    if fee_titles.empty:
        _logger.debug("No 'Transaction fee:' rows found in CSV")
        return None, 0.0

    # Clean product names: strip prefix, normalize
    products = set()
    for title in fee_titles:
        product = title.replace("Transaction fee:", "").strip()
        if product and product.lower() != "shipping":
            products.add(product.lower())

    if not products:
        _logger.debug("No product names extracted from transaction fees")
        return None, 0.0

    _logger.debug("Extracted %d unique product names from transaction fees", len(products))

    # Score each store by how many fee products match its listings
    store_scores = {}
    for store_slug in KNOWN_STORES:
        try:
            raw = config_loader(f"listings_csv_{store_slug}")
            if not raw:
                continue
            records = json.loads(raw) if isinstance(raw, str) else raw
            if not records:
                continue

            # Build a set of listing titles (lowercased) for matching
            listing_titles = set()
            for r in records:
                title = r.get("TITLE", "")
                if title:
                    listing_titles.add(title.lower())

            if not listing_titles:
                continue

            # Count matches: how many fee products appear in this store's listings
            matches = 0
            for product in products:
                for listing in listing_titles:
                    # Check if product name is contained in listing title or vice versa
                    if product in listing or listing in product:
                        matches += 1
                        break

            if matches > 0:
                store_scores[store_slug] = matches
                _logger.debug("Store '%s': %d/%d products matched", store_slug, matches, len(products))

        except Exception as e:
            _logger.warning("Error loading listings for '%s': %s", store_slug, e)
            continue

    if not store_scores:
        return None, 0.0

    # Pick the store with the most matches
    best_store = max(store_scores, key=store_scores.get)
    best_matches = store_scores[best_store]
    confidence = min(best_matches / max(len(products), 1), 1.0)

    _logger.info("Detected store: '%s' with confidence %.1f%% (%d/%d products matched)",
                 best_store, confidence * 100, best_matches, len(products))

    return best_store, confidence


def _check_overlap_with_existing(df, selected_store, existing_data):
    """Check if the uploaded CSV overlaps with existing data in memory.

    Args:
        df: New DataFrame with Date column.
        selected_store: Store slug the upload targets.
        existing_data: The current DATA DataFrame.

    Returns:
        dict with overlap info or None if no overlap.
    """
    if existing_data is None or len(existing_data) == 0:
        return None

    try:
        new_dates = pd.to_datetime(df["Date"], format="%B %d, %Y", errors="coerce")
        new_min, new_max = new_dates.min(), new_dates.max()
        if pd.isna(new_min) or pd.isna(new_max):
            return None

        # Filter existing data for the same store
        if "Store" not in existing_data.columns:
            return None
        store_data = existing_data[existing_data["Store"] == selected_store]
        if len(store_data) == 0:
            return None

        ex_dates = pd.to_datetime(store_data["Date"], format="%B %d, %Y", errors="coerce")
        ex_min, ex_max = ex_dates.min(), ex_dates.max()
        if pd.isna(ex_min) or pd.isna(ex_max):
            return None

        # Check date range overlap
        if new_min <= ex_max and ex_min <= new_max:
            # Calculate overlapping months
            new_months = set(new_dates.dt.to_period("M").dropna().unique())
            ex_months = set(ex_dates.dt.to_period("M").dropna().unique())
            overlap_months = new_months & ex_months

            overlap_rows = 0
            if overlap_months:
                store_data_months = store_data.copy()
                store_data_months["_month"] = pd.to_datetime(
                    store_data_months["Date"], format="%B %d, %Y", errors="coerce"
                ).dt.to_period("M")
                overlap_rows = len(store_data_months[store_data_months["_month"].isin(overlap_months)])

            return {
                "has_overlap": True,
                "overlap_months": sorted(str(m) for m in overlap_months),
                "overlap_month_count": len(overlap_months),
                "existing_rows_affected": overlap_rows,
                "existing_range": f"{ex_min.strftime('%b %d, %Y')} - {ex_max.strftime('%b %d, %Y')}",
            }
        return None

    except Exception as e:
        _logger.warning("Overlap check error: %s", e)
        return None


def validate_etsy_csv(decoded_bytes, selected_store, config_loader, existing_data=None):
    """Analyze an uploaded CSV and return a preview without saving anything.

    Args:
        decoded_bytes: Raw bytes of the uploaded CSV file.
        selected_store: Store slug from the active tab (e.g. 'keycomponentmfg').
        config_loader: callable(key) -> value for fetching config (e.g. listings).
        existing_data: Optional DataFrame of current DATA for overlap checking.

    Returns:
        UploadPreview with all findings.
    """
    preview = UploadPreview()
    preview.selected_store = selected_store or "keycomponentmfg"

    # Step 1: Parse CSV bytes
    try:
        text = decoded_bytes.decode("utf-8")
    except UnicodeDecodeError:
        preview.errors.append("File is not valid UTF-8 text")
        return preview

    try:
        df = pd.read_csv(io.StringIO(text))
    except Exception as e:
        preview.errors.append(f"Could not parse CSV: {e}")
        return preview

    # Step 2: Check required columns
    missing = REQUIRED_COLUMNS - set(df.columns)
    if missing:
        preview.errors.append(f"Missing required columns: {', '.join(sorted(missing))}")
        return preview

    # If we got here, the CSV structure is valid
    preview.is_valid = True
    preview.df = df
    preview.row_count = len(df)

    # Step 3: Extract date range and months
    try:
        dates = pd.to_datetime(df["Date"], format="%B %d, %Y", errors="coerce")
        valid_dates = dates.dropna()
        if len(valid_dates) > 0:
            min_d, max_d = valid_dates.min(), valid_dates.max()
            preview.date_range = (min_d.strftime("%b %d, %Y"), max_d.strftime("%b %d, %Y"))
            months = sorted(valid_dates.dt.to_period("M").unique())
            preview.months = [str(m) for m in months]
            preview.month_count = len(months)
        else:
            preview.warnings.append("No valid dates found in the Date column")
    except Exception as e:
        preview.warnings.append(f"Date parsing issue: {e}")

    # Step 4: Count transactions by type
    if "Type" in df.columns:
        type_counts = df["Type"].value_counts().to_dict()
        preview.transaction_count_by_type = dict(type_counts)

    # Step 5: Auto-detect store from product names
    try:
        detected, confidence = detect_store_from_csv(df, config_loader)
        preview.detected_store = detected
        preview.store_confidence = confidence

        if detected and detected != preview.selected_store:
            preview.store_mismatch = True
            detected_name = KNOWN_STORES.get(detected, detected)
            selected_name = KNOWN_STORES.get(preview.selected_store, preview.selected_store)
            preview.warnings.append(
                f"Store mismatch: CSV products match '{detected_name}' "
                f"(confidence: {confidence:.0%}), but you're uploading to '{selected_name}'"
            )
    except Exception as e:
        _logger.warning("Store detection error: %s", e)

    # Step 6: Check for data overlap
    if existing_data is not None:
        overlap = _check_overlap_with_existing(df, preview.selected_store, existing_data)
        if overlap:
            preview.overlap_info = overlap
            months_str = ", ".join(overlap["overlap_months"][:5])
            if overlap["overlap_month_count"] > 5:
                months_str += f" (+{overlap['overlap_month_count'] - 5} more)"
            preview.warnings.append(
                f"Data overlap: {overlap['overlap_month_count']} month(s) overlap "
                f"({months_str}) — {overlap['existing_rows_affected']} existing rows will be replaced"
            )

    # Step 7: Additional sanity checks
    if preview.row_count == 0:
        preview.errors.append("CSV file is empty (0 rows)")
        preview.is_valid = False
    elif preview.row_count < 5:
        preview.warnings.append(f"Very small file — only {preview.row_count} rows")

    # Check for unexpected currency
    if "Currency" in df.columns:
        currencies = df["Currency"].dropna().unique()
        non_usd = [c for c in currencies if str(c).upper() != "USD"]
        if non_usd:
            preview.warnings.append(f"Non-USD currency detected: {', '.join(str(c) for c in non_usd)}")

    return preview
