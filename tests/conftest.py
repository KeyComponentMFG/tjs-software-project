"""
tests/conftest.py — Shared fixtures and helpers for the golden-dataset QA suite.

Provides:
- Golden dataset loader (JSON → DataFrame + bank_txns + invoices + config + expected)
- Pipeline runner helper (runs full pipeline on fixture data, returns metrics + results)
- Assertion helpers for Decimal-precision comparison
"""

import json
import os
from dataclasses import dataclass
from decimal import Decimal
from pathlib import Path
from typing import Optional

import pandas as pd
import pytest

# ── Path constants ──
TESTS_DIR = Path(__file__).parent
GOLDEN_DIR = TESTS_DIR / "golden"


# ── Golden dataset loader ──

@dataclass
class GoldenDataset:
    """A fully loaded golden test dataset."""
    name: str
    etsy_df: pd.DataFrame
    bank_txns: list[dict]
    invoices: list[dict]
    config: dict
    expected: dict

    def expect(self, key: str) -> float:
        """Get expected value, raising KeyError with helpful message if missing."""
        if key not in self.expected:
            raise KeyError(f"Golden dataset '{self.name}' has no expected value for '{key}'")
        return self.expected[key]


def load_golden(filename: str) -> GoldenDataset:
    """Load a golden test dataset from JSON."""
    path = GOLDEN_DIR / filename
    with open(path, "r", encoding="utf-8") as f:
        raw = json.load(f)

    # Build DataFrame from Etsy rows (same columns as real CSVs)
    etsy_df = pd.DataFrame(raw["etsy_rows"])
    if not etsy_df.empty:
        # Add computed columns that the pipeline expects
        etsy_df["Amount_Clean"] = etsy_df["Amount"].apply(_parse_money)
        etsy_df["Net_Clean"] = etsy_df["Net"].apply(_parse_money)
        etsy_df["Fees_Clean"] = etsy_df["Fees & Taxes"].apply(_parse_money)
        etsy_df["Date_Parsed"] = pd.to_datetime(etsy_df["Date"], format="%B %d, %Y", errors="coerce")
        etsy_df["Month"] = etsy_df["Date_Parsed"].dt.to_period("M").astype(str)

    return GoldenDataset(
        name=filename,
        etsy_df=etsy_df,
        bank_txns=raw["bank_txns"],
        invoices=raw["invoices"],
        config=raw["config"],
        expected=raw["expected"],
    )


def _parse_money(val) -> float:
    """Parse Etsy money strings: '$40.00' → 40.0, '--' → 0.0."""
    if pd.isna(val) or val in ("--", "", None):
        return 0.0
    s = str(val).replace("$", "").replace(",", "").replace('"', "").strip()
    if not s or s == "--":
        return 0.0
    try:
        return float(s)
    except ValueError:
        return 0.0


# ── Pipeline runner ──

@dataclass
class PipelineResult:
    """Result of running the pipeline on a golden dataset."""
    ledger: object  # Ledger
    journal: object  # Journal
    pipeline: object  # AccountingPipeline
    recon: dict
    expense_result: object  # ExpenseCompletenessResult or None

    def metric(self, name: str) -> Optional[float]:
        """Get a metric value as float, or None if not found."""
        mv = self.ledger.get(name)
        return float(mv.value) if mv else None

    def metric_decimal(self, name: str) -> Optional[Decimal]:
        """Get a metric value as Decimal, or None."""
        mv = self.ledger.get(name)
        return mv.value if mv else None


def run_pipeline(ds: GoldenDataset, strict_mode: bool = False) -> PipelineResult:
    """Run the full accounting pipeline on a golden dataset."""
    from accounting.pipeline import AccountingPipeline

    pipeline = AccountingPipeline()
    ledger = pipeline.full_rebuild(
        ds.etsy_df, ds.bank_txns, ds.config, invoices=ds.invoices,
        strict_mode=strict_mode
    )

    # Get reconciliation results from ReconciliationResult
    recon_result = pipeline.get_reconciliation_result()
    recon = {
        "matched_count": len(recon_result.matched) if recon_result else 0,
        "etsy_unmatched_count": len(recon_result.etsy_unmatched) if recon_result else 0,
        "bank_unmatched_count": len(recon_result.bank_unmatched) if recon_result else 0,
    }

    return PipelineResult(
        ledger=ledger,
        journal=pipeline.journal,
        pipeline=pipeline,
        recon=recon,
        expense_result=pipeline.get_expense_completeness(),
    )


# ── Assertion helpers ──

def assert_money(actual: float, expected: float, name: str = "", tolerance: float = 0.01):
    """Assert two money values are equal within penny tolerance."""
    diff = abs(actual - expected)
    msg = f"{name}: expected ${expected:.2f}, got ${actual:.2f} (diff=${diff:.2f})"
    assert diff <= tolerance, msg


def assert_money_decimal(actual: Decimal, expected_float: float, name: str = "",
                         tolerance: float = 0.01):
    """Assert a Decimal metric matches an expected float within tolerance."""
    assert_money(float(actual), expected_float, name, tolerance)


# ── Fixtures ──

@pytest.fixture(params=[
    "scenario_1_simple_month.json",
    "scenario_2_refunds_after_payout.json",
    "scenario_3_split_payouts.json",
    "scenario_4_chargeback_tax.json",
    "scenario_5_missing_receipts.json",
    "scenario_6_clean_reconciliation.json",
])
def golden_dataset(request) -> GoldenDataset:
    """Parametrized fixture: yields each golden dataset in turn."""
    return load_golden(request.param)


@pytest.fixture
def golden_result(golden_dataset) -> PipelineResult:
    """Run the pipeline on each golden dataset."""
    return run_pipeline(golden_dataset)


@pytest.fixture
def scenario1():
    return load_golden("scenario_1_simple_month.json")

@pytest.fixture
def scenario2():
    return load_golden("scenario_2_refunds_after_payout.json")

@pytest.fixture
def scenario3():
    return load_golden("scenario_3_split_payouts.json")

@pytest.fixture
def scenario4():
    return load_golden("scenario_4_chargeback_tax.json")

@pytest.fixture
def scenario5():
    return load_golden("scenario_5_missing_receipts.json")

@pytest.fixture
def scenario6():
    return load_golden("scenario_6_clean_reconciliation.json")
