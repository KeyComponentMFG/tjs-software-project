"""
accounting/ — Agent-based accounting pipeline for TJ's Software Etsy dashboard.

Usage:
    from accounting import get_pipeline, current_ledger

    pipeline = get_pipeline()
    ledger = pipeline.full_rebuild(etsy_df, bank_txns, config)

    # Get a metric
    mv = ledger.get("real_profit")
    print(mv)  # MetricValue(real_profit=$1547.00, derived)
    print(mv.as_float)  # 1547.0
    print(mv.confidence)  # Confidence.DERIVED
"""

from __future__ import annotations

from typing import Optional

from .ledger import Ledger
from .models import Confidence, MetricValue
from .pipeline import AccountingPipeline

_pipeline_instance: Optional[AccountingPipeline] = None


def get_pipeline() -> AccountingPipeline:
    """Get or create the singleton pipeline instance."""
    global _pipeline_instance
    if _pipeline_instance is None:
        _pipeline_instance = AccountingPipeline()
    return _pipeline_instance


def current_ledger() -> Optional[Ledger]:
    """Get the current ledger (last pipeline result), or None if not built yet."""
    if _pipeline_instance is None:
        return None
    return _pipeline_instance.ledger
