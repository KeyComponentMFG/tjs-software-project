"""
accounting/ledger.py — Immutable metric snapshot.

The Ledger is the final output of the pipeline. Once published, it doesn't change
until the next full_rebuild(). The dashboard reads from here.
"""

from __future__ import annotations

from datetime import datetime
from decimal import Decimal
from typing import Optional

from .journal import Journal
from .models import Confidence, MetricValue, ValidationResult


class Ledger:
    """Immutable snapshot of all computed metrics with provenance."""

    def __init__(self, metrics: dict[str, MetricValue],
                 validations: list[ValidationResult],
                 built_at: Optional[datetime] = None,
                 journal: Optional[Journal] = None,
                 strict_mode: bool = False):
        self._metrics = dict(metrics)  # Defensive copy
        self._validations = list(validations)
        self.built_at = built_at or datetime.now()
        self._journal = journal
        self.strict_mode = strict_mode
        self._quarantined: set[str] = set()

        # Auto-quarantine metrics affected by CRITICAL failures
        for vr in self._validations:
            if vr.severity == "CRITICAL" and not vr.passed:
                for name in vr.affected_metrics:
                    self._quarantined.add(name)

    def get(self, name: str) -> Optional[MetricValue]:
        """Get a metric by name. Returns None if not found."""
        mv = self._metrics.get(name)
        if mv and name in self._quarantined:
            # Return quarantined version
            return MetricValue(
                name=mv.name,
                value=mv.value,
                confidence=Confidence.QUARANTINED,
                provenance=mv.provenance,
                display_format=mv.display_format,
            )
        return mv

    def get_float(self, name: str, default: float = 0.0) -> float:
        """Get a metric's value as float. For backward compatibility."""
        mv = self._metrics.get(name)
        if mv is None:
            return default
        return float(mv.value)

    def get_value(self, name: str, default: Decimal = Decimal("0")) -> Decimal:
        """Get a metric's Decimal value."""
        mv = self._metrics.get(name)
        if mv is None:
            return default
        return mv.value

    def get_confidence(self, name: str) -> Optional[Confidence]:
        """Get a metric's confidence level."""
        mv = self._metrics.get(name)
        if mv is None:
            return None
        if name in self._quarantined:
            return Confidence.QUARANTINED
        return mv.confidence

    @property
    def metrics(self) -> dict[str, MetricValue]:
        """All metrics (read-only view)."""
        return dict(self._metrics)

    @property
    def validations(self) -> list[ValidationResult]:
        return list(self._validations)

    @property
    def quarantined_metrics(self) -> set[str]:
        return set(self._quarantined)

    @property
    def is_healthy(self) -> bool:
        """True if no CRITICAL validations failed."""
        return not any(v.severity == "CRITICAL" and not v.passed for v in self._validations)

    @property
    def warnings(self) -> list[ValidationResult]:
        return [v for v in self._validations if v.severity == "HIGH" and not v.passed]

    def summary(self) -> str:
        """Short summary string for logging."""
        total = len(self._metrics)
        quarantined = len(self._quarantined)
        passed = sum(1 for v in self._validations if v.passed)
        failed = sum(1 for v in self._validations if not v.passed)
        return (f"Ledger: {total} metrics, {quarantined} quarantined, "
                f"{passed} checks passed, {failed} failed, built {self.built_at:%H:%M:%S}")

    def get_source_entries(self, metric_name: str) -> list:
        """Return journal entries that contributed to a metric (via source_entry_ids)."""
        mv = self._metrics.get(metric_name)
        if mv is None or self._journal is None:
            return []
        return self._journal.get_by_hashes(mv.provenance.source_entry_ids)

    def get_missing_inputs(self, metric_name: str) -> tuple[str, ...]:
        """Return missing input names for a metric."""
        mv = self._metrics.get(metric_name)
        if mv is None:
            return ()
        return mv.provenance.missing_inputs

    def metrics_by_source(self, source_type: str) -> dict[str, MetricValue]:
        """Return metrics whose provenance includes the given source type."""
        return {
            name: mv for name, mv in self._metrics.items()
            if source_type in mv.provenance.source_types
        }

    def unknown_count(self) -> int:
        """Count metrics with confidence below DERIVED."""
        non_display = {Confidence.ESTIMATED, Confidence.PROJECTION,
                       Confidence.HEURISTIC, Confidence.UNKNOWN,
                       Confidence.QUARANTINED, Confidence.NEEDS_REVIEW}
        return sum(1 for mv in self._metrics.values()
                   if mv.confidence in non_display)

    def __contains__(self, name: str) -> bool:
        return name in self._metrics

    def __repr__(self) -> str:
        return self.summary()
