"""
accounting/agents/audit.py — Agent 5: Audit trail and provenance reporting.

Generates human-readable audit trails for any metric and diffs between
ledger snapshots.
"""

from __future__ import annotations

from decimal import Decimal

from ..models import Confidence, MetricValue


class AuditAgent:
    """Generate audit trails and provenance reports."""

    def explain_metric(self, metric: MetricValue) -> str:
        """Human-readable explanation of how a metric was computed.

        Example output:
            real_profit ($1,547.00) = bank_cash_on_hand ($897.00) + draws ($650.00)
            [DERIVED from VERIFIED inputs]
        """
        p = metric.provenance
        conf_str = metric.confidence.value.upper()

        if metric.display_format == "money":
            val_str = f"${metric.value:,.2f}"
        elif metric.display_format == "percent":
            val_str = f"{metric.value:.1f}%"
        elif metric.display_format == "count":
            val_str = str(int(metric.value))
        else:
            val_str = str(metric.value)

        parts = [f"{metric.name} ({val_str})"]

        if p.formula and p.formula != "NOT_AVAILABLE":
            parts.append(f"= {p.formula}")

        if p.source_entries:
            parts.append(f"from {p.source_entries} entries")

        if p.source_types:
            parts.append(f"[types: {', '.join(p.source_types)}]")

        parts.append(f"[{conf_str}]")

        if p.notes:
            parts.append(f"Note: {p.notes}")

        return " ".join(parts)

    def explain_all(self, metrics: dict[str, MetricValue]) -> str:
        """Generate a full audit report for all metrics."""
        lines = ["=" * 60, "ACCOUNTING AUDIT REPORT", "=" * 60, ""]

        # Group by confidence
        by_conf: dict[Confidence, list[MetricValue]] = {}
        for mv in metrics.values():
            by_conf.setdefault(mv.confidence, []).append(mv)

        for conf in [Confidence.VERIFIED, Confidence.DERIVED, Confidence.ESTIMATED,
                     Confidence.UNKNOWN, Confidence.QUARANTINED]:
            mvs = by_conf.get(conf, [])
            if not mvs:
                continue
            lines.append(f"-- {conf.value.upper()} ({len(mvs)} metrics) --")
            for mv in sorted(mvs, key=lambda x: x.name):
                lines.append(f"  {self.explain_metric(mv)}")
            lines.append("")

        return "\n".join(lines)

    def diff_snapshots(self, old: dict[str, MetricValue],
                       new: dict[str, MetricValue]) -> list[str]:
        """Compare two metric snapshots and report changes."""
        changes = []
        all_keys = sorted(set(old.keys()) | set(new.keys()))

        for key in all_keys:
            old_mv = old.get(key)
            new_mv = new.get(key)

            if old_mv and not new_mv:
                changes.append(f"REMOVED: {key} (was ${old_mv.value:.2f})")
            elif new_mv and not old_mv:
                changes.append(f"ADDED: {key} = ${new_mv.value:.2f} [{new_mv.confidence.value}]")
            elif old_mv and new_mv:
                if old_mv.value != new_mv.value:
                    diff = new_mv.value - old_mv.value
                    sign = "+" if diff > 0 else ""
                    changes.append(
                        f"CHANGED: {key}: ${old_mv.value:.2f} -> ${new_mv.value:.2f} "
                        f"({sign}${diff:.2f}) [{new_mv.confidence.value}]"
                    )
                elif old_mv.confidence != new_mv.confidence:
                    changes.append(
                        f"CONFIDENCE: {key}: {old_mv.confidence.value} -> {new_mv.confidence.value}"
                    )

        return changes
