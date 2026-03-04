"""
accounting/agents/governance.py — Agent 6: Code governance scanner.

Scans source files for forbidden calculation patterns that violate
the "NO ESTIMATES, NO BACK-SOLVING, NO IMPLIED REVENUE" rule.

Forbidden patterns:
  - /0.065  → back-solving buyer-paid shipping from 6.5% transaction fee
  - /0.15   → back-solving offsite ad sales from 15% fee
  - * avg_  → count * average estimates (e.g., free_ship_count * avg_label)
  - est_ prefix calculations that produce dollar amounts from guesses

This agent is run as part of the pipeline to catch regressions.
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from pathlib import Path


@dataclass
class GovernanceViolation:
    """A forbidden calculation pattern found in source code."""
    file: str
    line_number: int
    line_text: str
    pattern: str
    severity: str  # "CRITICAL", "HIGH", "MEDIUM"
    description: str


# Patterns to scan for. Each tuple: (regex, severity, description)
FORBIDDEN_PATTERNS = [
    # CRITICAL: Fee-percentage back-solving
    (r'/ *0\.065', "CRITICAL",
     "Back-solving buyer-paid shipping from 6.5% transaction fee (/ 0.065)"),
    (r'/ *0\.15\b', "CRITICAL",
     "Back-solving offsite ad sales from 15% fee (/ 0.15)"),

    # HIGH: Count * average estimates (shipping labels, revenue guesses)
    # Excludes: avg_unit_cost (actual inventory data), growth rate calculations
    (r'\*\s*avg_(?:outbound_label|return_label|order)\b', "HIGH",
     "Count * average estimate — use actual per-item data instead"),
    (r'avg_(?:outbound_label|return_label|order)\s*\*', "HIGH",
     "Average * count estimate — use actual per-item data instead"),

    # MEDIUM: Hardcoded dollar guesses assigned to deduction variables
    (r'(?:home_office|internet|phone|mileage)_est\s*=\s*\d', "MEDIUM",
     "Hardcoded dollar amount for tax deduction — require actual document"),
    (r'est_biz_miles\s*=\s*\d', "MEDIUM",
     "Hardcoded mileage guess — require actual mileage log"),
]

# Files/patterns to exclude from scanning (comments, docstrings, removed code notes)
EXCLUDE_LINE_PATTERNS = [
    r'^\s*#',       # Comment lines
    r'^\s*"""',     # Docstring boundaries
    r"^\s*'''",     # Docstring boundaries
    r'was:',        # "was: old formula" documentation
    r'REMOVED:',    # Explicitly marked as removed
]


class GovernanceAgent:
    """Scans source code for forbidden calculation patterns."""

    def __init__(self, project_root: str | Path | None = None):
        self.project_root = Path(project_root) if project_root else Path(".")
        self.violations: list[GovernanceViolation] = []

    def scan_file(self, filepath: str | Path) -> list[GovernanceViolation]:
        """Scan a single file for forbidden patterns."""
        filepath = Path(filepath)
        violations = []

        try:
            content = filepath.read_text(encoding="utf-8")
        except (OSError, UnicodeDecodeError):
            return violations

        for line_num, line in enumerate(content.splitlines(), 1):
            # Skip excluded lines (comments, docstrings, removal notes)
            if any(re.search(pat, line) for pat in EXCLUDE_LINE_PATTERNS):
                continue

            for pattern, severity, description in FORBIDDEN_PATTERNS:
                if re.search(pattern, line):
                    violations.append(GovernanceViolation(
                        file=str(filepath),
                        line_number=line_num,
                        line_text=line.strip()[:120],
                        pattern=pattern,
                        severity=severity,
                        description=description,
                    ))

        return violations

    def scan_project(self, files: list[str | Path] | None = None) -> list[GovernanceViolation]:
        """Scan project files for forbidden patterns.

        Parameters:
            files: Specific files to scan. If None, scans default project files.

        Returns:
            List of GovernanceViolation objects.
        """
        self.violations = []

        if files is None:
            # Default: scan main dashboard + accounting agents
            files = [
                self.project_root / "etsy_dashboard.py",
                self.project_root / "accounting" / "agents" / "computation.py",
                self.project_root / "accounting" / "compat.py",
            ]

        for f in files:
            self.violations.extend(self.scan_file(f))

        return self.violations

    @property
    def has_critical(self) -> bool:
        return any(v.severity == "CRITICAL" for v in self.violations)

    def summary(self) -> str:
        """Human-readable summary of scan results."""
        if not self.violations:
            return "Governance scan PASSED — no forbidden patterns found."

        lines = [f"Governance scan FAILED — {len(self.violations)} violation(s) found:"]
        for v in self.violations:
            lines.append(f"  [{v.severity}] {v.file}:{v.line_number} — {v.description}")
            lines.append(f"    Code: {v.line_text}")
        return "\n".join(lines)
