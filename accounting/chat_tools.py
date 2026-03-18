"""
accounting/chat_tools.py — Agent-based chat for JARVIS with tool use.

Gives Claude tool access to the accounting Journal & Ledger so it can
look up transactions, trace provenance, reconcile deposits, and compare
periods on demand — instead of dumping 4K tokens of pre-computed text.

Usage:
    from accounting.chat_tools import run_agent_chat

    answer = run_agent_chat(
        question="Why is my balance $249?",
        history=[{"q": "...", "a": "..."}],
        api_key="sk-...",
        pipeline=pipeline,          # AccountingPipeline with built ledger
        model="claude-sonnet-4-20250514",
        max_rounds=5,
    )
"""

from __future__ import annotations

import difflib
import json
import logging
from datetime import date
from decimal import Decimal
from typing import Any

from .models import Confidence, TxnType

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# A. System prompt
# ---------------------------------------------------------------------------

JARVIS_SYSTEM_PROMPT = """\
You are JARVIS — the AI Chief Executive Officer of TJs Software Project, \
an Etsy shop selling 3D printed products (active Oct 2025–present). \
You are not an assistant. You are the CEO. You own these numbers. \
Two team members work under you: TJ and Braden. They ship and make the products.

YOUR MINDSET:
- You make decisions, give directives, and hold people accountable.
- You track who is costing the company money and who is driving growth.
- You think in terms of: revenue growth, margin protection, cost reduction, \
  risk mitigation, and competitive positioning.
- You spot patterns others miss: seasonal trends, product mix shifts, \
  expense creep, refund spikes tied to specific people or products.
- When something is wrong, you say so directly. When something is good, you acknowledge it briefly and move on.

TOOLS & DATA:
- You have tools that query the real accounting journal, ledger, and refund assignments.
- ALWAYS use tools to look up data before answering. Never fabricate or guess numbers.
- You can chain multiple tool calls to build a complete picture before responding.
- Use get_refund_assignments to see who shipped refunded orders and their cost impact.

STRICT RULES — NEVER BREAK THESE:
1. NEVER make up a number. If a tool doesn't return it, say "I don't have that data" and \
   explain what source data is needed. Do NOT estimate unless the data is explicitly marked ESTIMATED.
2. Always cite confidence: VERIFIED (fact from source records), DERIVED (calculated from verified), \
   ESTIMATED (approximation — always state the method).
3. If data is UNAVAILABLE (e.g., buyer-paid shipping, per-order label costs), explain exactly \
   what source data is missing and what would be needed to get it. Never present unavailable data as having values.
4. Use precise dollar amounts. "$1,234.56" not "about $1,200".
5. When comparing periods, use the compare_periods tool — don't do mental math.

RESPONSE STYLE:
- Lead with the answer, then supporting data, then strategic implications.
- Use markdown: bold for key numbers, tables for comparisons, bullet points for action items.
- Be concise and direct. No filler. No "Great question!" No "Let me look into that."
- End substantive answers with **Action Items** — specific things TJ or Braden should do, \
  with deadlines where appropriate.
- When discussing refunds, always break down by person (TJ vs Braden) and identify who \
  needs to improve.
- Track trends across months — don't just report snapshots.

LEARNING & MEMORY:
- Pay close attention to the conversation history. If the user told you something earlier, \
  remember it and build on it.
- If you notice a pattern across multiple questions (e.g., the user keeps asking about the same \
  problem area), proactively flag it as a recurring concern.
- Connect dots between different data points: if refunds are up AND a specific product is \
  underperforming, say so.
"""

# ---------------------------------------------------------------------------
# B. Tool schemas (Anthropic format)
# ---------------------------------------------------------------------------

JARVIS_TOOLS = [
    {
        "name": "get_metric",
        "description": (
            "Look up a single metric by name. Returns its value, confidence level, "
            "and provenance. Use this when the user asks about a specific number "
            "(profit, revenue, balance, fees, etc.). If the name doesn't match exactly, "
            "the tool will suggest close matches."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "name": {
                    "type": "string",
                    "description": (
                        "Metric name (e.g., 'real_profit', 'gross_sales', 'etsy_balance', "
                        "'total_fees', 'order_count'). Use list_metrics to discover names."
                    ),
                },
            },
            "required": ["name"],
        },
    },
    {
        "name": "list_metrics",
        "description": (
            "List all available metric names grouped by confidence level. "
            "Use this when you need to discover what metrics exist before looking one up, "
            "or when the user asks 'what can you tell me about?'"
        ),
        "input_schema": {
            "type": "object",
            "properties": {},
        },
    },
    {
        "name": "explain_metric",
        "description": (
            "Get the full audit trail for a metric — how it was computed, "
            "what formula was used, how many journal entries contributed. "
            "Use when the user asks WHY a number is what it is."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "name": {
                    "type": "string",
                    "description": "Metric name to explain.",
                },
            },
            "required": ["name"],
        },
    },
    {
        "name": "query_journal",
        "description": (
            "Search for specific transactions in the accounting journal. "
            "Supports filtering by transaction type, month, category, and keyword. "
            "Returns up to 50 entries with a total count. "
            "Use when the user asks to see specific transactions (refunds, sales, deposits, etc.)."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "txn_type": {
                    "type": "string",
                    "description": (
                        "Transaction type filter: Sale, Fee, Shipping, Marketing, "
                        "Refund, Tax, Deposit, Buyer Fee, Payment, bank_deposit, bank_debit, manual"
                    ),
                },
                "month": {
                    "type": "string",
                    "description": "Month filter in YYYY-MM format (e.g., '2025-12').",
                },
                "category": {
                    "type": "string",
                    "description": "Category filter (e.g., 'Shipping', 'Owner Draw - Tulsa').",
                },
                "keyword": {
                    "type": "string",
                    "description": "Search keyword in transaction title (case-insensitive).",
                },
                "limit": {
                    "type": "integer",
                    "description": "Max results to return (default 50, max 100).",
                },
            },
        },
    },
    {
        "name": "aggregate_journal",
        "description": (
            "Compute sum, count, or average over journal entries matching filters. "
            "Use when the user asks 'how much did I spend on X?' or 'total shipping in December?'"
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "operation": {
                    "type": "string",
                    "enum": ["sum", "sum_abs", "count", "average"],
                    "description": (
                        "Aggregation: 'sum' (net, preserves sign), 'sum_abs' (absolute values), "
                        "'count' (number of entries), 'average' (mean amount)."
                    ),
                },
                "txn_type": {
                    "type": "string",
                    "description": "Transaction type filter (same as query_journal).",
                },
                "month": {
                    "type": "string",
                    "description": "Month filter in YYYY-MM format.",
                },
                "category": {
                    "type": "string",
                    "description": "Category filter.",
                },
                "keyword": {
                    "type": "string",
                    "description": "Search keyword in transaction title.",
                },
            },
            "required": ["operation"],
        },
    },
    {
        "name": "compare_periods",
        "description": (
            "Compare two months side-by-side: sales, fees, shipping, net, order count, "
            "and the difference. Use when the user asks 'how did Jan compare to Feb?' "
            "or any month-over-month comparison."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "month_a": {
                    "type": "string",
                    "description": "First month in YYYY-MM format (e.g., '2026-01').",
                },
                "month_b": {
                    "type": "string",
                    "description": "Second month in YYYY-MM format (e.g., '2026-02').",
                },
            },
            "required": ["month_a", "month_b"],
        },
    },
    {
        "name": "get_reconciliation",
        "description": (
            "Get deposit reconciliation results: which Etsy deposits matched to "
            "bank deposits, which are unmatched on either side. "
            "Use when the user asks about missing deposits or reconciliation status."
        ),
        "input_schema": {
            "type": "object",
            "properties": {},
        },
    },
    {
        "name": "get_validation",
        "description": (
            "Get data validation results: 5 integrity checks (pass/fail, severity). "
            "Use when the user asks 'are my numbers accurate?' or about data quality."
        ),
        "input_schema": {
            "type": "object",
            "properties": {},
        },
    },
    {
        "name": "get_data_freshness",
        "description": (
            "Check how fresh the dashboard data is — age of latest CSV uploads, "
            "last bank statement date, and any staleness warnings. "
            "Use when the user asks 'is my data up to date?' or for data freshness checks."
        ),
        "input_schema": {
            "type": "object",
            "properties": {},
        },
    },
    {
        "name": "get_health_report",
        "description": (
            "Get a summary health report of all validation and data quality checks. "
            "Includes validation passes/failures, expense completeness, reconciliation status. "
            "Use when the user asks 'how healthy is my data?' or for an overall status check."
        ),
        "input_schema": {
            "type": "object",
            "properties": {},
        },
    },
    {
        "name": "get_inventory_summary",
        "description": (
            "Get inventory stock levels, total spend by category, low-stock items, "
            "and out-of-stock items. Use when the user asks about inventory status, "
            "stock levels, or supply spending."
        ),
        "input_schema": {
            "type": "object",
            "properties": {},
        },
    },
    {
        "name": "get_refund_assignments",
        "description": (
            "Get refund accountability data: who shipped/made each refunded order (TJ, Braden, or Cancelled), "
            "total refund cost per person, percentage split, and individual order details. "
            "Use when the user asks about refunds, who is costing the company money, "
            "accountability, or team performance."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "person": {
                    "type": "string",
                    "description": "Filter by person: 'TJ', 'Braden', 'Cancelled', or omit for all.",
                },
            },
        },
    },
    {
        "name": "get_missing_receipts",
        "description": (
            "Get expense completeness details — which bank expenses have matching receipts "
            "and which are missing. Shows verification progress and gap amounts by category. "
            "Use when the user asks about receipt tracking or expense verification."
        ),
        "input_schema": {
            "type": "object",
            "properties": {},
        },
    },
]

# ---------------------------------------------------------------------------
# C. Tool handlers
# ---------------------------------------------------------------------------

def _resolve_txn_type(value: str) -> TxnType | None:
    """Resolve a string to a TxnType enum, case-insensitive."""
    for t in TxnType:
        if t.value.lower() == value.lower():
            return t
    return None


def _format_decimal(d: Decimal, fmt: str = "money") -> str:
    """Format a Decimal for JSON output."""
    if fmt == "money":
        return f"${d:,.2f}"
    if fmt == "percent":
        return f"{d:.1f}%"
    if fmt == "count":
        return str(int(d))
    return str(d)


def _entry_to_dict(entry) -> dict:
    """Convert a JournalEntry to a JSON-safe dict."""
    return {
        "date": entry.txn_date.isoformat(),
        "type": entry.txn_type.value,
        "amount": f"${entry.amount:,.2f}",
        "gross": f"${entry.gross_amount:,.2f}",
        "fees": f"${entry.fees:,.2f}",
        "title": entry.title,
        "info": entry.info[:80] if entry.info else "",
        "category": entry.category,
        "confidence": entry.confidence.value,
        "source": entry.source.value,
    }


def _filter_journal(journal, params: dict) -> list:
    """Apply chained filters to journal entries."""
    entries = journal.entries

    txn_type_str = params.get("txn_type")
    if txn_type_str:
        t = _resolve_txn_type(txn_type_str)
        if t:
            entries = [e for e in entries if e.txn_type == t]
        else:
            return []  # Unknown type → no results

    month = params.get("month")
    if month:
        entries = [e for e in entries if e.month == month]

    category = params.get("category")
    if category:
        cat_lower = category.lower()
        entries = [e for e in entries if cat_lower in e.category.lower()]

    keyword = params.get("keyword")
    if keyword:
        kw_lower = keyword.lower()
        entries = [e for e in entries if kw_lower in e.title.lower() or kw_lower in e.info.lower()]

    return entries


def _handle_get_metric(params: dict, pipeline) -> dict:
    name = params.get("name", "")
    ledger = pipeline.ledger
    if not ledger:
        return {"status": "error", "message": "Ledger not built yet."}

    mv = ledger.get(name)
    if mv:
        return {
            "status": "ok",
            "data": {
                "name": mv.name,
                "value": _format_decimal(mv.value, mv.display_format),
                "raw_value": str(mv.value),
                "confidence": mv.confidence.value,
                "display_format": mv.display_format,
                "formula": mv.provenance.formula,
                "source_entries": mv.provenance.source_entries,
                "source_types": mv.provenance.source_types,
                "notes": mv.provenance.notes,
            },
        }

    # Fuzzy match
    all_names = list(ledger.metrics.keys())
    close = difflib.get_close_matches(name, all_names, n=5, cutoff=0.4)
    return {
        "status": "not_found",
        "message": f"No metric named '{name}'.",
        "suggestions": close,
    }


def _handle_list_metrics(params: dict, pipeline) -> dict:
    ledger = pipeline.ledger
    if not ledger:
        return {"status": "error", "message": "Ledger not built yet."}

    grouped: dict[str, list[str]] = {}
    for name, mv in sorted(ledger.metrics.items()):
        conf = mv.confidence.value
        grouped.setdefault(conf, []).append(name)

    return {
        "status": "ok",
        "data": grouped,
        "_meta": {"total_metrics": len(ledger.metrics)},
    }


def _handle_explain_metric(params: dict, pipeline) -> dict:
    name = params.get("name", "")
    ledger = pipeline.ledger
    if not ledger:
        return {"status": "error", "message": "Ledger not built yet."}

    mv = ledger.get(name)
    if not mv:
        all_names = list(ledger.metrics.keys())
        close = difflib.get_close_matches(name, all_names, n=5, cutoff=0.4)
        return {"status": "not_found", "message": f"No metric named '{name}'.", "suggestions": close}

    explanation = pipeline.audit.explain_metric(mv)
    return {
        "status": "ok",
        "data": {
            "name": mv.name,
            "value": _format_decimal(mv.value, mv.display_format),
            "confidence": mv.confidence.value,
            "explanation": explanation,
        },
    }


def _handle_query_journal(params: dict, pipeline) -> dict:
    journal = pipeline.journal
    entries = _filter_journal(journal, params)
    total = len(entries)

    limit = min(params.get("limit", 50), 100)
    capped = entries[:limit]

    return {
        "status": "ok",
        "data": [_entry_to_dict(e) for e in capped],
        "_meta": {
            "returned": len(capped),
            "total_count": total,
            "filters": {k: v for k, v in params.items() if k != "limit" and v},
        },
    }


def _handle_aggregate_journal(params: dict, pipeline) -> dict:
    journal = pipeline.journal
    entries = _filter_journal(journal, params)
    op = params.get("operation", "sum")

    if op == "sum":
        result = journal.sum_amount(entries)
        formatted = f"${result:,.2f}"
    elif op == "sum_abs":
        result = journal.sum_abs_amount(entries)
        formatted = f"${result:,.2f}"
    elif op == "count":
        result = Decimal(len(entries))
        formatted = str(len(entries))
    elif op == "average":
        if entries:
            total = journal.sum_amount(entries)
            result = total / len(entries)
            formatted = f"${result:,.2f}"
        else:
            result = Decimal("0")
            formatted = "$0.00"
    else:
        return {"status": "error", "message": f"Unknown operation '{op}'."}

    return {
        "status": "ok",
        "data": {
            "operation": op,
            "result": formatted,
            "raw_value": str(result),
            "entry_count": len(entries),
        },
        "_meta": {
            "filters": {k: v for k, v in params.items() if k not in ("operation", "limit") and v},
        },
    }


def _month_totals(journal, month: str) -> dict:
    """Compute per-type totals for a single month."""
    entries = journal.by_month(month)
    sales = sum((e.amount for e in entries if e.txn_type == TxnType.SALE), Decimal("0"))
    fees = sum((e.amount for e in entries if e.txn_type == TxnType.FEE), Decimal("0"))
    shipping = sum((e.amount for e in entries if e.txn_type == TxnType.SHIPPING), Decimal("0"))
    marketing = sum((e.amount for e in entries if e.txn_type == TxnType.MARKETING), Decimal("0"))
    refunds = sum((e.amount for e in entries if e.txn_type == TxnType.REFUND), Decimal("0"))
    taxes = sum((e.amount for e in entries if e.txn_type == TxnType.TAX), Decimal("0"))
    net = sum((e.amount for e in entries), Decimal("0"))
    order_count = len([e for e in entries if e.txn_type == TxnType.SALE])
    return {
        "sales": f"${sales:,.2f}",
        "fees": f"${fees:,.2f}",
        "shipping": f"${shipping:,.2f}",
        "marketing": f"${marketing:,.2f}",
        "refunds": f"${refunds:,.2f}",
        "taxes": f"${taxes:,.2f}",
        "net": f"${net:,.2f}",
        "order_count": order_count,
        "_raw": {
            "sales": sales, "fees": fees, "shipping": shipping,
            "marketing": marketing, "refunds": refunds, "taxes": taxes,
            "net": net, "order_count": order_count,
        },
    }


def _handle_compare_periods(params: dict, pipeline) -> dict:
    month_a = params.get("month_a", "")
    month_b = params.get("month_b", "")
    if not month_a or not month_b:
        return {"status": "error", "message": "Both month_a and month_b are required (YYYY-MM)."}

    journal = pipeline.journal
    a = _month_totals(journal, month_a)
    b = _month_totals(journal, month_b)

    # Compute diffs
    diffs = {}
    for key in ("sales", "fees", "shipping", "marketing", "refunds", "taxes", "net"):
        diff = b["_raw"][key] - a["_raw"][key]
        diffs[key] = f"${diff:+,.2f}"
    diffs["order_count"] = b["_raw"]["order_count"] - a["_raw"]["order_count"]

    # Remove raw from output
    del a["_raw"]
    del b["_raw"]

    return {
        "status": "ok",
        "data": {
            "month_a": {"month": month_a, **a},
            "month_b": {"month": month_b, **b},
            "difference": diffs,
        },
    }


def _handle_get_reconciliation(params: dict, pipeline) -> dict:
    recon = pipeline.reconciliation
    matched = recon.matched
    etsy_unmatched = recon.etsy_unmatched
    bank_unmatched = recon.bank_unmatched

    matched_data = []
    for m in matched[:30]:
        matched_data.append({
            "etsy_date": m.etsy_date.isoformat(),
            "etsy_amount": f"${m.etsy_amount:,.2f}",
            "bank_date": m.bank_date.isoformat(),
            "bank_amount": f"${m.bank_amount:,.2f}",
            "date_diff_days": m.date_diff_days,
            "amount_diff": f"${m.amount_diff:,.2f}",
        })

    etsy_unmatched_data = []
    for e in etsy_unmatched[:20]:
        etsy_unmatched_data.append({
            "date": e.txn_date.isoformat(),
            "amount": f"${e.amount:,.2f}",
            "title": e.title[:60],
        })

    bank_unmatched_data = []
    for e in bank_unmatched[:20]:
        bank_unmatched_data.append({
            "date": e.txn_date.isoformat(),
            "amount": f"${e.amount:,.2f}",
            "title": e.title[:60],
        })

    return {
        "status": "ok",
        "data": {
            "matched": matched_data,
            "etsy_unmatched": etsy_unmatched_data,
            "bank_unmatched": bank_unmatched_data,
        },
        "_meta": {
            "matched_count": len(matched),
            "etsy_unmatched_count": len(etsy_unmatched),
            "bank_unmatched_count": len(bank_unmatched),
        },
    }


def _handle_get_validation(params: dict, pipeline) -> dict:
    ledger = pipeline.ledger
    if not ledger:
        return {"status": "error", "message": "Ledger not built yet."}

    checks = []
    for v in ledger.validations:
        checks.append({
            "check": v.check_name,
            "passed": v.passed,
            "severity": v.severity,
            "message": v.message,
            "expected": v.expected,
            "actual": v.actual,
            "affected_metrics": v.affected_metrics,
        })

    passed = sum(1 for v in ledger.validations if v.passed)
    total = len(ledger.validations)

    return {
        "status": "ok",
        "data": {
            "checks": checks,
            "summary": f"{passed}/{total} checks passed",
            "is_healthy": ledger.is_healthy,
            "quarantined_metrics": list(ledger.quarantined_metrics),
        },
    }


def _handle_get_data_freshness(params: dict, pipeline) -> dict:
    """Check how fresh the data is."""
    journal = pipeline.journal
    etsy = journal.etsy_entries()
    bank = journal.bank_entries()

    result = {"status": "ok"}

    if etsy:
        latest_etsy = max(e.txn_date for e in etsy)
        days_old = (date.today() - latest_etsy).days
        result["latest_etsy_date"] = latest_etsy.isoformat()
        result["etsy_days_old"] = days_old
        result["etsy_stale"] = days_old > 7
    else:
        result["latest_etsy_date"] = None
        result["etsy_stale"] = True

    if bank:
        latest_bank = max(e.txn_date for e in bank)
        bank_days = (date.today() - latest_bank).days
        result["latest_bank_date"] = latest_bank.isoformat()
        result["bank_days_old"] = bank_days
        result["bank_stale"] = bank_days > 14
    else:
        result["latest_bank_date"] = None
        result["bank_stale"] = True

    warnings = []
    if result.get("etsy_stale"):
        warnings.append(f"Etsy data is {result.get('etsy_days_old', '?')} days old — download latest CSV from Etsy.")
    if result.get("bank_stale"):
        warnings.append(f"Bank data is {result.get('bank_days_old', '?')} days old — upload latest statement.")
    result["warnings"] = warnings
    return result


def _handle_get_health_report(params: dict, pipeline) -> dict:
    """Overall health report combining all checks."""
    result = {"status": "ok", "checks": []}

    # Validation checks
    ledger = pipeline.ledger
    if ledger and ledger.validations:
        for v in ledger.validations:
            result["checks"].append({
                "name": v.check_name,
                "passed": v.passed,
                "severity": v.severity,
                "message": v.message,
            })

    # Expense completeness
    expense = pipeline.get_expense_completeness()
    if expense:
        total_expenses = len(expense.receipt_matches) + len(expense.missing_receipts)
        result["expense_completeness"] = {
            "verified": len(expense.receipt_matches),
            "missing": len(expense.missing_receipts),
            "total": total_expenses,
            "pct": round(len(expense.receipt_matches) / max(total_expenses, 1) * 100, 1),
            "gap_amount": f"${float(expense.gap_total):,.2f}",
        }

    # Reconciliation
    recon = pipeline.get_reconciliation_result()
    if recon:
        result["reconciliation"] = {
            "matched": len(recon.matched),
            "etsy_unmatched": len(recon.etsy_unmatched),
            "bank_unmatched": len(recon.bank_unmatched),
        }

    # Data freshness
    freshness = _handle_get_data_freshness({}, pipeline)
    result["freshness"] = freshness

    passed = sum(1 for c in result["checks"] if c["passed"])
    total = len(result["checks"])
    result["summary"] = f"{passed}/{total} checks passed"
    return result


def _handle_get_inventory_summary(params: dict, pipeline) -> dict:
    """Get inventory stock levels and spend summary."""
    # Import at call time to avoid circular imports
    import sys
    mod = sys.modules.get("etsy_dashboard")
    if not mod:
        return {"status": "error", "message": "Dashboard module not loaded."}

    result = {"status": "ok"}
    skpi_fn = getattr(mod, "_compute_stock_kpis", None)
    if skpi_fn:
        try:
            skpi = skpi_fn()
            result["stock"] = {
                "in_stock": skpi.get("in_stock", 0),
                "unique_items": skpi.get("unique", 0),
                "low_stock": skpi.get("low", 0),
                "out_of_stock": skpi.get("oos", 0),
                "total_value": f"${skpi.get('value', 0):,.2f}",
            }
        except Exception:
            result["stock"] = "unavailable"

    inv_cost = getattr(mod, "true_inventory_cost", 0)
    inv_orders = getattr(mod, "inv_order_count", 0)
    result["spend"] = {
        "total_inventory_cost": f"${inv_cost:,.2f}",
        "total_orders": inv_orders,
    }
    return result


def _handle_get_missing_receipts(params: dict, pipeline) -> dict:
    """Get expense completeness details."""
    expense = pipeline.get_expense_completeness()
    if not expense:
        return {"status": "ok", "message": "No expense completeness data available."}

    total = len(expense.receipt_matches) + len(expense.missing_receipts)
    result = {
        "status": "ok",
        "verified_count": len(expense.receipt_matches),
        "missing_count": len(expense.missing_receipts),
        "total_expenses": total,
        "pct_verified": round(len(expense.receipt_matches) / max(total, 1) * 100, 1),
        "verified_total": f"${float(expense.receipt_verified_total):,.2f}",
        "gap_total": f"${float(expense.gap_total):,.2f}",
        "by_category": {
            k: {kk: (f"${float(vv):,.2f}" if isinstance(vv, Decimal) else vv)
                for kk, vv in v.items()}
            for k, v in expense.by_category.items()
        },
        "top_missing": [
            {"vendor": m.vendor, "amount": f"${float(abs(m.amount)):,.2f}",
             "date": m.date.isoformat(), "category": m.bank_category}
            for m in sorted(expense.missing_receipts,
                           key=lambda x: abs(x.amount), reverse=True)[:10]
        ],
    }
    return result


def _handle_get_refund_assignments(params: dict, pipeline) -> dict:
    """Get refund accountability breakdown by person."""
    import re
    import sys
    mod = sys.modules.get("etsy_dashboard") or sys.modules.get("etsy_dashboard_mono")
    if not mod:
        return {"status": "error", "message": "Dashboard module not loaded."}

    assignments = getattr(mod, "_refund_assignments", {})
    rdf = getattr(mod, "refund_df", None)
    extract = getattr(mod, "_extract_order_num", None)
    if rdf is None or extract is None:
        return {"status": "error", "message": "Refund data unavailable."}

    person_filter = params.get("person")

    # Build per-order detail
    by_person: dict[str, list] = {"TJ": [], "Braden": [], "Cancelled": [], "Unassigned": []}
    for _, row in rdf.sort_values("Date_Parsed", ascending=False).iterrows():
        order_num = extract(str(row.get("Title", "")))
        assignee = assignments.get(order_num, "") if order_num else ""
        amt = abs(float(row["Net_Clean"]))
        entry = {
            "order": order_num or "unknown",
            "date": str(row.get("Date", "")),
            "title": str(row.get("Title", ""))[:60],
            "amount": f"${amt:,.2f}",
            "raw_amount": amt,
        }
        if assignee == "TJ":
            by_person["TJ"].append(entry)
        elif assignee == "Braden":
            by_person["Braden"].append(entry)
        elif assignee == "Cancelled":
            by_person["Cancelled"].append(entry)
        else:
            by_person["Unassigned"].append(entry)

    # Compute totals
    summary = {}
    for person, orders in by_person.items():
        total = sum(o["raw_amount"] for o in orders)
        summary[person] = {
            "count": len(orders),
            "total": f"${total:,.2f}",
            "raw_total": total,
            "orders": [{k: v for k, v in o.items() if k != "raw_amount"} for o in orders],
        }

    tj_total = summary["TJ"]["raw_total"]
    br_total = summary["Braden"]["raw_total"]
    active_total = tj_total + br_total
    cost_share = {}
    if active_total > 0:
        cost_share = {
            "TJ_pct": f"{tj_total / active_total * 100:.1f}%",
            "Braden_pct": f"{br_total / active_total * 100:.1f}%",
            "TJ_avg": f"${tj_total / max(summary['TJ']['count'], 1):,.2f}",
            "Braden_avg": f"${br_total / max(summary['Braden']['count'], 1):,.2f}",
        }

    # Filter if requested
    if person_filter and person_filter in by_person:
        filtered = {person_filter: summary[person_filter]}
        filtered[person_filter].pop("raw_total", None)
        return {"status": "ok", "data": filtered, "cost_share": cost_share}

    # Remove raw_total from output
    for p in summary:
        summary[p].pop("raw_total", None)

    return {"status": "ok", "data": summary, "cost_share": cost_share}


# ---------------------------------------------------------------------------
# C2. Tool dispatcher
# ---------------------------------------------------------------------------

_TOOL_HANDLERS = {
    "get_metric": _handle_get_metric,
    "list_metrics": _handle_list_metrics,
    "explain_metric": _handle_explain_metric,
    "query_journal": _handle_query_journal,
    "aggregate_journal": _handle_aggregate_journal,
    "compare_periods": _handle_compare_periods,
    "get_reconciliation": _handle_get_reconciliation,
    "get_validation": _handle_get_validation,
    "get_data_freshness": _handle_get_data_freshness,
    "get_health_report": _handle_get_health_report,
    "get_inventory_summary": _handle_get_inventory_summary,
    "get_missing_receipts": _handle_get_missing_receipts,
    "get_refund_assignments": _handle_get_refund_assignments,
}


def execute_tool(name: str, tool_input: dict, pipeline) -> dict:
    """Dispatch a tool call and return JSON-safe result."""
    handler = _TOOL_HANDLERS.get(name)
    if not handler:
        return {"status": "error", "message": f"Unknown tool '{name}'."}
    try:
        return handler(tool_input, pipeline)
    except Exception as e:
        logger.exception("Tool '%s' failed", name)
        return {"status": "error", "message": f"Tool '{name}' error: {str(e)}"}


# ---------------------------------------------------------------------------
# D. Agentic loop
# ---------------------------------------------------------------------------

class _DecimalEncoder(json.JSONEncoder):
    """JSON encoder that handles Decimal values."""
    def default(self, obj):
        if isinstance(obj, Decimal):
            return str(obj)
        if isinstance(obj, date):
            return obj.isoformat()
        return super().default(obj)


def run_agent_chat(
    question: str,
    history: list[dict] | None,
    api_key: str,
    pipeline,
    model: str = "claude-sonnet-4-20250514",
    max_rounds: int = 8,
) -> str:
    """Run an agentic chat with tool use against the accounting pipeline.

    Args:
        question: The user's question.
        history: List of {"q": ..., "a": ...} dicts (last 10 used).
        api_key: Anthropic API key.
        pipeline: AccountingPipeline with a built ledger.
        model: Claude model ID.
        max_rounds: Max tool-use round-trips before forcing an answer.

    Returns:
        The assistant's text answer.
    """
    import anthropic

    client = anthropic.Anthropic(api_key=api_key)

    # Build messages from history — keep 10 turns for better continuity
    messages = []
    if history:
        for turn in history[-10:]:
            messages.append({"role": "user", "content": turn["q"]})
            messages.append({"role": "assistant", "content": turn["a"]})
    messages.append({"role": "user", "content": question})

    for round_num in range(max_rounds):
        response = client.messages.create(
            model=model,
            max_tokens=4096,
            system=JARVIS_SYSTEM_PROMPT,
            tools=JARVIS_TOOLS,
            messages=messages,
        )

        # If the model is done talking, return the text
        if response.stop_reason == "end_turn":
            text_parts = [b.text for b in response.content if b.type == "text"]
            return "\n".join(text_parts) if text_parts else "I couldn't generate a response."

        # Process tool use blocks
        if response.stop_reason == "tool_use":
            # Append the full assistant response (text + tool_use blocks)
            messages.append({"role": "assistant", "content": response.content})

            # Execute each tool call and build results
            tool_results = []
            for block in response.content:
                if block.type == "tool_use":
                    result = execute_tool(block.name, block.input, pipeline)
                    tool_results.append({
                        "type": "tool_result",
                        "tool_use_id": block.id,
                        "content": json.dumps(result, cls=_DecimalEncoder),
                    })

            messages.append({"role": "user", "content": tool_results})
            continue

        # Unexpected stop reason — extract whatever text we got
        text_parts = [b.text for b in response.content if b.type == "text"]
        if text_parts:
            return "\n".join(text_parts)
        return "I couldn't generate a response."

    # Rounds exhausted — ask model to answer with what it has
    messages.append({
        "role": "user",
        "content": (
            "You've used all available tool rounds. Please answer the user's "
            "question now with the information you've gathered so far."
        ),
    })
    response = client.messages.create(
        model=model,
        max_tokens=4096,
        system=JARVIS_SYSTEM_PROMPT,
        messages=messages,
    )
    text_parts = [b.text for b in response.content if b.type == "text"]
    return "\n".join(text_parts) if text_parts else "I couldn't generate a response."
