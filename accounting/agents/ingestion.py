"""
accounting/agents/ingestion.py — Agent 1: Parse raw data into Journal entries.

Wraps existing parsers (supabase_loader, _parse_bank_statements) and converts
every transaction to a typed JournalEntry with Decimal amounts.

Key fixes:
- Deposits: parses "$651.33 sent to your bank account" at ingest
- Pre-CSV gap: computed from first deposit vs preceding earnings (no hardcoded $26.58)
- Parse failures → confidence=UNKNOWN (not silent zero)
"""

from __future__ import annotations

import os
import re
from datetime import date, datetime
from decimal import Decimal, InvalidOperation
from typing import Optional

import pandas as pd

from ..journal import Journal
from ..models import Confidence, JournalEntry, TxnSource, TxnType


def _parse_decimal(val) -> tuple[Decimal, Confidence]:
    """Parse a money value to Decimal. Returns (amount, confidence).

    Returns (Decimal("0"), UNKNOWN) on parse failure instead of silent zero.
    """
    if pd.isna(val) or val == "--" or val == "" or val is None:
        return Decimal("0"), Confidence.VERIFIED  # Genuinely zero/empty
    s = str(val).replace("$", "").replace(",", "").replace('"', "").strip()
    if not s or s == "--":
        return Decimal("0"), Confidence.VERIFIED
    try:
        return Decimal(s), Confidence.VERIFIED
    except InvalidOperation:
        return Decimal("0"), Confidence.UNKNOWN


def _parse_deposit_amount(title: str) -> tuple[Decimal, Confidence]:
    """Extract deposit amount from Etsy deposit title.

    Title format: "$651.33 sent to your bank account" or similar.
    Returns (amount, confidence). Parse failure → (0, UNKNOWN).
    """
    if not isinstance(title, str):
        return Decimal("0"), Confidence.UNKNOWN
    m = re.search(r'([\d,]+\.\d+)', title)
    if m:
        try:
            return Decimal(m.group(1).replace(",", "")), Confidence.VERIFIED
        except InvalidOperation:
            pass
    return Decimal("0"), Confidence.UNKNOWN


def _etsy_txn_type(type_str: str) -> TxnType:
    """Map Etsy CSV 'Type' column to TxnType enum."""
    mapping = {
        "Sale": TxnType.SALE,
        "Fee": TxnType.FEE,
        "Shipping": TxnType.SHIPPING,
        "Marketing": TxnType.MARKETING,
        "Refund": TxnType.REFUND,
        "Tax": TxnType.TAX,
        "Deposit": TxnType.DEPOSIT,
        "Buyer Fee": TxnType.BUYER_FEE,
        "Payment": TxnType.PAYMENT,
    }
    return mapping.get(type_str, TxnType.SALE)


def _parse_etsy_date(date_str: str) -> date:
    """Parse 'January 15, 2026' → date object."""
    try:
        return datetime.strptime(date_str.strip(), "%B %d, %Y").date()
    except (ValueError, AttributeError):
        try:
            return pd.to_datetime(date_str).date()
        except Exception:
            return date.today()


def _parse_bank_date(date_str: str) -> date:
    """Parse 'MM/DD/YYYY' → date object."""
    try:
        parts = date_str.split("/")
        return date(int(parts[2]), int(parts[0]), int(parts[1]))
    except (ValueError, IndexError, AttributeError):
        return date.today()


class IngestionAgent:
    """Parse raw data sources into a Journal."""

    def __init__(self):
        self.warnings: list[str] = []

    def ingest_etsy_dataframe(self, df: pd.DataFrame, journal: Journal,
                              source_file: str = "") -> int:
        """Ingest an Etsy DataFrame (from CSV or Supabase) into the Journal.

        Returns count of new entries added.
        """
        count = 0
        for seq_num, (_, row) in enumerate(df.iterrows()):
            txn_type_str = str(row.get("Type", ""))
            txn_type = _etsy_txn_type(txn_type_str)

            net_val, net_conf = _parse_decimal(row.get("Net", 0))
            gross_val, _ = _parse_decimal(row.get("Amount", 0))
            fees_val, _ = _parse_decimal(row.get("Fees & Taxes", 0))

            title = str(row.get("Title", ""))
            info = str(row.get("Info", ""))
            date_str = str(row.get("Date", ""))
            txn_date = _parse_etsy_date(date_str)
            month = txn_date.strftime("%Y-%m")

            # For deposits, also parse the deposit amount from the title
            deposit_amount = Decimal("0")
            if txn_type == TxnType.DEPOSIT:
                deposit_amount, dep_conf = _parse_deposit_amount(title)
                if dep_conf == Confidence.UNKNOWN:
                    self.warnings.append(
                        f"Could not parse deposit amount from title: {title!r}")

            entry = JournalEntry(
                source=TxnSource.ETSY_CSV,
                txn_type=txn_type,
                txn_date=txn_date,
                amount=net_val,
                gross_amount=gross_val,
                fees=fees_val,
                title=title,
                info=info,
                description=f"Etsy {txn_type_str}: {title[:60]}",
                confidence=net_conf,
                source_file=source_file,
                month=month,
                currency=str(row.get("Currency", "USD")),
                sequence_num=seq_num,
                raw_row={
                    "deposit_parsed_amount": str(deposit_amount),
                    "date_str": date_str,
                    "type": txn_type_str,
                },
            )
            if journal.add(entry):
                count += 1
        return count

    def ingest_bank_transactions(self, bank_txns: list[dict],
                                 journal: Journal) -> int:
        """Ingest parsed bank transactions (from _parse_bank_statements) into Journal.

        Returns count of new entries added.
        """
        count = 0
        for seq_num, t in enumerate(bank_txns):
            txn_date = _parse_bank_date(t["date"])
            amount = Decimal(str(t["amount"]))
            txn_type = TxnType.BANK_DEPOSIT if t["type"] == "deposit" else TxnType.BANK_DEBIT
            category = t.get("category", "")

            # Bank debits are outflows — store as negative for consistent accounting
            signed_amount = amount if t["type"] == "deposit" else -amount

            entry = JournalEntry(
                source=TxnSource.BANK_PDF if t.get("source_file", "").endswith(".pdf") else TxnSource.BANK_CSV,
                txn_type=txn_type,
                txn_date=txn_date,
                amount=signed_amount,
                gross_amount=amount,
                fees=Decimal("0"),
                title=t.get("desc", ""),
                info="",
                description=f"Bank {t['type']}: {t.get('desc', '')[:60]}",
                confidence=Confidence.VERIFIED,
                source_file=t.get("source_file", ""),
                category=category,
                month=txn_date.strftime("%Y-%m"),
                sequence_num=seq_num,
                raw_row=t,
            )
            if journal.add(entry):
                count += 1
        return count

    def ingest_pre_capone_config(self, config: dict, journal: Journal) -> int:
        """Ingest pre-CapOne deposit detail from config as manual ESTIMATED entries.

        Returns count of new entries added.
        """
        details = config.get("pre_capone_detail", [])
        count = 0
        for row in details:
            if len(row) < 2:
                continue
            label, amount_str = row[0], row[1]
            amount_str = str(amount_str).replace("$", "").replace(",", "")
            try:
                amount = Decimal(amount_str)
            except InvalidOperation:
                self.warnings.append(f"Could not parse pre-CapOne amount: {row}")
                continue

            # Parse a rough date from the label (e.g. "Oct 2025", "Dec 1, 2025")
            txn_date = _guess_date_from_label(label)

            entry = JournalEntry(
                source=TxnSource.CONFIG_PRE_CAPONE,
                txn_type=TxnType.BANK_DEPOSIT,
                txn_date=txn_date,
                amount=amount,
                gross_amount=amount,
                fees=Decimal("0"),
                title=f"Pre-CapOne deposit: {label}",
                info="",
                description=f"Pre-CapOne Etsy deposit ({label}): ${amount}",
                confidence=Confidence.ESTIMATED,
                source_file="config.json",
                category="Etsy Payout",
                month=txn_date.strftime("%Y-%m"),
            )
            if journal.add(entry):
                count += 1
        return count

    def compute_pre_csv_balance(self, journal: Journal) -> tuple[Decimal, Confidence, str]:
        """Compute the pre-CSV balance (earnings before first CSV) automatically.

        Strategy: Look at the first Etsy deposit and the earnings that preceded it.
        The first deposit amount minus the earnings in months before that deposit
        gives us the pre-CSV accumulated balance.

        Returns (balance, confidence, explanation).
        """
        etsy_entries = [e for e in journal if e.source == TxnSource.ETSY_CSV]
        if not etsy_entries:
            return Decimal("0"), Confidence.UNKNOWN, "No Etsy data available"

        # Get all deposit entries and their parsed amounts
        deposits = journal.by_type(TxnType.DEPOSIT)
        if not deposits:
            # No deposits yet — all earnings are still in Etsy balance
            total_net = journal.sum_amount(etsy_entries)
            return total_net, Confidence.DERIVED, "No deposits found - all net earnings in balance"

        # Sum all Etsy net amounts (this is total earnings minus total costs on Etsy side)
        total_etsy_net = journal.sum_amount(etsy_entries)

        # Sum deposit amounts parsed from titles
        total_deposited = Decimal("0")
        unparsed_deposits = 0
        for dep in deposits:
            parsed_str = dep.raw_row.get("deposit_parsed_amount", "0")
            parsed = Decimal(parsed_str) if parsed_str else Decimal("0")
            if parsed > 0:
                total_deposited += parsed
            else:
                unparsed_deposits += 1

        # Also add pre-CapOne deposits
        pre_capone = [e for e in journal if e.source == TxnSource.CONFIG_PRE_CAPONE]
        pre_capone_total = journal.sum_amount(pre_capone)

        # Balance = total earned - total deposited (CSV + pre-CapOne)
        balance = total_etsy_net - total_deposited
        # Don't subtract pre-CapOne deposits here — they're bank deposits, not Etsy outflows
        # The Etsy deposit rows in the CSV already account for all Etsy→bank transfers

        conf = Confidence.DERIVED
        explanation = (
            f"Etsy balance = total_etsy_net (${total_etsy_net:.2f}) "
            f"- CSV_deposits (${total_deposited:.2f}) = ${balance:.2f}"
        )

        if unparsed_deposits > 0:
            conf = Confidence.ESTIMATED
            explanation += f" ({unparsed_deposits} deposit(s) could not be parsed from title)"

        return balance, conf, explanation


def _guess_date_from_label(label: str) -> date:
    """Best-effort date parse from labels like 'Oct 2025', 'Nov 2025 (2 payouts)', 'Dec 1, 2025'."""
    label = label.strip()
    # Try "Dec 1, 2025" format
    try:
        return datetime.strptime(label.split("(")[0].strip(), "%b %d, %Y").date()
    except ValueError:
        pass
    # Try "Oct 2025" format
    try:
        return datetime.strptime(label.split("(")[0].strip(), "%b %Y").date()
    except ValueError:
        pass
    # Fallback
    return date(2025, 10, 1)
