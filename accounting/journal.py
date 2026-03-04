"""
accounting/journal.py — Append-only transaction store.

The Journal is the single source of truth. All metrics are derived from it.
Entries are deduplicated by hash and stored with Decimal precision.
"""

from __future__ import annotations

from collections import defaultdict
from datetime import date
from decimal import Decimal
from typing import Iterator, Optional

from .models import Confidence, JournalEntry, TxnSource, TxnType


class Journal:
    """Append-only, deduplicated transaction store."""

    def __init__(self):
        self._entries: list[JournalEntry] = []
        self._hashes: set[str] = set()
        self._hash_index: dict[str, JournalEntry] = {}
        self._by_type: dict[TxnType, list[JournalEntry]] = defaultdict(list)
        self._by_source: dict[TxnSource, list[JournalEntry]] = defaultdict(list)
        self._by_month: dict[str, list[JournalEntry]] = defaultdict(list)

    def add(self, entry: JournalEntry) -> bool:
        """Add an entry. Returns False if duplicate (already seen hash)."""
        h = entry.dedup_hash
        if h in self._hashes:
            return False
        self._hashes.add(h)
        self._hash_index[h] = entry
        self._entries.append(entry)
        self._by_type[entry.txn_type].append(entry)
        self._by_source[entry.source].append(entry)
        if entry.month:
            self._by_month[entry.month].append(entry)
        return True

    def add_many(self, entries: list[JournalEntry]) -> int:
        """Add multiple entries. Returns count of new (non-duplicate) entries."""
        return sum(1 for e in entries if self.add(e))

    # ── Query methods ──

    @property
    def entries(self) -> list[JournalEntry]:
        return list(self._entries)

    def __len__(self) -> int:
        return len(self._entries)

    def __iter__(self) -> Iterator[JournalEntry]:
        return iter(self._entries)

    def by_type(self, txn_type: TxnType) -> list[JournalEntry]:
        return list(self._by_type.get(txn_type, []))

    def by_source(self, source: TxnSource) -> list[JournalEntry]:
        return list(self._by_source.get(source, []))

    def by_month(self, month: str) -> list[JournalEntry]:
        return list(self._by_month.get(month, []))

    def by_category(self, category: str) -> list[JournalEntry]:
        return [e for e in self._entries if e.category == category]

    def by_category_prefix(self, prefix: str) -> list[JournalEntry]:
        return [e for e in self._entries if e.category.startswith(prefix)]

    def sum_amount(self, entries: Optional[list[JournalEntry]] = None) -> Decimal:
        """Sum the net amount of given entries (or all entries if None)."""
        target = entries if entries is not None else self._entries
        return sum((e.amount for e in target), Decimal("0"))

    def sum_abs_amount(self, entries: Optional[list[JournalEntry]] = None) -> Decimal:
        """Sum the absolute net amount of given entries."""
        target = entries if entries is not None else self._entries
        return sum((abs(e.amount) for e in target), Decimal("0"))

    def count(self, entries: Optional[list[JournalEntry]] = None) -> int:
        target = entries if entries is not None else self._entries
        return len(target)

    def months_sorted(self) -> list[str]:
        """Return sorted list of all months with entries."""
        return sorted(self._by_month.keys())

    def etsy_entries(self) -> list[JournalEntry]:
        """All entries from Etsy CSVs."""
        return self.by_source(TxnSource.ETSY_CSV)

    def bank_entries(self) -> list[JournalEntry]:
        """All entries from bank statements (PDF + CSV)."""
        return (self.by_source(TxnSource.BANK_PDF)
                + self.by_source(TxnSource.BANK_CSV))

    def bank_deposits(self) -> list[JournalEntry]:
        return self.by_type(TxnType.BANK_DEPOSIT)

    def bank_debits(self) -> list[JournalEntry]:
        return self.by_type(TxnType.BANK_DEBIT)

    def etsy_deposits(self) -> list[JournalEntry]:
        return self.by_type(TxnType.DEPOSIT)

    def filter(self, **kwargs) -> list[JournalEntry]:
        """Filter entries by arbitrary attributes.

        Example: journal.filter(txn_type=TxnType.FEE, month="2025-12")
        """
        results = self._entries
        for attr, val in kwargs.items():
            results = [e for e in results if getattr(e, attr, None) == val]
        return results

    def title_contains(self, txn_type: TxnType, substring: str,
                       case_sensitive: bool = False) -> list[JournalEntry]:
        """Filter entries of a type by title substring match."""
        entries = self.by_type(txn_type)
        if case_sensitive:
            return [e for e in entries if substring in e.title]
        sub_lower = substring.lower()
        return [e for e in entries if sub_lower in e.title.lower()]

    def get_by_hash(self, dedup_hash: str) -> Optional[JournalEntry]:
        """Look up a single entry by its dedup_hash. O(1)."""
        return self._hash_index.get(dedup_hash)

    def get_by_hashes(self, hashes) -> list[JournalEntry]:
        """Look up multiple entries by dedup_hash. Returns found entries in order."""
        return [self._hash_index[h] for h in hashes if h in self._hash_index]

    def clear(self):
        """Reset the journal (for rebuilds)."""
        self._entries.clear()
        self._hashes.clear()
        self._hash_index.clear()
        self._by_type.clear()
        self._by_source.clear()
        self._by_month.clear()
