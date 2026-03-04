# STRICT MODE — Product Requirements Document

## Overview

STRICT MODE is a global toggle that changes the dashboard from "best-effort display" to "source-verified only" display. When enabled, every metric either proves its provenance chain back to source documents (Etsy CSV, bank statement, receipt PDF) or shows UNKNOWN with an explanation of what's missing.

---

## 1. UI Requirements

### 1.1 Global Toggle

| Property | Spec |
|----------|------|
| Location | Sidebar, below the navigation links, above the reload button |
| Component | `dbc.Switch` with label "STRICT MODE" |
| Default | OFF (current behavior preserved) |
| Persistence | Stored in `data/config.json` as `"strict_mode": true/false` |
| Visual | OFF = ghost outline, ON = red border glow + "STRICT" badge in sidebar header |
| Scope | Applies to ALL tabs simultaneously on toggle |

**Callback behavior**: Toggling STRICT MODE does NOT re-run the pipeline. It only changes how existing `Ledger` data is rendered. The `Ledger` already contains `Confidence` and `Provenance` for every metric — STRICT MODE simply enforces display rules based on those values.

### 1.2 UNKNOWN Display Rules

When STRICT MODE is ON, any metric with `Confidence` below `VERIFIED` or `DERIVED` displays as UNKNOWN:

| Confidence Level | STRICT MODE Display | Color |
|-----------------|---------------------|-------|
| VERIFIED | Normal value + green badge | `#2ecc71` |
| DERIVED | Normal value + blue badge "DERIVED" | `#3498db` |
| ESTIMATED | "UNKNOWN" + orange badge | `#f39c12` |
| UNKNOWN | "UNKNOWN" + red badge | `#e74c3c` |
| QUARANTINED | "QUARANTINED" + red pulsing badge | `#e74c3c` |

**UNKNOWN card format** (replaces the metric value):

```
┌─────────────────────────────────┐
│  TOTAL SHIPPING COST    [UNKNOWN]│
│                                  │
│  UNKNOWN                         │
│                                  │
│  Missing: buyer_paid_shipping,   │
│           label_cost_per_order   │
│                                  │
│  Last verified: never            │
│  Source: Etsy CSV rows only      │
└─────────────────────────────────┘
```

**Required fields in UNKNOWN display**:
- `missing_inputs`: List of specific data points needed to compute this metric. Derived from `Provenance.notes` field or a new `Provenance.missing_inputs: list[str]` field.
- `last_verified_range`: Date range of the most recent period where this metric WAS verifiable, or "never" if it has never been source-backed. Requires new field `Provenance.last_verified: str | None`.
- `source_types`: Which source systems contribute to this metric (from `Provenance.source_types`).

### 1.3 Click-to-Trace (Source Drilldown)

Every displayed number becomes clickable in STRICT MODE. Clicking opens a modal showing the source rows that produced that number.

**Modal layout**:

```
┌──────────────────────────────────────────────────┐
│  SOURCE TRACE: gross_sales = $487.23             │
│  Formula: abs(sum(Sale amounts))                 │
│  Confidence: VERIFIED                            │
│  Sources: 14 Etsy CSV rows                       │
│ ─────────────────────────────────────────────────│
│  ID              │ Date       │ Type │ Amount    │
│  etsy_a1b2c3d4   │ 2025-01-03 │ Sale │ $34.99   │
│  etsy_e5f6g7h8   │ 2025-01-05 │ Sale │ $22.50   │
│  etsy_i9j0k1l2   │ 2025-01-05 │ Sale │ $44.99   │
│  ... (scrollable, max 500 rows)                  │
│ ─────────────────────────────────────────────────│
│  Provenance chain:                               │
│  Etsy CSV row → JournalEntry → ComputationAgent  │
│  → MetricValue(gross_sales) → Ledger             │
│                                           [Close]│
└──────────────────────────────────────────────────┘
```

**Source row columns by source type**:

| Source | Columns Shown |
|--------|---------------|
| Etsy CSV | `dedup_hash` (truncated), Date, Type, Title, Amount, Net |
| Bank Statement | `dedup_hash`, Date, Description, Amount, Category |
| Receipt/Invoice | `invoice_id`, Date, Vendor, Amount, Source File |

**Implementation**:
- Each `kpi_card()` and `row_item()` wraps the value in an invisible `dcc.Link` or `html.Span(id={"type": "trace-trigger", "metric": name})`.
- A single pattern-matching callback handles all trace clicks: `@callback(Output("trace-modal", "is_open"), Input({"type": "trace-trigger", "metric": ALL}, "n_clicks"))`.
- The callback queries `Ledger.get(metric_name)` for `Provenance` and `Ledger.get_source_entries(metric_name)` (new method) for the journal entries.

### 1.4 Three Separate Views

STRICT MODE introduces a view selector (3-button radio group) at the top of the Financials tab:

```
[ Platform View ]  [ Bank View ]  [ Accounting View ]
```

#### Platform View (Etsy Truth)
- Shows ONLY metrics derivable from `TxnSource.ETSY_CSV` entries
- Metrics shown: `gross_sales`, `total_fees`, `total_refunds`, `total_shipping_cost`, `total_marketing`, `total_taxes`, `total_buyer_fees`, `total_payments`, `etsy_net_earned`, `etsy_balance`, `order_count`, `net_sales`
- Metrics hidden: All bank-derived metrics (`bank_*`, `real_profit`, `bank_cash_on_hand`, `bank_owner_draw_total`)
- Expense completeness section: HIDDEN (no bank data in this view)
- Reconciliation: HIDDEN
- Label: "What Etsy says happened"

#### Bank View (Cash Truth)
- Shows ONLY metrics derivable from `TxnSource.BANK_STMT` entries
- Metrics shown: `bank_total_deposits`, `bank_total_debits`, `bank_net_cash`, `bank_owner_draw_total`
- Expense completeness: Shows `bank_recorded_total` only (no receipt matching in this view)
- Etsy metrics: HIDDEN
- Reconciliation: HIDDEN (requires cross-source data)
- Label: "What the bank says happened"

#### Accounting View (Reconciled Truth)
- Shows ALL metrics, but ONLY when cross-source verification exists
- Reconciliation section: VISIBLE with match details
- Expense completeness: Full display (verified + gap + missing queue)
- Metrics that require both sources show normally if both sources present
- Metrics missing cross-verification show UNKNOWN with explanation
- `real_profit` and `bank_cash_on_hand`: Shown only if reconciliation passed
- Label: "What we can prove with documents"

**Default view**: Platform View (safest — pure Etsy data).

### 1.5 Visual Differentiation

When STRICT MODE is ON, the entire dashboard gets subtle visual cues:

| Element | Change |
|---------|--------|
| Sidebar header | Red "STRICT" badge next to "TJ's Software" |
| Card borders | Thin left-border color matches confidence (green/blue/orange/red) |
| Background | No change (keep dark theme) |
| KPI values | Font changes to monospace for all numbers |
| UNKNOWN cards | Dashed border instead of solid, muted background `#1a1a2e` |
| Tab headers | Show count of UNKNOWN metrics: "Financials (3 unknown)" |

---

## 2. Data Model Requirements

### 2.1 Provenance Enhancements

Current `Provenance` dataclass (in `accounting/models.py`):

```python
@dataclass(frozen=True)
class Provenance:
    formula: str
    source_entries: int = 0
    source_types: tuple[str, ...] = ()
    notes: str = ""
```

**Required additions**:

```python
@dataclass(frozen=True)
class Provenance:
    formula: str
    source_entries: int = 0
    source_types: tuple[str, ...] = ()
    notes: str = ""
    # ── New fields for STRICT MODE ──
    missing_inputs: tuple[str, ...] = ()       # What's needed but absent
    last_verified: str | None = None           # ISO date range or None
    source_entry_ids: tuple[str, ...] = ()     # dedup_hash values of contributing entries
    requires_sources: tuple[str, ...] = ()     # Which TxnSource types are needed
```

### 2.2 Ledger Enhancements

Current `Ledger` class (in `accounting/ledger.py`) needs new methods:

```python
class Ledger:
    # Existing methods preserved...

    def get_source_entries(self, metric_name: str) -> list[JournalEntry]:
        """Return the journal entries that contributed to this metric.
        Uses Provenance.source_entry_ids to look up entries by dedup_hash."""

    def get_missing_inputs(self, metric_name: str) -> list[str]:
        """Return list of missing input names for a metric."""

    def get_provenance_chain(self, metric_name: str) -> list[str]:
        """Return human-readable provenance chain:
        ['Etsy CSV row', 'JournalEntry', 'ComputationAgent', 'MetricValue', 'Ledger']"""

    def metrics_by_source(self, source: str) -> dict[str, MetricValue]:
        """Return only metrics whose Provenance.source_types includes the given source."""

    def unknown_count(self) -> int:
        """Count metrics with Confidence below DERIVED."""
```

### 2.3 Journal Enhancements

`Journal` class needs an index for fast dedup_hash lookups:

```python
class Journal:
    def __init__(self):
        self._entries: list[JournalEntry] = []
        self._hash_index: dict[str, JournalEntry] = {}  # NEW

    def add(self, entry: JournalEntry):
        self._entries.append(entry)
        self._hash_index[entry.dedup_hash] = entry       # NEW

    def get_by_hash(self, dedup_hash: str) -> JournalEntry | None:  # NEW
        return self._hash_index.get(dedup_hash)

    def get_by_hashes(self, hashes: Iterable[str]) -> list[JournalEntry]:  # NEW
        return [self._hash_index[h] for h in hashes if h in self._hash_index]
```

### 2.4 ComputationAgent Changes

The `ComputationAgent` must populate `Provenance.source_entry_ids` when computing each metric. For example:

```python
# When computing gross_sales:
sale_entries = journal.by_type(TxnType.SALE)
gross = sum(abs(e.amount) for e in sale_entries)
provenance = Provenance(
    formula="abs(sum(Sale amounts))",
    source_entries=len(sale_entries),
    source_types=("ETSY_CSV",),
    source_entry_ids=tuple(e.dedup_hash for e in sale_entries),  # NEW
    requires_sources=("ETSY_CSV",),                               # NEW
)
```

For metrics that CANNOT be computed (missing source data), the agent should set:

```python
provenance = Provenance(
    formula="buyer_paid_shipping / 0.065 — REMOVED (no source)",
    missing_inputs=("buyer_paid_shipping_raw", "actual_label_costs"),
    requires_sources=("ETSY_CSV", "SHIPPING_API"),
)
```

### 2.5 Config Addition

`data/config.json` gains one field:

```json
{
  "strict_mode": false
}
```

### 2.6 Compat Layer Changes

`accounting/compat.py` must publish the strict_mode flag and view-filtered metrics:

```python
setattr(mod, "strict_mode", config.get("strict_mode", False))
setattr(mod, "ledger_ref", ledger)  # Direct Ledger reference for trace queries
```

The `ledger_ref` global gives callbacks direct access to the Ledger for source-trace modal rendering without re-running the pipeline.

---

## 3. Error States and Messaging

### 3.1 Toggle Errors

| Scenario | Behavior |
|----------|----------|
| Toggle ON with no data loaded | Show toast: "Load data first. STRICT MODE requires a completed pipeline run." Toggle reverts to OFF. |
| Toggle ON during reload | Defer until reload completes. Show spinner on toggle. |
| Toggle OFF while viewing UNKNOWN cards | Cards revert to current behavior (show estimates/zeros). No data loss. |

### 3.2 Trace Modal Errors

| Scenario | Behavior |
|----------|----------|
| Click metric with 0 source entries | Modal shows: "No source entries found. This metric was computed without traceable inputs." |
| Click metric with >500 source entries | Modal shows first 500 with note: "Showing 500 of {N} source entries. Export full list with button below." + CSV export button. |
| Source entry hash not found in journal | Row shows: "Entry {hash} not found — may have been filtered during ingestion." Highlighted in orange. |
| Provenance chain incomplete | Show chain up to the break point with "?" for missing links. |

### 3.3 View Errors

| Scenario | Behavior |
|----------|----------|
| Switch to Bank View with no bank data | Show empty state: "No bank transactions loaded. Import bank statements in Data Hub to populate this view." |
| Switch to Accounting View with no receipts | Show partial state: Etsy + Bank metrics that are cross-verified, with UNKNOWN for receipt-dependent metrics. Banner: "Receipt coverage: {X}% of bank debits verified." |
| Switch to Platform View with no Etsy data | Show empty state: "No Etsy CSV data loaded. This shouldn't happen — check Data Hub." |

### 3.4 Confidence Downgrade Notifications

When STRICT MODE is ON and the pipeline produces metrics with lower confidence than a previous run:

| Scenario | Behavior |
|----------|----------|
| Metric was VERIFIED, now ESTIMATED | Toast warning: "{metric_name} downgraded from VERIFIED to ESTIMATED. Check source data." |
| Metric was DERIVED, now UNKNOWN | Toast warning with orange icon. |
| Metric newly QUARANTINED | Red toast that persists until dismissed: "{metric_name} QUARANTINED — validation failed." |

### 3.5 Messaging Tone

All STRICT MODE messages use factual, non-judgmental language:

- YES: "3 metrics cannot be verified — missing receipt data"
- NO: "WARNING: Your data is incomplete!"
- YES: "UNKNOWN — requires: buyer_paid_shipping, label_costs"
- NO: "ERROR: This number might be wrong"
- YES: "Last verified: Jan 2025 – Mar 2025 (Etsy CSV only)"
- NO: "This hasn't been checked in a while"

---

## 4. Implementation Phases

### Phase 1: Foundation (Data Model + Toggle)
1. Add new `Provenance` fields to `accounting/models.py`
2. Add `Journal._hash_index` and lookup methods
3. Add `Ledger.get_source_entries()`, `metrics_by_source()`, `unknown_count()`
4. Update `ComputationAgent` to populate `source_entry_ids` and `requires_sources`
5. Add `strict_mode` to config.json
6. Add sidebar toggle + callback (toggles a global, triggers tab re-render)

### Phase 2: UNKNOWN Display
1. Modify `kpi_card()` to accept `strict_mode` param and render UNKNOWN format
2. Modify `row_item()` to render UNKNOWN with missing inputs
3. Modify `_verification_badge()` to read actual `Confidence` from Ledger instead of hardcoded status
4. Add tab-level UNKNOWN counts to tab headers
5. Wire all Financials/Overview metrics to read Confidence from Ledger

### Phase 3: Source Trace Modal
1. Create `trace-modal` component (single global modal)
2. Add pattern-matching callback for all `trace-trigger` clicks
3. Implement source entry table rendering (Etsy/Bank/Receipt column sets)
4. Implement provenance chain visualization
5. Add CSV export for large entry sets

### Phase 4: Three Views
1. Add view selector radio group to Financials tab
2. Implement `metrics_by_source()` filtering for Platform/Bank views
3. Create view-specific layouts (which sections show/hide)
4. Handle empty-state rendering for each view
5. Persist selected view in session (not config — it's a UI preference)

---

## 5. Acceptance Criteria

### Must Have
- [ ] STRICT MODE toggle in sidebar, persisted in config.json
- [ ] UNKNOWN display for all metrics with Confidence < DERIVED when toggle is ON
- [ ] Missing inputs listed on every UNKNOWN card
- [ ] Click any metric value → modal showing source journal entries with dedup_hash IDs
- [ ] Three views on Financials tab: Platform, Bank, Accounting
- [ ] All existing tests pass (272 tests) with STRICT MODE OFF
- [ ] New tests for STRICT MODE rendering logic

### Should Have
- [ ] Last-verified date range on UNKNOWN cards
- [ ] Confidence downgrade toast notifications
- [ ] Tab-header UNKNOWN counts
- [ ] CSV export from trace modal for >500 entries
- [ ] Provenance chain visualization in trace modal

### Nice to Have
- [ ] Keyboard shortcut to toggle STRICT MODE (Ctrl+Shift+S)
- [ ] URL query param `?strict=1` to force STRICT MODE on load
- [ ] Per-metric "verify" button that opens relevant upload dialog
- [ ] Color-coded left borders on cards matching confidence level

---

## 6. Non-Goals

- STRICT MODE does NOT change how the pipeline computes metrics. It only changes display.
- STRICT MODE does NOT add new data sources. It surfaces what's missing.
- STRICT MODE does NOT replace the governance agent. Governance prevents bad code patterns; STRICT MODE prevents bad display patterns.
- STRICT MODE does NOT affect the `/api/*` endpoints. API consumers get raw Ledger data regardless of toggle state.
