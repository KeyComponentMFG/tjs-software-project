"""Pure helper functions shared across the Etsy dashboard.

These functions have NO runtime state dependencies — they take inputs
and return outputs without reading or mutating any globals.
"""
from __future__ import annotations

from typing import Optional

import pandas as pd


# ---------------------------------------------------------------------------
# Money parsing
# ---------------------------------------------------------------------------

def parse_money(val, warnings: list | None = None):
    """Parse a dollar-amount string (e.g. '$1,234.56') to a float.

    Parameters
    ----------
    val : str | float | None
        The raw value from a CSV cell.
    warnings : list, optional
        If provided, unparseable values are appended here instead of silently
        swallowed.  When called from ``etsy_dashboard.py`` the module-level
        ``_parse_warnings`` list is passed in so existing logging still works.

    Returns
    -------
    float
    """
    if pd.isna(val) or val == "--" or val == "":
        return 0.0
    val = str(val).replace("$", "").replace(",", "").replace('"', "")
    try:
        return float(val)
    except Exception:
        if warnings is not None:
            warnings.append(f"Could not parse money value: {val!r}")
        return 0.0


# ---------------------------------------------------------------------------
# Product-name normalisation
# ---------------------------------------------------------------------------

def _normalize_product_name(name: str, aliases: dict | None = None) -> str:
    """Normalize Etsy product names that get truncated differently in CSVs.

    If *aliases* dict is provided, maps known variants to canonical names.
    Otherwise strips trailing ``'...'`` and everything after ``' | '``
    separator.
    """
    if not isinstance(name, str):
        return name
    name = name.rstrip(".")  # strip trailing "..."
    if aliases:
        for canonical, variants in aliases.items():
            if name in variants or name == canonical:
                return canonical
    # Fallback for unmapped names: strip after |
    if " | " in name:
        name = name.split(" | ")[0]
    return name.strip()


def _merge_product_prefixes(
    series: pd.Series,
    aliases: dict | None = None,
) -> pd.Series:
    """Merge truncated product names that are prefixes of known shorter
    canonical names (e.g. 'Ski Gondola Table Lamp Rustic Cabin Deco' merges
    into 'Ski Gondola Table Lamp' if that canonical name exists).

    Parameters
    ----------
    series : pd.Series
        A Series of product name strings.
    aliases : dict, optional
        Listing-alias mapping from CONFIG.  When provided the alias lookup
        is used instead of the prefix-matching heuristic.
    """
    if aliases:
        return series.apply(lambda n: _normalize_product_name(n, aliases=aliases))
    canonical = sorted(series.unique(), key=len)  # shortest first
    mapping: dict[str, str] = {}
    for name in canonical:
        for shorter in canonical:
            if shorter != name and len(shorter) >= 10 and name.startswith(shorter):
                mapping[name] = shorter
                break
    if mapping:
        return series.map(lambda n: mapping.get(n, n))
    return series
