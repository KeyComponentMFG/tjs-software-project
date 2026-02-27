"""Scan Etsy CSVs for product-name variants and suggest listing_aliases."""
import argparse, json
from collections import defaultdict
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parent
DATA = ROOT / "data"
CSV_DIR = DATA / "etsy_statements"
CONFIG = DATA / "config.json"
PREFIX = "Transaction fee: "

# ── 1. Load all Etsy CSVs and extract product names ─────────────────
def extract_product_names() -> list[str]:
    names = set()
    for f in sorted(CSV_DIR.glob("*.csv")):
        df = pd.read_csv(f)
        fees = df[df["Title"].str.startswith(PREFIX, na=False)]
        for t in fees["Title"]:
            raw = t[len(PREFIX):]               # strip prefix
            raw = raw.rstrip(".")               # strip trailing "..."
            if raw.lower() == "shipping":
                continue
            names.add(raw)
    return sorted(names)

# ── 2. Group by shared prefix (first 25 chars) ──────────────────────
def group_by_prefix(names: list[str], width: int = 25) -> dict[str, list[str]]:
    buckets: dict[str, list[str]] = defaultdict(list)
    for n in names:
        key = n[:width].rstrip()
        buckets[key].append(n)
    return {k: v for k, v in buckets.items() if len(v) > 1}

# ── 3. Pick canonical name (shortest, or text before first " | ") ───
def canonical(variants: list[str]) -> str:
    shortest = min(variants, key=len)
    if " | " in shortest:
        shortest = shortest.split(" | ")[0].strip()
    return shortest

# ── 4. Build the listing_aliases dict ────────────────────────────────
def build_aliases(groups: dict[str, list[str]]) -> dict[str, list[str]]:
    aliases: dict[str, list[str]] = {}
    for _key, variants in sorted(groups.items()):
        canon = canonical(variants)
        others = [v for v in variants if v != canon]
        if others:
            aliases[canon] = others
    return aliases

# ── main ─────────────────────────────────────────────────────────────
def main():
    ap = argparse.ArgumentParser(description="Build listing aliases from Etsy CSVs")
    ap.add_argument("--write", action="store_true", help="Write aliases into config.json")
    args = ap.parse_args()

    names = extract_product_names()
    groups = group_by_prefix(names)

    print(f"\n=== Listing Alias Groups ({len(groups)} groups from {len(names)} unique names) ===")
    aliases = {}
    for key, variants in sorted(groups.items()):
        canon = canonical(variants)
        others = [v for v in variants if v != canon]
        if not others:
            continue
        aliases[canon] = others
        print(f'\nGroup: "{canon}"')
        for v in variants:
            print(f"  - {v}")

    print("\n=== Suggested listing_aliases for config.json ===")
    print(json.dumps({"listing_aliases": aliases}, indent=2))

    if args.write:
        cfg = json.loads(CONFIG.read_text(encoding="utf-8"))
        cfg["listing_aliases"] = aliases
        CONFIG.write_text(json.dumps(cfg, indent=2) + "\n", encoding="utf-8")
        print(f"\n[OK] Wrote listing_aliases ({len(aliases)} entries) into {CONFIG}")

if __name__ == "__main__":
    main()
