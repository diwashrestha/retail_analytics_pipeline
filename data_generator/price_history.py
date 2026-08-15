"""
Einkaufpark DE — SCD2 Price History Generator
==============================================
Produces dim_products_scd2.csv: a Slowly-Changing-Dimension Type 2 table
where each product has multiple rows representing price changes over time.

Each row carries effective_from / effective_to dates. The pipeline performs
range-joins on (product_id, order_date BETWEEN effective_from AND effective_to)
to recover the list price that applied at sale time.

Price event model (per product):
  - Initial list price drawn from [price_min, price_max] at start_date.
  - 2-8 price events per year on average:
      ~30% temporary promos     — 7-day discount of 15-30%, then revert
      ~70% permanent adjustments — ±2 to +10% (slight upward inflation bias)
  - Minimum 8-day gap between events (so promos never overlap).

Success criteria — verified at end of run:

  S1. Coverage           — every product in the catalogue has ≥1 SCD2 row.
  S2. Continuity         — for each product, intervals form a contiguous
                           sequence with no gaps and no overlaps.
  S3. Bounded            — first interval starts at start_date, last ends
                           at end_date (full coverage of the requested range).
  S4. Promo correctness  — every is_promo_price=True row spans ≤8 days.

The transaction generator consumes this SCD2 table through PriceIndex, so the
unit price on every valid sale is the effective shelf price for its order date.
Only deliberately injected DQ rows may differ from the catalogue price.

What this module deliberately does NOT do:
  - Generate price events for products not in PRODUCTS.
"""

from __future__ import annotations

import argparse
import bisect
import csv
import sys
from datetime import datetime, timedelta
from pathlib import Path
from random import Random

from data_generator.generator import (
    VAT_BY_CATEGORY,
    product_id_for,
)

# Reuse infrastructure from the main generator.
from data_generator.product_catalogue import PRODUCTS

# ═══════════════════════════════════════════════════════════════════════════
# Constants
# ═══════════════════════════════════════════════════════════════════════════

PROMO_PROB = 0.30  # fraction of events that are promos
PROMO_DURATION = 7  # days
PROMO_DISCOUNT = (0.70, 0.85)  # multiplier range (15-30% off)
PERMANENT_CHANGE = (-0.06, 0.10)  # multiplier range (slight upward bias)
MIN_EVENT_GAP_DAYS = 8
EVENTS_PER_YEAR = (2, 8)  # range


SCD2_COLUMNS = [
    "product_id",
    "product_name",
    "category",
    "subcategory",
    "default_brand",
    "effective_price_eur",
    "effective_from",
    "effective_to",
    "is_promo_price",
    "unit",
    "vat_rate",
]


# ═══════════════════════════════════════════════════════════════════════════
# Core: build intervals for one product
# ═══════════════════════════════════════════════════════════════════════════


def pick_change_dates(
    rng: Random, start: datetime, end: datetime, n_target: int
) -> list[datetime]:
    """Sample change dates within [start+8, end-1] with min 8-day spacing.

    Returns dates sorted ascending. May return fewer than n_target if the
    window is too narrow for the spacing constraint.
    """
    total_days = (end - start).days
    if total_days < MIN_EVENT_GAP_DAYS + 1 or n_target == 0:
        return []

    # Sample from valid offset range, enforce gap, take what fits.
    max_candidates = min(n_target * 3, (total_days - MIN_EVENT_GAP_DAYS - 1))
    if max_candidates < 1:
        return []
    candidates = sorted(
        rng.sample(range(MIN_EVENT_GAP_DAYS, total_days - 1), k=max_candidates)
    )

    accepted: list[int] = []
    last = -MIN_EVENT_GAP_DAYS - 1
    for d in candidates:
        if d - last >= MIN_EVENT_GAP_DAYS:
            accepted.append(d)
            last = d
            if len(accepted) >= n_target:
                break

    return [start + timedelta(days=d) for d in accepted]


def build_intervals(
    rng: Random, p_min: float, p_max: float, start: datetime, end: datetime
) -> list[tuple[datetime, datetime, float, bool]]:
    """Build SCD2 intervals for one product.

    Returns list of (interval_start, interval_end, price, is_promo) tuples,
    contiguous and covering [start, end] inclusive.
    """
    current_price = round(rng.uniform(p_min, p_max), 2)
    total_days = (end - start).days
    n_years = max(total_days / 365.25, 0.5)
    n_events = max(0, int(rng.uniform(*EVENTS_PER_YEAR) * n_years))

    change_dates = pick_change_dates(rng, start, end, n_events)
    if not change_dates:
        return [(start, end, current_price, False)]

    intervals: list[tuple[datetime, datetime, float, bool]] = []
    period_start = start

    for change_date in change_dates:
        is_promo = rng.random() < PROMO_PROB

        if is_promo:
            promo_price = round(current_price * rng.uniform(*PROMO_DISCOUNT), 2)
            promo_end = min(change_date + timedelta(days=PROMO_DURATION - 1), end)

            # Close current non-promo period (if it has any days in it).
            if period_start < change_date:
                intervals.append(
                    (
                        period_start,
                        change_date - timedelta(days=1),
                        current_price,
                        False,
                    )
                )
            # Promo period.
            intervals.append((change_date, promo_end, promo_price, True))
            # Resume at the same pre-promo price after the promo ends.
            period_start = promo_end + timedelta(days=1)
        else:
            # Permanent inflation/rebalance.
            if period_start < change_date:
                intervals.append(
                    (
                        period_start,
                        change_date - timedelta(days=1),
                        current_price,
                        False,
                    )
                )
            new_price = round(current_price * (1 + rng.uniform(*PERMANENT_CHANGE)), 2)
            # Clamp to a wide multiple of catalogue range — prevents runaway drift.
            current_price = max(p_min * 0.5, min(new_price, p_max * 1.5))
            period_start = change_date

    # Close the final period.
    if period_start <= end:
        intervals.append((period_start, end, current_price, False))

    return intervals


# ═══════════════════════════════════════════════════════════════════════════
# Write
# ═══════════════════════════════════════════════════════════════════════════


def write_scd2(rng: Random, start: datetime, end: datetime, output_dir: Path) -> int:
    """Generate dim_products_scd2.csv. Returns total row count."""
    path = output_dir / "dim_products_scd2.csv"
    n_rows = 0

    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=SCD2_COLUMNS)
        w.writeheader()
        for p in PRODUCTS:
            cat, subcat, name, brand = p[0], p[1], p[2], p[3]
            p_min, p_max = p[5], p[6]
            unit = p[9]
            pid = product_id_for(name)
            vat = VAT_BY_CATEGORY.get(cat, 0.19)

            for s, e, price, is_promo in build_intervals(rng, p_min, p_max, start, end):
                w.writerow(
                    {
                        "product_id": pid,
                        "product_name": name,
                        "category": cat,
                        "subcategory": subcat,
                        "default_brand": brand,
                        "effective_price_eur": price,
                        "effective_from": s.strftime("%Y-%m-%d"),
                        "effective_to": e.strftime("%Y-%m-%d"),
                        "is_promo_price": is_promo,
                        "unit": unit,
                        "vat_rate": vat,
                    }
                )
                n_rows += 1

    print(
        f"  dim_products_scd2.csv  : {n_rows:,} rows "
        f"({n_rows / len(PRODUCTS):.1f} intervals/product on average)"
    )
    return n_rows


class PriceIndex:
    """In-memory product/date lookup for generated SCD2 prices."""

    def __init__(
        self, rows_by_product: dict[str, list[tuple[datetime, datetime, float]]]
    ):
        self._rows = rows_by_product
        self._starts = {
            pid: [row[0] for row in rows] for pid, rows in rows_by_product.items()
        }

    @classmethod
    def from_csv(cls, path: Path) -> PriceIndex:
        rows: dict[str, list[tuple[datetime, datetime, float]]] = {}
        with open(path, encoding="utf-8") as f:
            for row in csv.DictReader(f):
                pid = row["product_id"]
                rows.setdefault(pid, []).append(
                    (
                        datetime.strptime(row["effective_from"], "%Y-%m-%d"),
                        datetime.strptime(row["effective_to"], "%Y-%m-%d"),
                        float(row["effective_price_eur"]),
                    )
                )
        for pid in rows:
            rows[pid].sort(key=lambda value: value[0])
        return cls(rows)

    def get_price(self, product_id: str, order_date: datetime) -> float:
        intervals = self._rows.get(product_id)
        if not intervals:
            raise KeyError(f"No SCD2 price history for {product_id}")
        starts = self._starts[product_id]
        idx = bisect.bisect_right(starts, order_date) - 1
        if idx < 0:
            raise KeyError(f"No price for {product_id} on {order_date:%Y-%m-%d}")
        effective_from, effective_to, price = intervals[idx]
        if not effective_from <= order_date <= effective_to:
            raise KeyError(
                f"SCD2 coverage gap for {product_id} on {order_date:%Y-%m-%d}"
            )
        return price

    def __call__(self, product_id: str, order_date: datetime) -> float:
        return self.get_price(product_id, order_date)


# ═══════════════════════════════════════════════════════════════════════════
# Validation — success criteria from the docstring, enforced
# ═══════════════════════════════════════════════════════════════════════════


def _load_scd2(path: Path) -> dict[str, list[dict]]:
    """Group SCD2 rows by product_id, sorted by effective_from."""
    by_product: dict[str, list[dict]] = {}
    with open(path, encoding="utf-8") as f:
        for row in csv.DictReader(f):
            by_product.setdefault(row["product_id"], []).append(row)
    for pid in by_product:
        by_product[pid].sort(key=lambda r: r["effective_from"])
    return by_product


def check_coverage(by_product: dict[str, list[dict]]) -> tuple[bool, str]:
    """S1: every product in PRODUCTS appears in SCD2 table."""
    expected = {product_id_for(p[2]) for p in PRODUCTS}
    found = set(by_product.keys())
    missing = expected - found
    if missing:
        return False, f"FAIL: {len(missing)} products missing from SCD2"
    return True, f"{len(found)}/{len(expected)} products covered"


def check_continuity(by_product: dict[str, list[dict]]) -> tuple[bool, str]:
    """S2: intervals are contiguous, no gaps, no overlaps."""
    n_gaps = n_overlaps = 0
    for pid, rows in by_product.items():
        for prev, curr in zip(rows, rows[1:]):
            prev_to = datetime.strptime(prev["effective_to"], "%Y-%m-%d")
            curr_fr = datetime.strptime(curr["effective_from"], "%Y-%m-%d")
            gap = (curr_fr - prev_to).days
            if gap > 1:
                n_gaps += 1
            elif gap < 1:
                n_overlaps += 1
    if n_gaps or n_overlaps:
        return False, f"FAIL: {n_gaps} gaps, {n_overlaps} overlaps"
    return True, "all intervals contiguous"


def check_bounded(
    by_product: dict[str, list[dict]], start: datetime, end: datetime
) -> tuple[bool, str]:
    """S3: first row starts at start_date, last row ends at end_date."""
    s_str, e_str = start.strftime("%Y-%m-%d"), end.strftime("%Y-%m-%d")
    bad_start = bad_end = 0
    for rows in by_product.values():
        if rows[0]["effective_from"] != s_str:
            bad_start += 1
        if rows[-1]["effective_to"] != e_str:
            bad_end += 1
    if bad_start or bad_end:
        return False, f"FAIL: {bad_start} bad starts, {bad_end} bad ends"
    return True, f"all products span {s_str} → {e_str}"


def check_promo_duration(by_product: dict[str, list[dict]]) -> tuple[bool, str]:
    """S4: every promo interval spans at most PROMO_DURATION days."""
    n_bad = 0
    for rows in by_product.values():
        for r in rows:
            if r["is_promo_price"] != "True":
                continue
            fr = datetime.strptime(r["effective_from"], "%Y-%m-%d")
            to = datetime.strptime(r["effective_to"], "%Y-%m-%d")
            if (to - fr).days >= PROMO_DURATION:
                n_bad += 1
    if n_bad:
        return False, f"FAIL: {n_bad} promos exceed {PROMO_DURATION} days"
    return True, f"all promos ≤ {PROMO_DURATION} days"


def validate(path: Path, start: datetime, end: datetime) -> bool:
    by_product = _load_scd2(path)
    print(f"\n  Validation {chr(9472) * 52}")
    checks = [
        ("S1 coverage", lambda: check_coverage(by_product)),
        ("S2 continuity", lambda: check_continuity(by_product)),
        ("S3 bounded", lambda: check_bounded(by_product, start, end)),
        ("S4 promo duration", lambda: check_promo_duration(by_product)),
    ]
    all_pass = True
    for name, fn in checks:
        ok, msg = fn()
        print(f"    [{'PASS' if ok else 'FAIL'}] {name:<22} {msg}")
        if not ok:
            all_pass = False
    print(f"  {chr(9472) * 60}")
    return all_pass


# ═══════════════════════════════════════════════════════════════════════════
# CLI
# ═══════════════════════════════════════════════════════════════════════════


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Generate dim_products_scd2.csv")
    p.add_argument("--start-date", type=str, default="2023-01-01")
    p.add_argument("--end-date", type=str, default="2026-03-31")
    p.add_argument("--seed", type=int, default=10)
    p.add_argument("--output-dir", type=str, default="data/raw")
    return p.parse_args()


def main() -> int:
    args = parse_args()
    out_dir = Path(args.output_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    start = datetime.strptime(args.start_date, "%Y-%m-%d")
    end = datetime.strptime(args.end_date, "%Y-%m-%d")
    rng = Random(args.seed)

    print("\n  Einkaufpark DE — SCD2 Price History")
    print(f"  {chr(9472) * 60}")
    print(f"  date range : {args.start_date} → {args.end_date}")
    print(f"  seed       : {args.seed}")
    print(f"  output     : {out_dir}/")
    print(f"  {chr(9472) * 60}")

    write_scd2(rng, start, end, out_dir)
    ok = validate(out_dir / "dim_products_scd2.csv", start, end)
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(main())
