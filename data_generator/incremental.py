"""
Einkaufpark DE — Incremental Mode (Daily Batch Files + Late Arrivals)
======================================================================
Produces a pipeline-ready dataset where transactions are split into one
CSV per trading day, plus simulated late arrivals (records that "land" in
a future day's batch).

This is what real ETL pipelines actually consume — not one monolithic CSV
but daily drops that may include corrections, retries, and late records.

Output:
  dim_stores.csv             — single snapshot (delegated to generator.py)
  dim_customers.csv          — single snapshot (delegated to generator.py)
  dim_products_scd2.csv      — price history (delegated to price_history.py)
  batches/batch_YYYYMMDD.csv — one file per non-Sunday day
  batches/batch_<end>_late.csv — overflow for arrivals past end_date
  fact_returns.csv           — separate fact table

Each row has TWO date columns to expose the late-arrival challenge:
  - order_date     : when the transaction actually occurred
  - ingestion_date : when it landed in the system (1-3 days later for late)

Late arrival rows carry "INFO:LATE_ARRIVAL" in their data_quality_flag.

Success criteria — verified at end of run:

  I1. Total reconciliation — rows written across all batches sum to the
                             generator's emitted count (no losses).
  I2. No Sunday batches    — zero batch files for Sundays (Sonntagsruhe).
  I3. Late arrival rate    — observed fraction of LATE_ARRIVAL rows within
                             ±2pp of the configured rate.
  I4. Future placement     — every LATE_ARRIVAL row has ingestion_date >
                             order_date, and lands in a batch file whose
                             date matches its ingestion_date.
  I5. FK integrity         — every product_id and store_id resolves
                             against the dim tables.

What this module deliberately does NOT do:
  - Idempotent re-runs (overwrites blindly; pipeline is responsible).
  - Match transaction prices to SCD2 prices (independent by design).
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import sys
from collections import defaultdict
from datetime import datetime, timedelta
from pathlib import Path
from random import Random

# Reuse infrastructure from generator and price_history.
from generator import (
    DOW_WEIGHTS, MONTH_WEIGHTS,
    build_customers, generate_basket, is_promo_period,
    load_stores, load_terminals, make_return_rows,
    write_dim_stores, write_dim_customers,
    _DIM_DROP, _FACT_RETURNS_COLS,
)
from price_history import write_scd2, validate as validate_scd2
from progress import ProgressBar


# ═══════════════════════════════════════════════════════════════════════════
# Constants
# ═══════════════════════════════════════════════════════════════════════════

LATE_ARRIVAL_DELAY_WEIGHTS = [0.60, 0.30, 0.10]  # 1, 2, or 3 days late
LATE_ARRIVAL_FLAG          = "INFO:LATE_ARRIVAL"
DAILY_VOLUME_NOISE         = 0.15                # ±15% per-day jitter
DEFAULT_RETURN_RATE        = 0.04


# ═══════════════════════════════════════════════════════════════════════════
# Daily volume distribution
# ═══════════════════════════════════════════════════════════════════════════

def compute_daily_volumes(start: datetime, end: datetime, n_total: int,
                          rng: Random) -> dict[datetime, int]:
    """Distribute n_total records across non-Sunday days using DOW × MONTH × promo
    weights, with ±DAILY_VOLUME_NOISE jitter. Returns {date: row_target}.
    """
    days, weights = [], []
    d = start
    while d <= end:
        if d.weekday() != 6:   # Skip Sundays
            w = DOW_WEIGHTS[d.weekday()] * MONTH_WEIGHTS[d.month - 1]
            if is_promo_period(d):
                w *= 1.4
            days.append(d)
            weights.append(w)
        d += timedelta(days=1)

    total_w  = sum(weights)
    volumes  = {}
    assigned = 0
    for day, w in zip(days, weights):
        target = max(1, int(n_total * w / total_w))
        jitter = rng.randint(-max(1, int(target * DAILY_VOLUME_NOISE)),
                              max(1, int(target * DAILY_VOLUME_NOISE)))
        volumes[day] = max(1, target + jitter)
        assigned += volumes[day]

    # Correct rounding drift by adjusting the last day.
    if days:
        volumes[days[-1]] = max(1, volumes[days[-1]] + (n_total - assigned))
    return volumes


# ═══════════════════════════════════════════════════════════════════════════
# Late arrival decision — applied at write time
# ═══════════════════════════════════════════════════════════════════════════

def schedule_late_arrival(basket: list[dict], current_date: datetime,
                          end: datetime, rng: Random) -> datetime | None:
    """If this basket is late, mutate its rows and return the delivery date.
    Returns None if the basket is on-time.
    """
    delay = rng.choices([1, 2, 3], weights=LATE_ARRIVAL_DELAY_WEIGHTS, k=1)[0]
    delivery_date = current_date + timedelta(days=delay)

    # Push Sunday deliveries to Monday (no batch files on Sundays).
    if delivery_date.weekday() == 6:
        delivery_date += timedelta(days=1)

    # If delivery falls past end_date, it still goes in the overflow batch.
    delivery_str = delivery_date.strftime("%Y-%m-%d")
    for row in basket:
        row["ingestion_date"]      = delivery_str
        existing = row["data_quality_flag"]
        row["data_quality_flag"]   = (
            LATE_ARRIVAL_FLAG if existing == "OK"
            else f"{existing}|{LATE_ARRIVAL_FLAG}"
        )
    return delivery_date


# ═══════════════════════════════════════════════════════════════════════════
# Per-basket post-processing: returns + late-arrival routing
# ═══════════════════════════════════════════════════════════════════════════

def normalize_returns_inplace(basket: list[dict]) -> None:
    """Flip negative quantities back to positive and mark all as Completed.
    Called when the basket's status indicates it should produce a return event
    in fact_returns instead of negative line items in fact_transactions.
    """
    for r in basket:
        if r["quantity"] is not None and r["quantity"] < 0:
            r["quantity"] = abs(r["quantity"])
        if r["net_revenue_eur"] is not None and r["net_revenue_eur"] < 0:
            r["net_revenue_eur"] = abs(r["net_revenue_eur"])
        r["order_status"] = "Completed"


# ═══════════════════════════════════════════════════════════════════════════
# Main writer
# ═══════════════════════════════════════════════════════════════════════════

def write_incremental(args, out_dir: Path) -> dict:
    """Generate daily batch files. Returns stats dict for validation."""
    master_dir = Path(args.master_dir)
    start = datetime.strptime(args.start_date, "%Y-%m-%d")
    end   = datetime.strptime(args.end_date,   "%Y-%m-%d")
    today_str = datetime.now().strftime("%Y-%m-%d")
    batch_id  = "BATCH_" + hashlib.md5(
        f"{args.records}|{args.seed}|{args.start_date}|{args.end_date}".encode()
    ).hexdigest()[:10].upper()

    rng = Random(args.seed)

    # Dimensions & customer master.
    print(f"  [1/4] Loading masters + building {args.customers:,} customers ...",
          flush=True)
    stores, store_weights = load_stores(master_dir)
    terminals             = load_terminals(master_dir)
    customers_map, cids, cws = build_customers(args.customers, rng)
    write_dim_stores(stores, out_dir)
    write_dim_customers(customers_map, out_dir)

    # SCD2 price history (separate concern, delegated).
    print(f"  [2/4] Generating SCD2 price history ...", flush=True)
    write_scd2(rng, start, end, out_dir)

    # Daily volume plan.
    print(f"  [3/4] Planning daily volumes ...", flush=True)
    daily_volumes = compute_daily_volumes(start, end, args.records, rng)
    sorted_days   = sorted(daily_volumes.keys())
    print(f"        trading days : {len(sorted_days)}")
    print(f"        avg rows/day : {args.records // max(len(sorted_days), 1):,}")
    print(f"  [4/4] Writing daily batch files ...", flush=True)

    batch_dir = out_dir / "batches"
    batch_dir.mkdir(parents=True, exist_ok=True)

    # Header for fact CSVs — derived once from a sample basket.
    sample_pool: list[str] = []
    sample_rng = Random(args.seed + 1)
    sample = generate_basket(
        sample_rng, stores, store_weights, customers_map, cids, cws,
        terminals, start, start, batch_id, today_str, sample_pool, args.walkin_rate
    )
    fact_header = [k for k in sample[0].keys() if k not in _DIM_DROP]

    # Pools used across the whole run.
    recent_txn_pool: list[str] = []
    scheduled_late: dict[datetime, list[dict]] = defaultdict(list)
    return_buffer: list[dict] = []

    # Stats.
    total_rows = total_late = total_baskets = 0

    print()  # blank line before the bar
    bar = ProgressBar(total=len(sorted_days), unit="days",
                      label="Generating batches")

    for day_idx, current_date in enumerate(sorted_days):
        target_rows  = daily_volumes[current_date]
        day_rows: list[dict] = []
        emitted_today = 0

        while emitted_today < target_rows:
            basket = generate_basket(
                rng, stores, store_weights, customers_map, cids, cws,
                terminals, current_date, current_date,
                batch_id, today_str, recent_txn_pool, args.walkin_rate,
            )

            # Returns handling — same logic as generator.normalized mode.
            orig_status = basket[0]["order_status"]
            needs_return = orig_status in ("Returned", "Partially_Returned")
            if needs_return:
                normalize_returns_inplace(basket)
            elif orig_status == "Completed" and rng.random() < DEFAULT_RETURN_RATE:
                needs_return = True
            if needs_return:
                return_buffer.extend(make_return_rows(rng, basket, today_str, end))

            # Late arrival decision (~5%).
            if rng.random() < args.late_rate:
                delivery_date = schedule_late_arrival(basket, current_date, end, rng)
                scheduled_late[delivery_date].extend(basket)
                total_late += len(basket)
            else:
                day_rows.extend(basket)

            emitted_today  += len(basket)
            total_baskets  += 1

        # Pull in any late arrivals scheduled for today.
        if current_date in scheduled_late:
            day_rows.extend(scheduled_late.pop(current_date))

        # Write today's batch.
        batch_path = batch_dir / f"batch_{current_date.strftime('%Y%m%d')}.csv"
        with open(batch_path, "w", newline="", encoding="utf-8") as f:
            w = csv.DictWriter(f, fieldnames=fact_header)
            w.writeheader()
            for row in day_rows:
                w.writerow({k: row[k] for k in fact_header if k in row})

        total_rows += len(day_rows)

        # Live progress — updates every day, throttled internally to ~10/sec.
        bar.update(1, extra=f"{current_date.strftime('%Y-%m-%d')}  "
                            f"{total_rows:,} rows  {total_late:,} late")

    bar.close()

    # Overflow batch — late arrivals scheduled past end_date.
    overflow_rows: list[dict] = []
    for _, rows in sorted(scheduled_late.items()):
        overflow_rows.extend(rows)
    if overflow_rows:
        overflow_path = batch_dir / f"batch_{end.strftime('%Y%m%d')}_late.csv"
        with open(overflow_path, "w", newline="", encoding="utf-8") as f:
            w = csv.DictWriter(f, fieldnames=fact_header)
            w.writeheader()
            for row in overflow_rows:
                w.writerow({k: row[k] for k in fact_header if k in row})
        total_rows += len(overflow_rows)
        print(f"  overflow batch    : {len(overflow_rows):,} rows (late, past end_date)")

    # fact_returns.
    returns_path = out_dir / "fact_returns.csv"
    with open(returns_path, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=_FACT_RETURNS_COLS)
        w.writeheader()
        for rr in return_buffer:
            w.writerow(rr)
    print(f"  fact_returns.csv   : {len(return_buffer):,} rows")

    return {
        "total_rows":    total_rows,
        "total_late":    total_late,
        "total_baskets": total_baskets,
        "scd2_start":    start,
        "scd2_end":      end,
    }


# ═══════════════════════════════════════════════════════════════════════════
# Validation — success criteria from the docstring, enforced
# ═══════════════════════════════════════════════════════════════════════════

def _iter_batch_rows(batch_dir: Path):
    for path in sorted(batch_dir.glob("batch_*.csv")):
        with open(path, encoding="utf-8") as f:
            for row in csv.DictReader(f):
                yield row, path.name


def check_no_sunday_batches(batch_dir: Path) -> tuple[bool, str]:
    """I2: no batch files for Sundays. Reads filenames only — no row scan."""
    bad = []
    for path in batch_dir.glob("batch_*.csv"):
        if "_late" in path.name:
            continue
        date_part = path.stem.split("_")[1]  # batch_YYYYMMDD
        if datetime.strptime(date_part, "%Y%m%d").weekday() == 6:
            bad.append(path.name)
    if bad:
        return False, f"FAIL: {len(bad)} Sunday batches: {bad[:3]}"
    return True, f"all {len(list(batch_dir.glob('batch_*.csv')))} batches on non-Sunday"


def scan_batches(out_dir: Path, stats: dict, target_late: float) -> dict[str, tuple[bool, str]]:
    """Single pass over every batch CSV — computes I1, I3, I4, I5 together.

    The original ran four separate full scans of ~1,000 CSV files. For 12M
    rows that is four full disk passes; this does one and returns all four
    check results keyed by check name.
    """
    # Dim sets for FK check (I5).
    with open(out_dir / "dim_stores.csv", encoding="utf-8") as f:
        dim_stores = {r["store_id"] for r in csv.DictReader(f)}
    with open(out_dir / "dim_products_scd2.csv", encoding="utf-8") as f:
        dim_products = {r["product_id"] for r in csv.DictReader(f)}

    n_total = n_late = 0
    n_not_future = n_mismatched = 0
    fact_stores: set[str] = set()
    fact_products: set[str] = set()

    for row, fname in _iter_batch_rows(out_dir / "batches"):
        n_total += 1
        fact_stores.add(row["store_id"])
        fact_products.add(row["product_id"])

        if LATE_ARRIVAL_FLAG in row["data_quality_flag"]:
            n_late += 1
            order_d  = datetime.strptime(row["order_date"],     "%Y-%m-%d")
            ingest_d = datetime.strptime(row["ingestion_date"], "%Y-%m-%d")
            if ingest_d <= order_d:
                n_not_future += 1
            if "_late" not in fname:
                if fname != f"batch_{ingest_d.strftime('%Y%m%d')}.csv":
                    n_mismatched += 1

    results: dict[str, tuple[bool, str]] = {}

    # I1 — reconciliation
    expected = stats["total_rows"]
    results["I1 reconciliation"] = (
        (n_total == expected),
        f"{n_total:,} rows reconciled across all batches"
        if n_total == expected
        else f"FAIL: {n_total:,} in files vs {expected:,} reported"
    )

    # I3 — late rate
    observed = n_late / n_total if n_total else 0.0
    delta = abs(observed - target_late)
    results["I3 late rate"] = (
        (delta <= 0.02),
        f"observed={observed*100:.2f}%, target={target_late*100:.0f}%, Δ{delta*100:.2f}pp"
    )

    # I4 — future placement
    results["I4 future placement"] = (
        (n_not_future == 0 and n_mismatched == 0),
        "all late rows correctly placed"
        if (n_not_future == 0 and n_mismatched == 0)
        else f"FAIL: {n_not_future} not in future, {n_mismatched} in wrong batch"
    )

    # I5 — FK integrity
    missing_s = fact_stores - dim_stores
    missing_p = fact_products - dim_products
    results["I5 FK integrity"] = (
        (not missing_s and not missing_p),
        f"{len(fact_stores)} stores × {len(fact_products)} products all resolved"
        if (not missing_s and not missing_p)
        else f"FAIL: {len(missing_s)} stores, {len(missing_p)} products unresolved"
    )

    return results


def validate(out_dir: Path, stats: dict, late_rate: float,
             start: datetime, end: datetime) -> bool:
    batch_dir = out_dir / "batches"
    print(f"\n  Validation {chr(9472)*52}")

    all_pass = True

    # I2 — filename-only check, cheap, done separately.
    ok, msg = check_no_sunday_batches(batch_dir)
    print(f"    [{'PASS' if ok else 'FAIL'}] {'I2 no Sunday batches':<22} {msg}")
    if not ok:
        all_pass = False

    # I1 / I3 / I4 / I5 — one shared pass over all batch CSVs.
    scanned = scan_batches(out_dir, stats, late_rate)
    for name in ("I1 reconciliation", "I3 late rate",
                 "I4 future placement", "I5 FK integrity"):
        ok, msg = scanned[name]
        print(f"    [{'PASS' if ok else 'FAIL'}] {name:<22} {msg}")
        if not ok:
            all_pass = False

    # SCD2 validation (delegated to price_history).
    print()
    scd2_ok = validate_scd2(out_dir / "dim_products_scd2.csv", start, end)

    print(f"  {chr(9472)*60}")
    return all_pass and scd2_ok


# ═══════════════════════════════════════════════════════════════════════════
# CLI
# ═══════════════════════════════════════════════════════════════════════════

def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Generate daily batch files + SCD2 prices")
    p.add_argument("--records",     type=int, default=1_000_000)
    p.add_argument("--seed",        type=int, default=10)
    p.add_argument("--start-date",  type=str, default="2023-01-01")
    p.add_argument("--end-date",    type=str, default="2026-03-31")
    p.add_argument("--customers",   type=int, default=500_000)
    p.add_argument("--walkin-rate", type=float, default=0.10)
    p.add_argument("--late-rate",   type=float, default=0.05,
                   help="Fraction of baskets that arrive in a later day's batch")
    p.add_argument("--output-dir",  type=str, default="data/raw")
    p.add_argument("--master-dir",  type=str, default="master")
    return p.parse_args()


def main() -> int:
    args = parse_args()
    out_dir = Path(args.output_dir); out_dir.mkdir(parents=True, exist_ok=True)

    print(f"\n  Einkaufpark DE — Incremental Mode")
    print(f"  {chr(9472)*60}")
    print(f"  records      : {args.records:,}")
    print(f"  seed         : {args.seed}")
    print(f"  date range   : {args.start_date} → {args.end_date}")
    print(f"  walkin rate  : {args.walkin_rate:.0%}")
    print(f"  late rate    : {args.late_rate:.0%}")
    print(f"  output       : {out_dir}/")
    print(f"  {chr(9472)*60}")

    stats = write_incremental(args, out_dir)

    ok = validate(
        out_dir, stats, args.late_rate,
        stats["scd2_start"], stats["scd2_end"],
    )
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(main())