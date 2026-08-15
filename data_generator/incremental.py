"""
Einkaufpark DE — Reproducible Daily Batch Generator
====================================================

Modes
-----
demo
    Creates the initial deterministic data set. It refuses to overwrite an
    existing landing zone unless the exact batch manifest already exists, in
    which case the command is an idempotent no-op.

incremental
    Appends a new, non-overlapping business-date range. Existing dimensions
    and SCD2 price history are reused and never rewritten.

reset
    Explicitly removes only generator-owned directories from the configured
    landing root.

Landing layout
--------------
<root>/
  dimensions/
  transactions/
  returns/
  _manifests/
  _staging/

Files are generated in a batch-specific staging directory, validated, and
published with immutable names. The manifest is written last, making an exact
rerun a safe no-op.
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
import shutil
import sys
from collections import defaultdict
from collections.abc import Iterable, Iterator
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from random import Random

# Make data_generator importable in both environments:
#
# Local:
#   cwd = <repo root>
#
# Databricks Python task:
#   cwd = <repo root>/data_generator

CURRENT_DIR = Path.cwd()

if (CURRENT_DIR / "data_generator").is_dir():
    # Running locally from repository root.
    PACKAGE_ROOT = CURRENT_DIR

elif CURRENT_DIR.name == "data_generator":
    # Running as a Databricks Python script task.
    PACKAGE_ROOT = CURRENT_DIR.parent

else:
    raise RuntimeError(
        f"Cannot determine project root from working directory: {CURRENT_DIR}"
    )

if str(PACKAGE_ROOT) not in sys.path:
    sys.path.insert(0, str(PACKAGE_ROOT))

# ---------------------------------------------------------------------------
# Imports compatible with both:
#   1. package execution/tests: import data_generator.incremental
#   2. Databricks Python script task: incremental.py executed directly
# ---------------------------------------------------------------------------

from data_generator.generator import (
    _DIM_DROP,
    _FACT_RETURNS_COLS,
    DOW_WEIGHTS,
    DUPLICATE_BASKET_RATE,
    GENERATOR_VERSION,
    MONTH_WEIGHTS,
    build_customers,
    generate_basket,
    is_promo_period,
    load_stores,
    load_terminals,
    make_return_rows,
    mark_duplicate_basket,
    write_dim_customers,
    write_dim_products,
    write_dim_stores,
)
from data_generator.price_history import (
    PriceIndex,
    write_scd2,
)
from data_generator.price_history import (
    validate as validate_scd2,
)
from data_generator.progress import ProgressBar

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

LATE_ARRIVAL_DELAY_WEIGHTS = [0.60, 0.30, 0.10]
LATE_ARRIVAL_FLAG = "INFO:LATE_ARRIVAL"
DAILY_VOLUME_NOISE = 0.15
DEFAULT_RETURN_RATE = 0.04

REQUIRED_DIMENSION_FILES = (
    "dim_stores.csv",
    "dim_customers.csv",
    "dim_products.csv",
    "dim_products_scd2.csv",
)

OWNED_DIRECTORIES = (
    "dimensions",
    "transactions",
    "returns",
    "_manifests",
    "_staging",
)


# ---------------------------------------------------------------------------
# Paths and run metadata
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class LandingPaths:
    root: Path
    dimensions: Path
    transactions: Path
    returns: Path
    manifests: Path
    staging: Path


@dataclass(frozen=True)
class GenerationContext:
    mode: str
    batch_id: str
    start: datetime
    end: datetime
    price_history_end: datetime
    generation_date: str
    landing: LandingPaths
    stage: LandingPaths


def get_landing_paths(root: Path) -> LandingPaths:
    return LandingPaths(
        root=root,
        dimensions=root / "dimensions",
        transactions=root / "transactions",
        returns=root / "returns",
        manifests=root / "_manifests",
        staging=root / "_staging",
    )


def create_landing_directories(paths: LandingPaths) -> None:
    for path in (
        paths.dimensions,
        paths.transactions,
        paths.returns,
        paths.manifests,
        paths.staging,
    ):
        path.mkdir(parents=True, exist_ok=True)


def create_stage_paths(landing: LandingPaths, batch_id: str) -> LandingPaths:
    stage_root = landing.staging / batch_id
    if stage_root.exists():
        # A manifest is the commit marker. A leftover staging directory means a
        # previous run failed before publication and is safe to discard.
        shutil.rmtree(stage_root)

    stage = get_landing_paths(stage_root)
    create_landing_directories(stage)
    return stage


def parse_date(value: str, argument_name: str) -> datetime:
    try:
        return datetime.strptime(value, "%Y-%m-%d")
    except ValueError as exc:
        raise ValueError(
            f"{argument_name} must use YYYY-MM-DD format; received {value!r}."
        ) from exc


def validate_common_arguments(args: argparse.Namespace) -> None:
    if args.records <= 0:
        raise ValueError("--records must be greater than zero.")
    if args.customers <= 0:
        raise ValueError("--customers must be greater than zero.")

    for name in ("walkin_rate", "late_rate", "return_rate", "duplicate_rate"):
        value = float(getattr(args, name))
        if not 0.0 <= value <= 1.0:
            raise ValueError(f"--{name.replace('_', '-')} must be between 0 and 1.")

    start = parse_date(args.start_date, "--start-date")
    end = parse_date(args.end_date, "--end-date")
    price_end = parse_date(args.price_history_end_date, "--price-history-end-date")

    if start > end:
        raise ValueError("--start-date cannot be after --end-date.")
    if args.mode == "demo" and price_end < end:
        raise ValueError(
            "For demo mode, --price-history-end-date must be on or after "
            "--end-date so future incremental batches can reuse the SCD2 file."
        )


def make_batch_id(args: argparse.Namespace) -> str:
    config = {
        "generator_version": GENERATOR_VERSION,
        "mode": args.mode,
        "records": args.records,
        "customers": args.customers,
        "seed": args.seed,
        "start_date": args.start_date,
        "end_date": args.end_date,
        "price_history_end_date": args.price_history_end_date,
        "walkin_rate": args.walkin_rate,
        "late_rate": args.late_rate,
        "return_rate": args.return_rate,
        "duplicate_rate": args.duplicate_rate,
        "generation_date": args.generation_date or args.end_date,
    }
    digest = (
        hashlib.sha256(json.dumps(config, sort_keys=True).encode("utf-8"))
        .hexdigest()[:16]
        .upper()
    )
    return f"BATCH_{digest}"


def stable_seed(base_seed: int, namespace: str) -> int:
    digest = hashlib.sha256(f"{base_seed}:{namespace}".encode()).digest()
    return int.from_bytes(digest[:8], byteorder="big", signed=False)


def get_rng(base_seed: int, namespace: str) -> Random:
    return Random(stable_seed(base_seed, namespace))


# ---------------------------------------------------------------------------
# Reset safety
# ---------------------------------------------------------------------------


def validate_reset_path(root: Path) -> None:
    text = root.as_posix().rstrip("/")
    forbidden = {"", ".", "/", "/Volumes", "/Workspace", "/dbfs"}

    if text in forbidden:
        raise ValueError(f"Refusing to reset unsafe path: {text!r}")

    is_volume = text.startswith("/Volumes/") and len(Path(text).parts) >= 5
    is_local_raw = text == "data/raw" or text.endswith("/data/raw")

    if not (is_volume or is_local_raw):
        raise ValueError(
            "Reset is allowed only for data/raw or a full Unity Catalog Volume "
            "path such as /Volumes/<catalog>/<schema>/<volume>."
        )


def reset_generated_data(root: Path) -> None:
    validate_reset_path(root)
    for directory_name in OWNED_DIRECTORIES:
        target = root / directory_name
        if target.exists():
            shutil.rmtree(target)
            print(f"Deleted {target}")
    print("Generator-managed landing data reset successfully.")


# ---------------------------------------------------------------------------
# Manifest and overlap handling
# ---------------------------------------------------------------------------


def manifest_path(paths: LandingPaths, batch_id: str) -> Path:
    return paths.manifests / f"{batch_id}.json"


def load_manifests(paths: LandingPaths) -> list[dict]:
    manifests: list[dict] = []
    if not paths.manifests.exists():
        return manifests

    for path in sorted(paths.manifests.glob("BATCH_*.json")):
        try:
            manifests.append(json.loads(path.read_text(encoding="utf-8")))
        except (OSError, json.JSONDecodeError) as exc:
            raise RuntimeError(f"Cannot read manifest {path}: {exc}") from exc
    return manifests


def get_demo_manifest(paths: LandingPaths) -> dict:
    demos = [item for item in load_manifests(paths) if item.get("mode") == "demo"]
    if len(demos) != 1:
        raise RuntimeError(
            "Incremental mode requires exactly one demo manifest. "
            f"Found {len(demos)}. Run --mode reset followed by --mode demo."
        )
    return demos[0]


def date_ranges_overlap(
    first_start: datetime,
    first_end: datetime,
    second_start: datetime,
    second_end: datetime,
) -> bool:
    return first_start <= second_end and second_start <= first_end


def reject_overlapping_period(
    paths: LandingPaths,
    start: datetime,
    end: datetime,
) -> None:
    for item in load_manifests(paths):
        if "start_date" not in item or "end_date" not in item:
            continue
        existing_start = parse_date(item["start_date"], "manifest start_date")
        existing_end = parse_date(item["end_date"], "manifest end_date")
        if date_ranges_overlap(start, end, existing_start, existing_end):
            raise RuntimeError(
                "Requested period overlaps an already published batch: "
                f"{item.get('batch_id', '<unknown>')} "
                f"({item['start_date']} to {item['end_date']})."
            )


def landing_contains_published_data(paths: LandingPaths) -> bool:
    for directory in (paths.dimensions, paths.transactions, paths.returns):
        if directory.exists() and any(directory.iterdir()):
            return True
    return False


def require_existing_dimensions(paths: LandingPaths) -> None:
    missing = [
        filename
        for filename in REQUIRED_DIMENSION_FILES
        if not (paths.dimensions / filename).is_file()
    ]
    if missing:
        raise RuntimeError(
            "Incremental mode requires an existing demo data set. Missing: "
            + ", ".join(missing)
        )


def get_scd2_bounds(path: Path) -> tuple[datetime, datetime]:
    minimum: datetime | None = None
    maximum: datetime | None = None
    with path.open(encoding="utf-8") as handle:
        for row in csv.DictReader(handle):
            start = parse_date(row["effective_from"], "effective_from")
            end = parse_date(row["effective_to"], "effective_to")
            minimum = start if minimum is None or start < minimum else minimum
            maximum = end if maximum is None or end > maximum else maximum

    if minimum is None or maximum is None:
        raise RuntimeError(f"SCD2 file is empty: {path}")
    return minimum, maximum


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def write_manifest(
    context: GenerationContext,
    args: argparse.Namespace,
    stats: dict,
    published_files: list[Path],
) -> None:
    destination = manifest_path(context.landing, context.batch_id)
    if destination.exists():
        raise FileExistsError(f"Manifest already exists: {destination}")

    payload = {
        "batch_id": context.batch_id,
        "generator_version": GENERATOR_VERSION,
        "mode": context.mode,
        "seed": args.seed,
        "customers": args.customers,
        "start_date": args.start_date,
        "end_date": args.end_date,
        "price_history_end_date": args.price_history_end_date,
        "generation_date": context.generation_date,
        "records_requested": args.records,
        "walkin_rate": args.walkin_rate,
        "late_rate": args.late_rate,
        "return_rate": args.return_rate,
        "duplicate_rate": args.duplicate_rate,
        "rows_written": stats["total_rows"],
        "late_rows": stats["total_late"],
        "baskets_generated": stats["total_baskets"],
        "return_rows_written": stats["return_rows"],
        "published_at_utc": datetime.now(timezone.utc).isoformat(),
        "files": [
            {
                "path": path.relative_to(context.landing.root).as_posix(),
                "size_bytes": path.stat().st_size,
                "sha256": sha256_file(path),
            }
            for path in sorted(published_files)
        ],
    }

    temporary = destination.with_suffix(".json.tmp")
    temporary.write_text(
        json.dumps(payload, indent=2, sort_keys=True),
        encoding="utf-8",
    )
    temporary.replace(destination)


# ---------------------------------------------------------------------------
# Daily planning and row generation
# ---------------------------------------------------------------------------


def compute_daily_volumes(
    start: datetime,
    end: datetime,
    n_total: int,
    rng: Random,
) -> dict[datetime, int]:
    """Distribute requested line volume across non-Sunday business dates."""

    days: list[datetime] = []
    weights: list[float] = []

    current = start

    while current <= end:
        if current.weekday() != 6:
            weight = DOW_WEIGHTS[current.weekday()] * MONTH_WEIGHTS[current.month - 1]

            if is_promo_period(current):
                weight *= 1.4

            days.append(current)
            weights.append(weight)

        current += timedelta(days=1)

    if not days:
        raise ValueError("The requested period contains no trading days.")

    # When there are enough requested rows, preserve at least one
    # transaction line on every trading day.
    minimum_per_day = 1 if n_total >= len(days) else 0

    total_weight = sum(weights)

    raw_targets = [n_total * weight / total_weight for weight in weights]

    volumes = {
        day: max(minimum_per_day, int(value)) for day, value in zip(days, raw_targets)
    }

    # Correct the initial rounding while spreading adjustments
    # across the full period rather than concentrating them at
    # the end of the date range.
    difference = n_total - sum(volumes.values())

    if difference > 0:
        ranked = sorted(
            zip(days, raw_targets),
            key=lambda pair: pair[1] - int(pair[1]),
            reverse=True,
        )

        for index in range(difference):
            day = ranked[index % len(ranked)][0]
            volumes[day] += 1

    elif difference < 0:
        to_remove = -difference

        while to_remove > 0:
            removed_this_pass = 0

            for day in days:
                if to_remove == 0:
                    break

                if volumes[day] > minimum_per_day:
                    volumes[day] -= 1
                    to_remove -= 1
                    removed_this_pass += 1

            if removed_this_pass == 0:
                raise RuntimeError(
                    "Unable to reconcile daily volumes "
                    "without violating the minimum-per-day constraint."
                )

    # Apply realistic daily noise.
    for day in days:
        base = volumes[day]

        if base <= 0:
            continue

        jitter_limit = max(
            1,
            int(base * DAILY_VOLUME_NOISE),
        )

        volumes[day] = max(
            minimum_per_day,
            base
            + rng.randint(
                -jitter_limit,
                jitter_limit,
            ),
        )

    # Restore the requested total after jitter without erasing
    # the final business dates.
    difference = n_total - sum(volumes.values())

    if difference > 0:
        for index in range(difference):
            day = days[index % len(days)]
            volumes[day] += 1

    elif difference < 0:
        to_remove = -difference

        while to_remove > 0:
            removed_this_pass = 0

            for day in days:
                if to_remove == 0:
                    break

                if volumes[day] > minimum_per_day:
                    volumes[day] -= 1
                    to_remove -= 1
                    removed_this_pass += 1

            if removed_this_pass == 0:
                raise RuntimeError(
                    "Unable to reconcile jittered daily volumes "
                    "without violating the minimum-per-day constraint."
                )

    final_total = sum(volumes.values())

    if final_total != n_total:
        raise RuntimeError(
            "Daily volume reconciliation failed: "
            f"expected {n_total}, got {final_total}."
        )

    return {day: count for day, count in volumes.items() if count > 0}


def schedule_late_arrival(
    basket: list[dict],
    current_date: datetime,
    rng: Random,
) -> datetime:
    delay = rng.choices(
        [1, 2, 3],
        weights=LATE_ARRIVAL_DELAY_WEIGHTS,
        k=1,
    )[0]
    delivery_date = current_date + timedelta(days=delay)
    if delivery_date.weekday() == 6:
        delivery_date += timedelta(days=1)

    delivery_text = delivery_date.strftime("%Y-%m-%d")
    for row in basket:
        row["ingestion_date"] = delivery_text
        existing = row.get("data_quality_flag") or "OK"
        row["data_quality_flag"] = (
            LATE_ARRIVAL_FLAG if existing == "OK" else f"{existing}|{LATE_ARRIVAL_FLAG}"
        )
    return delivery_date


def write_csv_exclusive(
    path: Path,
    fieldnames: list[str],
    rows: Iterable[dict],
) -> int:
    path.parent.mkdir(parents=True, exist_ok=True)
    count = 0
    with path.open("x", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        for row in rows:
            writer.writerow({key: row.get(key) for key in fieldnames})
            count += 1
    return count


def build_generation_context(
    args: argparse.Namespace,
    landing: LandingPaths,
) -> GenerationContext:
    batch_id = make_batch_id(args)
    start = parse_date(args.start_date, "--start-date")
    end = parse_date(args.end_date, "--end-date")
    price_history_end = parse_date(
        args.price_history_end_date,
        "--price-history-end-date",
    )
    stage = create_stage_paths(landing, batch_id)
    return GenerationContext(
        mode=args.mode,
        batch_id=batch_id,
        start=start,
        end=end,
        price_history_end=price_history_end,
        generation_date=args.generation_date or args.end_date,
        landing=landing,
        stage=stage,
    )


def prepare_dimensions_and_price_index(
    context: GenerationContext,
    args: argparse.Namespace,
) -> tuple[list, list[float], dict, list[str], list[float], dict, PriceIndex]:
    master_dir = Path(args.master_dir)
    stores, store_weights = load_stores(master_dir)
    terminals = load_terminals(master_dir)

    customer_rng = get_rng(args.seed, "customers")
    customers_map, customer_ids, customer_weights = build_customers(
        args.customers,
        customer_rng,
    )

    if context.mode == "demo":
        write_dim_stores(stores, context.stage.dimensions)
        write_dim_customers(customers_map, context.stage.dimensions)
        write_dim_products(context.stage.dimensions)

        price_rng = get_rng(
            args.seed,
            f"prices:{args.start_date}:{args.price_history_end_date}",
        )
        write_scd2(
            price_rng,
            context.start,
            context.price_history_end,
            context.stage.dimensions,
        )
        price_path = context.stage.dimensions / "dim_products_scd2.csv"
    else:
        require_existing_dimensions(context.landing)
        demo = get_demo_manifest(context.landing)
        expected = {
            "seed": args.seed,
            "customers": args.customers,
        }
        actual = {
            "seed": demo.get("seed"),
            "customers": demo.get("customers"),
        }
        if actual != expected:
            raise RuntimeError(
                "Incremental generation must reuse the demo customer universe. "
                f"Expected seed/customers {actual}, received {expected}."
            )

        price_path = context.landing.dimensions / "dim_products_scd2.csv"
        scd2_start, scd2_end = get_scd2_bounds(price_path)
        if context.start < scd2_start or context.end > scd2_end:
            raise RuntimeError(
                "Incremental period is outside SCD2 price coverage: "
                f"requested {context.start:%Y-%m-%d} to {context.end:%Y-%m-%d}; "
                f"available {scd2_start:%Y-%m-%d} to {scd2_end:%Y-%m-%d}."
            )

    return (
        stores,
        store_weights,
        customers_map,
        customer_ids,
        customer_weights,
        terminals,
        PriceIndex.from_csv(price_path),
    )


def generate_batch(
    context: GenerationContext,
    args: argparse.Namespace,
) -> tuple[dict, list[Path]]:
    (
        stores,
        store_weights,
        customers_map,
        customer_ids,
        customer_weights,
        terminals,
        price_index,
    ) = prepare_dimensions_and_price_index(context, args)

    volume_rng = get_rng(
        args.seed,
        f"volumes:{args.mode}:{args.start_date}:{args.end_date}:{args.records}",
    )
    daily_volumes = compute_daily_volumes(
        context.start,
        context.end,
        args.records,
        volume_rng,
    )

    sorted_days = sorted(daily_volumes)

    if not sorted_days:
        raise ValueError(
            f"No trading days available between "
            f"{context.start:%Y-%m-%d} and {context.end:%Y-%m-%d}."
        )

    # Use the first valid trading day to create a representative basket
    # for deriving the transaction CSV header.
    header_date = sorted_days[0]

    sample_rng = get_rng(args.seed, f"header:{context.batch_id}")
    sample = generate_basket(
        sample_rng,
        stores,
        store_weights,
        customers_map,
        customer_ids,
        customer_weights,
        terminals,
        header_date,
        header_date,
        context.batch_id,
        header_date.strftime("%Y-%m-%d"),
        args.walkin_rate,
        price_index,
    )

    if not sample:
        raise RuntimeError("Could not create a sample basket for the CSV header.")

    fact_header = [key for key in sample[0] if key not in _DIM_DROP]
    if not sample:
        raise RuntimeError("Could not create a sample basket for the CSV header.")
    fact_header = [key for key in sample[0] if key not in _DIM_DROP]

    scheduled_late: dict[datetime, list[dict]] = defaultdict(list)
    return_buffer: list[dict] = []
    generated_files: list[Path] = []
    total_rows = 0
    total_late = 0
    total_baskets = 0

    print(f"  trading days : {len(sorted_days)}")
    print(f"  target rows  : {args.records:,}")
    print()

    bar = ProgressBar(
        total=len(sorted_days),
        unit="days",
        label="Generating batches",
    )

    return_cutoff = context.end + timedelta(days=7)

    for current_date in sorted_days:
        day_rng = get_rng(
            args.seed,
            f"transactions:{context.batch_id}:{current_date:%Y-%m-%d}",
        )
        target_rows = daily_volumes[current_date]
        day_rows: list[dict] = []
        emitted_today = 0
        recent_day_baskets: list[list[dict]] = []

        while emitted_today < target_rows:
            is_duplicate = (
                bool(recent_day_baskets) and day_rng.random() < args.duplicate_rate
            )
            if is_duplicate:
                basket = mark_duplicate_basket(day_rng.choice(recent_day_baskets))
            else:
                basket = generate_basket(
                    day_rng,
                    stores,
                    store_weights,
                    customers_map,
                    customer_ids,
                    customer_weights,
                    terminals,
                    current_date,
                    current_date,
                    context.batch_id,
                    current_date.strftime("%Y-%m-%d"),
                    args.walkin_rate,
                    price_index,
                )
                recent_day_baskets.append([dict(row) for row in basket])
                if len(recent_day_baskets) > 5_000:
                    recent_day_baskets.pop(0)

            is_completed = basket[0]["order_status"] == "Completed"
            has_valid_sale = any(
                row.get("quantity")
                and row["quantity"] > 0
                and row.get("net_revenue_eur") is not None
                for row in basket
            )
            if (
                not is_duplicate
                and is_completed
                and has_valid_sale
                and day_rng.random() < args.return_rate
            ):
                return_buffer.extend(
                    make_return_rows(
                        day_rng,
                        basket,
                        current_date.strftime("%Y-%m-%d"),
                        return_cutoff,
                    )
                )

            if day_rng.random() < args.late_rate:
                delivery_date = schedule_late_arrival(
                    basket,
                    current_date,
                    day_rng,
                )
                scheduled_late[delivery_date].extend(basket)
                total_late += len(basket)
            else:
                day_rows.extend(basket)

            emitted_today += len(basket)
            total_baskets += 1

        # Rows generated on this date plus earlier rows landing on this date are
        # written together, so every row in a file shares its ingestion date.
        day_rows.extend(scheduled_late.pop(current_date, []))

        batch_path = context.stage.transactions / (
            f"transactions_{current_date:%Y%m%d}_{context.batch_id}.csv"
        )
        write_csv_exclusive(batch_path, fact_header, day_rows)
        generated_files.append(batch_path)
        total_rows += len(day_rows)

        bar.update(
            1,
            extra=(
                f"{current_date:%Y-%m-%d}  {total_rows:,} rows  {total_late:,} late"
            ),
        )

    bar.close()

    # Late rows landing after the requested business-date range are grouped by
    # their actual ingestion date, preserving file-date/ingestion-date parity.
    for delivery_date, rows in sorted(scheduled_late.items()):
        path = context.stage.transactions / (
            f"transactions_{delivery_date:%Y%m%d}_late_{context.batch_id}.csv"
        )
        write_csv_exclusive(path, fact_header, rows)
        generated_files.append(path)
        total_rows += len(rows)

    returns_path = context.stage.returns / (
        f"returns_{context.start:%Y%m%d}_{context.end:%Y%m%d}_{context.batch_id}.csv"
    )
    write_csv_exclusive(returns_path, list(_FACT_RETURNS_COLS), return_buffer)
    generated_files.append(returns_path)

    if context.mode == "demo":
        generated_files.extend(
            context.stage.dimensions / filename for filename in REQUIRED_DIMENSION_FILES
        )

    stats = {
        "total_rows": total_rows,
        "total_late": total_late,
        "total_baskets": total_baskets,
        "return_rows": len(return_buffer),
        "scd2_start": (
            context.start
            if context.mode == "demo"
            else get_scd2_bounds(context.landing.dimensions / "dim_products_scd2.csv")[
                0
            ]
        ),
        "scd2_end": (
            context.price_history_end
            if context.mode == "demo"
            else get_scd2_bounds(context.landing.dimensions / "dim_products_scd2.csv")[
                1
            ]
        ),
    }
    return stats, generated_files


# ---------------------------------------------------------------------------
# Validation of the current staged batch
# ---------------------------------------------------------------------------


def transaction_file_date(filename: str) -> datetime:
    parts = Path(filename).stem.split("_")
    if len(parts) < 3 or parts[0] != "transactions":
        raise ValueError(f"Unexpected transaction filename: {filename}")
    return parse_date(
        datetime.strptime(parts[1], "%Y%m%d").strftime("%Y-%m-%d"),
        "transaction filename date",
    )


def iter_transaction_rows(
    transaction_dir: Path,
) -> Iterator[tuple[dict[str, str], Path]]:
    for path in sorted(transaction_dir.glob("transactions_*.csv")):
        with path.open(encoding="utf-8") as handle:
            for row in csv.DictReader(handle):
                yield row, path


def validate_current_batch(
    context: GenerationContext,
    args: argparse.Namespace,
    stats: dict,
) -> bool:
    if context.mode == "demo":
        dimensions_dir = context.stage.dimensions
    else:
        dimensions_dir = context.landing.dimensions

    with (dimensions_dir / "dim_stores.csv").open(encoding="utf-8") as handle:
        valid_stores = {row["store_id"] for row in csv.DictReader(handle)}
    with (dimensions_dir / "dim_products_scd2.csv").open(encoding="utf-8") as handle:
        valid_products = {row["product_id"] for row in csv.DictReader(handle)}

    row_count = 0
    late_count = 0
    wrong_file_date = 0
    invalid_late_dates = 0
    stores_seen: set[str] = set()
    products_seen: set[str] = set()
    sunday_files: list[str] = []

    files = sorted(context.stage.transactions.glob("transactions_*.csv"))
    for path in files:
        file_date = transaction_file_date(path.name)
        if file_date.weekday() == 6:
            sunday_files.append(path.name)

    for row, path in iter_transaction_rows(context.stage.transactions):
        row_count += 1
        stores_seen.add(row["store_id"])
        products_seen.add(row["product_id"])

        ingestion_date = parse_date(row["ingestion_date"], "ingestion_date")
        order_date = parse_date(row["order_date"], "order_date")
        if ingestion_date != transaction_file_date(path.name):
            wrong_file_date += 1

        if LATE_ARRIVAL_FLAG in (row.get("data_quality_flag") or ""):
            late_count += 1
            if ingestion_date <= order_date:
                invalid_late_dates += 1

    missing_stores = stores_seen - valid_stores
    missing_products = products_seen - valid_products

    # Row-level late percentage is close to, but not exactly equal to, the
    # basket-level sampling rate. Small batches receive a wider tolerance.
    observed_late = late_count / row_count if row_count else 0.0
    late_tolerance = 0.10 if row_count < 10_000 else 0.02

    checks = {
        "row reconciliation": row_count == stats["total_rows"],
        "no Sunday ingestion files": not sunday_files,
        "file date equals ingestion date": wrong_file_date == 0,
        "late rows arrive after order date": invalid_late_dates == 0,
        "store foreign keys": not missing_stores,
        "product foreign keys": not missing_products,
        "late-rate tolerance": abs(observed_late - args.late_rate) <= late_tolerance,
    }

    for name, passed in checks.items():
        print(f"    [{'PASS' if passed else 'FAIL'}] {name}")

    if sunday_files:
        print(f"      Sunday files: {sunday_files[:5]}")
    if wrong_file_date:
        print(f"      Rows in wrong ingestion-date file: {wrong_file_date}")
    if invalid_late_dates:
        print(f"      Invalid late-arrival dates: {invalid_late_dates}")
    if missing_stores:
        print(f"      Missing stores: {sorted(missing_stores)[:5]}")
    if missing_products:
        print(f"      Missing products: {sorted(missing_products)[:5]}")
    print(
        f"      Late rows: {observed_late:.2%}; target {args.late_rate:.2%}; "
        f"tolerance ±{late_tolerance:.0%}"
    )

    scd2_path = dimensions_dir / "dim_products_scd2.csv"
    print()
    scd2_ok = validate_scd2(
        scd2_path,
        stats["scd2_start"],
        stats["scd2_end"],
    )
    return all(checks.values()) and scd2_ok


# ---------------------------------------------------------------------------
# Atomic publication
# ---------------------------------------------------------------------------


def destination_for_staged_file(
    context: GenerationContext,
    staged_path: Path,
) -> Path:
    relative = staged_path.relative_to(context.stage.root)
    return context.landing.root / relative


def publish_batch(
    context: GenerationContext,
    staged_files: list[Path],
) -> list[Path]:
    destinations = [destination_for_staged_file(context, path) for path in staged_files]
    conflicts = [path for path in destinations if path.exists()]
    if conflicts:
        raise FileExistsError(
            "Refusing to overwrite immutable landing files: "
            + ", ".join(str(path) for path in conflicts[:5])
        )

    published: list[Path] = []
    for staged, destination in zip(staged_files, destinations):
        destination.parent.mkdir(parents=True, exist_ok=True)
        shutil.move(str(staged), str(destination))
        published.append(destination)
    return published


def cleanup_stage(context: GenerationContext) -> None:
    if context.stage.root.exists():
        shutil.rmtree(context.stage.root)


# ---------------------------------------------------------------------------
# Orchestration
# ---------------------------------------------------------------------------


def run_generation(args: argparse.Namespace, root: Path) -> int:
    landing = get_landing_paths(root)
    create_landing_directories(landing)

    batch_id = make_batch_id(args)
    committed_manifest = manifest_path(landing, batch_id)
    if committed_manifest.exists():
        print(
            f"Batch {batch_id} is already published; generation is an idempotent no-op."
        )
        return 0

    start = parse_date(args.start_date, "--start-date")
    end = parse_date(args.end_date, "--end-date")

    if args.mode == "demo":
        if landing_contains_published_data(landing) or load_manifests(landing):
            raise RuntimeError(
                "Demo mode found existing landing data. Run --mode reset "
                "explicitly before rebuilding the baseline."
            )
    else:
        require_existing_dimensions(landing)
        reject_overlapping_period(landing, start, end)

    context = build_generation_context(args, landing)
    try:
        stats, staged_files = generate_batch(context, args)
        print("\n  Validating staged batch")
        if not validate_current_batch(context, args, stats):
            raise RuntimeError(
                "Generated batch failed validation and was not published."
            )

        published_files = publish_batch(context, staged_files)
        write_manifest(context, args, stats, published_files)
        print(
            f"\nPublished {context.batch_id}: {stats['total_rows']:,} transaction "
            f"rows and {stats['return_rows']:,} return rows."
        )
        return 0
    finally:
        cleanup_stage(context)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Generate reproducible retail daily drops for Databricks."
    )
    parser.add_argument(
        "--mode",
        choices=("demo", "incremental", "reset"),
        default="demo",
    )
    parser.add_argument("--records", type=int, default=100_000)
    parser.add_argument("--customers", type=int, default=10_000)
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument("--start-date", default="2023-01-01")
    parser.add_argument("--end-date", default="2026-03-31")
    parser.add_argument("--price-history-end-date", default="2026-12-31")
    parser.add_argument(
        "--generation-date",
        default=None,
        help="Deterministic run date; defaults to --end-date.",
    )
    parser.add_argument("--walkin-rate", type=float, default=0.10)
    parser.add_argument("--late-rate", type=float, default=0.05)
    parser.add_argument("--return-rate", type=float, default=DEFAULT_RETURN_RATE)
    parser.add_argument(
        "--duplicate-rate",
        type=float,
        default=DUPLICATE_BASKET_RATE,
    )
    parser.add_argument("--output-dir", default="data/raw")
    parser.add_argument("--master-dir", default="master")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    root = Path(args.output_dir)

    try:
        if args.mode == "reset":
            reset_generated_data(root)
            return 0

        validate_common_arguments(args)
        root.mkdir(parents=True, exist_ok=True)

        print("\nEinkaufpark DE — Reproducible Data Generation")
        print("─" * 64)
        print(f"mode          : {args.mode}")
        print(f"records       : {args.records:,}")
        print(f"customers     : {args.customers:,}")
        print(f"seed          : {args.seed}")
        print(f"date range    : {args.start_date} → {args.end_date}")
        print(f"price horizon : {args.price_history_end_date}")
        print(f"output        : {root}")
        print("─" * 64)

        return run_generation(args, root)
    except (ValueError, RuntimeError, FileExistsError, OSError, KeyError) as exc:
        print(f"ERROR: {exc}", file=sys.stderr, flush=True)
        raise


if __name__ == "__main__":
    exit_code = main()

    if exit_code != 0:
        raise RuntimeError(f"Retail data generator failed with exit code {exit_code}.")
