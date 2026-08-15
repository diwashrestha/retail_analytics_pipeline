"""
Einkaufpark DE — Synthetic Sales Data Generator
================================================
Generates reproducible German physical-retail POS data in flat or normalized
form. Valid sales use stable master-data attributes and deterministic IDs.
Controlled malformed rows remain available for Bronze/Silver DQ exercises.

Validated invariants:
  R1 reproducibility, R2 foreign keys, R3 Sunday closure, R4 DQ rates,
  R5 walk-in rate, R6 unique loyalty cards, R7 transaction identity,
  R8 stable product attributes, and R9 refund integrity.

Returns are emitted only in fact_returns; the sales fact contains Completed and
Voided baskets. Exact POS retries replay the complete original basket and retain
its record hashes so Silver can deduplicate them correctly.
"""

from __future__ import annotations

import argparse
import bisect
import csv
import hashlib
import itertools
import json
import sys
from collections import Counter
from collections.abc import Callable, Iterator
from dataclasses import dataclass
from datetime import datetime, timedelta
from pathlib import Path
from random import Random

# Product catalogue is a domain artifact — kept as a separate module.
# Expected shape:
#   PRODUCTS: list of 11-tuples
#     (category, subcategory, name, brand, pl_eligible,
#      p_min, p_max, qty_min, qty_max, unit, seasonal_months)
#   get_available_products(month: int) -> (products_list, weights_list)
from data_generator.product_catalogue import PRODUCTS, get_available_products

# ─────────────────────────────────────────────────────────────────────────
# Product sampling — precomputed cumulative weights per month.
#
# rng.choices() rebuilds the cumulative weight distribution on EVERY call.
# With a ~1,950-product catalogue and ~15M product picks across a full run,
# that re-summing dominates runtime (~13 min of pure overhead). We instead
# build the cumulative distribution once per month and sample with bisect.
# Measured: ~68x faster on the hot path.
# ─────────────────────────────────────────────────────────────────────────

_MONTH_CACHE: dict[int, tuple[list, list, float]] = {}


def _month_distribution(month: int) -> tuple[list, list, float]:
    """Return (products, cumulative_weights, total) for a month, cached."""
    cached = _MONTH_CACHE.get(month)
    if cached is None:
        products, weights = get_available_products(month)
        cumulative = list(itertools.accumulate(weights))
        total = cumulative[-1]
        cached = (products, cumulative, total)
        _MONTH_CACHE[month] = cached
    return cached


def sample_product(rng: Random, month: int):
    """Draw one product for the given month using the cached distribution."""
    products, cumulative, total = _month_distribution(month)
    idx = bisect.bisect(cumulative, rng.random() * total)
    # bisect can return len(cumulative) on the rare r==total edge — clamp.
    if idx >= len(products):
        idx = len(products) - 1
    return products[idx]


class WeightedPicker:
    """Fast weighted choice over a FIXED set of options.

    rng.choices() re-accumulates the weight list on every call. For small
    fixed distributions (order status, payment method, etc.) sampled millions
    of times, precomputing the cumulative weights once is meaningfully faster.
    """

    __slots__ = ("cumulative", "options", "total")

    def __init__(self, options: list, weights: list[float]):
        self.options = options
        self.cumulative = list(itertools.accumulate(weights))
        self.total = self.cumulative[-1]

    def pick(self, rng: Random):
        idx = bisect.bisect(self.cumulative, rng.random() * self.total)
        if idx >= len(self.options):
            idx = len(self.options) - 1
        return self.options[idx]


# ═══════════════════════════════════════════════════════════════════════════
# Constants
# ═══════════════════════════════════════════════════════════════════════════

PRIVATE_LABELS = [
    "EKP-Classic",
    "EKP-Bio",
    "EKP-Favourites",
    "EKP-take it easy",
    "EKP-Free",
]

# Day-of-week weights — Python weekday() convention (Mon=0 … Sun=6).
# Sunday is 0.0 → Sonntagsruhe (German Sunday trading ban).
DOW_WEIGHTS = [0.11, 0.12, 0.13, 0.14, 0.21, 0.29, 0.00]
MONTH_WEIGHTS = [0.07, 0.06, 0.08, 0.09, 0.08, 0.07, 0.07, 0.08, 0.09, 0.09, 0.10, 0.12]

# Hour-of-day weights for store traffic. Applied within each store's hours.
HOUR_WEIGHTS = {
    7: 0.02,
    8: 0.04,
    9: 0.06,
    10: 0.08,
    11: 0.10,
    12: 0.11,
    13: 0.09,
    14: 0.07,
    15: 0.07,
    16: 0.08,
    17: 0.10,
    18: 0.09,
    19: 0.05,
    20: 0.03,
    21: 0.01,
}

# Basket size distribution — (min_items, max_items, weight).
BASKET_SIZE_BUCKETS = [
    (1, 2, 0.15),
    (3, 5, 0.30),
    (6, 9, 0.25),
    (10, 15, 0.20),
    (16, 25, 0.08),
    (26, 40, 0.02),
]

# Basket status distribution. Returns are represented only in fact_returns.
# Keeping return events separate avoids negative sales lines and double-counting.
ORDER_STATUSES = ["Completed", "Voided"]
ORDER_STATUS_WEIGHTS = [0.97, 0.03]

GENERATOR_VERSION = "2.0.0"
DUPLICATE_BASKET_RATE = 0.004

# Loyalty membership — German grocery programs (Payback, Lidl Plus, dm app)
# are FLAT: you hold the card or you don't. No Bronze/Silver/Gold tiers — that
# is a US airline/hotel pattern that doesn't fit the German market.
LOYALTY_MEMBER_RATE = 0.62  # ~62% of shoppers hold a loyalty card
LOYALTY_LAUNCH = datetime(2023, 3, 1)
LOYALTY_POINTS_PER_EUR = 1  # flat 1 point per €1 spent (Payback-style)

# Payment method weights by basket total (€). Last entry catches anything above.
PAYMENT_TYPES = ["Card", "Cash", "Apple_Pay", "Google_Pay", "Voucher", "Gift_Card"]
PAYMENT_BRACKETS = [
    (10, [0.25, 0.55, 0.08, 0.06, 0.04, 0.02]),
    (30, [0.45, 0.32, 0.10, 0.07, 0.04, 0.02]),
    (75, [0.55, 0.20, 0.10, 0.07, 0.05, 0.03]),
    (None, [0.62, 0.10, 0.11, 0.08, 0.06, 0.03]),
]

# VAT by category — German rates (7% reduced for food, 19% standard).
VAT_BY_CATEGORY = {
    "Fresh & Perishables": 0.07,
    "Pantry Staples": 0.07,
    "Frozen & Convenience": 0.07,
    "Beverages": 0.19,
    "Snacks & Confectionery": 0.19,
    "Household": 0.19,
    "Health & Beauty": 0.19,
    "Non-Food": 0.19,
}

# Return reason codes for fact_returns.
RETURN_REASONS = ["Changed_Mind", "Damaged", "Wrong_Item", "Defective", "Expired"]
RETURN_REASON_W = [0.40, 0.25, 0.15, 0.12, 0.08]

# Customer shopping frequency buckets (Pareto-like).
# (probability, min_weight, max_weight).
FREQ_BUCKETS = [
    (0.05, 20.0, 40.0),  # heavy shoppers
    (0.15, 5.0, 12.0),  # regular
    (0.30, 1.5, 3.0),  # occasional
    (0.50, 0.2, 0.8),  # rare
]

# Precomputed pickers for fixed distributions — built once at import.
# (WeightedPicker is defined above; constants must exist first, hence here.)
_ORDER_STATUS_PICKER = WeightedPicker(ORDER_STATUSES, ORDER_STATUS_WEIGHTS)
_BASKET_SIZE_PICKER = WeightedPicker(
    list(range(len(BASKET_SIZE_BUCKETS))),
    [b[2] for b in BASKET_SIZE_BUCKETS],
)


# ═══════════════════════════════════════════════════════════════════════════
# Small data classes (no god-objects)
# ═══════════════════════════════════════════════════════════════════════════


@dataclass(frozen=True)
class Store:
    store_id: str
    city: str
    district: str
    postal_code: str
    street: str
    region: str
    country_code: str
    country_name: str
    size_class: str
    terminal_count: int
    source_system: str
    opening_hours: dict[str, str]


@dataclass(frozen=True)
class Customer:
    customer_id: str
    age: int | None
    gender_code: str | None
    is_member: bool  # flat loyalty membership — no tiers
    loyalty_card_id: str | None  # NULL for non-members


# ═══════════════════════════════════════════════════════════════════════════
# Deterministic helpers — every random call routes through the seeded RNG
# ═══════════════════════════════════════════════════════════════════════════


def make_id(rng: Random, n_bytes: int = 6) -> str:
    """Hex ID drawn from the seeded RNG. Never call uuid4() — it bypasses --seed."""
    return rng.randbytes(n_bytes).hex().upper()


def product_id_for(product_name: str) -> str:
    """Stable product_id from the catalogue name. Same hash on both sides of FK."""
    return "PROD" + hashlib.md5(product_name.encode()).hexdigest()[:6].upper()


def record_hash(*args) -> str:
    return hashlib.md5("|".join(str(a) for a in args).encode()).hexdigest()


# ═══════════════════════════════════════════════════════════════════════════
# Loaders
# ═══════════════════════════════════════════════════════════════════════════


def load_json(path: Path) -> dict:
    if not path.exists():
        sys.exit(f"  ERROR: {path} not found")
    with open(path, encoding="utf-8") as f:
        return json.load(f)


def load_stores(master_dir: Path) -> tuple[list[Store], list[float]]:
    """Load stores and compute size-weighted selection probabilities."""
    data = load_json(master_dir / "store_master.json")
    stores = [
        Store(
            store_id=s["store_id"],
            city=s["city"],
            district=s["district"],
            postal_code=s["postal_code"],
            street=s["street"],
            region=s["state"],
            country_code=s["country_code"],
            country_name=s["country_name"],
            size_class=s["size_class"],
            terminal_count=s["terminal_count"],
            source_system=s["source_system"],
            opening_hours=s["opening_hours"],
        )
        for s in data["stores"]
    ]
    size_weight = {"L": 5.0, "M": 2.5, "S": 1.0}
    weights = [size_weight.get(s.size_class, 1.0) for s in stores]
    return stores, weights


def load_terminals(master_dir: Path) -> dict[str, tuple[str, bool]]:
    """Map terminal_id → (terminal_type, is_self_checkout)."""
    data = load_json(master_dir / "terminal_master.json")
    return {
        t["terminal_id"]: (t["terminal_type"], t["is_self_checkout"])
        for t in data["terminals"]
    }


def load_schema(master_dir: Path) -> dict:
    """Load raw_schema.json — single source of truth for DQ rules & rates."""
    return load_json(master_dir / "raw_schema.json")


# ═══════════════════════════════════════════════════════════════════════════
# Customer master construction
# ═══════════════════════════════════════════════════════════════════════════


def build_customers(
    n: int, rng: Random
) -> tuple[dict[str, Customer], list[str], list[float]]:
    """Build customer master with Pareto shopping frequency.

    Returns (master_by_id, id_list, freq_weights). The id_list and freq_weights
    are aligned so rng.choices(id_list, weights=freq_weights) gives Pareto picks.
    """
    master: dict[str, Customer] = {}
    ids: list[str] = []
    freq_weights: list[float] = []

    for i in range(1, n + 1):
        cid = f"CUST{i}"

        # Age distribution (deliberately includes ~5% nulls).
        r = rng.random()
        if r < 0.05:
            age = None
        elif r < 0.15:
            age = rng.randint(18, 24)
        elif r < 0.35:
            age = rng.randint(25, 34)
        elif r < 0.65:
            age = rng.randint(35, 49)
        elif r < 0.87:
            age = rng.randint(50, 64)
        else:
            age = rng.randint(65, 85)

        # Gender — raw code; final encoding depends on source_system at txn time.
        gr = rng.random()
        if gr < 0.465:
            gender_code = "M"
        elif gr < 0.930:
            gender_code = "F"
        elif gr < 0.970:
            gender_code = "Divers"
        else:
            gender_code = None

        # Loyalty membership — decided ONCE here, a stable property of the
        # customer. Members get a card ID; non-members get NULL.
        is_member = rng.random() < LOYALTY_MEMBER_RATE
        # Derive the card from the customer sequence so it is guaranteed unique
        # and reproducible. Random sampling created thousands of collisions.
        loyalty_card_id = f"KLC{i:08d}" if is_member else None

        # Frequency weight via Pareto bucket. Shopping frequency is driven by
        # the Pareto distribution alone — no tier multiplier (no tiers exist).
        fr = rng.random()
        cumulative = 0.0
        freq_w = FREQ_BUCKETS[-1][1]  # default to rarest bucket's low end
        for prob, lo, hi in FREQ_BUCKETS:
            cumulative += prob
            if fr < cumulative:
                freq_w = rng.uniform(lo, hi)
                break

        master[cid] = Customer(
            customer_id=cid,
            age=age,
            gender_code=gender_code,
            is_member=is_member,
            loyalty_card_id=loyalty_card_id,
        )
        ids.append(cid)
        freq_weights.append(freq_w)

    return master, ids, freq_weights


# ═══════════════════════════════════════════════════════════════════════════
# Date / time pickers
# ═══════════════════════════════════════════════════════════════════════════


def is_promo_period(d: datetime) -> bool:
    """Black Friday week, December run-up, and Easter weeks."""
    iso_week = d.isocalendar()[1]
    if iso_week == 47:
        return True  # Black Friday
    if d.month == 12 and d.day <= 23:
        return True  # pre-Xmas
    if iso_week in (13, 14, 15) and d.month in (3, 4):
        return True  # Easter
    return False


def pick_date(rng: Random, start: datetime, end: datetime) -> datetime:
    """Weighted date pick. Sundays rejected outright. Promo periods boosted.

    Single-day range (start == end): the caller has fixed the date, so we
    honor it as-is. The Sunday ban is a property of RANGE sampling — it must
    not override an explicit caller choice (and incremental mode passes
    start == end == current_date for every basket).
    """
    if start == end:
        return start

    delta_days = (end - start).days
    max_dow_w = max(DOW_WEIGHTS[:6])  # exclude Sunday from normalization
    max_month_w = max(MONTH_WEIGHTS)
    for _ in range(200):
        d = start + timedelta(days=rng.randint(0, delta_days))
        if d.weekday() == 6:  # Sunday — Sonntagsruhe
            continue
        dow_accept = DOW_WEIGHTS[d.weekday()] / max_dow_w
        month_accept = MONTH_WEIGHTS[d.month - 1] / max_month_w
        promo_mult = 1.4 if is_promo_period(d) else 1.0
        if rng.random() < dow_accept * promo_mult and rng.random() < month_accept:
            return d
    # Pathological fallback — caller is unlikely to reach this.
    while True:
        d = start + timedelta(days=rng.randint(0, delta_days))
        if d.weekday() != 6:
            return d


_HOUR_PICKER_CACHE: dict[tuple[int, int], WeightedPicker] = {}


def _opening_window(store: Store, order_date: datetime) -> tuple[int, int]:
    """Read the store-specific opening window from store_master.json."""
    day_name = order_date.strftime("%A").lower()
    value = store.opening_hours.get(day_name, "closed")
    if value == "closed":
        raise ValueError(f"Store {store.store_id} is closed on {day_name}")
    start_text, end_text = value.split("-", 1)
    return int(start_text.split(":")[0]), int(end_text.split(":")[0])


def pick_time_of_day(rng: Random, store: Store, order_date: datetime) -> str:
    """Realistic transaction time using the store master's opening hours."""
    open_h, close_h = _opening_window(store, order_date)
    key = (open_h, close_h)
    picker = _HOUR_PICKER_CACHE.get(key)
    if picker is None:
        hours = [h for h in HOUR_WEIGHTS if open_h <= h < close_h]
        if not hours:
            raise ValueError(
                f"No configured traffic weights for {store.store_id} "
                f"between {open_h}:00 and {close_h}:00"
            )
        weights = [HOUR_WEIGHTS[h] for h in hours]
        picker = WeightedPicker(hours, weights)
        _HOUR_PICKER_CACHE[key] = picker

    h = picker.pick(rng)
    return f"{h:02d}:{rng.randint(0, 59):02d}:{rng.randint(0, 59):02d}"


# ═══════════════════════════════════════════════════════════════════════════
# DQ noise injection — kept short and field-specific (no abstract framework)
# ═══════════════════════════════════════════════════════════════════════════


def dirty_age(age: int | None, rng: Random) -> int | None:
    r = rng.random()
    if r < 0.995:
        return age
    if r < 0.998:
        return rng.randint(121, 160)
    if r < 0.9995:
        return rng.randint(-5, -1)
    return None


def dirty_price(effective_price: float, rng: Random) -> float | None:
    """Inject controlled raw-price defects around the actual shelf price."""
    r = rng.random()
    if r < 0.995:
        return round(effective_price, 2)
    if r < 0.998:
        return None
    return round(-max(0.01, effective_price * rng.uniform(0.05, 0.30)), 2)


def dirty_discount(rng: Random) -> float | None:
    r = rng.random()
    if r < 0.80:
        return 0.0
    if r < 0.995:
        return round(rng.uniform(0.5, 35.0), 2)
    if r < 0.998:
        return None
    return round(rng.uniform(100.1, 102.0), 2)


def dirty_quantity(qty_min: int, qty_max: int, rng: Random) -> int | None:
    qty_min = max(1, qty_min)
    qty_max = max(qty_min, qty_max)
    r = rng.random()
    if r < 0.992:
        return rng.randint(qty_min, qty_max)
    if r < 0.995:
        return None
    if r < 0.998:
        return 0
    return -rng.randint(1, 3)


# ═══════════════════════════════════════════════════════════════════════════
# Payment picker — table-driven
# ═══════════════════════════════════════════════════════════════════════════


def pick_payment(rng: Random, basket_total: float) -> str:
    for threshold, weights in PAYMENT_BRACKETS:
        if threshold is None or basket_total < threshold:
            return rng.choices(PAYMENT_TYPES, weights=weights, k=1)[0]
    return PAYMENT_TYPES[0]  # unreachable; satisfies type checkers


# ═══════════════════════════════════════════════════════════════════════════
# Basket size / pick logic
# ═══════════════════════════════════════════════════════════════════════════


def pick_basket_size(rng: Random) -> int:
    i = _BASKET_SIZE_PICKER.pick(rng)
    lo, hi, _ = BASKET_SIZE_BUCKETS[i]
    return rng.randint(lo, hi)


def encode_gender(code: str | None, source_system: str) -> str | None:
    if code is None or code == "Divers":
        return code
    if source_system == "LEGACY_POS_CSV":
        return {"M": "Male", "F": "Female"}[code]
    return code


# ═══════════════════════════════════════════════════════════════════════════
# Line item & basket generation
# ═══════════════════════════════════════════════════════════════════════════


def generate_line_item(
    rng: Random,
    prod: tuple,
    *,
    txn_id: str,
    basket_id: str,
    order_date_str: str,
    order_time_str: str,
    is_promo_week: bool,
    promo_week_id: str,
    store: Store,
    customer: Customer | None,
    gender: str | None,
    has_loyalty: bool,
    coupon_applied: bool,
    coupon_code: str | None,
    payment_type: str,
    pos_terminal_id: str,
    terminal_type: str,
    is_sco: bool,
    cashier_id: str | None,
    source_system: str,
    order_status: str,
    batch_id: str,
    ingestion_date_str: str,
    effective_price_eur: float,
    line_seq: int,
) -> dict:
    """Build one line-item row. Pure function — all inputs explicit."""
    (
        cat,
        subcat,
        cat_name,
        brand_pool,
        pl_possible,
        p_min,
        p_max,
        q_min,
        q_max,
        unit,
        _,
    ) = prod

    # Product identity is a stable dimension attribute. The catalogue brand
    # must not change randomly from transaction to transaction.
    pid = product_id_for(cat_name)
    brand = brand_pool
    product_name = cat_name
    is_pl = brand.startswith("EKP-") or brand == "bulk"
    if brand == "bulk":
        brand = "EKP-Classic"
        is_pl = True

    # The unit price comes from the SCD2 record effective on the order date.
    # Transaction-level discounts are applied separately to calculate revenue.
    if order_status == "Voided":
        unit_price = round(effective_price_eur, 2)
        quantity, discount_pct, net_revenue = 0, 0.0, 0.0
    else:
        unit_price = dirty_price(effective_price_eur, rng)
        discount_pct = dirty_discount(rng)
        quantity = dirty_quantity(int(q_min), int(q_max), rng)
        # Preserve non-null malformed values in raw data so Silver can route
        # them to REVIEW. Only missing inputs produce a null revenue/ERR row.
        net_revenue = (
            round(unit_price * quantity * (1 - discount_pct / 100), 2)
            if unit_price is not None
            and quantity is not None
            and discount_pct is not None
            else None
        )

    # Loyalty points — flat 1 point per €1 (Payback-style), only when the
    # customer is an active member and the line has positive revenue.
    points = None
    if has_loyalty and net_revenue and net_revenue > 0:
        points = int(net_revenue * LOYALTY_POINTS_PER_EUR)

    # DQ flags — observed from the actual values.
    flags = []
    if unit_price is None:
        flags.append("ERR:PRICE_NULL")
    elif unit_price < 0:
        flags.append("WARN:PRICE_NEGATIVE")
    if quantity is None:
        flags.append("ERR:QTY_NULL")
    elif quantity == 0 and order_status != "Voided":
        flags.append("WARN:QTY_ZERO")
    elif quantity < 0 and order_status not in ("Returned", "Partially_Returned"):
        flags.append("WARN:QTY_NEGATIVE")
    age = customer.age if customer else None
    if age is not None and (age < 0 or age > 120):
        flags.append("WARN:AGE_INVALID")
    if discount_pct is None:
        flags.append("ERR:DISCOUNT_NULL")
    elif discount_pct > 100:
        flags.append("WARN:DISCOUNT_OVER_100")
    if net_revenue is None:
        flags.append("ERR:REVENUE_NULL")
    dq_flag = "|".join(flags) if flags else "OK"

    return {
        "transaction_id": txn_id,
        "basket_id": basket_id,
        "batch_id": batch_id,
        "source_system": source_system,
        "record_hash": record_hash(
            txn_id,
            order_date_str,
            customer.customer_id if customer else "WALKIN",
            pid,
            store.store_id,
            line_seq,
        ),
        "order_date": order_date_str,
        "order_time": order_time_str,
        "ingestion_date": ingestion_date_str,
        "sales_channel": "IN_STORE",
        "order_status": order_status,
        "store_id": store.store_id,
        "store_city": store.city,
        "store_district": store.district,
        "store_postal_code": store.postal_code,
        "store_area": store.street,
        "store_region": store.region,
        "store_country_code": store.country_code,
        "store_country_name": store.country_name,
        "store_size_class": store.size_class,
        "customer_id": customer.customer_id if customer else None,
        "customer_age": dirty_age(age, rng),
        "gender": gender,
        "membership_active": has_loyalty,
        "loyalty_card_id": customer.loyalty_card_id
        if (customer and has_loyalty)
        else None,
        "loyalty_points_earned": points,
        "coupon_applied": coupon_applied,
        "coupon_code": coupon_code,
        "product_id": pid,
        "product_name": product_name,
        "product_category": cat,
        "product_subcategory": subcat,
        "product_unit": unit,
        "is_private_label": is_pl,
        "brand": brand,
        "quantity": quantity,
        "unit_price_eur": unit_price,
        "discount_pct": discount_pct,
        "transaction_currency": "EUR",
        "net_revenue_eur": net_revenue,
        "payment_type": payment_type,
        "pos_terminal_id": pos_terminal_id,
        "terminal_type": terminal_type,
        "is_self_checkout": is_sco,
        "cashier_id": cashier_id,
        "promo_week_id": promo_week_id,
        "is_promo_period": is_promo_week,
        "data_quality_flag": dq_flag,
    }


# Caches keyed by id() of the weight list — built once, reused every basket.
# Avoids re-accumulating a 500K-element customer-weight list on every call.
_PICKER_CACHE: dict[int, WeightedPicker] = {}


def _cached_picker(options: list, weights: list[float]) -> WeightedPicker:
    key = id(weights)
    picker = _PICKER_CACHE.get(key)
    if picker is None:
        picker = WeightedPicker(options, weights)
        _PICKER_CACHE[key] = picker
    return picker


def generate_basket(
    rng: Random,
    stores: list[Store],
    store_weights: list[float],
    customers_map: dict[str, Customer],
    customer_ids: list[str],
    customer_freq_weights: list[float],
    terminals: dict[str, tuple[str, bool]],
    start: datetime,
    end: datetime,
    batch_id: str,
    ingestion_date_str: str,
    walkin_rate: float,
    price_lookup: Callable[[str, datetime], float] | None = None,
) -> list[dict]:
    """Generate one shopping trip (basket) → list of line-item rows."""
    store_picker = _cached_picker(stores, store_weights)
    store = store_picker.pick(rng)
    source_system = store.source_system

    # Every newly generated basket receives a unique transaction ID. Exact POS
    # retry duplicates are created later by replaying the complete basket.
    txn_id = f"TXN-{store.store_id}-{make_id(rng)}"

    order_date = pick_date(rng, start, end)
    order_date_str = order_date.strftime("%Y-%m-%d")
    order_time_str = pick_time_of_day(rng, store, order_date)
    is_promo_week = is_promo_period(order_date)
    promo_week_id = f"PW{order_date.strftime('%Y-%V')}"  # computed once, not per line
    basket_id = "BSK-" + record_hash(txn_id, store.store_id, order_date_str)[:12]

    # Walk-in vs registered customer — fixes the silver/gold contract mismatch.
    if rng.random() < walkin_rate:
        customer = None
        gender = None
    else:
        customer_picker = _cached_picker(customer_ids, customer_freq_weights)
        customer = customers_map[customer_picker.pick(rng)]
        gender = encode_gender(customer.gender_code, source_system)

    # Membership is a STABLE property of the customer (decided in
    # build_customers), not a per-basket coin flip. It only "activates" once
    # the loyalty program launched — pre-launch transactions show no
    # membership even for customers who later hold a card.
    has_loyalty = (
        customer is not None and customer.is_member and order_date >= LOYALTY_LAUNCH
    )
    coupon_applied = has_loyalty and (rng.random() < 0.15)
    coupon_code = None
    if coupon_applied:
        coupon_code = f"KL-{rng.choice(['SAVE5', 'SAVE10', 'BIO15', 'WEEK20', 'VIP30'])}-{rng.randint(1000, 9999)}"

    # Terminal — picked from this store's range.
    term_num = rng.randint(1, store.terminal_count)
    pos_term_id = f"POS-{store.store_id}-T{term_num:02d}"
    term_type, is_sco = terminals.get(pos_term_id, ("CASHIER", False))
    cashier_id = None if is_sco else f"EMP{rng.randint(1, 3000):04d}"

    order_status = _ORDER_STATUS_PICKER.pick(rng)

    # Pick unique products for this basket. Uses the cached cumulative
    # distribution (sample_product) — ~68x faster than rng.choices per call.
    n_items = pick_basket_size(rng)
    month = order_date.month
    seen, picked = set(), []
    # Guard against the (impossible-in-practice) case of a basket larger than
    # the month's catalogue: cap retries so we can never spin forever.
    max_attempts = n_items * 20
    attempts = 0
    while len(picked) < n_items and attempts < max_attempts:
        attempts += 1
        prod = sample_product(rng, month)
        if prod[2] not in seen:
            seen.add(prod[2])
            picked.append(prod)

    # Build rows with placeholder payment, then fix payment after we know the total.
    rows = []
    for seq, prod in enumerate(picked, start=1):
        rows.append(
            generate_line_item(
                rng,
                prod,
                txn_id=txn_id,
                basket_id=basket_id,
                order_date_str=order_date_str,
                order_time_str=order_time_str,
                is_promo_week=is_promo_week,
                promo_week_id=promo_week_id,
                store=store,
                customer=customer,
                gender=gender,
                has_loyalty=has_loyalty,
                coupon_applied=coupon_applied,
                coupon_code=coupon_code,
                payment_type="__PENDING__",
                pos_terminal_id=pos_term_id,
                terminal_type=term_type,
                is_sco=is_sco,
                cashier_id=cashier_id,
                source_system=source_system,
                order_status=order_status,
                batch_id=batch_id,
                ingestion_date_str=ingestion_date_str,
                effective_price_eur=(
                    price_lookup(product_id_for(prod[2]), order_date)
                    if price_lookup is not None
                    else round((float(prod[5]) + float(prod[6])) / 2, 2)
                ),
                line_seq=seq,
            )
        )

    basket_total = sum(
        r["net_revenue_eur"]
        for r in rows
        if r["net_revenue_eur"] and r["net_revenue_eur"] > 0
    )
    payment = pick_payment(rng, basket_total)
    for r in rows:
        r["payment_type"] = payment

    return rows


# ═══════════════════════════════════════════════════════════════════════════
# Writers — one function per output mode, no shared god-loop
# ═══════════════════════════════════════════════════════════════════════════

# Columns that belong in dim tables and get stripped from fact_transactions.
_DIM_DROP = {
    "store_city",
    "store_district",
    "store_postal_code",
    "store_area",
    "store_region",
    "store_country_code",
    "store_country_name",
    "store_size_class",
    "customer_age",
    "gender",
    "loyalty_card_id",
    "product_name",
    "product_category",
    "product_subcategory",
    "product_unit",
}


def mark_duplicate_basket(basket: list[dict]) -> list[dict]:
    """Return an exact retry copy with an INFO flag and identical record hashes."""
    duplicate = [dict(row) for row in basket]
    for row in duplicate:
        flag = row["data_quality_flag"]
        if "INFO:DUPLICATE_TXN" not in flag:
            row["data_quality_flag"] = (
                "INFO:DUPLICATE_TXN" if flag == "OK" else f"{flag}|INFO:DUPLICATE_TXN"
            )
    return duplicate


def basket_stream(
    rng: Random,
    n_records: int,
    stores,
    store_weights,
    customers_map,
    customer_ids,
    customer_freq_weights,
    terminals,
    start: datetime,
    end: datetime,
    batch_id: str,
    ingestion_date_str: str,
    walkin_rate: float,
    price_lookup: Callable[[str, datetime], float] | None = None,
    duplicate_rate: float = DUPLICATE_BASKET_RATE,
) -> Iterator[list[dict]]:
    """Yield new baskets and exact POS-retry copies until n_records are emitted."""
    emitted = 0
    recent_baskets: list[list[dict]] = []
    while emitted < n_records:
        if recent_baskets and rng.random() < duplicate_rate:
            basket = mark_duplicate_basket(rng.choice(recent_baskets))
        else:
            basket = generate_basket(
                rng,
                stores,
                store_weights,
                customers_map,
                customer_ids,
                customer_freq_weights,
                terminals,
                start,
                end,
                batch_id,
                ingestion_date_str,
                walkin_rate,
                price_lookup,
            )
            recent_baskets.append([dict(row) for row in basket])
            if len(recent_baskets) > 5000:
                recent_baskets.pop(0)

        emitted += len(basket)
        yield basket


def write_dim_stores(stores: list[Store], out_dir: Path) -> int:
    path = out_dir / "dim_stores.csv"
    cols = [
        "store_id",
        "city",
        "district",
        "postal_code",
        "street",
        "region",
        "country_code",
        "country_name",
        "size_class",
        "terminal_count",
        "source_system",
        "opening_hours",
        "currency",
    ]
    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=cols)
        w.writeheader()
        for s in stores:
            row = {
                **s.__dict__,
                "opening_hours": json.dumps(s.opening_hours),
                "currency": "EUR",
            }
            w.writerow(row)
    return len(stores)


def write_dim_products(out_dir: Path) -> int:
    path = out_dir / "dim_products.csv"
    cols = [
        "product_id",
        "product_name",
        "category",
        "subcategory",
        "default_brand",
        "is_private_label_eligible",
        "price_min_eur",
        "price_max_eur",
        "unit",
        "seasonal_months",
        "vat_rate",
    ]
    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=cols)
        w.writeheader()
        for p in PRODUCTS:
            cat, subcat, name, brand, pl_ok, p_min, p_max, _, _, unit, seasonal = p
            w.writerow(
                {
                    "product_id": product_id_for(name),
                    "product_name": name,
                    "category": cat,
                    "subcategory": subcat,
                    "default_brand": brand,
                    "is_private_label_eligible": pl_ok,
                    "price_min_eur": p_min,
                    "price_max_eur": p_max,
                    "unit": unit,
                    "seasonal_months": json.dumps(seasonal) if seasonal else "",
                    "vat_rate": VAT_BY_CATEGORY.get(cat, 0.19),
                }
            )
    return len(PRODUCTS)


def write_dim_customers(customers_map: dict[str, Customer], out_dir: Path) -> int:
    path = out_dir / "dim_customers.csv"
    cols = ["customer_id", "age", "gender_code", "is_member", "loyalty_card_id"]
    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=cols)
        w.writeheader()
        for c in customers_map.values():
            w.writerow(
                {
                    "customer_id": c.customer_id,
                    "age": c.age,
                    "gender_code": c.gender_code,
                    "is_member": c.is_member,
                    "loyalty_card_id": c.loyalty_card_id,
                }
            )
    return len(customers_map)


def make_return_rows(
    rng: Random, basket: list[dict], ingestion_date_str: str, end_date: datetime
) -> list[dict]:
    """Build return facts whose refunds never exceed the amount paid."""
    base = basket[0]
    order_dt = datetime.strptime(base["order_date"], "%Y-%m-%d")
    return_dt = order_dt + timedelta(days=rng.randint(1, 7))
    if return_dt.weekday() == 6:
        return_dt += timedelta(days=1)
    if return_dt > end_date:
        return []

    return_date_str = return_dt.strftime("%Y-%m-%d")
    return_time_str = pick_time_of_day_for_return(
        rng, base.get("store_size_class", "M"), return_dt
    )
    full_return = rng.random() < 0.40
    valid_items = [
        row
        for row in basket
        if row.get("quantity")
        and row["quantity"] > 0
        and row.get("net_revenue_eur") is not None
    ]
    if not valid_items:
        return []
    items = (
        valid_items
        if full_return
        else rng.sample(valid_items, k=rng.randint(1, min(2, len(valid_items))))
    )
    reason = rng.choices(RETURN_REASONS, weights=RETURN_REASON_W, k=1)[0]
    cashier = f"EMP{rng.randint(1, 3000):04d}"

    out = []
    for seq, row in enumerate(items, start=1):
        original_qty = int(row["quantity"])
        return_qty = original_qty if full_return else rng.randint(1, original_qty)
        net_unit_price = row["net_revenue_eur"] / original_qty
        refund = round(net_unit_price * return_qty, 2)
        return_key = record_hash(
            base["basket_id"], row["product_id"], return_date_str, seq
        )[:16].upper()
        out.append(
            {
                "return_id": f"RET-{return_key}",
                "original_transaction_id": base["transaction_id"],
                "original_basket_id": base["basket_id"],
                "return_date": return_date_str,
                "return_time": return_time_str,
                "store_id": base["store_id"],
                "customer_id": base["customer_id"],
                "product_id": row["product_id"],
                "original_quantity": original_qty,
                "return_quantity": return_qty,
                "original_unit_price_eur": row.get("unit_price_eur"),
                "original_discount_pct": row.get("discount_pct"),
                "net_unit_price_eur": round(net_unit_price, 4),
                "refund_amount_eur": refund,
                "reason_code": reason,
                "cashier_id": cashier,
                "ingestion_date": return_date_str,
            }
        )
    return out


def pick_time_of_day_for_return(
    rng: Random, size_class: str, return_date: datetime
) -> str:
    """Returns are processed within conservative store-service hours."""
    is_saturday = return_date.weekday() == 5
    close_h = (
        14
        if (size_class == "S" and is_saturday)
        else (18 if size_class == "S" else (20 if is_saturday else 21))
    )
    hours = list(range(9, close_h))
    h = rng.choice(hours)
    return f"{h:02d}:{rng.randint(0, 59):02d}:{rng.randint(0, 59):02d}"


_FACT_RETURNS_COLS = [
    "return_id",
    "original_transaction_id",
    "original_basket_id",
    "return_date",
    "return_time",
    "store_id",
    "customer_id",
    "product_id",
    "original_quantity",
    "return_quantity",
    "original_unit_price_eur",
    "original_discount_pct",
    "net_unit_price_eur",
    "refund_amount_eur",
    "reason_code",
    "cashier_id",
    "ingestion_date",
]


def write_flat(stream: Iterator[list[dict]], out_dir: Path, n_target: int) -> int:
    """Single denormalised CSV. Quick-tests / legacy."""
    path = out_dir / "einkaufpark_de_sales_raw.csv"
    rows_written = 0
    writer = None
    with open(path, "w", newline="", encoding="utf-8") as f:
        for basket in stream:
            if rows_written >= n_target:
                break
            for row in basket:
                if writer is None:
                    writer = csv.DictWriter(f, fieldnames=list(row.keys()))
                    writer.writeheader()
                writer.writerow(row)
                rows_written += 1
    print(f"  flat CSV         : {rows_written:>10,} rows  → {path.name}")
    return rows_written


def write_normalized(
    stream: Iterator[list[dict]],
    out_dir: Path,
    n_target: int,
    stores: list[Store],
    customers_map: dict[str, Customer],
    rng: Random,
    today_str: str,
    end_date: datetime,
    return_rate: float = 0.04,
) -> int:
    """Five CSVs: 3 dims + fact_transactions + fact_returns."""
    write_dim_stores(stores, out_dir)
    write_dim_products(out_dir)
    write_dim_customers(customers_map, out_dir)
    print(
        f"  dimensions       :        50 stores + {len(PRODUCTS)} products + {len(customers_map):,} customers"
    )

    fact_path = out_dir / "fact_transactions.csv"
    ret_path = out_dir / "fact_returns.csv"

    rows_written, n_returns = 0, 0
    fact_w, ret_w = None, None
    with (
        open(fact_path, "w", newline="", encoding="utf-8") as ff,
        open(ret_path, "w", newline="", encoding="utf-8") as rf,
    ):
        ret_w = csv.DictWriter(rf, fieldnames=_FACT_RETURNS_COLS)
        ret_w.writeheader()

        for basket in stream:
            if rows_written >= n_target:
                break

            is_completed = basket[0]["order_status"] == "Completed"
            has_valid_sale = any(
                row.get("quantity")
                and row["quantity"] > 0
                and row.get("net_revenue_eur") is not None
                for row in basket
            )
            needs_return = (
                is_completed and has_valid_sale and rng.random() < return_rate
            )

            if needs_return:
                for rr in make_return_rows(rng, basket, today_str, end_date):
                    ret_w.writerow(rr)
                    n_returns += 1

            for row in basket:
                fact_row = {k: v for k, v in row.items() if k not in _DIM_DROP}
                if fact_w is None:
                    fact_w = csv.DictWriter(ff, fieldnames=list(fact_row.keys()))
                    fact_w.writeheader()
                fact_w.writerow(fact_row)
                rows_written += 1

    print(f"  fact_transactions: {rows_written:>10,} rows  → {fact_path.name}")
    print(f"  fact_returns     : {n_returns:>10,} rows  → {ret_path.name}")
    return rows_written


# ═══════════════════════════════════════════════════════════════════════════
# Validation — success criteria from the module docstring, enforced
# ═══════════════════════════════════════════════════════════════════════════


def check_reproducibility(args, n_canary: int = 1000) -> tuple[bool, str]:
    """R1: regenerate canary rows with same seed, compare digest."""

    def digest_once() -> str:
        rng = Random(args.seed)
        stores, sw = load_stores(Path(args.master_dir))
        terminals = load_terminals(Path(args.master_dir))
        cmap, cids, cw = build_customers(args.customers, rng)
        today = args.generation_date or args.end_date
        start = datetime.strptime(args.start_date, "%Y-%m-%d")
        end = datetime.strptime(args.end_date, "%Y-%m-%d")
        h = hashlib.sha256()
        for basket in basket_stream(
            rng,
            n_canary,
            stores,
            sw,
            cmap,
            cids,
            cw,
            terminals,
            start,
            end,
            "CANARY",
            today,
            args.walkin_rate,
            duplicate_rate=args.duplicate_rate,
        ):
            for row in basket:
                h.update(json.dumps(row, sort_keys=True, default=str).encode())
        return h.hexdigest()

    d1, d2 = digest_once(), digest_once()
    ok = d1 == d2
    return (
        ok,
        f"digest match: {d1[:12]}" if ok else f"MISMATCH ({d1[:12]} vs {d2[:12]})",
    )


def check_fk_integrity(out_dir: Path, mode: str) -> tuple[bool, str]:
    """R2: every product_id / store_id in facts must exist in dims."""
    if mode == "flat":
        return True, "skipped (single-file mode)"

    def load_col(path: Path, col: str) -> set:
        with open(path, encoding="utf-8") as f:
            return {row[col] for row in csv.DictReader(f)}

    dim_products = load_col(out_dir / "dim_products.csv", "product_id")
    dim_stores = load_col(out_dir / "dim_stores.csv", "store_id")
    fact_pids = load_col(out_dir / "fact_transactions.csv", "product_id")
    fact_sids = load_col(out_dir / "fact_transactions.csv", "store_id")

    missing_p = fact_pids - dim_products
    missing_s = fact_sids - dim_stores
    if missing_p or missing_s:
        return False, f"missing: {len(missing_p)} products, {len(missing_s)} stores"
    return True, f"{len(fact_pids)} products × {len(fact_sids)} stores all resolved"


def check_no_sundays(out_dir: Path, mode: str) -> tuple[bool, str]:
    """R3: zero records on Sundays (Sonntagsruhe)."""
    fname = (
        "einkaufpark_de_sales_raw.csv" if mode == "flat" else "fact_transactions.csv"
    )
    n_sun = 0
    with open(out_dir / fname, encoding="utf-8") as f:
        for row in csv.DictReader(f):
            if datetime.strptime(row["order_date"], "%Y-%m-%d").weekday() == 6:
                n_sun += 1
    return n_sun == 0, "0 Sunday rows" if n_sun == 0 else f"FAIL: {n_sun} Sunday rows"


def check_dq_rates(out_dir: Path, mode: str, schema: dict) -> tuple[bool, str]:
    """R4: observed row classes remain close to the configured contract."""
    fname = (
        "einkaufpark_de_sales_raw.csv" if mode == "flat" else "fact_transactions.csv"
    )
    counts: Counter[str] = Counter()
    total = 0
    with open(out_dir / fname, encoding="utf-8") as f:
        for row in csv.DictReader(f):
            total += 1
            flag = row["data_quality_flag"]
            if "ERR" in flag:
                counts["err"] += 1
            elif "WARN" in flag:
                counts["warn"] += 1
            elif "INFO" in flag:
                counts["info"] += 1
            else:
                counts["ok"] += 1

    expected = schema["expected_dq_rates"]
    observed = {
        name: 100 * counts[name] / max(total, 1)
        for name in ("ok", "warn", "err", "info")
    }
    expected_keys = {
        "ok": "ok_rows_pct",
        "warn": "warn_rows_pct",
        "err": "err_rows_pct",
        "info": "info_rows_pct",
    }
    messages = []
    passing = True
    for name, key in expected_keys.items():
        target = float(expected.get(key, 0.0))
        delta = abs(observed[name] - target)
        if delta > 0.75:
            passing = False
        messages.append(
            f"{name}={observed[name]:.2f}%(exp {target:.1f}%, Δ{delta:.2f}pp)"
        )
    return passing, " ".join(messages)


def check_walkin_rate(out_dir: Path, mode: str, target: float) -> tuple[bool, str]:
    """R5: walk-in basket rate within ±2pp of configured walkin_rate."""
    fname = (
        "einkaufpark_de_sales_raw.csv" if mode == "flat" else "fact_transactions.csv"
    )
    seen_baskets: set[str] = set()
    walkin_baskets: set[str] = set()
    with open(out_dir / fname, encoding="utf-8") as f:
        for row in csv.DictReader(f):
            b = row["basket_id"]
            seen_baskets.add(b)
            if row["customer_id"] == "" or row["customer_id"] is None:
                walkin_baskets.add(b)
    if not seen_baskets:
        return False, "no baskets found"
    observed = len(walkin_baskets) / len(seen_baskets)
    delta = abs(observed - target)
    return (
        delta <= 0.02,
        f"walk-in rate={observed * 100:.1f}% (target {target * 100:.0f}%, Δ{delta * 100:.1f}pp)",
    )


def check_customer_card_uniqueness(out_dir: Path, mode: str) -> tuple[bool, str]:
    if mode == "flat":
        return True, "skipped (customer dimension not written)"
    cards: list[str] = []
    with open(out_dir / "dim_customers.csv", encoding="utf-8") as f:
        for row in csv.DictReader(f):
            if row["loyalty_card_id"]:
                cards.append(row["loyalty_card_id"])
    duplicates = len(cards) - len(set(cards))
    return duplicates == 0, (
        f"{len(cards):,} member cards unique"
        if duplicates == 0
        else f"FAIL: {duplicates:,} duplicate loyalty-card assignments"
    )


def check_transaction_identity(out_dir: Path, mode: str) -> tuple[bool, str]:
    fname = (
        "einkaufpark_de_sales_raw.csv" if mode == "flat" else "fact_transactions.csv"
    )
    contexts: dict[str, set[tuple[str, str, str, str]]] = {}
    hash_counts: Counter[str] = Counter()
    duplicate_hashes: list[str] = []
    with open(out_dir / fname, encoding="utf-8") as f:
        for row in csv.DictReader(f):
            txn = row["transaction_id"]
            context = (
                row["basket_id"],
                row["store_id"],
                row["order_date"],
                row.get("customer_id") or "WALKIN",
            )
            contexts.setdefault(txn, set()).add(context)
            hash_counts[row["record_hash"]] += 1
            if "INFO:DUPLICATE_TXN" in row["data_quality_flag"]:
                duplicate_hashes.append(row["record_hash"])

    conflicts = sum(1 for values in contexts.values() if len(values) > 1)
    orphan_retries = sum(1 for value in duplicate_hashes if hash_counts[value] < 2)
    ok = conflicts == 0 and orphan_retries == 0
    return ok, (
        f"0 key conflicts; {len(duplicate_hashes):,} retry rows are exact copies"
        if ok
        else f"FAIL: {conflicts} transaction-context conflicts, {orphan_retries} non-exact retry rows"
    )


def check_product_attribute_stability(out_dir: Path, mode: str) -> tuple[bool, str]:
    fname = (
        "einkaufpark_de_sales_raw.csv" if mode == "flat" else "fact_transactions.csv"
    )
    attrs: dict[str, set[tuple[str, str]]] = {}
    with open(out_dir / fname, encoding="utf-8") as f:
        for row in csv.DictReader(f):
            attrs.setdefault(row["product_id"], set()).add(
                (row["brand"], row["is_private_label"])
            )
    unstable = sum(1 for values in attrs.values() if len(values) > 1)
    return unstable == 0, (
        f"{len(attrs):,} product IDs have stable brand/private-label attributes"
        if unstable == 0
        else f"FAIL: {unstable} product IDs have conflicting attributes"
    )


def check_refund_integrity(out_dir: Path, mode: str) -> tuple[bool, str]:
    if mode == "flat":
        return True, "skipped (returns fact not written)"
    returns_path = out_dir / "fact_returns.csv"
    if not returns_path.exists():
        return False, "FAIL: fact_returns.csv missing"
    return_ids: set[str] = set()
    duplicates = over_refunds = 0
    rows = 0
    with open(returns_path, encoding="utf-8") as f:
        for row in csv.DictReader(f):
            rows += 1
            if row["return_id"] in return_ids:
                duplicates += 1
            return_ids.add(row["return_id"])
            refund = float(row["refund_amount_eur"])
            maximum = float(row["net_unit_price_eur"]) * int(row["return_quantity"])
            if refund > maximum + 0.011:
                over_refunds += 1
    ok = duplicates == 0 and over_refunds == 0
    return ok, (
        f"{rows:,} return rows: unique IDs and refund ≤ amount paid"
        if ok
        else f"FAIL: {duplicates} duplicate return IDs, {over_refunds} over-refunds"
    )


def validate(args, out_dir: Path, schema: dict) -> bool:
    print(f"\n  Validation ({chr(9472) * 46}")
    checks = [
        ("R1 reproducibility", lambda: check_reproducibility(args)),
        ("R2 FK integrity", lambda: check_fk_integrity(out_dir, args.mode)),
        ("R3 no Sundays", lambda: check_no_sundays(out_dir, args.mode)),
        ("R4 DQ rates", lambda: check_dq_rates(out_dir, args.mode, schema)),
        (
            "R5 walk-in rate",
            lambda: check_walkin_rate(out_dir, args.mode, args.walkin_rate),
        ),
        (
            "R6 loyalty cards",
            lambda: check_customer_card_uniqueness(out_dir, args.mode),
        ),
        ("R7 transaction IDs", lambda: check_transaction_identity(out_dir, args.mode)),
        (
            "R8 product identity",
            lambda: check_product_attribute_stability(out_dir, args.mode),
        ),
        ("R9 refund integrity", lambda: check_refund_integrity(out_dir, args.mode)),
    ]
    all_pass = True
    for name, fn in checks:
        passed, msg = fn()
        mark = "PASS" if passed else "FAIL"
        print(f"    [{mark}] {name:<24} {msg}")
        if not passed:
            all_pass = False
    print(f"  {chr(9472) * 60}")
    return all_pass


# ═══════════════════════════════════════════════════════════════════════════
# CLI / main
# ═══════════════════════════════════════════════════════════════════════════


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Einkaufpark DE synthetic data generator")
    p.add_argument("--records", type=int, default=1_000_000)
    p.add_argument("--seed", type=int, default=10)
    p.add_argument("--start-date", type=str, default="2023-01-01")
    p.add_argument("--end-date", type=str, default="2026-03-31")
    p.add_argument(
        "--customers", type=int, default=500_000, help="Size of customer master"
    )
    p.add_argument(
        "--walkin-rate",
        type=float,
        default=0.10,
        help="Fraction of baskets with customer_id=NULL",
    )
    p.add_argument("--mode", choices=["flat", "normalized"], default="normalized")
    p.add_argument(
        "--duplicate-rate",
        type=float,
        default=DUPLICATE_BASKET_RATE,
        help="Exact POS retry basket rate",
    )
    p.add_argument(
        "--generation-date",
        type=str,
        default=None,
        help="Deterministic ingestion date; defaults to --end-date",
    )
    p.add_argument("--output-dir", type=str, default="data/raw")
    p.add_argument("--master-dir", type=str, default="master")
    return p.parse_args()


def main() -> int:
    args = parse_args()

    out_dir = Path(args.output_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    master_dir = Path(args.master_dir)
    start_dt = datetime.strptime(args.start_date, "%Y-%m-%d")
    end_dt = datetime.strptime(args.end_date, "%Y-%m-%d")
    today_str = args.generation_date or args.end_date
    batch_config = {
        "generator_version": GENERATOR_VERSION,
        "records": args.records,
        "customers": args.customers,
        "seed": args.seed,
        "start_date": args.start_date,
        "end_date": args.end_date,
        "walkin_rate": args.walkin_rate,
        "duplicate_rate": args.duplicate_rate,
        "generation_date": today_str,
    }
    batch_id = (
        "BATCH_"
        + hashlib.sha256(json.dumps(batch_config, sort_keys=True).encode())
        .hexdigest()[:16]
        .upper()
    )

    rng = Random(args.seed)

    print("\n  Einkaufpark DE — Generator")
    print(f"  {chr(9472) * 60}")
    print(f"  mode       : {args.mode}")
    print(f"  records    : {args.records:,}")
    print(f"  seed       : {args.seed}")
    print(f"  date range : {args.start_date} → {args.end_date}")
    print(f"  walkin rate: {args.walkin_rate:.0%}")
    print(f"  output     : {out_dir}/")
    print(f"  {chr(9472) * 60}")

    stores, store_weights = load_stores(master_dir)
    terminals = load_terminals(master_dir)
    schema = load_schema(master_dir)
    customers_map, customer_ids, customer_freq_weights = build_customers(
        args.customers, rng
    )
    print(
        f"  Loaded {len(stores)} stores, {len(terminals)} terminals, "
        f"{len(customers_map):,} customers"
    )

    stream = basket_stream(
        rng,
        args.records,
        stores,
        store_weights,
        customers_map,
        customer_ids,
        customer_freq_weights,
        terminals,
        start_dt,
        end_dt,
        batch_id,
        today_str,
        args.walkin_rate,
        duplicate_rate=args.duplicate_rate,
    )

    if args.mode == "flat":
        write_flat(stream, out_dir, args.records)
    else:
        write_normalized(
            stream, out_dir, args.records, stores, customers_map, rng, today_str, end_dt
        )

    ok = validate(args, out_dir, schema)
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(main())
