"""
Einkaufpark DE — Synthetic Sales Data Generator v3.3
=====================================================
Scope   : Physical (in-store) retail, Germany only
Channel : IN_STORE exclusively

Three output modes (--mode):

  flat         Single denormalised CSV (49 cols). Legacy / quick tests.
  normalized   5 separate CSVs: 3 dims + 2 facts. Pipeline has to JOIN.
  incremental  Daily batch files + SCD2 price history + late arrivals.
               The full pipeline-ready format for portfolio demos.

v3.3 (Phase 3 — incremental simulation):
  1.  --mode incremental: daily batch files under batches/batch_YYYYMMDD.csv
  2.  SCD2 price history: dim_products_scd2.csv with effective_from/to,
      including weekly promos (15-30% off, 7 days) and quarterly inflation.
  3.  Late-arriving records: ~5% of baskets land 1-3 days late in a future
      batch, marked INFO:LATE_ARRIVAL. Tests pipeline idempotency.
  4.  Daily volume distribution: records spread across dates using DOW/month
      weights with ±15% Poisson-like noise per day.
  5.  Overflow batch: late arrivals past end_date captured in _late.csv.

v3.2 (Phase 2 — normalized output):
  Separate dim + fact CSVs. Returns as independent fact table.

v3.1 (Phase 1 — realism fixes):
  Customer frequency (Pareto), product popularity (Zipf),
  payment-by-basket-value, store-aware hours.

v3.0: Sunday closures, store weights, terminal master, brand consistency,
  promo fix, gender encoding, loyalty cards, timestamps, DQ flags.
"""

import argparse
import csv
import hashlib
import json
import os
import random
import sys
import uuid
from collections import deque
from dataclasses import dataclass
from datetime import datetime, timedelta
from pathlib import Path
from typing import Optional

# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def parse_args():
    p = argparse.ArgumentParser(description="Generate Einkaufpark DE synthetic sales data v3.3")
    p.add_argument("--records",        type=int, default=12_752_000,       help="Number of transaction rows to generate")
    p.add_argument("--seed",           type=int, default=10,           help="Random seed")
    p.add_argument("--start-date",     type=str, default="2023-01-01")
    p.add_argument("--end-date",       type=str, default="2026-03-31")
    p.add_argument("--output-dir",     type=str, default=None,         help="Override output directory")
    p.add_argument("--master-dir",     type=str, default=None,         help="Path to master/ folder")
    p.add_argument("--checkpoint",     type=int, default=10_000,       help="Checkpoint interval (rows)")
    p.add_argument("--mode",           type=str, default="normalized", choices=["flat","normalized","incremental"],
                   help="'flat' = single denormalised CSV. "
                        "'normalized' = separate dim + fact CSVs. "
                        "'incremental' = daily batch files + SCD2 prices (default for pipeline demos).")
    return p.parse_args()

# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------

BASE_DIR        = Path(os.getcwd())
DEFAULT_OUT     = BASE_DIR / "data" / "raw"
DEFAULT_MASTER  = BASE_DIR / "master"
OUTPUT_FILE     = "einkaufpark_de_sales_raw.csv"

# ---------------------------------------------------------------------------
# Store master — loaded from JSON (single source of truth)
# ---------------------------------------------------------------------------

def load_store_master(master_dir: Path) -> list:
    path = master_dir / "store_master.json"
    if not path.exists():
        print(f"\n  ERROR: store_master.json not found at {path}")
        sys.exit(1)
    with open(path, encoding="utf-8") as f:
        data = json.load(f)

    stores = []
    for s in data["stores"]:
        stores.append({
            "country_code":  s["country_code"],
            "country_name":  s["country_name"],
            "region":        s["state"],
            "city":          s["city"],
            "district":      s["district"],
            "postal_code":   s["postal_code"],
            "area":          s["street"],
            "currency":      "EUR",
            "store_id":      s["store_id"],
            "size_class":    s["size_class"],
            "terminal_count": s["terminal_count"],
            "source_system": s.get("source_system", "SAP_POS"),
        })
    print(f"  Loaded store master: {len(stores)} stores")
    return stores


def build_store_weights(stores: list) -> list:
    """Large hypermarkets do 5-10x the volume of small stores."""
    SIZE_WEIGHT = {"L": 5.0, "M": 2.5, "S": 1.0}
    return [SIZE_WEIGHT.get(s["size_class"], 1.0) for s in stores]


# ---------------------------------------------------------------------------
# Terminal master loader
# ---------------------------------------------------------------------------

def load_terminal_master(master_dir: Path) -> dict:
    path = master_dir / "terminal_master.json"
    if not path.exists():
        print(f"\n  ERROR: terminal_master.json not found at {path}")
        sys.exit(1)
    with open(path, encoding="utf-8") as f:
        data = json.load(f)
    lookup = {}
    for t in data["terminals"]:
        lookup[t["terminal_id"]] = {
            "terminal_type":    t["terminal_type"],
            "is_self_checkout": t["is_self_checkout"],
        }
    print(f"  Loaded terminal master: {len(lookup)} terminals across all stores")
    return lookup


def get_terminal_info(terminal_id: str, terminal_master: dict) -> tuple:
    entry = terminal_master.get(terminal_id)
    if entry:
        return entry["terminal_type"], entry["is_self_checkout"]
    return "CASHIER", False

# ---------------------------------------------------------------------------
# Products — loaded from the v3 catalogue (typo fixed)
# ---------------------------------------------------------------------------

from product_catalogue import PRODUCTS, pick_product, get_available_products  # noqa: E402

PRIVATE_LABELS = ["EKP-Classic", "EKP-Bio", "EKP-Favourites", "EKP-take it easy", "EKP-Free"]

# Cache seasonal product filtering (only 12 possible months)
_product_cache = {}
def get_products_cached(month: int):
    if month not in _product_cache:
        _product_cache[month] = get_available_products(month)
    return _product_cache[month]


# ---------------------------------------------------------------------------
# Payment / status
# ---------------------------------------------------------------------------

# PAYMENT_TYPES moved to _select_payment_by_value() — basket-value-dependent (Fix 3)

ORDER_STATUSES       = ["Completed","Voided","Partially_Returned","Returned"]
ORDER_STATUS_WEIGHTS = [0.93, 0.03, 0.025, 0.015]

SOURCE_SYSTEMS        = ["SAP_POS","LEGACY_POS_CSV"]
SOURCE_SYSTEM_WEIGHTS = [0.85, 0.15]

# ---------------------------------------------------------------------------
# Seasonality
# ---------------------------------------------------------------------------

# Sunday = 0 — Germany has Sonntagsruhe.
DOW_WEIGHTS   = [0.11, 0.12, 0.13, 0.14, 0.21, 0.29, 0.00]
MONTH_WEIGHTS = [0.07, 0.06, 0.08, 0.09, 0.08, 0.07, 0.07, 0.08, 0.09, 0.09, 0.10, 0.12]

# Intraday pattern: realistic German store hours (07:00-21:00)
HOUR_WEIGHTS = {
    7: 0.02, 8: 0.04, 9: 0.06, 10: 0.08, 11: 0.10, 12: 0.11,
    13: 0.09, 14: 0.07, 15: 0.07, 16: 0.08, 17: 0.10, 18: 0.09,
    19: 0.05, 20: 0.03, 21: 0.01,
}
HOURS = list(HOUR_WEIGHTS.keys())
HOUR_W = list(HOUR_WEIGHTS.values())


def is_promo_period(d: datetime) -> bool:
    iso_week = d.isocalendar()[1]
    month, day = d.month, d.day
    if iso_week == 47:                                return True
    if month == 12 and day <= 23:                     return True
    if iso_week in (13, 14, 15) and month in (3, 4):  return True
    return False

# ---------------------------------------------------------------------------
# Loyalty
# ---------------------------------------------------------------------------

LOYALTY_LAUNCH      = datetime(2023, 3, 1)
LOYALTY_TIERS       = ["Bronze","Silver","Gold","Platinum"]
LOYALTY_TIER_WEIGHTS= [0.55, 0.28, 0.12, 0.05]

# ---------------------------------------------------------------------------
# Customer master — with STABLE loyalty card IDs + fixed demographics
# ---------------------------------------------------------------------------

def _build_customer_master(n: int, rng: random.Random) -> tuple:
    """Build customer master with Pareto-distributed shopping frequency.

    Returns (master_dict, customer_ids_list, customer_freq_weights_list)
    so we can do weighted selection without rebuilding arrays every basket.

    Frequency model (mirrors real grocery retail):
      ~5%  heavy shoppers   (3-5 visits/week)  → weight 20-40
      ~15% regular shoppers (1-2 visits/week)  → weight 5-12
      ~30% occasional       (2-4 visits/month) → weight 1.5-3
      ~50% rare             (few times/year)   → weight 0.2-0.8
    """
    master = {}
    cid_list = []
    freq_weights = []

    for i in range(1, n + 1):
        cid = f"CUST{i}"
        r = rng.random()
        # Rebalanced: 18-24 ~10%, 25-34 ~20%, 35-49 ~30%, 50-64 ~22%, 65+ ~13%, null ~5%
        if   r < 0.05:  age = None
        elif r < 0.15:  age = rng.randint(18, 24)
        elif r < 0.35:  age = rng.randint(25, 34)
        elif r < 0.65:  age = rng.randint(35, 49)
        elif r < 0.87:  age = rng.randint(50, 64)
        else:           age = rng.randint(65, 85)

        # Raw gender code — encoding applied later based on source_system
        gr = rng.random()
        if   gr < 0.465: gender_code = "M"
        elif gr < 0.93:  gender_code = "F"
        elif gr < 0.97:  gender_code = "Divers"
        else:            gender_code = None

        tier = rng.choices(LOYALTY_TIERS, weights=LOYALTY_TIER_WEIGHTS, k=1)[0]
        loyalty_card_id = f"KLC{rng.randint(1, 3_000_000):08d}"

        # Shopping frequency — Pareto / power-law distribution
        # Higher-tier customers tend to shop more (realistic correlation)
        freq_r = rng.random()
        if freq_r < 0.05:                              # heavy shoppers
            freq_weight = rng.uniform(20.0, 40.0)
        elif freq_r < 0.20:                             # regular shoppers
            freq_weight = rng.uniform(5.0, 12.0)
        elif freq_r < 0.50:                             # occasional
            freq_weight = rng.uniform(1.5, 3.0)
        else:                                           # rare visitors
            freq_weight = rng.uniform(0.2, 0.8)

        # Tier boost — Gold/Platinum shop more often (loyalty = retention)
        tier_freq_mult = {"Bronze": 1.0, "Silver": 1.3, "Gold": 1.8, "Platinum": 2.5}
        freq_weight *= tier_freq_mult.get(tier, 1.0)

        master[cid] = {
            "age":             age,
            "gender_code":     gender_code,
            "tier":            tier,
            "loyalty_card_id": loyalty_card_id,
        }
        cid_list.append(cid)
        freq_weights.append(freq_weight)

    return master, cid_list, freq_weights

# ---------------------------------------------------------------------------
# Date helpers
# ---------------------------------------------------------------------------

def weighted_random_date(start: datetime, end: datetime, rng: random.Random) -> datetime:
    """Sunday rejected outright. Promo periods BOOSTED 1.4x."""
    delta_days = (end - start).days
    for _ in range(200):
        d = start + timedelta(days=rng.randint(0, delta_days))
        if d.weekday() == 6:
            continue
        dow_accept = DOW_WEIGHTS[d.weekday()] / max(DOW_WEIGHTS)
        month_accept = MONTH_WEIGHTS[d.month - 1] / max(MONTH_WEIGHTS)
        promo_mult = 1.4 if is_promo_period(d) else 1.0
        if (rng.random() < dow_accept * promo_mult
                and rng.random() < month_accept):
            return d
    # Fallback — still reject Sunday
    for _ in range(100):
        d = start + timedelta(days=rng.randint(0, delta_days))
        if d.weekday() != 6:
            return d
    return start


def generate_time_of_day(rng: random.Random, size_class: str = "M",
                         is_saturday: bool = False) -> str:
    """Generate realistic transaction time respecting store opening hours.

    Store hours (German retail norms):
      L / M : Mo-Fr 07:00-21:00,  Sa 07:00-20:00
      S     : Mo-Fr 08:00-18:00,  Sa 08:00-14:00
    """
    if size_class == "S":
        open_h  = 8
        close_h = 14 if is_saturday else 18
    else:
        open_h  = 7
        close_h = 20 if is_saturday else 21

    valid_hours   = [h for h in HOURS if open_h <= h < close_h]
    valid_weights = [HOUR_W[HOURS.index(h)] for h in valid_hours]

    hour = rng.choices(valid_hours, weights=valid_weights, k=1)[0]
    minute = rng.randint(0, 59)
    second = rng.randint(0, 59)
    return f"{hour:02d}:{minute:02d}:{second:02d}"


# ---------------------------------------------------------------------------
# Gender encoding — tied to source system
# ---------------------------------------------------------------------------

def encode_gender(gender_code: Optional[str], source_system: str) -> Optional[str]:
    if gender_code is None:
        return None
    if gender_code == "Divers":
        return "Divers"
    if source_system == "LEGACY_POS_CSV":
        return {"M": "Male", "F": "Female"}.get(gender_code, gender_code)
    return gender_code


# ---------------------------------------------------------------------------
# DQ noise helpers
# ---------------------------------------------------------------------------

def maybe_dirty_age(age, rng):
    r = rng.random()
    if r < 0.995:   return age
    elif r < 0.998: return rng.randint(121, 160)
    elif r < 0.9995:return rng.randint(-5, -1)
    else:           return None

def generate_unit_price(p_min, p_max, rng):
    r = rng.random()
    if r < 0.985:   return round(rng.uniform(p_min, p_max), 2)
    elif r < 0.995: return None
    else:           return round(-rng.uniform(0.01, p_max * 0.3), 2)

def generate_discount(rng):
    r = rng.random()
    if r < 0.80:    return 0.0
    elif r < 0.98:  return round(rng.uniform(0.5, 50.0), 2)
    elif r < 0.995: return None
    else:           return round(rng.uniform(100.1, 102.0), 2)

def generate_quantity(qty_min, qty_max, rng):
    qty_min = max(1, int(qty_min))
    qty_max = max(qty_min, int(qty_max))
    r = rng.random()
    if r < 0.975:   return rng.randint(qty_min, qty_max)
    elif r < 0.990: return 0
    else:           return rng.randint(-3, -1)


# ---------------------------------------------------------------------------
# Record hash
# ---------------------------------------------------------------------------

def record_hash(*args) -> str:
    return hashlib.md5("|".join(str(a) for a in args).encode()).hexdigest()

def derive_basket_id(transaction_id: str, store_id: str, order_date: str) -> str:
    return "BSK-" + hashlib.md5(f"{transaction_id}|{store_id}|{order_date}".encode()).hexdigest()[:12]


# ---------------------------------------------------------------------------
# Basket context dataclass
# ---------------------------------------------------------------------------

@dataclass
class BasketContext:
    txn_id: str
    basket_id: str
    batch_id: str
    order_date: datetime
    order_date_str: str
    order_time_str: str
    ship_date_str: str
    is_promo_week: bool
    promo_week_id: str
    store_id: str
    city: str
    district: str
    postal_code: str
    area: str
    region: str
    country_code: str
    country_name: str
    size_class: str
    source_system: str
    customer_id: str
    customer_age: Optional[int]
    gender: Optional[str]
    membership_active: bool
    loyalty_card_id: Optional[str]
    loyalty_tier: Optional[str]
    coupon_applied: bool
    coupon_code: Optional[str]
    pos_terminal_id: str
    terminal_type: str
    is_sco: bool
    cashier_id: Optional[str]
    payment_type: str
    order_status: str
    today_str: str
    is_dup: bool


# ---------------------------------------------------------------------------
# Basket size distribution
# ---------------------------------------------------------------------------

BASKET_SIZE_DIST = [
    (1,  2,  0.15),
    (3,  5,  0.30),
    (6,  9,  0.25),
    (10, 15, 0.20),
    (16, 25, 0.08),
    (26, 40, 0.02),
]
BASKET_MIN_SIZES = [b[0] for b in BASKET_SIZE_DIST]
BASKET_MAX_SIZES = [b[1] for b in BASKET_SIZE_DIST]
BASKET_WEIGHTS   = [b[2] for b in BASKET_SIZE_DIST]

def pick_basket_size(rng: random.Random) -> int:
    bucket = rng.choices(range(len(BASKET_SIZE_DIST)), weights=BASKET_WEIGHTS, k=1)[0]
    lo, hi = BASKET_MIN_SIZES[bucket], BASKET_MAX_SIZES[bucket]
    return rng.randint(lo, hi)


# ---------------------------------------------------------------------------
# Product line item generator
# ---------------------------------------------------------------------------

def generate_line_item(rng, ctx, line_item_seq, prod):
    cat, subcat, product_name, brand_pool, pl_possible, p_min, p_max, qty_min, qty_max, unit, _seasonal = prod

    product_id = "PROD" + hashlib.md5(product_name.encode()).hexdigest()[:6].upper()

    is_pl = pl_possible and (rng.random() < 0.35)
    if is_pl:
        brand = rng.choice(PRIVATE_LABELS)
        # FIX: Update product_name to reflect the actual brand
        name_parts = product_name.split(" ", 1)
        if len(name_parts) > 1:
            product_name = f"{brand} {name_parts[1]}"
        else:
            product_name = f"{brand} {product_name}"
    elif brand_pool == "bulk":
        brand = "EKP-Classic"
        is_pl = True
    else:
        brand = brand_pool

    if ctx.is_promo_week:
        p_min_eff, p_max_eff = p_min * 0.85, p_max * 0.90
    else:
        p_min_eff, p_max_eff = p_min, p_max
    if is_pl:
        p_min_eff *= 0.82
        p_max_eff *= 0.88

    # Order status drives quantity/revenue
    if ctx.order_status == "Voided":
        unit_price_eur = generate_unit_price(p_min_eff, p_max_eff, rng)
        quantity = 0
        discount_pct = 0.0
        net_revenue_eur = 0.0
    elif ctx.order_status == "Returned":
        unit_price_eur = generate_unit_price(p_min_eff, p_max_eff, rng)
        quantity = -rng.randint(max(1, int(qty_min)), max(1, int(qty_max)))
        discount_pct = 0.0
        if unit_price_eur is not None and unit_price_eur > 0:
            net_revenue_eur = round(unit_price_eur * quantity, 2)
        else:
            net_revenue_eur = None
    elif ctx.order_status == "Partially_Returned":
        if rng.random() < 0.40:
            unit_price_eur = generate_unit_price(p_min_eff, p_max_eff, rng)
            quantity = -rng.randint(1, max(1, int(qty_max)))
            discount_pct = 0.0
            if unit_price_eur is not None and unit_price_eur > 0:
                net_revenue_eur = round(unit_price_eur * quantity, 2)
            else:
                net_revenue_eur = None
        else:
            unit_price_eur = generate_unit_price(p_min_eff, p_max_eff, rng)
            discount_pct   = generate_discount(rng)
            quantity       = generate_quantity(qty_min, qty_max, rng)
            if (unit_price_eur is not None and quantity is not None and discount_pct is not None
                    and quantity > 0 and unit_price_eur > 0 and 0 <= discount_pct <= 100):
                net_revenue_eur = round(unit_price_eur * quantity * (1 - discount_pct / 100), 2)
            else:
                net_revenue_eur = None
    else:
        unit_price_eur = generate_unit_price(p_min_eff, p_max_eff, rng)
        discount_pct   = generate_discount(rng)
        quantity       = generate_quantity(qty_min, qty_max, rng)
        if (unit_price_eur is not None and quantity is not None and discount_pct is not None
                and quantity > 0 and unit_price_eur > 0 and 0 <= discount_pct <= 100):
            net_revenue_eur = round(unit_price_eur * quantity * (1 - discount_pct / 100), 2)
        else:
            net_revenue_eur = None

    tier_mult = {"Bronze": 1.0, "Silver": 1.5, "Gold": 2.0, "Platinum": 3.0}
    if ctx.membership_active and net_revenue_eur and net_revenue_eur > 0:
        loyalty_points_earned = int(net_revenue_eur * tier_mult.get(ctx.loyalty_tier or "Bronze", 1.0))
    else:
        loyalty_points_earned = None

    r_hash = record_hash(ctx.txn_id, ctx.order_date_str, ctx.customer_id, product_id, ctx.store_id, line_item_seq)

    dq = []
    if unit_price_eur is None:                                                 dq.append("ERR:PRICE_NULL")
    elif unit_price_eur < 0:                                                   dq.append("WARN:PRICE_NEGATIVE")
    if quantity is None:                                                        dq.append("ERR:QTY_NULL")
    elif quantity == 0 and ctx.order_status != "Voided":                        dq.append("WARN:QTY_ZERO")
    elif quantity < 0 and ctx.order_status not in ("Returned","Partially_Returned"): dq.append("WARN:QTY_NEGATIVE")
    if ctx.customer_age is not None and (ctx.customer_age < 0 or ctx.customer_age > 120): dq.append("WARN:AGE_INVALID")
    if discount_pct is not None and discount_pct > 100:                        dq.append("WARN:DISCOUNT_OVER_100")
    if net_revenue_eur is None:                                                dq.append("ERR:REVENUE_NULL")
    if ctx.ship_date_str < ctx.order_date_str:                                 dq.append("WARN:DATE_SEQUENCE_ERROR")
    if ctx.is_dup:                                                             dq.append("INFO:DUPLICATE_TXN")
    data_quality_flag = "|".join(dq) if dq else "OK"

    return {
        "transaction_id":        ctx.txn_id,
        "basket_id":             ctx.basket_id,
        "batch_id":              ctx.batch_id,
        "source_system":         ctx.source_system,
        "record_hash":           r_hash,
        "order_date":            ctx.order_date_str,
        "order_time":            ctx.order_time_str,
        "ship_date":             ctx.ship_date_str,
        "ingestion_date":        ctx.today_str,
        "sales_channel":         "IN_STORE",
        "order_status":          ctx.order_status,
        "store_id":              ctx.store_id,
        "store_city":            ctx.city,
        "store_district":        ctx.district,
        "store_postal_code":     ctx.postal_code,
        "store_area":            ctx.area,
        "store_region":          ctx.region,
        "store_country_code":    ctx.country_code,
        "store_country_name":    ctx.country_name,
        "store_size_class":      ctx.size_class,
        "customer_id":           ctx.customer_id,
        "customer_age":          ctx.customer_age,
        "gender":                ctx.gender,
        "membership_active":     ctx.membership_active,
        "loyalty_card_id":       ctx.loyalty_card_id,
        "loyalty_tier":          ctx.loyalty_tier,
        "loyalty_points_earned": loyalty_points_earned,
        "coupon_applied":        ctx.coupon_applied,
        "coupon_code":           ctx.coupon_code,
        "product_id":            product_id,
        "product_name":          product_name,
        "product_category":      cat,
        "product_subcategory":   subcat,
        "product_unit":          unit,
        "is_private_label":      is_pl,
        "brand":                 brand,
        "quantity":              quantity,
        "unit_price_eur":        unit_price_eur,
        "discount_pct":          discount_pct,
        "transaction_currency":  "EUR",
        "net_revenue_eur":       net_revenue_eur,
        "payment_type":          ctx.payment_type,
        "pos_terminal_id":       ctx.pos_terminal_id,
        "terminal_type":         ctx.terminal_type,
        "is_self_checkout":      ctx.is_sco,
        "cashier_id":            ctx.cashier_id,
        "promo_week_id":         ctx.promo_week_id,
        "is_promo_period":       ctx.is_promo_week,
        "data_quality_flag":     data_quality_flag,
    }


# ---------------------------------------------------------------------------
# Payment method selection — driven by basket total (Fix 3)
# ---------------------------------------------------------------------------

# Payment method options and basket-value-dependent weight profiles
# In German retail: cash dominates small purchases, EC-Karte (debit) dominates
# medium/large, credit cards are rare, mobile pay is growing but still small.
PAYMENT_TYPES   = ["Card","Cash","Apple_Pay","Google_Pay","Voucher","Gift_Card"]

def _select_payment_by_value(rng: random.Random, basket_total: float) -> str:
    """Pick payment method with probabilities that shift by basket value."""
    if basket_total < 10:
        # Small basket: cash is king
        weights = [0.25, 0.55, 0.08, 0.06, 0.04, 0.02]
    elif basket_total < 30:
        # Medium basket: card starts to dominate
        weights = [0.45, 0.32, 0.10, 0.07, 0.04, 0.02]
    elif basket_total < 75:
        # Larger weekly shop: card clearly preferred
        weights = [0.55, 0.20, 0.10, 0.07, 0.05, 0.03]
    else:
        # Big basket (Großeinkauf): almost nobody pays 100€+ cash
        weights = [0.62, 0.10, 0.11, 0.08, 0.06, 0.03]
    return rng.choices(PAYMENT_TYPES, weights=weights, k=1)[0]


# ---------------------------------------------------------------------------
# Basket generator
# ---------------------------------------------------------------------------

def generate_basket(
    rng, store_master, store_weights,
    customer_master, customer_ids, customer_freq_weights,
    terminal_master,
    start_date, end_date, batch_id, today_str, recent_txn_pool,
):
    store = rng.choices(store_master, weights=store_weights, k=1)[0]
    store_id     = store["store_id"]
    size_class   = store["size_class"]
    n_terminals  = store["terminal_count"]
    source_system = rng.choices(SOURCE_SYSTEMS, weights=SOURCE_SYSTEM_WEIGHTS, k=1)[0]

    if recent_txn_pool and rng.random() < 0.004:
        txn_id = rng.choice(list(recent_txn_pool))
        is_dup = True
    else:
        txn_id = f"TXN-{store_id}-{uuid.uuid4().hex[:12].upper()}"
        is_dup = False
    recent_txn_pool.append(txn_id)

    order_date     = weighted_random_date(start_date, end_date, rng)
    ship_date      = order_date + timedelta(days=rng.choice([-1, 1])) if rng.random() < 0.003 else order_date
    order_date_str = order_date.strftime("%Y-%m-%d")
    is_saturday    = order_date.weekday() == 5
    order_time_str = generate_time_of_day(rng, size_class=size_class,
                                          is_saturday=is_saturday)       # Fix 4
    ship_date_str  = ship_date.strftime("%Y-%m-%d")
    promo_week_id  = f"PW{order_date.strftime('%Y-%V')}"
    is_promo_week  = is_promo_period(order_date)

    basket_id = derive_basket_id(txn_id, store_id, order_date_str)

    # --- Fix 1: Weighted customer selection (Pareto frequency) ---
    customer_id  = rng.choices(customer_ids, weights=customer_freq_weights, k=1)[0]
    cust         = customer_master.get(customer_id, {"age": None, "gender_code": None, "tier": "Bronze", "loyalty_card_id": None})
    customer_age = maybe_dirty_age(cust["age"], rng)
    gender       = encode_gender(cust["gender_code"], source_system)

    has_loyalty = (order_date >= LOYALTY_LAUNCH and rng.random() < 0.54)
    if has_loyalty:
        loyalty_card_id = cust["loyalty_card_id"]
        loyalty_tier    = cust["tier"]
    else:
        loyalty_card_id = None
        loyalty_tier    = None
    membership_active = has_loyalty

    coupon_applied = has_loyalty and (rng.random() < 0.15)
    coupon_code    = (
        f"KL-{rng.choice(['SAVE5','SAVE10','BIO15','WEEK20','VIP30'])}-{rng.randint(1000,9999)}"
        if coupon_applied else None
    )

    pos_terminal_id       = f"POS-{store_id}-T{rng.randint(1, n_terminals):02d}"
    terminal_type, is_sco = get_terminal_info(pos_terminal_id, terminal_master)
    cashier_id            = None if is_sco else f"EMP{rng.randint(1, 3000):04d}"

    # Payment method assigned AFTER basket is built (Fix 3) — placeholder for now
    order_status = rng.choices(ORDER_STATUSES, weights=ORDER_STATUS_WEIGHTS, k=1)[0]

    ctx = BasketContext(
        txn_id=txn_id, basket_id=basket_id, batch_id=batch_id,
        order_date=order_date, order_date_str=order_date_str,
        order_time_str=order_time_str,
        ship_date_str=ship_date_str,
        is_promo_week=is_promo_week, promo_week_id=promo_week_id,
        store_id=store_id, city=store["city"], district=store["district"],
        postal_code=store["postal_code"], area=store["area"],
        region=store["region"], country_code=store["country_code"],
        country_name=store["country_name"], size_class=size_class,
        source_system=source_system,
        customer_id=customer_id, customer_age=customer_age, gender=gender,
        membership_active=membership_active, loyalty_card_id=loyalty_card_id,
        loyalty_tier=loyalty_tier,
        coupon_applied=coupon_applied, coupon_code=coupon_code,
        pos_terminal_id=pos_terminal_id, terminal_type=terminal_type,
        is_sco=is_sco, cashier_id=cashier_id,
        payment_type="__PENDING__", order_status=order_status,
        today_str=today_str, is_dup=is_dup,
    )

    n_items = pick_basket_size(rng)
    products_available, weights_available = get_products_cached(order_date.month)

    # Pick unique products per basket
    selected = {}
    for _ in range(n_items):
        prod = rng.choices(products_available, weights=weights_available, k=1)[0]
        name = prod[2]
        if name in selected:
            for _retry in range(5):
                prod = rng.choices(products_available, weights=weights_available, k=1)[0]
                if prod[2] not in selected:
                    break
        selected[prod[2]] = prod

    rows = []
    for seq, (name, prod) in enumerate(selected.items(), start=1):
        row = generate_line_item(rng=rng, ctx=ctx, line_item_seq=seq, prod=prod)
        rows.append(row)

    # --- Fix 3: Compute basket total, THEN assign payment method ---
    basket_total = sum(
        r["net_revenue_eur"] for r in rows
        if r["net_revenue_eur"] is not None and r["net_revenue_eur"] > 0
    )
    payment_type = _select_payment_by_value(rng, basket_total)
    for r in rows:
        r["payment_type"] = payment_type

    return rows


# ---------------------------------------------------------------------------
# Schema loader
# ---------------------------------------------------------------------------

def load_raw_schema(master_dir: Path) -> dict:
    path = master_dir / "raw_schema.json"
    if not path.exists():
        print(f"\n  ERROR: raw_schema.json not found at {path}")
        sys.exit(1)
    with open(path, encoding="utf-8") as f:
        return json.load(f)

def derive_header(schema: dict) -> list:
    return [col["name"] for col in schema["columns"]]

def validate_row_against_schema(row: dict, schema_columns: list) -> list:
    row_keys    = set(row.keys())
    schema_keys = set(schema_columns)
    missing = schema_keys - row_keys
    extra   = row_keys - schema_keys
    problems = []
    if missing: problems.append(f"Row missing columns: {sorted(missing)}")
    if extra:   problems.append(f"Row has extra columns not in schema: {sorted(extra)}")
    return problems


# ═══════════════════════════════════════════════════════════════════════════
# PHASE 2 — Normalized output (dimension + fact tables)
# ═══════════════════════════════════════════════════════════════════════════
#
# In --mode normalized, the generator writes FIVE separate CSVs:
#
#   dim_stores.csv         — store dimension (one row per store)
#   dim_products.csv       — product dimension (one row per catalogue SKU)
#   dim_customers.csv      — customer dimension (one row per customer profile)
#   fact_transactions.csv  — line-item-level sales facts (FK-only, no dim attrs)
#   fact_returns.csv       — return events (separate from transactions)
#
# The pipeline then joins these in the silver layer — that's the point.
# ═══════════════════════════════════════════════════════════════════════════

# Columns to DROP from the flat row when writing fact_transactions
# These belong in their respective dimension tables.
_DIM_COLUMNS_TO_DROP = {
    # Store attributes → dim_stores
    "store_city", "store_district", "store_postal_code", "store_area",
    "store_region", "store_country_code", "store_country_name", "store_size_class",
    # Customer attributes → dim_customers
    "customer_age", "gender", "loyalty_card_id", "loyalty_tier",
    # Product attributes → dim_products
    "product_name", "product_category", "product_subcategory", "product_unit",
}

# VAT mapping for dim_products (German rates: 7% food, 19% non-food)
_VAT_BY_CATEGORY = {
    "Fresh & Perishables": 0.07,
    "Pantry Staples":      0.07,
    "Frozen & Convenience": 0.07,
    "Beverages":           0.19,
    "Snacks & Confectionery": 0.19,
    "Household":           0.19,
    "Health & Beauty":     0.19,
    "Non-Food":            0.19,
}

# Return reason codes with realistic distribution
_RETURN_REASONS = ["Changed_Mind", "Damaged", "Wrong_Item", "Defective", "Expired"]
_RETURN_REASON_W = [0.40, 0.25, 0.15, 0.12, 0.08]


# ---------------------------------------------------------------------------
# Dimension writers
# ---------------------------------------------------------------------------

def _compute_product_id(product_name: str) -> str:
    """Same hash logic as generate_line_item — single source of truth."""
    return "PROD" + hashlib.md5(product_name.encode()).hexdigest()[:6].upper()


def write_dim_stores(store_master: list, output_dir: Path) -> int:
    """Write store dimension CSV."""
    path = output_dir / "dim_stores.csv"
    header = [
        "store_id", "city", "district", "postal_code", "street",
        "region", "country_code", "country_name", "size_class",
        "terminal_count", "currency",
    ]
    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=header)
        w.writeheader()
        for s in store_master:
            w.writerow({
                "store_id":       s["store_id"],
                "city":           s["city"],
                "district":       s["district"],
                "postal_code":    s["postal_code"],
                "street":         s["area"],
                "region":         s["region"],
                "country_code":   s["country_code"],
                "country_name":   s["country_name"],
                "size_class":     s["size_class"],
                "terminal_count": s["terminal_count"],
                "currency":       s["currency"],
            })
    print(f"  dim_stores.csv     : {len(store_master)} rows")
    return len(store_master)


def write_dim_products(output_dir: Path) -> int:
    """Write product dimension CSV from the catalogue.

    product_id is computed with the same hash function used in
    generate_line_item, so FK references will match.
    """
    path = output_dir / "dim_products.csv"
    header = [
        "product_id", "product_name", "category", "subcategory",
        "default_brand", "is_private_label_eligible",
        "price_min_eur", "price_max_eur", "unit",
        "seasonal_months", "vat_rate",
    ]
    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=header)
        w.writeheader()
        for p in PRODUCTS:
            cat, subcat, name, brand, pl_ok, p_min, p_max, q_min, q_max, unit, seasonal = p
            w.writerow({
                "product_id":                _compute_product_id(name),
                "product_name":              name,
                "category":                  cat,
                "subcategory":               subcat,
                "default_brand":             brand,
                "is_private_label_eligible":  pl_ok,
                "price_min_eur":             p_min,
                "price_max_eur":             p_max,
                "unit":                      unit,
                "seasonal_months":           json.dumps(seasonal) if seasonal else "",
                "vat_rate":                  _VAT_BY_CATEGORY.get(cat, 0.19),
            })
    print(f"  dim_products.csv   : {len(PRODUCTS)} rows")
    return len(PRODUCTS)


def write_dim_customers(customer_master: dict, output_dir: Path) -> int:
    """Write customer dimension CSV."""
    path = output_dir / "dim_customers.csv"
    header = [
        "customer_id", "age", "gender_code", "loyalty_tier", "loyalty_card_id",
    ]
    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=header)
        w.writeheader()
        for cid, attrs in customer_master.items():
            w.writerow({
                "customer_id":    cid,
                "age":            attrs["age"],
                "gender_code":    attrs["gender_code"],
                "loyalty_tier":   attrs["tier"],
                "loyalty_card_id": attrs["loyalty_card_id"],
            })
    print(f"  dim_customers.csv  : {len(customer_master):,} rows")
    return len(customer_master)


# ---------------------------------------------------------------------------
# Return generation (normalized mode only)
# ---------------------------------------------------------------------------

def _make_return_records(
    rng: random.Random,
    basket_rows: list,
    today_str: str,
    end_date: datetime,
) -> list:
    """Create fact_returns rows from a basket that was flagged for return.

    In normalized mode, ALL fact_transactions carry positive quantities and
    status 'Completed'. Returns are separate events in fact_returns that
    arrive 1-7 days after the original purchase.

    Returns either the full basket (full return, ~40%) or 1-2 random items
    (partial return, ~60%).
    """
    if not basket_rows:
        return []

    base_row     = basket_rows[0]
    order_date   = datetime.strptime(base_row["order_date"], "%Y-%m-%d")
    delay_days   = rng.randint(1, 7)
    return_date  = order_date + timedelta(days=delay_days)

    # Skip if return date falls on Sunday or outside data range
    if return_date.weekday() == 6:
        return_date += timedelta(days=1)
    if return_date > end_date:
        return []

    return_date_str = return_date.strftime("%Y-%m-%d")
    return_time_str = generate_time_of_day(
        rng,
        size_class=base_row.get("store_size_class", "M"),
        is_saturday=(return_date.weekday() == 5),
    )

    # Decide: full return (40%) or partial return (60%)
    is_full_return = rng.random() < 0.40
    if is_full_return:
        items_to_return = basket_rows
    else:
        n_return = rng.randint(1, min(2, len(basket_rows)))
        items_to_return = rng.sample(basket_rows, k=n_return)

    cashier_id = f"EMP{rng.randint(1, 3000):04d}"
    reason     = rng.choices(_RETURN_REASONS, weights=_RETURN_REASON_W, k=1)[0]

    records = []
    for seq, row in enumerate(items_to_return, start=1):
        qty = row.get("quantity")
        if qty is None or qty <= 0:
            continue
        price = row.get("unit_price_eur")
        refund = round(price * qty, 2) if price and price > 0 else None

        ret_id = f"RET-{base_row['transaction_id']}-{seq:02d}"
        records.append({
            "return_id":                 ret_id,
            "original_transaction_id":   base_row["transaction_id"],
            "original_basket_id":        base_row["basket_id"],
            "return_date":               return_date_str,
            "return_time":               return_time_str,
            "store_id":                  base_row["store_id"],
            "customer_id":              base_row["customer_id"],
            "product_id":               row["product_id"],
            "return_quantity":           qty,
            "unit_price_eur":            price,
            "refund_amount_eur":         refund,
            "reason_code":               reason,
            "cashier_id":                cashier_id,
            "ingestion_date":            today_str,
        })
    return records


_FACT_RETURNS_HEADER = [
    "return_id", "original_transaction_id", "original_basket_id",
    "return_date", "return_time", "store_id", "customer_id",
    "product_id", "return_quantity", "unit_price_eur",
    "refund_amount_eur", "reason_code", "cashier_id", "ingestion_date",
]


# ═══════════════════════════════════════════════════════════════════════════
# PHASE 3 — Incremental simulation (SCD2 prices + daily batches)
# ═══════════════════════════════════════════════════════════════════════════
#
# --mode incremental produces:
#
#   dim_stores.csv            — same as normalized
#   dim_customers.csv         — same as normalized
#   dim_products_scd2.csv     — product dimension WITH effective_from/to dates
#                               Multiple rows per product for price changes,
#                               promos, and inflation adjustments.
#   batches/
#     batch_YYYYMMDD.csv      — one file per day, contains that day's
#                               transactions PLUS late arrivals from prior days.
#   fact_returns.csv          — same as normalized
#
# Pipeline challenges this creates:
#   - SCD2 range-join:  product_id + order_date BETWEEN effective_from AND effective_to
#   - Late arrivals:    order_date ≠ ingestion_date for ~5% of rows
#   - Daily idempotency: re-running a batch must not duplicate records
#   - Price reconciliation: transaction price may differ from SCD2 list price
# ═══════════════════════════════════════════════════════════════════════════


# ---------------------------------------------------------------------------
# SCD2 Price History Generator
# ---------------------------------------------------------------------------

_SCD2_HEADER = [
    "product_id", "product_name", "category", "subcategory",
    "default_brand", "effective_price_eur",
    "effective_from", "effective_to", "is_promo_price",
    "unit", "vat_rate",
]


def generate_price_history(rng: random.Random,
                           start_date: datetime,
                           end_date: datetime,
                           output_dir: Path) -> int:
    """Generate SCD2 price change records for all catalogue products.

    Price events (per product):
      - Initial list price drawn from [price_min, price_max] at start_date
      - 2-8 price changes per year on average:
          ~30% are temporary promos (7-day discount of 15-30%)
          ~70% are permanent adjustments (±2-8%, slight upward bias for inflation)
      - Minimum 8-day gap between consecutive events to avoid overlaps

    Writes dim_products_scd2.csv and returns the number of SCD2 rows.
    """
    total_days = (end_date - start_date).days
    n_years    = max(total_days / 365.25, 0.5)
    scd2_rows  = []

    for p in PRODUCTS:
        cat, subcat, name, brand = p[0], p[1], p[2], p[3]
        p_min, p_max = p[5], p[6]
        product_id = _compute_product_id(name)
        vat_rate   = _VAT_BY_CATEGORY.get(cat, 0.19)

        # Initial list price
        base_price    = round(rng.uniform(p_min, p_max), 2)
        current_price = base_price

        # How many price events for this product over the full range?
        n_changes = max(0, int(rng.uniform(2, 8) * n_years))
        if total_days < 10 or n_changes == 0:
            # Single period — no price changes
            scd2_rows.append({
                "product_id":          product_id,
                "product_name":        name,
                "category":            cat,
                "subcategory":         subcat,
                "default_brand":       brand,
                "effective_price_eur": current_price,
                "effective_from":      start_date.strftime("%Y-%m-%d"),
                "effective_to":        end_date.strftime("%Y-%m-%d"),
                "is_promo_price":      False,
                "unit":                p[9],
                "vat_rate":            vat_rate,
            })
            continue

        # Pick random change dates with minimum 8-day gap
        candidates = sorted(rng.sample(
            range(8, total_days - 1),
            k=min(n_changes, (total_days - 9) // 8)
        ))
        change_days = []
        last_day = -10
        for d in candidates:
            if d - last_day >= 8:
                change_days.append(d)
                last_day = d

        # Build intervals
        intervals = []      # (start, end, price, is_promo)
        period_start = start_date

        for day_offset in change_days:
            change_date = start_date + timedelta(days=day_offset)

            is_promo = rng.random() < 0.30

            if is_promo:
                # Temporary promo — 7 day discount then revert
                promo_price = round(current_price * rng.uniform(0.70, 0.85), 2)
                promo_end   = min(change_date + timedelta(days=6), end_date)

                # Close current period
                if period_start <= change_date - timedelta(days=1):
                    intervals.append((period_start,
                                      change_date - timedelta(days=1),
                                      current_price, False))
                intervals.append((change_date, promo_end, promo_price, True))
                period_start = promo_end + timedelta(days=1)
                # price reverts after promo
            else:
                # Permanent price adjustment (inflation / rebalancing)
                if period_start <= change_date - timedelta(days=1):
                    intervals.append((period_start,
                                      change_date - timedelta(days=1),
                                      current_price, False))
                change_pct    = rng.uniform(-0.06, 0.10)   # slight upward bias
                current_price = round(current_price * (1 + change_pct), 2)
                current_price = max(p_min * 0.5,
                                    min(current_price, p_max * 1.5))
                period_start  = change_date

        # Close final period
        if period_start <= end_date:
            intervals.append((period_start, end_date, current_price, False))

        # Convert to SCD2 rows
        for s, e, price, is_promo in intervals:
            scd2_rows.append({
                "product_id":          product_id,
                "product_name":        name,
                "category":            cat,
                "subcategory":         subcat,
                "default_brand":       brand,
                "effective_price_eur": price,
                "effective_from":      s.strftime("%Y-%m-%d"),
                "effective_to":        e.strftime("%Y-%m-%d"),
                "is_promo_price":      is_promo,
                "unit":                p[9],
                "vat_rate":            vat_rate,
            })

    # Write
    path = output_dir / "dim_products_scd2.csv"
    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=_SCD2_HEADER)
        w.writeheader()
        for row in scd2_rows:
            w.writerow(row)

    print(f"  dim_products_scd2  : {len(scd2_rows):,} SCD2 rows "
          f"(avg {len(scd2_rows)/len(PRODUCTS):.1f} periods/product)")
    return len(scd2_rows)


# ---------------------------------------------------------------------------
# Daily volume distribution
# ---------------------------------------------------------------------------

def _compute_daily_volumes(
    start_date: datetime, end_date: datetime,
    total_records: int, rng: random.Random,
) -> dict:
    """Distribute total_records across non-Sunday days using DOW/month weights.

    Returns dict of {datetime: int} with approximate daily row targets.
    """
    days = []
    raw_weights = []
    d = start_date
    while d <= end_date:
        if d.weekday() != 6:
            w = DOW_WEIGHTS[d.weekday()] * MONTH_WEIGHTS[d.month - 1]
            if is_promo_period(d):
                w *= 1.4
            days.append(d)
            raw_weights.append(w)
        d += timedelta(days=1)

    total_w  = sum(raw_weights)
    volumes  = {}
    assigned = 0
    for day, w in zip(days, raw_weights):
        target = max(1, int(total_records * w / total_w))
        # ±15% noise
        noise  = rng.randint(-max(1, target // 7), max(1, target // 7))
        actual = max(1, target + noise)
        volumes[day] = actual
        assigned += actual

    # Adjust last day to hit total exactly
    if days:
        volumes[days[-1]] += (total_records - assigned)
        volumes[days[-1]] = max(1, volumes[days[-1]])

    return volumes


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def _print_summary(rows_written, baskets_written, rows_ok, dq_counter, batch_id):
    avg_basket = rows_written / baskets_written if baskets_written else 0
    print(f"\n  Done")
    print(f"  {chr(9472) * 40}")
    print(f"  Baskets (trips)  : {baskets_written:>10,}")
    print(f"  Rows (line items): {rows_written:>10,}")
    print(f"  Avg items/basket : {avg_basket:>10.1f}")
    print(f"\n  Data quality summary")
    print(f"  {chr(9472) * 40}")
    print(f"  OK rows         : {rows_ok:>10,}  ({rows_ok / rows_written * 100:.1f}%)")
    print(f"  Rows with flags : {rows_written - rows_ok:>9,}  ({(rows_written - rows_ok) / rows_written * 100:.1f}%)")
    if dq_counter:
        print(f"\n  Flag breakdown:")
        for flag, count in sorted(dq_counter.items(), key=lambda x: -x[1]):
            print(f"    {flag:<40} {count:>8,}  ({count / rows_written * 100:.2f}%)")
    print(f"\n  Batch ID : {batch_id}")


def main():
    args = parse_args()
    mode = args.mode

    output_dir = Path(args.output_dir) if args.output_dir else DEFAULT_OUT
    master_dir = Path(args.master_dir) if args.master_dir else DEFAULT_MASTER
    output_dir.mkdir(parents=True, exist_ok=True)

    start_date   = datetime.strptime(args.start_date, "%Y-%m-%d")
    end_date     = datetime.strptime(args.end_date,   "%Y-%m-%d")
    num_records  = args.records
    seed         = args.seed

    config_str = f"{num_records}|{seed}|{args.start_date}|{args.end_date}"
    batch_id   = "BATCH_" + hashlib.md5(config_str.encode()).hexdigest()[:10].upper()
    today_str  = datetime.now().strftime("%Y-%m-%d")

    rng = random.Random(seed)

    print(f"\n  Einkaufpark DE - Data Generator v3.3")
    print(f"  Mode : {mode.upper()}")
    print(f"  {chr(9472) * 40}")

    # --- Load masters (both modes) ---
    store_master    = load_store_master(master_dir)
    store_weights   = build_store_weights(store_master)
    terminal_master = load_terminal_master(master_dir)

    print(f"  Building customer master (500,000 profiles)...")
    customer_master, customer_ids, customer_freq_weights = _build_customer_master(500_000, rng)

    print(f"\n  Records    : {num_records:,}")
    print(f"  Batch ID   : {batch_id}")
    print(f"  Date range : {args.start_date} -> {args.end_date}")
    print(f"  Seed       : {seed}")
    print(f"  Output     : {output_dir}")
    print(f"  {chr(9472) * 40}")

    # ===================================================================
    #  FLAT MODE — single denormalised CSV (v3.1 behaviour)
    # ===================================================================
    if mode == "flat":
        file_path = output_dir / OUTPUT_FILE

        raw_schema = load_raw_schema(master_dir)
        HEADER     = derive_header(raw_schema)
        if "order_time" not in HEADER:
            idx = HEADER.index("order_date") + 1
            HEADER.insert(idx, "order_time")
        schema_version = raw_schema.get("schema_version", "unknown")
        print(f"  Schema          : v{schema_version} + order_time = {len(HEADER)} columns")

        recent_txn_pool: deque = deque(maxlen=5000)
        dq_counter:      dict  = {}
        rows_ok = rows_written = baskets_written = 0
        schema_validated = False

        with open(file_path, mode="w", newline="", encoding="utf-8") as fh:
            writer = csv.DictWriter(fh, fieldnames=HEADER)
            writer.writeheader()

            while rows_written < num_records:
                basket_rows = generate_basket(
                    rng, store_master, store_weights,
                    customer_master, customer_ids, customer_freq_weights,
                    terminal_master,
                    start_date, end_date,
                    batch_id, today_str, recent_txn_pool
                )
                for row in basket_rows:
                    if not schema_validated:
                        problems = validate_row_against_schema(row, HEADER)
                        if problems:
                            print(f"\n  SCHEMA CONTRACT VIOLATION:")
                            for p in problems:
                                print(f"    x  {p}")
                            sys.exit(1)
                        else:
                            print(f"  Schema contract : OK")
                        schema_validated = True

                    writer.writerow(row)
                    rows_written += 1
                    flag = row["data_quality_flag"]
                    if flag == "OK":
                        rows_ok += 1
                    else:
                        for f in flag.split("|"):
                            dq_counter[f] = dq_counter.get(f, 0) + 1

                baskets_written += 1
                if rows_written % args.checkpoint < len(basket_rows):
                    print(f"    ... {rows_written:,} rows written ({baskets_written:,} baskets)")

        _print_summary(rows_written, baskets_written, rows_ok, dq_counter, batch_id)
        return


    # ===================================================================
    #  NORMALIZED MODE — dimension + fact CSVs
    # ===================================================================
    if mode == "normalized":
        print(f"\n  Writing dimension tables...")
        write_dim_stores(store_master, output_dir)
        write_dim_products(output_dir)
        write_dim_customers(customer_master, output_dir)

        # Derive fact_transactions header from a sample basket
        sample_rng  = random.Random(seed + 999)
        sample_pool = deque(maxlen=10)
        sample_rows = generate_basket(
            sample_rng, store_master, store_weights,
            customer_master, customer_ids, customer_freq_weights,
            terminal_master, start_date, end_date,
            batch_id, today_str, sample_pool,
        )
        flat_keys   = list(sample_rows[0].keys())
        fact_header = [k for k in flat_keys if k not in _DIM_COLUMNS_TO_DROP]
        print(f"  fact_transactions  : {len(fact_header)} columns "
              f"({len(flat_keys) - len(fact_header)} dimension columns moved)")

        fact_path    = output_dir / "fact_transactions.csv"
        return_rate  = 0.04

        recent_txn_pool: deque = deque(maxlen=5000)
        dq_counter:      dict  = {}
        rows_ok = rows_written = baskets_written = 0
        return_buffer: list    = []

        with open(fact_path, mode="w", newline="", encoding="utf-8") as fh:
            writer = csv.DictWriter(fh, fieldnames=fact_header)
            writer.writeheader()

            while rows_written < num_records:
                basket_rows = generate_basket(
                    rng, store_master, store_weights,
                    customer_master, customer_ids, customer_freq_weights,
                    terminal_master, start_date, end_date,
                    batch_id, today_str, recent_txn_pool
                )
                original_status = basket_rows[0]["order_status"]

                needs_return = False
                if original_status in ("Returned", "Partially_Returned"):
                    needs_return = True
                    for row in basket_rows:
                        if row["quantity"] is not None and row["quantity"] < 0:
                            row["quantity"] = abs(row["quantity"])
                        if row["net_revenue_eur"] is not None and row["net_revenue_eur"] < 0:
                            row["net_revenue_eur"] = abs(row["net_revenue_eur"])
                        row["order_status"] = "Completed"
                elif original_status == "Completed" and rng.random() < return_rate:
                    needs_return = True

                if needs_return:
                    ret_records = _make_return_records(rng, basket_rows, today_str, end_date)
                    return_buffer.extend(ret_records)

                for row in basket_rows:
                    fact_row = {k: row[k] for k in fact_header if k in row}
                    writer.writerow(fact_row)
                    rows_written += 1
                    flag = row["data_quality_flag"]
                    if flag == "OK":
                        rows_ok += 1
                    else:
                        for f in flag.split("|"):
                            dq_counter[f] = dq_counter.get(f, 0) + 1

                baskets_written += 1
                if rows_written % args.checkpoint < len(basket_rows):
                    print(f"    ... {rows_written:,} fact rows ({baskets_written:,} baskets)")

        returns_path = output_dir / "fact_returns.csv"
        with open(returns_path, "w", newline="", encoding="utf-8") as fh:
            writer = csv.DictWriter(fh, fieldnames=_FACT_RETURNS_HEADER)
            writer.writeheader()
            for rec in return_buffer:
                writer.writerow(rec)
        print(f"  fact_returns.csv   : {len(return_buffer):,} return line items")

        _print_summary(rows_written, baskets_written, rows_ok, dq_counter, batch_id)
        print(f"\n  Output files:")
        print(f"  {chr(9472) * 40}")
        for fname in ["dim_stores.csv", "dim_products.csv", "dim_customers.csv",
                       "fact_transactions.csv", "fact_returns.csv"]:
            fpath = output_dir / fname
            if fpath.exists():
                size_kb = fpath.stat().st_size / 1024
                u = "KB" if size_kb < 1024 else "MB"
                sd = size_kb if size_kb < 1024 else size_kb / 1024
                print(f"    {fname:<28} {sd:>8.1f} {u}")
        return

    # ===================================================================
    #  INCREMENTAL MODE — daily batch files + SCD2 price history
    # ===================================================================

    # Step 1: Write dimension tables
    print(f"\n  Writing dimension tables...")
    write_dim_stores(store_master, output_dir)
    write_dim_customers(customer_master, output_dir)

    # Step 2: Generate SCD2 price history
    print(f"  Generating SCD2 price history...")
    n_scd2 = generate_price_history(rng, start_date, end_date, output_dir)

    # Step 3: Derive fact header (same column stripping as normalized)
    sample_rng  = random.Random(seed + 999)
    sample_pool = deque(maxlen=10)
    sample_rows = generate_basket(
        sample_rng, store_master, store_weights,
        customer_master, customer_ids, customer_freq_weights,
        terminal_master, start_date, end_date,
        batch_id, today_str, sample_pool,
    )
    flat_keys   = list(sample_rows[0].keys())
    fact_header = [k for k in flat_keys if k not in _DIM_COLUMNS_TO_DROP]
    print(f"  Fact columns       : {len(fact_header)}")

    # Step 4: Compute daily volumes
    daily_volumes = _compute_daily_volumes(start_date, end_date, num_records, rng)
    n_days = len(daily_volumes)
    print(f"  Trading days       : {n_days}")
    print(f"  Avg rows/day       : {num_records // max(n_days, 1):,}")

    # Step 5: Create batches directory
    batch_dir = output_dir / "batches"
    batch_dir.mkdir(parents=True, exist_ok=True)

    # Step 6: Generate daily batches with late arrivals
    from collections import defaultdict
    recent_txn_pool: deque  = deque(maxlen=5000)
    dq_counter:      dict   = {}
    rows_ok = rows_written = baskets_written = 0
    return_buffer: list     = []
    scheduled_late: dict    = defaultdict(list)
    sorted_days = sorted(daily_volumes.keys())

    for day_idx, current_date in enumerate(sorted_days):
        target_rows  = daily_volumes[current_date]
        day_rows     = []
        day_baskets  = 0

        # Generate baskets for this specific date
        generated_rows = 0
        while generated_rows < target_rows:
            basket_rows = generate_basket(
                rng, store_master, store_weights,
                customer_master, customer_ids, customer_freq_weights,
                terminal_master,
                current_date, current_date,
                batch_id, today_str, recent_txn_pool
            )
            original_status = basket_rows[0]["order_status"]

            # Normalize returns
            needs_return = False
            if original_status in ("Returned", "Partially_Returned"):
                needs_return = True
                for row in basket_rows:
                    if row["quantity"] is not None and row["quantity"] < 0:
                        row["quantity"] = abs(row["quantity"])
                    if row["net_revenue_eur"] is not None and row["net_revenue_eur"] < 0:
                        row["net_revenue_eur"] = abs(row["net_revenue_eur"])
                    row["order_status"] = "Completed"
            elif original_status == "Completed" and rng.random() < 0.04:
                needs_return = True

            if needs_return:
                ret_records = _make_return_records(rng, basket_rows, today_str, end_date)
                return_buffer.extend(ret_records)

            # Late arrival decision: 5% of baskets are delayed 1-3 days
            if rng.random() < 0.05:
                delay = rng.choices([1, 2, 3], weights=[0.60, 0.30, 0.10], k=1)[0]
                delivery_date = current_date + timedelta(days=delay)
                if delivery_date.weekday() == 6:
                    delivery_date += timedelta(days=1)

                for row in basket_rows:
                    row["ingestion_date"] = delivery_date.strftime("%Y-%m-%d")
                    flag = row["data_quality_flag"]
                    if flag == "OK":
                        row["data_quality_flag"] = "INFO:LATE_ARRIVAL"
                    else:
                        row["data_quality_flag"] += "|INFO:LATE_ARRIVAL"
                scheduled_late[delivery_date].extend(basket_rows)
            else:
                day_rows.extend(basket_rows)

            generated_rows += len(basket_rows)
            day_baskets    += 1

        # Pull in late arrivals scheduled for today
        if current_date in scheduled_late:
            day_rows.extend(scheduled_late.pop(current_date))

        # Write batch file
        batch_path = batch_dir / f"batch_{current_date.strftime('%Y%m%d')}.csv"
        with open(batch_path, "w", newline="", encoding="utf-8") as fh:
            writer = csv.DictWriter(fh, fieldnames=fact_header)
            writer.writeheader()
            for row in day_rows:
                fact_row = {k: row[k] for k in fact_header if k in row}
                writer.writerow(fact_row)
                flag = row["data_quality_flag"]
                if flag == "OK":
                    rows_ok += 1
                else:
                    for f in flag.split("|"):
                        dq_counter[f] = dq_counter.get(f, 0) + 1

        rows_written    += len(day_rows)
        baskets_written += day_baskets

        if (day_idx + 1) % 30 == 0 or day_idx == len(sorted_days) - 1:
            print(f"    ... day {day_idx+1}/{n_days}  "
                  f"{current_date.strftime('%Y-%m-%d')}  "
                  f"{rows_written:,} rows total")

    # Flush remaining late arrivals into a final overflow batch
    if scheduled_late:
        overflow_rows = []
        for late_date, rows in sorted(scheduled_late.items()):
            overflow_rows.extend(rows)
        if overflow_rows:
            overflow_path = batch_dir / f"batch_{end_date.strftime('%Y%m%d')}_late.csv"
            with open(overflow_path, "w", newline="", encoding="utf-8") as fh:
                writer = csv.DictWriter(fh, fieldnames=fact_header)
                writer.writeheader()
                for row in overflow_rows:
                    fact_row = {k: row[k] for k in fact_header if k in row}
                    writer.writerow(fact_row)
            rows_written += len(overflow_rows)
            print(f"  Late overflow batch : {len(overflow_rows)} rows")

    # Step 7: Write fact_returns
    returns_path = output_dir / "fact_returns.csv"
    with open(returns_path, "w", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(fh, fieldnames=_FACT_RETURNS_HEADER)
        writer.writeheader()
        for rec in return_buffer:
            writer.writerow(rec)
    print(f"  fact_returns.csv   : {len(return_buffer):,} return line items")

    # Step 8: Summary
    _print_summary(rows_written, baskets_written, rows_ok, dq_counter, batch_id)
    batch_files = sorted(batch_dir.glob("batch_*.csv"))
    total_batch_size = sum(f.stat().st_size for f in batch_files)
    print(f"\n  Output files:")
    print(f"  {chr(9472) * 40}")
    for fname in ["dim_stores.csv", "dim_products_scd2.csv", "dim_customers.csv",
                   "fact_returns.csv"]:
        fpath = output_dir / fname
        if fpath.exists():
            size_kb = fpath.stat().st_size / 1024
            u = "KB" if size_kb < 1024 else "MB"
            sd = size_kb if size_kb < 1024 else size_kb / 1024
            print(f"    {fname:<28} {sd:>8.1f} {u}")
    u = "KB" if total_batch_size / 1024 < 1024 else "MB"
    sd = total_batch_size / 1024 if total_batch_size / 1024 < 1024 else total_batch_size / 1024 / 1024
    print(f"    batches/ ({len(batch_files)} files){'':<9} {sd:>8.1f} {u}")


if __name__ == "__main__":
    main()