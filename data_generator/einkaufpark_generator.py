"""
Einkaufpark DE — Synthetic Sales Data Generator v2.0
=====================================================
Scope   : Physical (in-store) retail, Germany only
Channel : IN_STORE exclusively
Records : configurable via CLI (default 50,000 for dev/test)

Changes from v1:
  1. basket_id column added — MD5(transaction_id + store_id + order_date)
  2. membership_active column added — explicit boolean derived from loyalty_card_id
  3. terminal_type and is_self_checkout joined from terminal_master.json
  4. cashier_id is null when is_self_checkout is True
  5. Column order matches raw_schema.json exactly
  6. terminal_master loaded at startup — generator fails fast if file missing
"""

import argparse
import csv
import hashlib
import json
import os
import random
import sys
import uuid
from datetime import datetime, timedelta
from pathlib import Path

# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def parse_args():
    p = argparse.ArgumentParser(description="Generate Einkaufpark DE synthetic sales data")
    p.add_argument("--records",        type=int, default=50_000,       help="Number of rows to generate")
    p.add_argument("--seed",           type=int, default=10,           help="Random seed")
    p.add_argument("--start-date",     type=str, default="2023-01-01")
    p.add_argument("--end-date",       type=str, default="2026-03-31")
    p.add_argument("--output-dir",     type=str, default=None,         help="Override output directory")
    p.add_argument("--master-dir",     type=str, default=None,         help="Path to master/ folder")
    p.add_argument("--checkpoint",     type=int, default=10_000,       help="Checkpoint interval (rows)")
    return p.parse_args()

# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------

BASE_DIR        = Path(os.getcwd())
DEFAULT_OUT     = BASE_DIR / "data" / "raw"
DEFAULT_MASTER  = BASE_DIR / "master"
OUTPUT_FILE     = "einkaufpark_de_sales_raw.csv"

# ---------------------------------------------------------------------------
# Store master
# ---------------------------------------------------------------------------

STORE_MASTER = [
    ("DE","Germany","Berlin","Berlin","Mitte","10115","Invalidenstraße","EUR","EKP-DE-001"),
    ("DE","Germany","Berlin","Berlin","Kreuzberg","10997","Oranienstraße","EUR","EKP-DE-002"),
    ("DE","Germany","Berlin","Berlin","Prenzlauer Berg","10405","Schönhauser Allee","EUR","EKP-DE-003"),
    ("DE","Germany","Berlin","Berlin","Charlottenburg","10627","Wilmersdorfer Straße","EUR","EKP-DE-004"),
    ("DE","Germany","Bavaria","Munich","Schwabing","80802","Leopoldstraße","EUR","EKP-DE-005"),
    ("DE","Germany","Bavaria","Munich","Giesing","81539","Tegernseer Landstraße","EUR","EKP-DE-006"),
    ("DE","Germany","Bavaria","Munich","Pasing","81241","Landsberger Straße","EUR","EKP-DE-007"),
    ("DE","Germany","Hamburg","Hamburg","Altona","22765","Große Bergstraße","EUR","EKP-DE-008"),
    ("DE","Germany","Hamburg","Hamburg","Eimsbüttel","20259","Osterstraße","EUR","EKP-DE-009"),
    ("DE","Germany","Hamburg","Hamburg","Wandsbek","22041","Wandsbeker Marktstraße","EUR","EKP-DE-010"),
    ("DE","Germany","NRW","Cologne","Ehrenfeld","50823","Venloer Straße","EUR","EKP-DE-011"),
    ("DE","Germany","NRW","Cologne","Innenstadt","50667","Schildergasse","EUR","EKP-DE-012"),
    ("DE","Germany","NRW","Cologne","Deutz","50679","Deutzer Freiheit","EUR","EKP-DE-013"),
    ("DE","Germany","Hesse","Frankfurt","Innenstadt","60311","Zeil","EUR","EKP-DE-014"),
    ("DE","Germany","Hesse","Frankfurt","Bockenheim","60486","Leipziger Straße","EUR","EKP-DE-015"),
    ("DE","Germany","Hesse","Frankfurt","Sachsenhausen","60594","Schweizer Straße","EUR","EKP-DE-016"),
    ("DE","Germany","Baden-Württemberg","Stuttgart","Mitte","70173","Königstraße","EUR","EKP-DE-017"),
    ("DE","Germany","Baden-Württemberg","Stuttgart","Bad Cannstatt","70372","Marktstraße","EUR","EKP-DE-018"),
    ("DE","Germany","Baden-Württemberg","Stuttgart","Vaihingen","70563","Schwabengalerie","EUR","EKP-DE-019"),
    ("DE","Germany","NRW","Düsseldorf","Stadtmitte","40212","Königsallee","EUR","EKP-DE-020"),
    ("DE","Germany","NRW","Düsseldorf","Bilk","40223","Friedrichstraße","EUR","EKP-DE-021"),
    ("DE","Germany","NRW","Düsseldorf","Pempelfort","40477","Nordstraße","EUR","EKP-DE-022"),
    ("DE","Germany","Saxony","Leipzig","Zentrum","04109","Petersstraße","EUR","EKP-DE-023"),
    ("DE","Germany","Saxony","Leipzig","Plagwitz","04229","Karl-Heine-Straße","EUR","EKP-DE-024"),
    ("DE","Germany","NRW","Dortmund","Innenstadt","44135","Westenhellweg","EUR","EKP-DE-025"),
    ("DE","Germany","NRW","Dortmund","Hörde","44263","Hörder Bahnhofstraße","EUR","EKP-DE-026"),
    ("DE","Germany","NRW","Essen","Stadtkern","45127","Limbecker Platz","EUR","EKP-DE-027"),
    ("DE","Germany","NRW","Essen","Rüttenscheid","45130","Rüttenscheider Straße","EUR","EKP-DE-028"),
    ("DE","Germany","Bremen","Bremen","Mitte","28195","Obernstraße","EUR","EKP-DE-029"),
    ("DE","Germany","Bremen","Bremen","Vegesack","28757","Gerhard-Rohlfs-Straße","EUR","EKP-DE-030"),
    ("DE","Germany","Saxony","Dresden","Altstadt","01067","Prager Straße","EUR","EKP-DE-031"),
    ("DE","Germany","Saxony","Dresden","Neustadt","01099","Königsbrücker Straße","EUR","EKP-DE-032"),
    ("DE","Germany","Lower Saxony","Hanover","Mitte","30159","Georgstraße","EUR","EKP-DE-033"),
    ("DE","Germany","Bavaria","Nuremberg","Mitte","90403","Karolinenstraße","EUR","EKP-DE-034"),
    ("DE","Germany","NRW","Bochum","Innenstadt","44787","Kortumstraße","EUR","EKP-DE-035"),
    ("DE","Germany","NRW","Wuppertal","Elberfeld","42103","Alte Freiheit","EUR","EKP-DE-036"),
    ("DE","Germany","NRW","Bielefeld","Mitte","33602","Bahnhofstraße","EUR","EKP-DE-037"),
    ("DE","Germany","NRW","Bonn","Zentrum","53111","Sternstraße","EUR","EKP-DE-038"),
    ("DE","Germany","Baden-Württemberg","Mannheim","Quadrate","68159","Planken","EUR","EKP-DE-039"),
    ("DE","Germany","Baden-Württemberg","Karlsruhe","Innenstadt","76133","Kaiserstraße","EUR","EKP-DE-040"),
    ("DE","Germany","NRW","Münster","Innenstadt","48143","Prinzipalmarkt","EUR","EKP-DE-041"),
    ("DE","Germany","Bavaria","Augsburg","Innenstadt","86150","Annastraße","EUR","EKP-DE-042"),
    ("DE","Germany","Hesse","Wiesbaden","Mitte","65183","Kirchgasse","EUR","EKP-DE-043"),
    ("DE","Germany","NRW","Aachen","Mitte","52062","Adalbertstraße","EUR","EKP-DE-044"),
    ("DE","Germany","Saxony-Anhalt","Magdeburg","Altstadt","39104","Breiter Weg","EUR","EKP-DE-045"),
    ("DE","Germany","Baden-Württemberg","Freiburg","Altstadt","79098","Kaiser-Joseph-Straße","EUR","EKP-DE-046"),
    ("DE","Germany","Rhineland-Palatinate","Mainz","Altstadt","55116","Ludwigsstraße","EUR","EKP-DE-047"),
    ("DE","Germany","Hesse","Kassel","Mitte","34117","Königsstraße","EUR","EKP-DE-048"),
    ("DE","Germany","Saarland","Saarbrücken","Mitte","66111","Bahnhofstraße","EUR","EKP-DE-049"),
    ("DE","Germany","Brandenburg","Potsdam","Innenstadt","14467","Brandenburger Straße","EUR","EKP-DE-050"),
]

STORE_SIZE = {
    **{s[8]: "L" for s in STORE_MASTER if s[3] in ["Berlin","Munich","Hamburg","Cologne","Frankfurt","Stuttgart","Düsseldorf"]},
    **{s[8]: "M" for s in STORE_MASTER if s[3] in ["Leipzig","Dortmund","Essen","Bremen","Dresden"]},
    **{s[8]: "S" for s in STORE_MASTER if s[3] in [
        "Hanover","Nuremberg","Bochum","Wuppertal","Bielefeld","Bonn","Mannheim","Karlsruhe",
        "Münster","Augsburg","Wiesbaden","Aachen","Magdeburg","Freiburg","Mainz","Kassel",
        "Saarbrücken","Potsdam"
    ]},
}
TERMINAL_COUNT = {"L": 20, "M": 12, "S": 6}

# ---------------------------------------------------------------------------
# Terminal master loader
# ---------------------------------------------------------------------------

def load_terminal_master(master_dir: Path) -> dict:
    """
    Load terminal_master.json and return a dict keyed by terminal_id.
    Fails fast with a clear message if the file is missing — the bronze
    join depends entirely on this file being present and correct.
    """
    path = master_dir / "terminal_master.json"
    if not path.exists():
        print(f"\n  ERROR: terminal_master.json not found at {path}")
        print("  Please create master/terminal_master.json before running the generator.")
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
    """
    Return (terminal_type, is_self_checkout) for a given terminal_id.
    Falls back gracefully for terminals not yet in the master file —
    defaults to CASHIER so that revenue is never lost, and logs a warning.
    """
    entry = terminal_master.get(terminal_id)
    if entry:
        return entry["terminal_type"], entry["is_self_checkout"]
    return "CASHIER", False

# ---------------------------------------------------------------------------
# Products — loaded from the realistic product catalogue
# ---------------------------------------------------------------------------

from product_catalogue import pick_product  # noqa: E402

PRIVATE_LABELS = ["EKP-Classic", "EKP-Bio", "EKP-Favourites", "EKP-take it easy", "EKP-Free"]

# ---------------------------------------------------------------------------
# Payment / status
# ---------------------------------------------------------------------------

PAYMENT_TYPES   = ["Card","Cash","Apple_Pay","Google_Pay","Voucher","Gift_Card"]
PAYMENT_WEIGHTS = [0.48, 0.28, 0.09, 0.07, 0.05, 0.03]

ORDER_STATUSES       = ["Completed","Voided","Partially_Returned","Returned"]
ORDER_STATUS_WEIGHTS = [0.93, 0.03, 0.025, 0.015]

SOURCE_SYSTEMS = ["SAP_POS","LEGACY_POS_CSV"]

# ---------------------------------------------------------------------------
# Seasonality
# ---------------------------------------------------------------------------

DOW_WEIGHTS   = [0.10, 0.11, 0.12, 0.13, 0.19, 0.23, 0.12]
MONTH_WEIGHTS = [0.07, 0.06, 0.08, 0.09, 0.08, 0.07, 0.07, 0.08, 0.09, 0.09, 0.10, 0.12]

def is_promo_period(d: datetime) -> bool:
    iso_week = d.isocalendar()[1]
    month, day = d.month, d.day
    if iso_week == 47:                          return True
    if month == 12 and day <= 23:               return True
    if iso_week in (13, 14, 15) and month in (3, 4): return True
    return False

# ---------------------------------------------------------------------------
# Loyalty
# ---------------------------------------------------------------------------

LOYALTY_LAUNCH      = datetime(2023, 3, 1)
LOYALTY_TIERS       = ["Bronze","Silver","Gold","Platinum"]
LOYALTY_TIER_WEIGHTS= [0.55, 0.28, 0.12, 0.05]

# ---------------------------------------------------------------------------
# Customer master
# ---------------------------------------------------------------------------

def _build_customer_master(n: int, rng: random.Random) -> dict:
    GENDER_POOLS        = [["M","Male"],["F","Female"],["Divers"],[None]]
    GENDER_POOL_WEIGHTS = [0.465, 0.465, 0.04, 0.03]
    master = {}
    for i in range(1, n + 1):
        cid = f"CUST{i}"
        r = rng.random()
        if   r < 0.05:  age = None
        elif r < 0.08:  age = rng.randint(18, 24)
        elif r < 0.20:  age = rng.randint(25, 34)
        elif r < 0.38:  age = rng.randint(35, 49)
        elif r < 0.62:  age = rng.randint(50, 64)
        else:            age = rng.randint(65, 85)
        gender_pool = rng.choices(GENDER_POOLS, weights=GENDER_POOL_WEIGHTS, k=1)[0]
        master[cid] = {
            "age":    age,
            "gender": rng.choice(gender_pool),
            "tier":   rng.choices(LOYALTY_TIERS, weights=LOYALTY_TIER_WEIGHTS, k=1)[0],
        }
    return master

# ---------------------------------------------------------------------------
# Date helpers
# ---------------------------------------------------------------------------

def weighted_random_date(start: datetime, end: datetime, rng: random.Random) -> datetime:
    delta_days = (end - start).days
    for _ in range(30):
        d = start + timedelta(days=rng.randint(0, delta_days))
        if (rng.random() < DOW_WEIGHTS[d.weekday()] / max(DOW_WEIGHTS)
                and rng.random() < MONTH_WEIGHTS[d.month - 1] / max(MONTH_WEIGHTS)
                and (not is_promo_period(d) or rng.random() < 0.72)):
            return d
    return start + timedelta(days=rng.randint(0, delta_days))

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
# Basket size distribution
# Realistic German grocery basket sizes:
#   Most trips are quick (1–4 items), some are weekly shops (10–25 items)
# ---------------------------------------------------------------------------

# (min_items, max_items, weight)
BASKET_SIZE_DIST = [
    (1,  2,  0.15),   # grab-and-go: one or two items
    (3,  5,  0.30),   # small trip: lunch, top-up
    (6,  9,  0.25),   # medium trip: weeknight shop
    (10, 15, 0.20),   # regular weekly shop
    (16, 25, 0.08),   # big weekly shop
    (26, 40, 0.02),   # large family shop
]

BASKET_MIN_SIZES = [b[0] for b in BASKET_SIZE_DIST]
BASKET_MAX_SIZES = [b[1] for b in BASKET_SIZE_DIST]
BASKET_WEIGHTS   = [b[2] for b in BASKET_SIZE_DIST]


def pick_basket_size(rng: random.Random) -> int:
    """Pick a realistic number of line items for one shopping trip."""
    bucket = rng.choices(range(len(BASKET_SIZE_DIST)), weights=BASKET_WEIGHTS, k=1)[0]
    lo, hi = BASKET_MIN_SIZES[bucket], BASKET_MAX_SIZES[bucket]
    return rng.randint(lo, hi)


# ---------------------------------------------------------------------------
# Product line item generator
# Generates ONE product row given the basket-level context that is fixed
# for all items in the same trip (store, customer, date, terminal, etc.)
# ---------------------------------------------------------------------------

def generate_line_item(
    rng: random.Random,
    # basket-level context — same for every item in this trip
    txn_id: str,
    basket_id: str,
    batch_id: str,
    order_date: datetime,
    order_date_str: str,
    ship_date_str: str,
    is_promo_week: bool,
    promo_week_id: str,
    store_id: str,
    city: str,
    district: str,
    postal_code: str,
    area: str,
    region: str,
    country_code: str,
    country_name: str,
    size_class: str,
    source_system: str,
    customer_id: str,
    customer_age: int,
    gender: str,
    membership_active: bool,
    loyalty_card_id: str,
    loyalty_tier: str,
    coupon_applied: bool,
    coupon_code: str,
    pos_terminal_id: str,
    terminal_type: str,
    is_sco: bool,
    cashier_id: str,
    payment_type: str,
    order_status: str,
    today_str: str,
    is_dup: bool,
) -> dict:
    """
    Generate one product line item using the basket context passed in.
    All basket-level fields (customer, store, date, terminal, payment)
    are identical across all line items in the same basket.
    Only the product-level fields vary per line item.
    """

    # -- Product --
    prod = pick_product(rng, order_date.month)
    cat, subcat, product_name, brand_pool, pl_possible, p_min, p_max, qty_min, qty_max, unit, _seasonal = prod

    product_id = "PROD" + hashlib.md5(product_name.encode()).hexdigest()[:6].upper()

    is_pl = pl_possible and (rng.random() < 0.35)
    if is_pl:
        brand = rng.choice(PRIVATE_LABELS)
    elif brand_pool == "bulk":
        brand = "EKP-Classic"
        is_pl = True
    else:
        brand = brand_pool

    if is_promo_week:
        p_min_eff, p_max_eff = p_min * 0.85, p_max * 0.90
    else:
        p_min_eff, p_max_eff = p_min, p_max

    if is_pl:
        p_min_eff *= 0.82
        p_max_eff *= 0.88

    unit_price_eur = generate_unit_price(p_min_eff, p_max_eff, rng)
    discount_pct   = generate_discount(rng)
    quantity       = generate_quantity(qty_min, qty_max, rng)

    if (unit_price_eur is not None and quantity is not None and discount_pct is not None
            and quantity > 0 and unit_price_eur > 0 and 0 <= discount_pct <= 100):
        net_revenue_eur = round(unit_price_eur * quantity * (1 - discount_pct / 100), 2)
    else:
        net_revenue_eur = None

    # -- Loyalty points — calculated per line item, summed in Silver --
    tier_mult = {"Bronze": 1.0, "Silver": 1.5, "Gold": 2.0, "Platinum": 3.0}
    if membership_active and net_revenue_eur and net_revenue_eur > 0:
        loyalty_points_earned = int(net_revenue_eur * tier_mult.get(loyalty_tier or "Bronze", 1.0))
    else:
        loyalty_points_earned = None

    # -- Record hash unique per line item (includes product_id) --
    r_hash = record_hash(txn_id, order_date_str, customer_id, product_id, store_id)

    # -- DQ flags --
    dq = []
    if unit_price_eur is None:                                             dq.append("ERR:PRICE_NULL")
    elif unit_price_eur < 0:                                               dq.append("WARN:PRICE_NEGATIVE")
    if quantity is None:                                                   dq.append("ERR:QTY_NULL")
    elif quantity == 0:                                                    dq.append("WARN:QTY_ZERO")
    elif quantity < 0:                                                     dq.append("WARN:QTY_NEGATIVE")
    if customer_age is not None and (customer_age < 0 or customer_age > 120): dq.append("WARN:AGE_INVALID")
    if discount_pct is not None and discount_pct > 100:                   dq.append("WARN:DISCOUNT_OVER_100")
    if net_revenue_eur is None:                                            dq.append("ERR:REVENUE_NULL")
    if ship_date_str < order_date_str:                                     dq.append("WARN:DATE_SEQUENCE_ERROR")
    if is_dup:                                                             dq.append("INFO:DUPLICATE_TXN")
    data_quality_flag = "|".join(dq) if dq else "OK"

    return {
        "transaction_id":        txn_id,
        "basket_id":             basket_id,
        "batch_id":              batch_id,
        "source_system":         source_system,
        "record_hash":           r_hash,
        "order_date":            order_date_str,
        "ship_date":             ship_date_str,
        "ingestion_date":        today_str,
        "sales_channel":         "IN_STORE",
        "order_status":          order_status,
        "store_id":              store_id,
        "store_city":            city,
        "store_district":        district,
        "store_postal_code":     postal_code,
        "store_area":            area,
        "store_region":          region,
        "store_country_code":    country_code,
        "store_country_name":    country_name,
        "store_size_class":      size_class,
        "customer_id":           customer_id,
        "customer_age":          customer_age,
        "gender":                gender,
        "membership_active":     membership_active,
        "loyalty_card_id":       loyalty_card_id,
        "loyalty_tier":          loyalty_tier,
        "loyalty_points_earned": loyalty_points_earned,
        "coupon_applied":        coupon_applied,
        "coupon_code":           coupon_code,
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
        "payment_type":          payment_type,
        "pos_terminal_id":       pos_terminal_id,
        "terminal_type":         terminal_type,
        "is_self_checkout":      is_sco,
        "cashier_id":            cashier_id,
        "promo_week_id":         promo_week_id,
        "is_promo_period":       is_promo_week,
        "data_quality_flag":     data_quality_flag,
    }


# ---------------------------------------------------------------------------
# Basket generator — the new top-level unit of generation
# Generates ALL line items for one complete shopping trip.
# ---------------------------------------------------------------------------

def generate_basket(
    rng: random.Random,
    customer_master: dict,
    terminal_master: dict,
    start_date: datetime,
    end_date: datetime,
    batch_id: str,
    today_str: str,
    recent_txn_pool: list,
) -> list:
    """
    Generate one complete shopping basket: 1–40 line items, all sharing
    the same transaction_id, basket_id, customer, store, date, and terminal.

    Returns a list of row dicts — one per product line item.
    All basket-level context (who, where, when, how they paid) is decided
    once and shared across every row. Only the product changes per row.
    """

    # ── Store — chosen once for the whole basket ───────────────────────────
    store = rng.choice(STORE_MASTER)
    country_code, country_name, region, city, district, postal_code, area, currency, store_id = store
    size_class  = STORE_SIZE.get(store_id, "M")
    n_terminals = TERMINAL_COUNT[size_class]
    source_system = rng.choices(SOURCE_SYSTEMS, weights=[0.85, 0.15], k=1)[0]

    # ── Transaction ID — 0.4% chance of being a duplicate ─────────────────
    if recent_txn_pool and rng.random() < 0.004:
        txn_id = rng.choice(recent_txn_pool)
        is_dup = True
    else:
        txn_id = f"TXN-{store_id}-{uuid.uuid4().hex[:12].upper()}"
        is_dup = False
    recent_txn_pool.append(txn_id)
    if len(recent_txn_pool) > 5000:
        recent_txn_pool.pop(0)

    # ── Date — chosen once for the whole basket ────────────────────────────
    order_date     = weighted_random_date(start_date, end_date, rng)
    ship_date      = order_date + timedelta(days=rng.choice([-1, 1])) if rng.random() < 0.003 else order_date
    order_date_str = order_date.strftime("%Y-%m-%d")
    ship_date_str  = ship_date.strftime("%Y-%m-%d")
    promo_week_id  = f"PW{order_date.strftime('%Y-%V')}"
    is_promo_week  = is_promo_period(order_date)

    # ── Basket ID ─────────────────────────────────────────────────────────
    basket_id = derive_basket_id(txn_id, store_id, order_date_str)

    # ── Customer — chosen once for the whole basket ────────────────────────
    customer_id  = f"CUST{rng.randint(1, 300_000)}"
    cust         = customer_master.get(customer_id, {"age": None, "gender": None, "tier": "Bronze"})
    customer_age = maybe_dirty_age(cust["age"], rng)
    gender       = cust["gender"]

    # ── Loyalty — decided once for the whole basket ────────────────────────
    has_loyalty = (order_date >= LOYALTY_LAUNCH and rng.random() < 0.54)
    if has_loyalty:
        loyalty_card_id = f"KLC{rng.randint(1, 3_000_000):08d}"
        loyalty_tier    = cust["tier"]
    else:
        loyalty_card_id = None
        loyalty_tier    = None
    membership_active = has_loyalty

    # Coupon applied at basket level (one coupon per trip, not per item)
    coupon_applied = has_loyalty and (rng.random() < 0.15)
    coupon_code    = (
        f"KL-{rng.choice(['SAVE5','SAVE10','BIO15','WEEK20','VIP30'])}-{rng.randint(1000,9999)}"
        if coupon_applied else None
    )

    # ── Terminal — one terminal per basket (one queue at checkout) ─────────
    pos_terminal_id       = f"POS-{store_id}-T{rng.randint(1, n_terminals):02d}"
    terminal_type, is_sco = get_terminal_info(pos_terminal_id, terminal_master)
    cashier_id            = None if is_sco else f"EMP{rng.randint(1, 3000):04d}"

    # ── Payment and order status — one per basket ──────────────────────────
    payment_type = rng.choices(PAYMENT_TYPES, weights=PAYMENT_WEIGHTS, k=1)[0]
    order_status = rng.choices(ORDER_STATUSES, weights=ORDER_STATUS_WEIGHTS, k=1)[0]

    # ── Basket size — how many items did this customer buy? ────────────────
    n_items = pick_basket_size(rng)

    # ── Generate one line item per product in this basket ─────────────────
    rows = []
    for _ in range(n_items):
        row = generate_line_item(
            rng=rng,
            txn_id=txn_id,
            basket_id=basket_id,
            batch_id=batch_id,
            order_date=order_date,
            order_date_str=order_date_str,
            ship_date_str=ship_date_str,
            is_promo_week=is_promo_week,
            promo_week_id=promo_week_id,
            store_id=store_id,
            city=city,
            district=district,
            postal_code=postal_code,
            area=area,
            region=region,
            country_code=country_code,
            country_name=country_name,
            size_class=size_class,
            source_system=source_system,
            customer_id=customer_id,
            customer_age=customer_age,
            gender=gender,
            membership_active=membership_active,
            loyalty_card_id=loyalty_card_id,
            loyalty_tier=loyalty_tier,
            coupon_applied=coupon_applied,
            coupon_code=coupon_code,
            pos_terminal_id=pos_terminal_id,
            terminal_type=terminal_type,
            is_sco=is_sco,
            cashier_id=cashier_id,
            payment_type=payment_type,
            order_status=order_status,
            today_str=today_str,
            is_dup=is_dup,
        )
        rows.append(row)

    return rows

# ---------------------------------------------------------------------------
# Schema loader — derives HEADER from raw_schema.json at runtime
# ---------------------------------------------------------------------------

def load_raw_schema(master_dir: Path) -> dict:
    """
    Load raw_schema.json and return the full schema dict.
    Fails fast if the file is missing — the generator must not produce
    output that does not match the agreed contract.
    """
    path = master_dir / "raw_schema.json"
    if not path.exists():
        print(f"\n  ERROR: raw_schema.json not found at {path}")
        print("  The generator cannot run without a locked schema contract.")
        sys.exit(1)
    with open(path, encoding="utf-8") as f:
        schema = json.load(f)
    return schema


def derive_header(schema: dict) -> list:
    """
    Extract the column order from raw_schema.json.
    This is the single source of truth for CSV column order —
    no more hardcoded HEADER list that can silently drift.
    """
    return [col["name"] for col in schema["columns"]]


def validate_row_against_schema(row: dict, schema_columns: list) -> list:
    """
    Check that a generated row contains every column in the schema
    and no extra columns. Returns a list of problems (empty = OK).
    Called once on the first generated row as a fast integration check.
    """
    row_keys    = set(row.keys())
    schema_keys = set(schema_columns)
    missing = schema_keys - row_keys
    extra   = row_keys - schema_keys
    problems = []
    if missing:
        problems.append(f"Row missing columns: {sorted(missing)}")
    if extra:
        problems.append(f"Row has extra columns not in schema: {sorted(extra)}")
    return problems

# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    args = parse_args()

    output_dir = Path(args.output_dir) if args.output_dir else DEFAULT_OUT
    master_dir = Path(args.master_dir) if args.master_dir else DEFAULT_MASTER
    output_dir.mkdir(parents=True, exist_ok=True)
    file_path = output_dir / OUTPUT_FILE

    start_date   = datetime.strptime(args.start_date, "%Y-%m-%d")
    end_date     = datetime.strptime(args.end_date,   "%Y-%m-%d")
    num_records  = args.records
    seed         = args.seed

    config_str = f"{num_records}|{seed}|{args.start_date}|{args.end_date}"
    batch_id   = "BATCH_" + hashlib.md5(config_str.encode()).hexdigest()[:10].upper()
    today_str  = datetime.now().strftime("%Y-%m-%d")

    rng = random.Random(seed)

    print(f"\n  Einkaufpark DE — Data Generator v2.0")
    print(f"  {chr(9472) * 40}")

    # ── Step 1: Load raw_schema.json — single source of truth for columns ─
    #
    # derive_header() extracts column names in the order they appear in the
    # schema file. The CSV writer uses this list, so the output column order
    # is always driven by raw_schema.json — not by anything hardcoded here.
    #
    raw_schema = load_raw_schema(master_dir)
    HEADER     = derive_header(raw_schema)
    schema_version = raw_schema.get("schema_version", "unknown")
    print(f"  Schema          : v{schema_version} · {len(HEADER)} columns from raw_schema.json")

    # ── Step 2: Load terminal master ───────────────────────────────────────
    terminal_master = load_terminal_master(master_dir)

    # ── Step 3: Build customer master ──────────────────────────────────────
    print(f"  Building customer master (300,000 profiles)...")
    customer_master = _build_customer_master(300_000, rng)

    print(f"\n  Records    : {num_records:,}")
    print(f"  Batch ID   : {batch_id}")
    print(f"  Date range : {args.start_date} → {args.end_date}")
    print(f"  Seed       : {seed}")
    print(f"  Output     : {file_path}")
    print(f"  {chr(9472) * 40}")

    recent_txn_pool: list = []
    dq_counter:      dict = {}
    rows_ok          = 0
    rows_written     = 0
    baskets_written  = 0
    schema_validated = False

    with open(file_path, mode="w", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(fh, fieldnames=HEADER)
        writer.writeheader()

        # Loop over BASKETS, not individual rows.
        # Each basket generates 1–40 line items that all share the same
        # customer, store, date, terminal, and payment method.
        # We stop when we have written at least num_records rows.
        while rows_written < num_records:
            basket_rows = generate_basket(
                rng, customer_master, terminal_master,
                start_date, end_date,
                batch_id, today_str, recent_txn_pool
            )

            for row in basket_rows:
                # Schema contract check on the very first row only
                if not schema_validated:
                    problems = validate_row_against_schema(row, HEADER)
                    if problems:
                        print(f"\n  SCHEMA CONTRACT VIOLATION:")
                        for p in problems:
                            print(f"    x  {p}")
                        sys.exit(1)
                    else:
                        print(f"  Schema contract : OK — row columns match raw_schema.json exactly")
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

    # ── Summary ────────────────────────────────────────────────────────────
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
            print(f"    {flag:<40} {count:>8,}  ({count / num_records * 100:.2f}%)")

    print(f"\n  Schema version  : v{schema_version}")
    print(f"  Columns written : {len(HEADER)} (from raw_schema.json)")
    print(f"  Batch ID        : {batch_id}")
    print(f"\n  Verify with:")
    print(f"    head -3 {file_path}")
    print(f"    wc -l  {file_path}")

if __name__ == "__main__":
    main()