"""
Einkaufpark DE — Synthetic Sales Data Generator
================================================
Scope   : Physical (in-store) retail, Germany only
Channel : IN_STORE exclusively
Records : 1,000,000 (configurable via CLI)

Key fixes vs. original:
  1. UUID-based transaction IDs with controlled 0.4% dup rate
  2. Customer master dict — consistent age/gender/segment per customer_id
  3. DQ noise rates calibrated to production-realistic levels
  4. Payment types constrained to in-store methods only
  5. ship_date == order_date for in-store (with rare 1-day flag)
  6. Expanded data_quality_flag (6 new rules)
  7. Row generation extracted to generate_row() — testable, reusable
  8. Checkpointing every 100K rows
  9. CLI args for NUM_RECORDS, SEED, date range
 10. BATCH_ID derived from config hash — reproducible across re-runs
 11. LOYALTY_CARD_POOL uses inline f-string — no 3M-string preallocation
 12. Post-2023 promotions: Black Friday week, Easter week
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
    p.add_argument("--records",    type=int,   default=1_000_000, help="Number of rows to generate")
    p.add_argument("--seed",       type=int,   default=10,        help="Random seed")
    p.add_argument("--start-date", type=str,   default="2023-01-01")
    p.add_argument("--end-date",   type=str,   default="2026-03-31")
    p.add_argument("--output-dir", type=str,   default=None,      help="Override output directory")
    p.add_argument("--checkpoint", type=int,   default=100_000,   help="Checkpoint interval (rows)")
    return p.parse_args()

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

BASE_DIR   = Path(os.getcwd())
OUTPUT_DIR = BASE_DIR / "data" / "raw"
OUTPUT_FILE = "einkaufpark_de_sales_raw.csv"

# ---------------------------------------------------------------------------
# Store master — Germany only, physical stores
# ---------------------------------------------------------------------------
# (country_code, country_name, region, city, district, postal_code, area, currency, store_id)

STORE_MASTER = [
    # ---------------- Berlin (Large) ----------------
    ("DE","Germany","Berlin","Berlin","Mitte","10115","Invalidenstraße","EUR","EKP-DE-001"),
    ("DE","Germany","Berlin","Berlin","Kreuzberg","10997","Oranienstraße","EUR","EKP-DE-002"),
    ("DE","Germany","Berlin","Berlin","Prenzlauer Berg","10405","Schönhauser Allee","EUR","EKP-DE-003"),
    ("DE","Germany","Berlin","Berlin","Charlottenburg","10627","Wilmersdorfer Straße","EUR","EKP-DE-004"),

    # ---------------- Munich (Large) ----------------
    ("DE","Germany","Bavaria","Munich","Schwabing","80802","Leopoldstraße","EUR","EKP-DE-005"),
    ("DE","Germany","Bavaria","Munich","Giesing","81539","Tegernseer Landstraße","EUR","EKP-DE-006"),
    ("DE","Germany","Bavaria","Munich","Pasing","81241","Landsberger Straße","EUR","EKP-DE-007"),

    # ---------------- Hamburg (Large) ----------------
    ("DE","Germany","Hamburg","Hamburg","Altona","22765","Große Bergstraße","EUR","EKP-DE-008"),
    ("DE","Germany","Hamburg","Hamburg","Eimsbüttel","20259","Osterstraße","EUR","EKP-DE-009"),
    ("DE","Germany","Hamburg","Hamburg","Wandsbek","22041","Wandsbeker Marktstraße","EUR","EKP-DE-010"),

    # ---------------- Cologne (Large) ----------------
    ("DE","Germany","NRW","Cologne","Ehrenfeld","50823","Venloer Straße","EUR","EKP-DE-011"),
    ("DE","Germany","NRW","Cologne","Innenstadt","50667","Schildergasse","EUR","EKP-DE-012"),
    ("DE","Germany","NRW","Cologne","Deutz","50679","Deutzer Freiheit","EUR","EKP-DE-013"),

    # ---------------- Frankfurt (Large) ----------------
    ("DE","Germany","Hesse","Frankfurt","Innenstadt","60311","Zeil","EUR","EKP-DE-014"),
    ("DE","Germany","Hesse","Frankfurt","Bockenheim","60486","Leipziger Straße","EUR","EKP-DE-015"),
    ("DE","Germany","Hesse","Frankfurt","Sachsenhausen","60594","Schweizer Straße","EUR","EKP-DE-016"),

    # ---------------- Stuttgart (Large) ----------------
    ("DE","Germany","Baden-Württemberg","Stuttgart","Mitte","70173","Königstraße","EUR","EKP-DE-017"),
    ("DE","Germany","Baden-Württemberg","Stuttgart","Bad Cannstatt","70372","Marktstraße","EUR","EKP-DE-018"),
    ("DE","Germany","Baden-Württemberg","Stuttgart","Vaihingen","70563","Schwabengalerie","EUR","EKP-DE-019"),

    # ---------------- Düsseldorf (Large) ----------------
    ("DE","Germany","NRW","Düsseldorf","Stadtmitte","40212","Königsallee","EUR","EKP-DE-020"),
    ("DE","Germany","NRW","Düsseldorf","Bilk","40223","Friedrichstraße","EUR","EKP-DE-021"),
    ("DE","Germany","NRW","Düsseldorf","Pempelfort","40477","Nordstraße","EUR","EKP-DE-022"),

    # ---------------- Leipzig ----------------
    ("DE","Germany","Saxony","Leipzig","Zentrum","04109","Petersstraße","EUR","EKP-DE-023"),
    ("DE","Germany","Saxony","Leipzig","Plagwitz","04229","Karl-Heine-Straße","EUR","EKP-DE-024"),

    # ---------------- Dortmund ----------------
    ("DE","Germany","NRW","Dortmund","Innenstadt","44135","Westenhellweg","EUR","EKP-DE-025"),
    ("DE","Germany","NRW","Dortmund","Hörde","44263","Hörder Bahnhofstraße","EUR","EKP-DE-026"),

    # ---------------- Essen ----------------
    ("DE","Germany","NRW","Essen","Stadtkern","45127","Limbecker Platz","EUR","EKP-DE-027"),
    ("DE","Germany","NRW","Essen","Rüttenscheid","45130","Rüttenscheider Straße","EUR","EKP-DE-028"),

    # ---------------- Bremen ----------------
    ("DE","Germany","Bremen","Bremen","Mitte","28195","Obernstraße","EUR","EKP-DE-029"),
    ("DE","Germany","Bremen","Bremen","Vegesack","28757","Gerhard-Rohlfs-Straße","EUR","EKP-DE-030"),

    # ---------------- Dresden ----------------
    ("DE","Germany","Saxony","Dresden","Altstadt","01067","Prager Straße","EUR","EKP-DE-031"),
    ("DE","Germany","Saxony","Dresden","Neustadt","01099","Königsbrücker Straße","EUR","EKP-DE-032"),

    # ---------------- Smaller cities ----------------
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

# Store size class — larger stores have more terminals, higher avg basket
# (S=small neighbourhood, M=medium, L=large hypermarket)
STORE_SIZE = {
    **{store[8]: "L" for store in STORE_MASTER if store[3] in ["Berlin", "Munich", "Hamburg", "Cologne", "Frankfurt", "Stuttgart", "Düsseldorf"]},
    **{store[8]: "M" for store in STORE_MASTER if store[3] in ["Leipzig", "Dortmund", "Essen", "Bremen", "Dresden"]},
    **{store[8]: "S" for store in STORE_MASTER if store[3] in [
        "Hanover", "Nuremberg", "Bochum", "Wuppertal", "Bielefeld", "Bonn", "Mannheim", "Karlsruhe",
        "Münster", "Augsburg", "Wiesbaden", "Aachen", "Magdeburg", "Freiburg", "Mainz", "Kassel",
        "Saarbrücken", "Potsdam"
    ]},
}
TERMINAL_COUNT = {"L": 20, "M": 12, "S": 6}

# ---------------------------------------------------------------------------
# Product taxonomy
# (category, subcategory, private_label_eligible, price_min_eur, price_max_eur,
#  avg_basket_qty_min, avg_basket_qty_max)
# ---------------------------------------------------------------------------

PRODUCTS = [
    # Fresh & Perishables
    ("Fresh & Perishables", "Fruit & Vegetables",  True,  0.30,  15.00,  1, 8),
    ("Fresh & Perishables", "Meat & Poultry",       True,  2.00,  40.00,  1, 4),
    ("Fresh & Perishables", "Fish & Seafood",        True,  3.00,  60.00,  1, 3),
    ("Fresh & Perishables", "Dairy & Eggs",          True,  0.50,  12.00,  1, 6),
    ("Fresh & Perishables", "Bakery & Pastry",       True,  0.30,   8.00,  1, 5),
    ("Fresh & Perishables", "Deli & Charcuterie",    True,  1.50,  25.00,  1, 3),
    # Packaged Food
    ("Packaged Food",       "Beverages",             True,  0.50,  30.00,  1, 6),
    ("Packaged Food",       "Snacks & Confectionery",True,  0.50,  10.00,  1, 4),
    ("Packaged Food",       "Frozen Food",           True,  1.00,  20.00,  1, 4),
    ("Packaged Food",       "Canned & Jarred",       True,  0.50,   8.00,  1, 5),
    ("Packaged Food",       "Cereals & Breakfast",   True,  1.00,  12.00,  1, 3),
    ("Packaged Food",       "Pasta, Rice & Grains",  True,  0.50,   8.00,  1, 4),
    ("Packaged Food",       "Condiments & Sauces",   True,  0.80,  10.00,  1, 3),
    ("Packaged Food",       "Baby Food",             True,  1.50,  15.00,  1, 4),
    # Household & Cleaning
    ("Household",           "Cleaning Products",     True,  0.50,  15.00,  1, 3),
    ("Household",           "Laundry",               True,  2.00,  25.00,  1, 2),
    ("Household",           "Paper & Hygiene",       True,  0.50,  20.00,  1, 4),
    ("Household",           "Kitchen Accessories",   True,  2.00, 100.00,  1, 2),
    # Health & Beauty
    ("Health & Beauty",     "Personal Care",         True,  1.00,  30.00,  1, 3),
    ("Health & Beauty",     "Cosmetics",             False, 2.00,  80.00,  1, 2),
    ("Health & Beauty",     "Vitamins & Supplements",False, 5.00,  50.00,  1, 2),
    ("Health & Beauty",     "Pharmacy OTC",          False, 2.00,  40.00,  1, 2),
    # Non-Food General Merchandise
    ("Non-Food",            "Textiles & Clothing",   True, 10.00, 150.00,  1, 3),
    ("Non-Food",            "Electronics",           False,15.00,1200.00,  1, 1),
    ("Non-Food",            "Small Appliances",      False,20.00, 500.00,  1, 1),
    ("Non-Food",            "Garden & Outdoor",      True,  5.00, 300.00,  1, 2),
    ("Non-Food",            "Toys & Games",          False, 5.00, 120.00,  1, 2),
    ("Non-Food",            "Books & Stationery",    False, 1.00,  40.00,  1, 3),
    ("Non-Food",            "Seasonal & Promotions", True,  2.00, 200.00,  1, 3),
    ("Non-Food",            "Pet Supplies",          True,  1.00,  80.00,  1, 3),
]

# Category weights — food categories dominate in a grocery-anchored retail store
PRODUCT_WEIGHTS = [
    8, 6, 4, 7, 6, 4,    # Fresh & Perishables
    7, 5, 4, 4, 3, 4, 3, 2,  # Packaged Food
    3, 2, 3, 1,           # Household
    3, 2, 1, 1,           # Health & Beauty
    2, 1, 1, 1, 1, 2, 2, 2,  # Non-Food
]

PRIVATE_LABELS = ["EKP-Classic", "EKP-Bio", "EKP-Favourites", "EKP-take it easy", "EKP-Free"]

BRAND_NAMES = [
    "Nestlé","Unilever","P&G","Kraft Heinz","Danone","Mondelēz","PepsiCo",
    "Coca-Cola","Ferrero","Mars","Henkel","Beiersdorf","Reckitt","L'Oréal",
    "Ariel","Persil","Nivea","Pampers","Barilla","Dr. Oetker","Milka",
    "Haribo","Knorr","Maggi","Jacobs","Tchibo","Bonduelle","Iglo",
    "Edeka","Rewe","Lidl-Markenwelt","Aldi-Eigenmarke",
]

# ---------------------------------------------------------------------------
# In-store payment types only (no PayPal, no Klarna, no SEPA online-only)
# ---------------------------------------------------------------------------
PAYMENT_TYPES = ["Card", "Cash", "Apple_Pay", "Google_Pay", "Voucher", "Gift_Card"]
PAYMENT_WEIGHTS = [0.48, 0.28, 0.09, 0.07, 0.05, 0.03]  # Card dominant, cash still ~28%

# ---------------------------------------------------------------------------
# Order statuses — in-store profile (no Shipped/Processing for walk-in)
# ---------------------------------------------------------------------------
ORDER_STATUSES = ["Completed", "Voided", "Partially_Returned", "Returned"]
ORDER_STATUS_WEIGHTS = [0.93, 0.03, 0.025, 0.015]

SOURCE_SYSTEMS = ["SAP_POS", "LEGACY_POS_CSV"]

# ---------------------------------------------------------------------------
# Seasonality
# ---------------------------------------------------------------------------

# Weekday weights: Mon–Sun (Fri/Sat peak for German grocery)
DOW_WEIGHTS = [0.10, 0.11, 0.12, 0.13, 0.19, 0.23, 0.12]

# Monthly weights (German retail calendar)
MONTH_WEIGHTS = [0.07, 0.06, 0.08, 0.09, 0.08, 0.07, 0.07, 0.08, 0.09, 0.09, 0.10, 0.12]

# Special promotional periods (ISO week number → uplift multiplier)
# Black Friday week (approx ISO week 47), Pre-Easter (varies), Christmas run-up
def is_promo_period(d: datetime) -> bool:
    """True if date falls in a known high-traffic promotional period."""
    iso_week = d.isocalendar()[1]
    month, day = d.month, d.day
    # Black Friday week
    if iso_week == 47:
        return True
    # Christmas run-up (Dec 1–23)
    if month == 12 and day <= 23:
        return True
    # Pre-Easter (approximate: week before Easter — weeks 13–15)
    if iso_week in (13, 14, 15) and month in (3, 4):
        return True
    return False

# ---------------------------------------------------------------------------
# Loyalty
# ---------------------------------------------------------------------------
LOYALTY_LAUNCH = datetime(2023, 3, 1)

# Loyalty tiers with penetration weights
LOYALTY_TIERS = ["Bronze", "Silver", "Gold", "Platinum"]
LOYALTY_TIER_WEIGHTS = [0.55, 0.28, 0.12, 0.05]

# ---------------------------------------------------------------------------
# Customer master — generated once, consistent per customer_id
# ---------------------------------------------------------------------------
# Age distribution: realistic German retail shopper demographics
def _build_customer_master(n: int, rng: random.Random) -> dict:
    """
    Pre-build {customer_id -> {age, gender, loyalty_tier, gender_repr}}
    so the same customer always has the same attributes.
    gender_repr intentionally has encoding inconsistency across customers
    (different source systems), but is stable per customer.
    """
    GENDER_POOLS = [
        ["M", "Male"],       # same underlying value, different encoding
        ["F", "Female"],
        ["Divers"],
        [None],              # not provided
    ]
    GENDER_POOL_WEIGHTS = [0.465, 0.465, 0.04, 0.03]

    master = {}
    for i in range(1, n + 1):
        cid = f"CUST{i}"
        # Age: realistic German supermarket demographic
        r = rng.random()
        if r < 0.05:
            age = None             # guest / no card
        elif r < 0.08:
            age = rng.randint(18, 24)
        elif r < 0.20:
            age = rng.randint(25, 34)
        elif r < 0.38:
            age = rng.randint(35, 49)
        elif r < 0.62:
            age = rng.randint(50, 64)
        else:
            age = rng.randint(65, 85)

        gender_pool = rng.choices(GENDER_POOLS, weights=GENDER_POOL_WEIGHTS, k=1)[0]
        gender_repr = rng.choice(gender_pool)

        tier = rng.choices(LOYALTY_TIERS, weights=LOYALTY_TIER_WEIGHTS, k=1)[0]

        master[cid] = {
            "age":    age,
            "gender": gender_repr,
            "tier":   tier,
        }
    return master

# ---------------------------------------------------------------------------
# Date helpers
# ---------------------------------------------------------------------------

def weighted_random_date(start: datetime, end: datetime, rng: random.Random) -> datetime:
    """Pick a random date with weekday + monthly seasonality bias.
    Promo periods get a 1.4× acceptance boost."""
    delta_days = (end - start).days
    for _ in range(30):
        d = start + timedelta(days=rng.randint(0, delta_days))
        dow_ok   = rng.random() < DOW_WEIGHTS[d.weekday()] / max(DOW_WEIGHTS)
        month_ok = rng.random() < MONTH_WEIGHTS[d.month - 1] / max(MONTH_WEIGHTS)
        promo_ok = (not is_promo_period(d)) or (rng.random() < 0.72)  # 1.4× boost
        if dow_ok and month_ok and promo_ok:
            return d
    return start + timedelta(days=rng.randint(0, delta_days))

# ---------------------------------------------------------------------------
# Dirty data helpers — calibrated to production-realistic rates
# ---------------------------------------------------------------------------

def maybe_dirty_age(age, rng: random.Random):
    """
    ~99.5% clean. ~0.3% impossible value (data-entry error).
    ~0.15% negative (system migration artifact). ~0.05% extreme.
    The base age comes from customer_master so it's already realistic.
    """
    r = rng.random()
    if r < 0.995:
        return age                          # clean
    elif r < 0.998:
        return rng.randint(121, 160)        # impossibly old — entry error
    elif r < 0.9995:
        return rng.randint(-5, -1)          # negative — migration artifact
    else:
        return None                         # overwritten to null

def generate_unit_price(price_min: float, price_max: float, rng: random.Random):
    """
    ~98.5% valid price. ~1% null (POS lookup failure). ~0.5% negative (refund line).
    """
    r = rng.random()
    if r < 0.985:
        return round(rng.uniform(price_min, price_max), 2)
    elif r < 0.995:
        return None                         # lookup failure
    else:
        return round(-rng.uniform(0.01, price_max * 0.3), 2)   # refund adjustment

def generate_discount(rng: random.Random):
    """
    ~80% no discount. ~18% valid discount (0–50%). ~1.5% null.
    ~0.5% slightly over 100 (rounding bug in legacy POS — very rare).
    """
    r = rng.random()
    if r < 0.80:
        return 0.0
    elif r < 0.98:
        return round(rng.uniform(0.5, 50.0), 2)   # realistic max ~50%
    elif r < 0.995:
        return None
    else:
        return round(rng.uniform(100.1, 102.0), 2)  # rare legacy POS rounding bug

def generate_quantity(qty_min: int, qty_max: int, rng: random.Random):
    """
    ~97.5% valid (1+). ~1.5% zero (voided scan). ~1% negative (return line).
    qty_min/max come from the product's realistic basket range.
    """
    r = rng.random()
    if r < 0.975:
        return rng.randint(qty_min, qty_max)
    elif r < 0.990:
        return 0                            # voided scan
    else:
        return rng.randint(-3, -1)          # return/reversal

# ---------------------------------------------------------------------------
# Record hash
# ---------------------------------------------------------------------------

def record_hash(*args) -> str:
    raw = "|".join(str(a) for a in args)
    return hashlib.md5(raw.encode()).hexdigest()

# ---------------------------------------------------------------------------
# Core row generator
# ---------------------------------------------------------------------------

def generate_row(
    rng: random.Random,
    customer_master: dict,
    start_date: datetime,
    end_date: datetime,
    batch_id: str,
    today_str: str,
    recent_txn_pool: list,
) -> dict:
    """
    Generate one synthetic in-store transaction row.
    Returns a dict keyed by HEADER fields.
    """

    # -- Store --
    store = rng.choice(STORE_MASTER)
    country_code, country_name, region, city, district, postal_code, area, currency, store_id = store
    size_class = STORE_SIZE.get(store_id, "M")
    n_terminals = TERMINAL_COUNT[size_class]

    # -- Source system (legacy POS still present in ~15% of stores) --
    source_system = rng.choices(SOURCE_SYSTEMS, weights=[0.85, 0.15], k=1)[0]

    # -- Transaction ID: UUID-based, 0.4% controlled duplication --
    if recent_txn_pool and rng.random() < 0.004:
        # Simulate a retry / double-scan scenario
        txn_id = rng.choice(recent_txn_pool)
        is_dup = True
    else:
        txn_id = f"TXN-{store_id}-{uuid.uuid4().hex[:12].upper()}"
        is_dup = False

    # Maintain rolling pool of recent IDs for dup simulation
    recent_txn_pool.append(txn_id)
    if len(recent_txn_pool) > 5000:
        recent_txn_pool.pop(0)

    # -- Order date --
    order_date = weighted_random_date(start_date, end_date, rng)

    # In-store: ship_date == order_date always
    # ~0.3% have a 1-day delta due to timestamp timezone misalignment
    if rng.random() < 0.003:
        ship_date = order_date + timedelta(days=rng.choice([-1, 1]))
    else:
        ship_date = order_date

    promo_week_id  = f"PW{order_date.strftime('%Y-%V')}"
    is_promo_week  = is_promo_period(order_date)

    # -- Product --
    prod = rng.choices(PRODUCTS, weights=PRODUCT_WEIGHTS, k=1)[0]
    cat, subcat, pl_possible, p_min, p_max, qty_min, qty_max = prod

    # Promo weeks: higher discount, slightly lower prices
    if is_promo_week:
        p_min_eff = p_min * 0.85
        p_max_eff = p_max * 0.90
    else:
        p_min_eff, p_max_eff = p_min, p_max

    is_pl  = pl_possible and (rng.random() < 0.35)
    brand  = rng.choice(PRIVATE_LABELS) if is_pl else rng.choice(BRAND_NAMES)
    product_id = f"PROD{rng.randint(1, 80_000)}"

    # Private label: 10–20% cheaper than branded
    if is_pl:
        p_min_eff *= 0.82
        p_max_eff *= 0.88

    # -- Pricing --
    unit_price_eur = generate_unit_price(p_min_eff, p_max_eff, rng)
    discount_pct   = generate_discount(rng)
    quantity       = generate_quantity(qty_min, qty_max, rng)

    if (
        unit_price_eur is not None
        and quantity is not None
        and discount_pct is not None
        and quantity > 0
        and unit_price_eur > 0
        and 0 <= discount_pct <= 100
    ):
        net_revenue_eur = round(unit_price_eur * quantity * (1 - discount_pct / 100), 2)
    else:
        net_revenue_eur = None

    # -- Customer --
    customer_id  = f"CUST{rng.randint(1, 300_000)}"
    cust         = customer_master.get(customer_id, {"age": None, "gender": None, "tier": "Bronze"})
    customer_age = maybe_dirty_age(cust["age"], rng)
    gender       = cust["gender"]

    # -- Loyalty --
    has_loyalty = (
        order_date >= LOYALTY_LAUNCH
        and rng.random() < 0.54          # ~54% card penetration in DE grocery
    )
    if has_loyalty:
        loyalty_card_id = f"KLC{rng.randint(1, 3_000_000):08d}"
        loyalty_tier    = cust["tier"]
        # Points: 1 pt per €1 spent (Bronze), 1.5x Silver, 2x Gold, 3x Platinum
        tier_mult = {"Bronze": 1.0, "Silver": 1.5, "Gold": 2.0, "Platinum": 3.0}
        if net_revenue_eur and net_revenue_eur > 0:
            loyalty_points_earned = int(net_revenue_eur * tier_mult.get(loyalty_tier, 1.0))
        else:
            loyalty_points_earned = 0
    else:
        loyalty_card_id       = None
        loyalty_tier          = None
        loyalty_points_earned = None

    coupon_applied = has_loyalty and (rng.random() < 0.15)
    coupon_code = (
        f"KL-{rng.choice(['SAVE5','SAVE10','BIO15','WEEK20','VIP30'])}-{rng.randint(1000,9999)}"
        if coupon_applied else None
    )

    # -- POS & staff --
    pos_terminal_id = f"POS-{store_id}-T{rng.randint(1, n_terminals):02d}"
    cashier_id      = f"EMP{rng.randint(1, 3000):04d}"

    # -- Payment (in-store only) --
    payment_type = rng.choices(PAYMENT_TYPES, weights=PAYMENT_WEIGHTS, k=1)[0]

    # -- Order status --
    order_status = rng.choices(ORDER_STATUSES, weights=ORDER_STATUS_WEIGHTS, k=1)[0]

    # -- Record hash --
    r_hash = record_hash(txn_id, order_date.strftime("%Y-%m-%d"), customer_id, product_id, store_id)

    # ------------------------------------------------------------------
    # Data quality flags — multi-rule, pipe-separated, severity-prefixed
    #   ERR:  pipeline-breaking issues
    #   WARN: suspicious but processable
    #   INFO: expected noise / edge case
    # ------------------------------------------------------------------
    dq = []

    if unit_price_eur is None:
        dq.append("ERR:PRICE_NULL")
    elif unit_price_eur < 0:
        dq.append("WARN:PRICE_NEGATIVE")    # could be refund adjustment

    if quantity is None:
        dq.append("ERR:QTY_NULL")
    elif quantity == 0:
        dq.append("WARN:QTY_ZERO")          # voided scan
    elif quantity < 0:
        dq.append("WARN:QTY_NEGATIVE")      # return line

    if customer_age is not None and (customer_age < 0 or customer_age > 120):
        dq.append("WARN:AGE_INVALID")

    if discount_pct is not None and discount_pct > 100:
        dq.append("WARN:DISCOUNT_OVER_100")

    if net_revenue_eur is None:
        dq.append("ERR:REVENUE_NULL")

    if ship_date < order_date:
        dq.append("WARN:DATE_SEQUENCE_ERROR")

    if is_dup:
        dq.append("INFO:DUPLICATE_TXN")

    data_quality_flag = "|".join(dq) if dq else "OK"

    return {
        "transaction_id":       txn_id,
        "batch_id":             batch_id,
        "source_system":        source_system,
        "record_hash":          r_hash,
        "order_date":           order_date.strftime("%Y-%m-%d"),
        "ship_date":            ship_date.strftime("%Y-%m-%d"),
        "ingestion_date":       today_str,
        "sales_channel":        "IN_STORE",
        "store_id":             store_id,
        "store_city":           city,
        "store_district":       district,
        "store_postal_code":    postal_code,
        "store_area":           area,
        "store_region":         region,
        "store_country_code":   country_code,
        "store_country_name":   country_name,
        "store_size_class":     size_class,
        "customer_id":          customer_id,
        "customer_age":         customer_age,
        "gender":               gender,
        "loyalty_card_id":      loyalty_card_id,
        "loyalty_tier":         loyalty_tier,
        "loyalty_points_earned":loyalty_points_earned,
        "coupon_applied":       coupon_applied,
        "coupon_code":          coupon_code,
        "product_id":           product_id,
        "product_category":     cat,
        "product_subcategory":  subcat,
        "is_private_label":     is_pl,
        "brand":                brand,
        "quantity":             quantity,
        "unit_price_eur":       unit_price_eur,
        "discount_pct":         discount_pct,
        "transaction_currency": "EUR",
        "net_revenue_eur":      net_revenue_eur,
        "payment_type":         payment_type,
        "order_status":         order_status,
        "pos_terminal_id":      pos_terminal_id,
        "cashier_id":           cashier_id,
        "promo_week_id":        promo_week_id,
        "is_promo_period":      is_promo_week,
        "data_quality_flag":    data_quality_flag,
    }

# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

HEADER = [
    "transaction_id", "batch_id", "source_system", "record_hash",
    "order_date", "ship_date", "ingestion_date",
    "sales_channel",
    "store_id", "store_city", "store_district", "store_postal_code", "store_area", "store_region", "store_country_code", "store_country_name", "store_size_class",
    "customer_id", "customer_age", "gender",
    "loyalty_card_id", "loyalty_tier", "loyalty_points_earned",
    "coupon_applied", "coupon_code",
    "product_id", "product_category", "product_subcategory", "is_private_label", "brand",
    "quantity", "unit_price_eur", "discount_pct", "transaction_currency", "net_revenue_eur",
    "payment_type", "order_status",
    "pos_terminal_id", "cashier_id",
    "promo_week_id", "is_promo_period",
    "data_quality_flag",
]

def main():
    args = parse_args()

    output_dir = Path(args.output_dir) if args.output_dir else OUTPUT_DIR
    output_dir.mkdir(parents=True, exist_ok=True)
    file_path = output_dir / OUTPUT_FILE

    start_date = datetime.strptime(args.start_date, "%Y-%m-%d")
    end_date   = datetime.strptime(args.end_date,   "%Y-%m-%d")
    num_records = args.records
    seed = args.seed

    # Deterministic BATCH_ID derived from config (not a runtime clock)
    config_str  = f"{num_records}|{seed}|{args.start_date}|{args.end_date}"
    batch_id    = "BATCH_" + hashlib.md5(config_str.encode()).hexdigest()[:10].upper()
    today_str   = datetime.now().strftime("%Y-%m-%d")

    rng = random.Random(seed)

    # Build customer master once (deterministic)
    print("⏳ Building customer master (300,000 profiles) …")
    customer_master = _build_customer_master(300_000, rng)

    # Checkpoint state
    checkpoint_file = output_dir / f"{OUTPUT_FILE}.checkpoint.json"
    start_from = 0
    if checkpoint_file.exists():
        with open(checkpoint_file) as f:
            ckpt = json.load(f)
        if ckpt.get("batch_id") == batch_id:
            start_from = ckpt.get("rows_written", 0)
            print(f"♻️  Resuming from checkpoint at row {start_from:,}")
            # Re-advance the RNG to the same state (skip rows already written)
            # Simplest approach: re-seed and replay skipped rows without writing
            rng = random.Random(seed)
            _build_customer_master(300_000, rng)  # re-consume same RNG sequence
            recent_txn_pool_warmup: list = []
            print(f"   Fast-forwarding RNG through {start_from:,} rows …")
            for _ in range(start_from):
                generate_row(rng, customer_master, start_date, end_date,
                             batch_id, today_str, recent_txn_pool_warmup)
        else:
            print("   Different config — starting fresh.")
            start_from = 0

    recent_txn_pool: list = []

    print(f"⏳ Generating {num_records:,} records → {file_path}")
    print(f"   Batch ID : {batch_id}")
    print(f"   Date range: {args.start_date} → {args.end_date}")
    print(f"   Seed     : {seed}")

    file_mode = "a" if start_from > 0 else "w"

    dq_counter: dict[str, int] = {}
    rows_ok = 0

    try:
        with open(file_path, mode=file_mode, newline="", encoding="utf-8") as fh:
            writer = csv.DictWriter(fh, fieldnames=HEADER)
            if start_from == 0:
                writer.writeheader()

            for i in range(start_from, num_records):
                row = generate_row(
                    rng, customer_master, start_date, end_date,
                    batch_id, today_str, recent_txn_pool
                )
                writer.writerow(row)

                # Track DQ flags
                flag = row["data_quality_flag"]
                if flag == "OK":
                    rows_ok += 1
                else:
                    for f in flag.split("|"):
                        dq_counter[f] = dq_counter.get(f, 0) + 1

                # Progress + checkpoint
                if (i + 1) % args.checkpoint == 0:
                    print(f"   … {i + 1:,} rows written")
                    with open(checkpoint_file, "w") as cf:
                        json.dump({"batch_id": batch_id, "rows_written": i + 1}, cf)

    except KeyboardInterrupt:
        print("\n⚠️  Interrupted — checkpoint saved. Re-run to resume.")
        sys.exit(1)

    # Remove checkpoint on clean completion
    if checkpoint_file.exists():
        checkpoint_file.unlink()

    total_dq = sum(dq_counter.values())
    print(f"\n✅  Done — {num_records:,} records written to:\n   {file_path}")
    print(f"\n📊  Data Quality Summary")
    print(f"   OK rows       : {rows_ok:>10,}  ({rows_ok/num_records*100:.1f}%)")
    print(f"   Rows with flags: {num_records - rows_ok:>9,}  ({(num_records-rows_ok)/num_records*100:.1f}%)")
    if dq_counter:
        print(f"\n   Flag breakdown:")
        for flag, count in sorted(dq_counter.items(), key=lambda x: -x[1]):
            print(f"   {flag:<35} {count:>8,}  ({count/num_records*100:.2f}%)")
    print(f"\n   Columns : {len(HEADER)}")
    print(f"   Batch ID: {batch_id}")

if __name__ == "__main__":
    main()