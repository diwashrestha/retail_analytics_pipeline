"""
    Einkaufpark is a retail store
    This code simulates a sales data for an ETL pipeline project.
    It generates random data for customers, products, and transactions.
"""

import csv
import random
import hashlib
import uuid
from datetime import datetime, timedelta
import os

# Configuration

BASE_DIR = os.getcwd()
OUTPUT_DIR = os.path.join(BASE_DIR, "data", "raw")
OUTPUT_FILE = "einkaufpark_sales_raw.csv"
NUM_RECORDS = 1_000_000


os.makedirs(OUTPUT_DIR, exist_ok=True)
file_path = os.path.join(OUTPUT_DIR, OUTPUT_FILE)

random.seed(10)

# Date Range
START_DATE = datetime(2023, 1, 1)
END_DATE = datetime(2026, 3, 31)

# Store Master - country /city / store_id
# Each tuple : {country_code, country_name, city, currency, store_id}

STORE_MASTER = [
    # -- Germany (DE)--
    ("DE", "Germany", "Berlin", "EUR", "EKP-DE-001"),
    ("DE", "Germany", "Hamburg", "EUR", "EKP-DE-002"),
    ("DE","Germany","Munich","EUR","EKP-DE-003"),
    ("DE","Germany","Cologne","EUR","EKP-DE-004"),
    ("DE","Germany","Frankfurt","EUR","EKP-DE-005"),
    ("DE","Germany","Stuttgart","EUR","EKP-DE-006"),
    ("DE","Germany","Düsseldorf","EUR","EKP-DE-007"),
    ("DE","Germany","Leipzig","EUR","EKP-DE-008"),
    ("DE","Germany","Dortmund","EUR","EKP-DE-009"),
    ("DE","Germany","Essen","EUR","EKP-DE-010"),
    ("DE","Germany","Bremen","EUR","EKP-DE-011"),
    ("DE","Germany","Dresden","EUR","EKP-DE-012"),
    ("DE","Germany","Hanover","EUR","EKP-DE-013"),
    ("DE","Germany","Nuremberg","EUR","EKP-DE-014"),
    ("DE","Germany","Duisburg","EUR","EKP-DE-015"),
    ("DE","Germany","Bochum","EUR","EKP-DE-016"),
    ("DE","Germany","Wuppertal","EUR","EKP-DE-017"),
    ("DE","Germany","Bielefeld","EUR","EKP-DE-018"),
    ("DE","Germany","Bonn","EUR","EKP-DE-019"),
    ("DE","Germany","Mannheim","EUR","EKP-DE-020"),
    ("DE","Germany","Karlsruhe","EUR","EKP-DE-021"),
    ("DE","Germany","Münster","EUR","EKP-DE-022"),
    ("DE","Germany","Augsburg","EUR","EKP-DE-023"),
    ("DE","Germany","Wiesbaden","EUR","EKP-DE-024"),
    ("DE","Germany","Gelsenkirchen","EUR","EKP-DE-025"),
    ("DE","Germany","Mönchengladbach","EUR","EKP-DE-026"),
    ("DE","Germany","Braunschweig","EUR","EKP-DE-027"),
    ("DE","Germany","Kiel","EUR","EKP-DE-028"),
    ("DE","Germany","Chemnitz","EUR","EKP-DE-029"),
    ("DE","Germany","Aachen","EUR","EKP-DE-030"),
    ("DE","Germany","Halle","EUR","EKP-DE-031"),
    ("DE","Germany","Magdeburg","EUR","EKP-DE-032"),
    ("DE","Germany","Freiburg","EUR","EKP-DE-033"),
    ("DE","Germany","Krefeld","EUR","EKP-DE-034"),
    ("DE","Germany","Lübeck","EUR","EKP-DE-035"),
    ("DE","Germany","Oberhausen","EUR","EKP-DE-036"),
    ("DE","Germany","Erfurt","EUR","EKP-DE-037"),
    ("DE","Germany","Rostock","EUR","EKP-DE-038"),
    ("DE","Germany","Mainz","EUR","EKP-DE-039"),
    ("DE","Germany","Kassel","EUR","EKP-DE-040"),
    ("DE","Germany","Hagen","EUR","EKP-DE-041"),
    ("DE","Germany","Saarbrücken","EUR","EKP-DE-042"),
    ("DE","Germany","Potsdam","EUR","EKP-DE-043"),
    ("DE","Germany","Hamm","EUR","EKP-DE-044"),
    ("DE","Germany","Ludwigshafen","EUR","EKP-DE-045"),
    ("DE","Germany","Oldenburg","EUR","EKP-DE-046"),
    ("DE","Germany","Osnabrück","EUR","EKP-DE-047"),
    ("DE","Germany","Leverkusen","EUR","EKP-DE-048"),
    ("DE","Germany","Solingen","EUR","EKP-DE-049"),
    ("DE","Germany","Heidelberg","EUR","EKP-DE-050"),
    
    # -- Poland (PL) --
    ("PL","Poland","Warsaw","PLN","EKP-PL-001"),
    ("PL","Poland","Kraków","PLN","EKP-PL-002"),
    ("PL","Poland","Łódź","PLN","EKP-PL-003"),
    ("PL","Poland","Wrocław","PLN","EKP-PL-004"),
    ("PL","Poland","Poznań","PLN","EKP-PL-005"),
    ("PL","Poland","Gdańsk","PLN","EKP-PL-006"),
    ("PL","Poland","Szczecin","PLN","EKP-PL-007"),
    ("PL","Poland","Bydgoszcz","PLN","EKP-PL-008"),
    ("PL","Poland","Lublin","PLN","EKP-PL-009"),
    ("PL","Poland","Białystok","PLN","EKP-PL-010"),
    ("PL","Poland","Katowice","PLN","EKP-PL-011"),
    ("PL","Poland","Gdynia","PLN","EKP-PL-012"),
    ("PL","Poland","Częstochowa","PLN","EKP-PL-013"),
    ("PL","Poland","Radom","PLN","EKP-PL-014"),
    ("PL","Poland","Sosnowiec","PLN","EKP-PL-015"),
    ("PL","Poland","Toruń","PLN","EKP-PL-016"),
    ("PL","Poland","Kielce","PLN","EKP-PL-017"),
    ("PL","Poland","Gliwice","PLN","EKP-PL-018"),
    ("PL","Poland","Zabrze","PLN","EKP-PL-019"),
    ("PL","Poland","Bytom","PLN","EKP-PL-020"),
    
    # -- Czech Republic (CZ) --
    ("CZ","Czech Republic","Prague","CZK","EKP-CZ-001"),
    ("CZ","Czech Republic","Brno","CZK","EKP-CZ-002"),
    ("CZ","Czech Republic","Ostrava","CZK","EKP-CZ-003"),
    ("CZ","Czech Republic","Plzeň","CZK","EKP-CZ-004"),
    ("CZ","Czech Republic","Liberec","CZK","EKP-CZ-005"),
    ("CZ","Czech Republic","Olomouc","CZK","EKP-CZ-006"),
    ("CZ","Czech Republic","České Budějovice","CZK","EKP-CZ-007"),
    ("CZ","Czech Republic","Hradec Králové","CZK","EKP-CZ-008"),
    ("CZ","Czech Republic","Ústí nad Labem","CZK","EKP-CZ-009"),
    ("CZ","Czech Republic","Pardubice","CZK","EKP-CZ-010"),
    
    # -- Romania (RO) --
    ("RO","Romania","Bucharest","RON","EKP-RO-001"),
    ("RO","Romania","Cluj-Napoca","RON","EKP-RO-002"),
    ("RO","Romania","Timișoara","RON","EKP-RO-003"),
    ("RO","Romania","Iași","RON","EKP-RO-004"),
    ("RO","Romania","Constanța","RON","EKP-RO-005"),
    ("RO","Romania","Craiova","RON","EKP-RO-006"),
    ("RO","Romania","Brașov","RON","EKP-RO-007"),
    ("RO","Romania","Galați","RON","EKP-RO-008"),
    ("RO","Romania","Ploiești","RON","EKP-RO-009"),
    ("RO","Romania","Oradea","RON","EKP-RO-010"),
    ("RO","Romania","Brăila","RON","EKP-RO-011"),
    ("RO","Romania","Arad","RON","EKP-RO-012"),
    
    # -- Bulgaria (BG) --
    ("BG","Bulgaria","Sofia","BGN","EKP-BG-001"),
    ("BG","Bulgaria","Plovdiv","BGN","EKP-BG-002"),
    ("BG","Bulgaria","Varna","BGN","EKP-BG-003"),
    ("BG","Bulgaria","Burgas","BGN","EKP-BG-004"),
    ("BG","Bulgaria","Ruse","BGN","EKP-BG-005"),
    ("BG","Bulgaria","Stara Zagora","BGN","EKP-BG-006"),
    ("BG","Bulgaria","Pleven","BGN","EKP-BG-007"),
    ("BG","Bulgaria","Sliven","BGN","EKP-BG-008"),
    
    # -- Croatia (HR) --
    ("HR","Croatia","Zagreb","EUR","EKP-HR-001"),
    ("HR","Croatia","Split","EUR","EKP-HR-002"),
    ("HR","Croatia","Rijeka","EUR","EKP-HR-003"),
    ("HR","Croatia","Osijek","EUR","EKP-HR-004"),
    ("HR","Croatia","Zadar","EUR","EKP-HR-005"),
    ("HR","Croatia","Pula","EUR","EKP-HR-006"),
    
    # -- Slovakia (SK) --
    ("SK","Slovakia","Bratislava","EUR","EKP-SK-001"),
    ("SK","Slovakia","Košice","EUR","EKP-SK-002"),
    ("SK","Slovakia","Prešov","EUR","EKP-SK-003"),
    ("SK","Slovakia","Žilina","EUR","EKP-SK-004"),
    ("SK","Slovakia","Banská Bystrica","EUR","EKP-SK-005"),
    ("SK","Slovakia","Nitra","EUR","EKP-SK-006"),
    
    # -- Moldova (MD) --
    ("MD","Moldova","Chișinău","MDL","EKP-MD-001"),
    ("MD","Moldova","Bălți","MDL","EKP-MD-002"),
    ("MD","Moldova","Tiraspol","MDL","EKP-MD-003"),
    
    # -- Austria (AT) --
    ("AT","Austria","Vienna","EUR","EKP-AT-001"),
    ("AT","Austria","Graz","EUR","EKP-AT-002"),
    ("AT","Austria","Linz","EUR","EKP-AT-003"),
    ("AT","Austria","Salzburg","EUR","EKP-AT-004"),
    
    # -- France (FR) -- 
    ("FR","France","Paris","EUR","EKP-FR-001"),
    ("FR","France","Lyon","EUR","EKP-FR-002"),
    ("FR","France","Marseille","EUR","EKP-FR-003"),
    
    # -- Italy (IT) --
    ("IT","Italy","Rome","EUR","EKP-IT-001"),
    ("IT","Italy","Milan","EUR","EKP-IT-002"),
    ("IT","Italy","Naples","EUR","EKP-IT-003"),
    
]

# Countries with physical stores 
PHYSICAL_STORE_COUNTRIES = {"DE","PL","CZ","RO","BG","HR","SK","MD"}

# EUR conversion rates
EUR_RATES = {
    "EUR": 1.0,
    "PLN": 4.28,
    "CZK": 25.10,
    "RON": 4.97,
    "BGN": 1.956,
    "MDL": 19.20,
}


# Product Taxonomy
PRODUCTS = [
    # Fresh & Perishables
    ("Fresh & Perishables", "Fruit & Vegetables",  True,  0.30,  15.00),
    ("Fresh & Perishables", "Meat & Poultry",       True,  2.00,  40.00),
    ("Fresh & Perishables", "Fish & Seafood",        True,  3.00,  60.00),
    ("Fresh & Perishables", "Dairy & Eggs",          True,  0.50,  12.00),
    ("Fresh & Perishables", "Bakery & Pastry",       True,  0.30,   8.00),
    ("Fresh & Perishables", "Deli & Charcuterie",    True,  1.50,  25.00),
    # Packaged Food
    ("Packaged Food",       "Beverages",             True,  0.50,  30.00),
    ("Packaged Food",       "Snacks & Confectionery",True,  0.50,  10.00),
    ("Packaged Food",       "Frozen Food",           True,  1.00,  20.00),
    ("Packaged Food",       "Canned & Jarred",       True,  0.50,   8.00),
    ("Packaged Food",       "Cereals & Breakfast",   True,  1.00,  12.00),
    ("Packaged Food",       "Pasta, Rice & Grains",  True,  0.50,   8.00),
    ("Packaged Food",       "Condiments & Sauces",   True,  0.80,  10.00),
    ("Packaged Food",       "Baby Food",             True,  1.50,  15.00),
    # Household & Cleaning
    ("Household",           "Cleaning Products",     True,  0.50,  15.00),
    ("Household",           "Laundry",               True,  2.00,  25.00),
    ("Household",           "Paper & Hygiene",       True,  0.50,  20.00),
    ("Household",           "Kitchen Accessories",   True,  2.00, 100.00),
    # Health & Beauty
    ("Health & Beauty",     "Personal Care",         True,  1.00,  30.00),
    ("Health & Beauty",     "Cosmetics",             False, 2.00,  80.00),
    ("Health & Beauty",     "Vitamins & Supplements",False, 5.00,  50.00),
    ("Health & Beauty",     "Pharmacy OTC",          False, 2.00,  40.00),
    # Non-Food General Merchandise
    ("Non-Food",            "Textiles & Clothing",   True, 10.00, 150.00),
    ("Non-Food",            "Electronics",           False,15.00,1200.00),
    ("Non-Food",            "Small Appliances",      False,20.00, 500.00),
    ("Non-Food",            "Garden & Outdoor",      True, 5.00, 300.00),
    ("Non-Food",            "Toys & Games",          False, 5.00, 120.00),
    ("Non-Food",            "Books & Stationery",    False, 1.00,  40.00),
    ("Non-Food",            "Seasonal & Promotions", True,  2.00, 200.00),
    ("Non-Food",            "Pet Supplies",          True,  1.00,  80.00),
]



# Private label brand families
PRIVATE_LABELS = ["EKP-Classic", "EKP-Bio", "EKP-Favourites", "EKP-take it easy", "EKP-Free"]

# Major FMCG brands (non-private-label)
BRAND_NAMES = [
    "Nestlé","Unilever","P&G","Kraft Heinz","Danone","Mondelēz","PepsiCo",
    "Coca-Cola","Ferrero","Mars","Henkel","Beiersdorf","Reckitt","L'Oréal",
    "Ariel","Persil","Nivea","Pampers","Barilla","Dr. Oetker","Milka",
    "Haribo","Knorr","Maggi","Jacobs","Tchibo","Bonduelle","Iglo",
]

# Sales Channel
CHANNELS = ["IN_STORE", "ONLINE_OWN", "MARKETPLACE"]
CHANNEL_WEIGHTS = [0.65, 0.20, 0.15]          # in-store dominant


SOURCE_SYSTEMS = {
    "IN_STORE":    ["SAP_POS", "LEGACY_POS_CSV"],
    "ONLINE_OWN":  ["ECOM_API", "SAP_OMS"],
    "MARKETPLACE": ["MARKETPLACE_API", "MARKETPLACE_CSV_FEED"],
}


CARRIERS = ["DHL", "DPD", "GLS", "Hermes", "UPS", "PostNord", "InPost", None]
FULFILLMENT_TYPES = ["SHIP_FROM_STORE", "WAREHOUSE", "DROPSHIP", None]
MARKETPLACE_SELLERS = [f"SEL{str(i).zfill(5)}" for i in range(1, 5001)]

# Customer & Loyality

GENDERS = ["M", "F", "Male", "Female", "Divers", None]    # intentional inconsistency
PAYMENT_TYPES = ["Card", "Cash", "PayPal", "Klarna", "SEPA_Direct_Debit",
                 "Apple_Pay", "Google_Pay", "Voucher", None]
ORDER_STATUSES = ["Delivered", "Cancelled", "Returned", "Partially_Returned",
                  "Processing", "Shipped", "Failed_Delivery"]

# Loyalty card programme launched March 2023 — so pre-launch records have no card
LOYALTY_LAUNCH = datetime(2023, 3, 1)
LOYALTY_CARD_POOL = [f"KLC{str(i).zfill(8)}" for i in range(1, 3_000_001)]

# Promotions / Seasonal Weights

# Weekday weights: Mon–Sun (higher on Fri/Sat)
DOW_WEIGHTS = [0.11, 0.11, 0.12, 0.13, 0.18, 0.22, 0.13]

# Monthly seasonal index (Jan=1 … Dec=12); Christmas & pre-Easter peak
MONTH_WEIGHTS = [0.07,0.06,0.07,0.09,0.08,0.08,0.08,0.08,0.09,0.09,0.10,0.11]

# Helper Functions

def weighted_random_date(start: datetime, end: datetime) -> datetime:
    """Pick a random date with weekday + monthly seasonality bias."""
    # Build a candidate date then accept/reject based on weights
    delta_days = (end - start).days
    for _ in range(20):                         # max 20 tries then fall back
        d = start + timedelta(days=random.randint(0, delta_days))
        dow_ok   = random.random() < DOW_WEIGHTS[d.weekday()] / max(DOW_WEIGHTS)
        month_ok = random.random() < MONTH_WEIGHTS[d.month - 1] / max(MONTH_WEIGHTS)
        if dow_ok and month_ok:
            return d
    return start + timedelta(days=random.randint(0, delta_days))

def to_eur(amount, currency: str) -> float | None:
    """Convert local currency to EUR using approximate fixed rates."""
    if amount is None or currency not in EUR_RATES:
        return None
    return round(amount / EUR_RATES[currency], 2)

def record_hash(*args) -> str:
    """Lightweight MD5 hash of key fields for downstream dedup."""
    raw = "|".join(str(a) for a in args)
    return hashlib.md5(raw.encode()).hexdigest()

def dirty_age():
    """Returns a customer age with realistic noise."""
    roll = random.random()
    if roll < 0.80:
        return random.randint(18, 85)        # valid
    elif roll < 0.88:
        return None                          # missing
    elif roll < 0.93:
        return random.randint(120, 200)      # impossibly old
    else:
        return random.randint(-10, 17)       # negative / underage

def dirty_discount():
    """Discount % with intentional out-of-range and null values."""
    roll = random.random()
    if roll < 0.70:
        return round(random.uniform(0, 50), 2)
    elif roll < 0.80:
        return None
    elif roll < 0.90:
        return round(random.uniform(51, 150), 2)   # over 100 % — invalid
    else:
        return round(random.uniform(-20, -1), 2)    # negative — invalid

def dirty_quantity():
    roll = random.random()
    if roll < 0.82:
        return random.randint(1, 20)
    elif roll < 0.90:
        return 0
    else:
        return random.randint(-5, -1)

def dirty_unit_price(price_min_local, price_max_local):
    roll = random.random()
    if roll < 0.80:
        return round(random.uniform(price_min_local, price_max_local), 2)
    elif roll < 0.88:
        return None
    else:
        return round(-random.uniform(0.01, price_max_local * 0.5), 2)  # negative
    
# Main Generation Loop

BATCH_ID = f"BATCH_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
TODAY    = datetime.now().strftime("%Y-%m-%d")

HEADER = [
    # -- Transaction identifiers --
    "transaction_id",
    "batch_id",
    "source_system",
    "record_hash",
    # -- Dates --
    "order_date",
    "ship_date",
    "ingestion_date",
    # -- Channel --
    "sales_channel",
    "fulfillment_type",
    "delivery_carrier",
    "tracking_number",
    # -- Store / Geography --
    "store_id",
    "store_city",
    "store_country_code",
    "store_country_name",
    # -- Marketplace --
    "seller_id",
    "seller_country",
    # -- Customer --
    "customer_id",
    "customer_age",
    "gender",
    # -- Loyalty --
    "loyalty_card_id",
    "loyalty_points_earned",
    "coupon_applied",
    "coupon_code",
    # -- Product --
    "product_id",
    "product_category",
    "product_subcategory",
    "is_private_label",
    "brand",
    # -- Pricing (local currency) --
    "quantity",
    "unit_price_local",
    "discount_pct",
    "transaction_currency",
    # -- Pricing (EUR normalised)--
    "unit_price_eur",
    "net_revenue_eur",
    # -- Payment & Status  --
    "payment_type",
    "order_status",
    # -- POS / Operational metadata --
    "pos_terminal_id",
    "cashier_id",
    "promo_week_id",
    "data_quality_flag",
]

print(f"⏳ Generating {NUM_RECORDS:,} records …")

with open(file_path, mode="w", newline="", encoding="utf-8") as fh:
    writer = csv.writer(fh)
    writer.writerow(HEADER)

    for i in range(NUM_RECORDS):

        # -- Channel --
        channel = random.choices(CHANNELS, weights=CHANNEL_WEIGHTS, k=1)[0]

        # -- Store selection --
        # IN_STORE  → physical-store countries only
        # ONLINE/MARKETPLACE → all countries incl. marketplace-only
        if channel == "IN_STORE":
            store = random.choice(
                [s for s in STORE_MASTER if s[0] in PHYSICAL_STORE_COUNTRIES]
            )
        else:
            store = random.choice(STORE_MASTER)

        country_code, country_name, city, currency, store_id = store

        # Marketplace-only countries never have an in-store record
        # (guard — shouldn't be needed after filter above but kept for safety)
        if channel == "IN_STORE" and country_code not in PHYSICAL_STORE_COUNTRIES:
            channel = "ONLINE_OWN"

        # -- Source system --
        source_system = random.choice(SOURCE_SYSTEMS[channel])

        # -- Transaction ID — intentional duplicates --
        # Online channels have higher collision (known integration bug scenario)
        if channel == "IN_STORE":
            txn_id = f"TXN{random.randint(1, NUM_RECORDS // 2)}"
        else:
            txn_id = f"TXN{random.randint(1, NUM_RECORDS // 4)}"   # more dups

        # -- Dates --
        order_date = weighted_random_date(START_DATE, END_DATE)

        if channel == "IN_STORE":
            ship_date = order_date                               # same-day pickup
        else:
            ship_date = order_date + timedelta(days=random.randint(-1, 14))

        promo_week_id = f"PW{order_date.strftime('%Y-%V')}"     # ISO week

        # -- Product --
        cat, subcat, pl_possible, p_min_eur, p_max_eur = random.choice(PRODUCTS)

        # Convert EUR price range → local currency
        rate       = EUR_RATES.get(currency, 1.0)
        p_min_local = round(p_min_eur * rate, 2)
        p_max_local = round(p_max_eur * rate, 2)

        is_pl = pl_possible and (random.random() < 0.35)   # ~35 % private-label share
        brand = random.choice(PRIVATE_LABELS) if is_pl else random.choice(BRAND_NAMES)
        product_id = f"PROD{random.randint(1, 80_000)}"

        # -- Pricing --
        unit_price_local = dirty_unit_price(p_min_local, p_max_local)
        discount_pct     = dirty_discount()
        quantity         = dirty_quantity()

        unit_price_eur = to_eur(unit_price_local, currency)

        if (unit_price_eur is not None
                and quantity is not None
                and discount_pct is not None
                and quantity > 0
                and unit_price_eur > 0
                and 0 <= discount_pct <= 100):
            net_revenue_eur = round(
                unit_price_eur * quantity * (1 - discount_pct / 100), 2
            )
        else:
            net_revenue_eur = None          # intentionally uncalculable — dirty

        # -- Customer & demographics --
        customer_id  = f"CUST{random.randint(1, 500_000)}"
        customer_age = dirty_age()
        gender       = random.choice(GENDERS)

        # -- Loyalty --
        has_loyalty = (
            order_date >= LOYALTY_LAUNCH
            and channel != "MARKETPLACE"      # marketplace has separate identity
            and random.random() < 0.52        # ~52 % card penetration
        )
        loyalty_card_id = random.choice(LOYALTY_CARD_POOL) if has_loyalty else None

        if has_loyalty and unit_price_eur and unit_price_eur > 0:
            loyalty_points_earned = int(unit_price_eur * quantity * 10) if quantity and quantity > 0 else 0
        else:
            loyalty_points_earned = None

        coupon_applied = random.random() < 0.18 and has_loyalty
        coupon_code = (
            f"KL-{random.choice(['SAVE5','SAVE10','BIO15','WEEK20','VIP30'])}-{random.randint(1000,9999)}"
            if coupon_applied else None
        )
        
        # -- Channel-specific fields --
        if channel == "IN_STORE":
            fulfillment_type = None
            carrier          = None
            tracking_number  = None
            seller_id        = None
            seller_country   = None
            pos_terminal_id  = f"POS{store_id}-T{random.randint(1, 12)}"
            cashier_id       = f"EMP{random.randint(1, 3000)}"
        elif channel == "ONLINE_OWN":
            fulfillment_type = random.choice(FULFILLMENT_TYPES)
            carrier          = random.choice(CARRIERS)
            tracking_number  = str(uuid.uuid4()).replace("-", "").upper()[:16] if carrier else None
            seller_id        = None
            seller_country   = None
            pos_terminal_id  = None
            cashier_id       = None
        else:  # MARKETPLACE
            fulfillment_type = random.choice(FULFILLMENT_TYPES)
            carrier          = random.choice(CARRIERS)
            tracking_number  = str(uuid.uuid4()).replace("-", "").upper()[:16] if carrier else None
            seller_id        = random.choice(MARKETPLACE_SELLERS)
            seller_country   = random.choice(list({s[0] for s in STORE_MASTER}))
            pos_terminal_id  = None
            cashier_id       = None

        # -- Payment & Order status 
        payment_type  = random.choice(PAYMENT_TYPES)
        order_status  = random.choice(ORDER_STATUSES)

        # -- Record hash (for dedup)
        r_hash = record_hash(txn_id, order_date.strftime("%Y-%m-%d"),
                              customer_id, product_id, store_id)

        # -- Data quality flag (lightweight upstream check)
        dq_issues = []
        if unit_price_local is None or (unit_price_local is not None and unit_price_local < 0):
            dq_issues.append("INVALID_PRICE")
        if quantity is not None and quantity <= 0:
            dq_issues.append("INVALID_QTY")
        if customer_age is not None and (customer_age < 0 or customer_age > 120):
            dq_issues.append("INVALID_AGE")
        if discount_pct is not None and (discount_pct < 0 or discount_pct > 100):
            dq_issues.append("INVALID_DISCOUNT")
        data_quality_flag = "|".join(dq_issues) if dq_issues else "OK"

        # -- Write row -- 
        writer.writerow([
            txn_id,
            BATCH_ID,
            source_system,
            r_hash,
            order_date.strftime("%Y-%m-%d"),
            ship_date.strftime("%Y-%m-%d"),
            TODAY,
            channel,
            fulfillment_type,
            carrier,
            tracking_number,
            store_id,
            city,
            country_code,
            country_name,
            seller_id,
            seller_country,
            customer_id,
            customer_age,
            gender,
            loyalty_card_id,
            loyalty_points_earned,
            coupon_applied,
            coupon_code,
            product_id,
            cat,
            subcat,
            is_pl,
            brand,
            quantity,
            unit_price_local,
            discount_pct,
            currency,
            unit_price_eur,
            net_revenue_eur,
            payment_type,
            order_status,
            pos_terminal_id,
            cashier_id,
            promo_week_id,
            data_quality_flag,
        ])

        if (i + 1) % 100_000 == 0:
            print(f"  … {i + 1:,} rows written")

print(f"\n✅  Done — {NUM_RECORDS:,} records written to:\n   {file_path}")
print(f"   Columns : {len(HEADER)}")
print(f"   Batch ID: {BATCH_ID}")