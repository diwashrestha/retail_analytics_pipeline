"""
Einkaufpark DE — Product Catalogue v3.1
========================================
Combinatorial generation: base_names × variants × sizes → product entries.
~1,400 unique SKUs across 27 subcategories.

v3.1: Added per-product Zipf popularity scores so a few SKUs per subcategory
      dominate volume (realistic Pareto / 80-20 distribution).

Tuple structure per product:
  (category, subcategory, product_name, brand,
   is_private_label_eligible, price_min_eur, price_max_eur,
   qty_min, qty_max, unit, seasonal_months)
"""

import itertools
import random as _random

# ---------------------------------------------------------------------------
# Brand pools  — specific real brands per subcategory
# ---------------------------------------------------------------------------

_B = {
    "fruits": [
        "EKP-Bio",
        "EKP-Classic",
        "Chiquita",
        "Dole",
        "Fyffes",
        "Zespri",
        "Alnatura",
        "Del Monte",
    ],
    "veg": ["EKP-Bio", "EKP-Classic", "Bonduelle", "Alnatura", "Freshona", "Demeter"],
    "meat": [
        "EKP-Classic",
        "Rügenwalder",
        "Wiesenhof",
        "Hähnchenhof",
        "Reinert",
        "Gutfleisch",
    ],
    "fish": ["EKP-Classic", "Iglo", "Appel", "Followfish", "Räucherfisch", "Aldi Meer"],
    "dairy": [
        "EKP-Classic",
        "Weihenstephan",
        "Müller",
        "Ehrmann",
        "Danone",
        "Fage",
        "Kerrygold",
        "Arla",
        "Andechser",
        "Demeter",
        "Galbani",
        "President",
    ],
    "eggs": ["EKP-Bio", "EKP-Classic", "Demeter", "Landei"],
    "bakery": [
        "EKP-Classic",
        "Harry's",
        "Mestemacher",
        "Lieken",
        "Brandt",
        "Golden Toast",
    ],
    "deli": ["EKP-Classic", "Rügenwalder", "Reinert", "Beretta", "Aoste", "Herta"],
    "beverages_soft": [
        "Coca-Cola",
        "PepsiCo",
        "Fanta",
        "Sprite",
        "Bionade",
        "Fritz-Kola",
        "Volvic",
        "Gerolsteiner",
        "Evian",
        "Hohes C",
        "Tropicana",
        "Granini",
    ],
    "beverages_alc": [
        "Warsteiner",
        "Paulaner",
        "Berliner Kindl",
        "Bitburger",
        "Veltins",
        "Beck's",
        "Radeberger",
        "Erdinger",
        "Jägermeister",
        "Asbach",
    ],
    "coffee_tea": [
        "Jacobs",
        "Tchibo",
        "Nescafé",
        "Lavazza",
        "Melitta",
        "Twinings",
        "Teekanne",
        "Alnatura",
    ],
    "snacks_choc": [
        "Milka",
        "Ritter Sport",
        "Lindt",
        "Ferrero",
        "Mars",
        "Nestlé",
        "Kinder",
        "Storck",
        "Côte d'Or",
    ],
    "snacks_crisp": [
        "Pringles",
        "Lay's",
        "Chio",
        "Intersnack",
        "Lorenz",
        "Funny-Frisch",
    ],
    "snacks_candy": [
        "Haribo",
        "Trolli",
        "Nimm2",
        "Ricola",
        "Storck",
        "Katjes",
        "Maoam",
    ],
    "snacks_biscuit": [
        "Bahlsen",
        "Mondelēz",
        "Leibniz",
        "Oreo",
        "Lu",
        "McVitie's",
        "Griesson",
    ],
    "frozen": [
        "Iglo",
        "Dr. Oetker",
        "Wagner",
        "McCain",
        "EKP-Favourites",
        "Birds Eye",
        "Häagen-Dazs",
        "Unilever",
    ],
    "canned": [
        "Bonduelle",
        "Heinz",
        "Kühne",
        "EKP-Classic",
        "Hengstenberg",
        "Saupiquet",
        "Ro-Ro",
    ],
    "cereals": [
        "Kellogg's",
        "Nestlé",
        "Dr. Oetker",
        "Jordans",
        "Alnatura",
        "Wasa",
        "Brandt",
        "EKP-Classic",
    ],
    "pasta_rice": [
        "Barilla",
        "De Cecco",
        "Uncle Ben's",
        "EKP-Classic",
        "Rapunzel",
        "Alnatura",
    ],
    "condiments": [
        "Heinz",
        "Thomy",
        "Hela",
        "Kühne",
        "Kikkoman",
        "Maggi",
        "Knorr",
        "Bertolli",
        "EKP-Classic",
    ],
    "cleaning": [
        "Frosch",
        "Domestos",
        "Flash",
        "Mr Proper",
        "Pril",
        "Somat",
        "Fairy",
        "Method",
    ],
    "laundry": [
        "Ariel",
        "Persil",
        "Lenor",
        "Perwoll",
        "Frosch",
        "EKP-Classic",
        "Ecover",
    ],
    "paper": ["Zewa", "Hakle", "Plenty", "Tempo", "Bounty", "EKP-Classic"],
    "personal": [
        "Nivea",
        "Dove",
        "Head & Shoulders",
        "Oral-B",
        "Colgate",
        "Gillette",
        "Rexona",
        "Axe",
        "Schwarzkopf",
        "L'Oréal",
        "Garnier",
    ],
    "pharma": [
        "Bayer",
        "Ratiopharm",
        "Stada",
        "Doppelherz",
        "Klosterfrau",
        "Bepanthen",
    ],
    "pet": [
        "Mars (Whiskas)",
        "Mars (Pedigree)",
        "Mars (Sheba)",
        "Purina",
        "Royal Canin",
        "Catsan",
    ],
    "nonfood": ["EKP-Classic", "Varta", "Philips", "Oral-B", "Staedtler", "Pelikan"],
}

# Private-label eligibility per subcategory
_PL = {
    "fruits": True,
    "veg": True,
    "meat": True,
    "fish": True,
    "dairy": True,
    "eggs": True,
    "bakery": True,
    "deli": False,
    "beverages_soft": False,
    "beverages_alc": False,
    "coffee_tea": False,
    "snacks_choc": False,
    "snacks_crisp": False,
    "snacks_candy": False,
    "snacks_biscuit": False,
    "frozen": True,
    "canned": True,
    "cereals": True,
    "pasta_rice": True,
    "condiments": True,
    "cleaning": True,
    "laundry": True,
    "paper": True,
    "personal": False,
    "pharma": False,
    "pet": False,
    "nonfood": True,
}

PRODUCTS = []


def _add(
    category,
    subcategory,
    brand_key,
    base_names,
    variants,
    sizes,
    price_min,
    price_max,
    qty_min,
    qty_max,
    unit,
    seasonal=None,
):
    """
    Generate products via cartesian product of base_names × variants × sizes.
    Empty string variants/sizes are skipped from the name.
    """
    brands = _B[brand_key]
    pl_ok = _PL.get(brand_key, False)
    rng = _random.Random(42)  # deterministic brand assignment

    for base, variant, size in itertools.product(base_names, variants, sizes):
        brand = rng.choice(brands)
        parts = [brand, base]
        if variant:
            parts.append(variant)
        if size:
            parts.append(size)
        name = " ".join(parts)
        PRODUCTS.append(
            (
                category,
                subcategory,
                name,
                brand,
                pl_ok,
                price_min,
                price_max,
                qty_min,
                qty_max,
                unit,
                seasonal,
            )
        )


# ===========================================================================
# FRESH & PERISHABLES
# ===========================================================================

# -- Fruits --
_add(
    "Fresh & Perishables",
    "Fruits",
    "fruits",
    ["Apple Elstar", "Apple Braeburn", "Apple Gala", "Apple Fuji"],
    ["", "Organic"],
    ["500g bag", "1kg bag", "2kg bag"],
    0.49,
    2.99,
    1,
    6,
    "bag",
    None,
)

_add(
    "Fresh & Perishables",
    "Fruits",
    "fruits",
    ["Banana", "Banana Fairtrade"],
    ["", "Organic"],
    ["500g", "1kg", "1.5kg"],
    0.49,
    1.99,
    1,
    4,
    "bag",
    None,
)

_add(
    "Fresh & Perishables",
    "Fruits",
    "fruits",
    ["Strawberries"],
    ["", "Organic"],
    ["250g", "400g", "500g"],
    1.29,
    3.99,
    1,
    4,
    "punnet",
    [4, 5, 6, 7],
)

_add(
    "Fresh & Perishables",
    "Fruits",
    "fruits",
    ["Blueberries", "Raspberries"],
    ["", "Organic"],
    ["125g", "250g"],
    1.49,
    3.49,
    1,
    3,
    "punnet",
    [6, 7, 8],
)

_add(
    "Fresh & Perishables",
    "Fruits",
    "fruits",
    ["Grapes White", "Grapes Red"],
    ["Seedless"],
    ["500g", "1kg"],
    1.29,
    2.99,
    1,
    3,
    "bag",
    [7, 8, 9, 10],
)

_add(
    "Fresh & Perishables",
    "Fruits",
    "fruits",
    ["Orange Navel", "Orange Blood"],
    ["", "Organic"],
    ["500g net", "1kg net", "2kg net"],
    0.99,
    2.99,
    1,
    4,
    "bag",
    [11, 12, 1, 2, 3, 4],
)

_add(
    "Fresh & Perishables",
    "Fruits",
    "fruits",
    ["Clementines", "Mandarins"],
    ["Easy Peel", ""],
    ["500g net", "1kg net"],
    1.29,
    2.99,
    1,
    4,
    "bag",
    [10, 11, 12, 1, 2],
)

_add(
    "Fresh & Perishables",
    "Fruits",
    "fruits",
    ["Mango", "Papaya", "Pineapple"],
    ["", "Ready to Eat"],
    [""],
    0.99,
    2.99,
    1,
    3,
    "piece",
    None,
)

_add(
    "Fresh & Perishables",
    "Fruits",
    "fruits",
    ["Kiwi Green", "Kiwi Gold"],
    [""],
    ["4-pack", "6-pack"],
    0.99,
    2.49,
    1,
    3,
    "pack",
    None,
)

_add(
    "Fresh & Perishables",
    "Fruits",
    "fruits",
    ["Pear Conference", "Pear Williams"],
    ["", "Organic"],
    ["500g", "1kg"],
    0.79,
    2.29,
    1,
    4,
    "bag",
    [8, 9, 10, 11],
)

_add(
    "Fresh & Perishables",
    "Fruits",
    "fruits",
    ["Peach", "Nectarine"],
    ["", "Organic"],
    [""],
    0.39,
    0.99,
    2,
    8,
    "piece",
    [6, 7, 8, 9],
)

_add(
    "Fresh & Perishables",
    "Fruits",
    "fruits",
    ["Plum", "Mirabelle"],
    ["", "Organic"],
    ["500g", "1kg"],
    0.99,
    2.49,
    1,
    3,
    "bag",
    [7, 8, 9],
)

_add(
    "Fresh & Perishables",
    "Fruits",
    "fruits",
    ["Cherries Bing", "Cherries Morello"],
    [""],
    ["250g", "500g"],
    1.99,
    4.99,
    1,
    3,
    "punnet",
    [6, 7],
)

_add(
    "Fresh & Perishables",
    "Fruits",
    "fruits",
    ["Watermelon", "Cantaloupe Melon", "Honeydew Melon"],
    [""],
    [""],
    1.49,
    4.99,
    1,
    1,
    "piece",
    [6, 7, 8, 9],
)

_add(
    "Fresh & Perishables",
    "Fruits",
    "fruits",
    ["Avocado Hass"],
    ["", "Ready to Eat"],
    ["single", "2-pack", "4-pack"],
    0.79,
    2.99,
    1,
    4,
    "pack",
    None,
)

_add(
    "Fresh & Perishables",
    "Fruits",
    "fruits",
    ["Pomegranate", "Passion Fruit", "Star Fruit"],
    [""],
    [""],
    0.99,
    2.49,
    1,
    3,
    "piece",
    None,
)

_add(
    "Fresh & Perishables",
    "Fruits",
    "fruits",
    ["Lemon", "Lime"],
    ["Unwaxed"],
    ["3-pack", "5-pack", "500g net"],
    0.69,
    1.99,
    1,
    4,
    "pack",
    None,
)

# -- Vegetables --
_add(
    "Fresh & Perishables",
    "Vegetables",
    "veg",
    ["Tomato on Vine", "Beef Tomato"],
    ["", "Organic"],
    ["500g", "750g"],
    0.99,
    2.49,
    1,
    4,
    "pack",
    None,
)

_add(
    "Fresh & Perishables",
    "Vegetables",
    "veg",
    ["Cherry Tomatoes", "Cocktail Tomatoes"],
    ["", "Organic"],
    ["250g", "400g"],
    0.89,
    2.29,
    1,
    4,
    "pack",
    None,
)

_add(
    "Fresh & Perishables",
    "Vegetables",
    "veg",
    ["Cucumber", "Mini Cucumbers"],
    ["", "Organic"],
    [""],
    0.39,
    0.99,
    1,
    4,
    "piece",
    None,
)

_add(
    "Fresh & Perishables",
    "Vegetables",
    "veg",
    ["Iceberg Lettuce", "Romaine Lettuce", "Butterhead Lettuce"],
    ["", "Organic"],
    [""],
    0.49,
    1.29,
    1,
    3,
    "piece",
    None,
)

_add(
    "Fresh & Perishables",
    "Vegetables",
    "veg",
    ["Baby Spinach", "Rocket Salad", "Mixed Salad Leaves", "Lamb's Lettuce"],
    ["", "Organic"],
    ["100g bag", "150g bag"],
    0.99,
    2.49,
    1,
    3,
    "bag",
    None,
)

_add(
    "Fresh & Perishables",
    "Vegetables",
    "veg",
    ["Broccoli"],
    ["", "Organic"],
    ["350g", "500g"],
    0.89,
    1.99,
    1,
    3,
    "piece",
    None,
)

_add(
    "Fresh & Perishables",
    "Vegetables",
    "veg",
    ["Cauliflower", "Romanesco"],
    ["", "Organic"],
    [""],
    0.99,
    2.49,
    1,
    2,
    "piece",
    [9, 10, 11, 12, 1, 2],
)

_add(
    "Fresh & Perishables",
    "Vegetables",
    "veg",
    ["Savoy Cabbage", "White Cabbage", "Red Cabbage"],
    ["", "Organic"],
    [""],
    0.99,
    2.29,
    1,
    2,
    "piece",
    [9, 10, 11, 12, 1, 2, 3],
)

_add(
    "Fresh & Perishables",
    "Vegetables",
    "veg",
    ["Carrot"],
    ["Baby", "Organic"],
    ["500g bag", "1kg bag"],
    0.69,
    1.79,
    1,
    4,
    "bag",
    None,
)

_add(
    "Fresh & Perishables",
    "Vegetables",
    "veg",
    ["Potato Waxy", "Potato Floury", "New Potatoes"],
    ["", "Organic"],
    ["1kg bag", "2.5kg bag"],
    0.99,
    3.49,
    1,
    3,
    "bag",
    None,
)

_add(
    "Fresh & Perishables",
    "Vegetables",
    "veg",
    ["Sweet Potato"],
    ["", "Organic"],
    ["500g", "1kg"],
    0.79,
    2.29,
    1,
    3,
    "bag",
    None,
)

_add(
    "Fresh & Perishables",
    "Vegetables",
    "veg",
    ["White Onion", "Red Onion", "Shallots"],
    ["", "Organic"],
    ["500g net", "1kg net"],
    0.49,
    1.49,
    1,
    4,
    "bag",
    None,
)

_add(
    "Fresh & Perishables",
    "Vegetables",
    "veg",
    ["Garlic"],
    ["", "Organic"],
    ["1 bulb", "3-pack"],
    0.29,
    0.99,
    1,
    4,
    "pack",
    None,
)

_add(
    "Fresh & Perishables",
    "Vegetables",
    "veg",
    [
        "Bell Pepper Red",
        "Bell Pepper Yellow",
        "Bell Pepper Orange",
        "Bell Pepper Green",
    ],
    ["", "Organic"],
    [""],
    0.39,
    1.09,
    1,
    6,
    "piece",
    None,
)

_add(
    "Fresh & Perishables",
    "Vegetables",
    "veg",
    ["Mixed Peppers"],
    ["", "Organic"],
    ["3-pack", "600g pack"],
    1.29,
    2.99,
    1,
    3,
    "pack",
    None,
)

_add(
    "Fresh & Perishables",
    "Vegetables",
    "veg",
    ["Zucchini", "Aubergine", "Eggplant"],
    ["", "Organic"],
    [""],
    0.49,
    1.29,
    1,
    4,
    "piece",
    None,
)

_add(
    "Fresh & Perishables",
    "Vegetables",
    "veg",
    ["Leek"],
    ["", "Organic"],
    [""],
    0.49,
    1.09,
    1,
    4,
    "piece",
    [9, 10, 11, 12, 1, 2, 3],
)

_add(
    "Fresh & Perishables",
    "Vegetables",
    "veg",
    ["White Mushrooms", "Chestnut Mushrooms", "Portobello Mushrooms"],
    ["", "Organic"],
    ["250g", "400g", "500g"],
    0.89,
    2.49,
    1,
    4,
    "pack",
    None,
)

_add(
    "Fresh & Perishables",
    "Vegetables",
    "veg",
    ["Corn on the Cob"],
    ["", "Sweet"],
    ["2-pack", "4-pack"],
    0.69,
    1.79,
    1,
    3,
    "pack",
    [6, 7, 8, 9],
)

_add(
    "Fresh & Perishables",
    "Vegetables",
    "veg",
    ["Asparagus White", "Asparagus Green"],
    ["", "Organic"],
    ["500g", "1kg"],
    2.49,
    6.99,
    1,
    3,
    "pack",
    [4, 5, 6],
)

_add(
    "Fresh & Perishables",
    "Vegetables",
    "veg",
    ["Peas", "Broad Beans", "French Beans"],
    ["Fresh", "Organic"],
    ["200g", "400g"],
    0.99,
    2.49,
    1,
    3,
    "bag",
    [5, 6, 7, 8],
)

_add(
    "Fresh & Perishables",
    "Vegetables",
    "veg",
    ["Celery", "Fennel", "Kohlrabi", "Beetroot"],
    ["", "Organic"],
    [""],
    0.59,
    1.49,
    1,
    3,
    "piece",
    None,
)

_add(
    "Fresh & Perishables",
    "Vegetables",
    "veg",
    ["Spring Onion", "Radish"],
    ["", "Organic"],
    ["bunch"],
    0.39,
    0.89,
    1,
    3,
    "bunch",
    None,
)

_add(
    "Fresh & Perishables",
    "Vegetables",
    "veg",
    ["Pumpkin", "Butternut Squash"],
    ["", "Organic"],
    [""],
    1.49,
    3.99,
    1,
    2,
    "piece",
    [9, 10, 11],
)

_add(
    "Fresh & Perishables",
    "Vegetables",
    "veg",
    ["Fresh Parsley", "Fresh Basil", "Fresh Coriander", "Fresh Chives", "Fresh Dill"],
    [""],
    ["pot", "bunch"],
    0.49,
    1.29,
    1,
    3,
    "piece",
    None,
)

# -- Meat & Poultry --
_add(
    "Fresh & Perishables",
    "Meat & Poultry",
    "meat",
    ["Chicken Breast"],
    ["", "Free-Range", "Organic"],
    ["300g", "500g", "750g"],
    2.99,
    7.99,
    1,
    3,
    "pack",
    None,
)

_add(
    "Fresh & Perishables",
    "Meat & Poultry",
    "meat",
    ["Chicken Thighs", "Chicken Drumsticks"],
    ["", "Free-Range"],
    ["500g", "1kg"],
    2.49,
    6.49,
    1,
    3,
    "pack",
    None,
)

_add(
    "Fresh & Perishables",
    "Meat & Poultry",
    "meat",
    ["Whole Chicken"],
    ["", "Free-Range", "Organic"],
    ["1.3kg", "1.6kg"],
    4.99,
    9.99,
    1,
    2,
    "piece",
    None,
)

_add(
    "Fresh & Perishables",
    "Meat & Poultry",
    "meat",
    ["Turkey Breast Steak", "Turkey Escalope"],
    ["", "Organic"],
    ["300g", "400g"],
    3.49,
    6.99,
    1,
    3,
    "pack",
    None,
)

_add(
    "Fresh & Perishables",
    "Meat & Poultry",
    "meat",
    ["Pork Schnitzel", "Pork Fillet", "Pork Chops"],
    ["", "Marinaded"],
    ["300g", "500g"],
    2.99,
    7.99,
    1,
    3,
    "pack",
    None,
)

_add(
    "Fresh & Perishables",
    "Meat & Poultry",
    "meat",
    ["Pork Mince", "Beef Mince", "Mixed Mince"],
    ["", "Extra Lean"],
    ["400g", "500g"],
    2.49,
    5.99,
    1,
    4,
    "pack",
    None,
)

_add(
    "Fresh & Perishables",
    "Meat & Poultry",
    "meat",
    ["Beef Rump Steak", "Beef Sirloin", "Beef Ribeye"],
    ["", "Dry-Aged"],
    ["200g", "300g"],
    5.99,
    17.99,
    1,
    2,
    "pack",
    None,
)

_add(
    "Fresh & Perishables",
    "Meat & Poultry",
    "meat",
    ["Beef Braising Steak"],
    [""],
    ["400g", "600g"],
    3.99,
    8.49,
    1,
    2,
    "pack",
    None,
)

_add(
    "Fresh & Perishables",
    "Meat & Poultry",
    "meat",
    ["Lamb Chops", "Lamb Mince", "Lamb Leg Steak"],
    ["", "New Season"],
    ["300g", "400g"],
    5.99,
    13.99,
    1,
    2,
    "pack",
    None,
)

_add(
    "Fresh & Perishables",
    "Meat & Poultry",
    "meat",
    ["Bratwurst", "Grillwurst", "Currywurst"],
    ["Classic", "Spicy", "Cheese-filled"],
    ["2-pack", "4-pack"],
    1.99,
    5.49,
    1,
    4,
    "pack",
    None,
)

_add(
    "Fresh & Perishables",
    "Meat & Poultry",
    "meat",
    ["Frankfurter", "Wiener Würstchen", "Bockwurst"],
    ["Classic"],
    ["4-pack", "6-pack"],
    1.79,
    4.49,
    1,
    4,
    "pack",
    None,
)

# -- Fish & Seafood --
_add(
    "Fresh & Perishables",
    "Fish & Seafood",
    "fish",
    ["Salmon Fillet", "Salmon Steak"],
    ["", "Organic ASC"],
    ["200g", "300g", "500g"],
    3.99,
    9.99,
    1,
    3,
    "pack",
    None,
)

_add(
    "Fresh & Perishables",
    "Fish & Seafood",
    "fish",
    ["Cod Fillet", "Haddock Fillet", "Pollock Fillet"],
    ["", "MSC Certified"],
    ["300g", "400g"],
    3.49,
    7.99,
    1,
    2,
    "pack",
    None,
)

_add(
    "Fresh & Perishables",
    "Fish & Seafood",
    "fish",
    ["Trout", "Sea Bass", "Sea Bream"],
    ["", "Whole"],
    ["300g", "400g"],
    2.99,
    7.49,
    1,
    2,
    "piece",
    None,
)

_add(
    "Fresh & Perishables",
    "Fish & Seafood",
    "fish",
    ["Tuna Steak"],
    ["", "Sushi Grade"],
    ["150g", "200g"],
    3.49,
    7.99,
    1,
    2,
    "pack",
    None,
)

_add(
    "Fresh & Perishables",
    "Fish & Seafood",
    "fish",
    ["King Prawns", "Tiger Prawns"],
    ["Cooked", "Raw"],
    ["150g", "200g", "400g"],
    3.49,
    8.99,
    1,
    3,
    "pack",
    None,
)

_add(
    "Fresh & Perishables",
    "Fish & Seafood",
    "fish",
    ["Smoked Salmon"],
    ["", "Scottish"],
    ["100g", "200g"],
    3.49,
    6.99,
    1,
    3,
    "pack",
    None,
)

_add(
    "Fresh & Perishables",
    "Fish & Seafood",
    "fish",
    ["Herring Rollmops", "Matjes Herring"],
    ["Classic", "Cream"],
    ["4-pack"],
    1.99,
    3.99,
    1,
    3,
    "pack",
    None,
)

_add(
    "Fresh & Perishables",
    "Fish & Seafood",
    "fish",
    ["Surimi Sticks", "Crab Sticks"],
    [""],
    ["150g", "250g"],
    0.99,
    2.49,
    1,
    4,
    "pack",
    None,
)

# -- Dairy & Eggs --
_add(
    "Fresh & Perishables",
    "Dairy & Eggs",
    "dairy",
    ["Whole Milk", "Semi-Skimmed Milk", "Skimmed Milk"],
    ["", "Organic", "Lactose-Free"],
    ["1L", "2L"],
    0.79,
    2.29,
    1,
    6,
    "bottle",
    None,
)

_add(
    "Fresh & Perishables",
    "Dairy & Eggs",
    "dairy",
    ["Oat Drink", "Almond Drink", "Soy Drink", "Rice Drink"],
    ["Organic", "Barista"],
    ["1L"],
    1.29,
    2.49,
    1,
    4,
    "bottle",
    None,
)

_add(
    "Fresh & Perishables",
    "Dairy & Eggs",
    "dairy",
    ["Unsalted Butter", "Salted Butter"],
    ["", "Organic"],
    ["250g"],
    1.49,
    3.29,
    1,
    4,
    "pack",
    None,
)

_add(
    "Fresh & Perishables",
    "Dairy & Eggs",
    "dairy",
    ["Margarine", "Plant-Based Butter"],
    ["Organic"],
    ["250g", "500g"],
    0.99,
    2.49,
    1,
    3,
    "pack",
    None,
)

_add(
    "Fresh & Perishables",
    "Dairy & Eggs",
    "dairy",
    ["Natural Yoghurt", "Greek Yoghurt", "Skyr"],
    ["", "Organic", "High Protein"],
    ["150g", "400g", "500g"],
    0.59,
    2.49,
    1,
    4,
    "pack",
    None,
)

_add(
    "Fresh & Perishables",
    "Dairy & Eggs",
    "dairy",
    [
        "Fruit Yoghurt Strawberry",
        "Fruit Yoghurt Raspberry",
        "Fruit Yoghurt Mango",
        "Fruit Yoghurt Peach",
    ],
    [""],
    ["150g", "4×125g"],
    0.49,
    2.49,
    1,
    6,
    "pack",
    None,
)

_add(
    "Fresh & Perishables",
    "Dairy & Eggs",
    "dairy",
    ["Quark Plain", "Quark Vanilla"],
    ["", "Low Fat"],
    ["250g", "500g"],
    0.79,
    1.99,
    1,
    4,
    "pack",
    None,
)

_add(
    "Fresh & Perishables",
    "Dairy & Eggs",
    "dairy",
    ["Soured Cream", "Crème Fraîche", "Whipping Cream"],
    [""],
    ["200g", "200ml"],
    0.59,
    1.79,
    1,
    3,
    "pack",
    None,
)

_add(
    "Fresh & Perishables",
    "Dairy & Eggs",
    "dairy",
    ["Gouda Sliced", "Gouda Block", "Edam Block"],
    ["", "Mature", "Mild"],
    ["200g", "400g"],
    1.49,
    4.49,
    1,
    3,
    "pack",
    None,
)

_add(
    "Fresh & Perishables",
    "Dairy & Eggs",
    "dairy",
    ["Emmental Sliced", "Emmental Block"],
    ["", "Aged"],
    ["200g"],
    1.99,
    3.99,
    1,
    3,
    "pack",
    None,
)

_add(
    "Fresh & Perishables",
    "Dairy & Eggs",
    "dairy",
    ["Mozzarella"],
    ["", "Buffalo", "Mini"],
    ["125g", "2×125g"],
    0.79,
    2.49,
    1,
    4,
    "pack",
    None,
)

_add(
    "Fresh & Perishables",
    "Dairy & Eggs",
    "dairy",
    ["Feta"],
    ["", "Organic", "PDO Greek"],
    ["150g", "200g"],
    1.29,
    3.49,
    1,
    3,
    "pack",
    None,
)

_add(
    "Fresh & Perishables",
    "Dairy & Eggs",
    "dairy",
    ["Camembert"],
    ["", "Normandy"],
    ["125g", "250g"],
    1.29,
    3.49,
    1,
    3,
    "pack",
    None,
)

_add(
    "Fresh & Perishables",
    "Dairy & Eggs",
    "dairy",
    ["Cream Cheese", "Ricotta", "Mascarpone"],
    ["", "Light"],
    ["150g", "200g"],
    0.99,
    2.99,
    1,
    3,
    "pack",
    None,
)

_add(
    "Fresh & Perishables",
    "Dairy & Eggs",
    "eggs",
    ["Free-Range Eggs", "Organic Eggs"],
    [""],
    ["6-pack", "10-pack", "15-pack"],
    1.49,
    4.99,
    1,
    4,
    "pack",
    None,
)

_add(
    "Fresh & Perishables",
    "Dairy & Eggs",
    "eggs",
    ["Happy Egg Medium", "Happy Egg Large"],
    ["Free-Range"],
    ["6-pack"],
    1.99,
    3.49,
    1,
    3,
    "pack",
    None,
)

# -- Bakery & Pastry --
_add(
    "Fresh & Perishables",
    "Bakery & Pastry",
    "bakery",
    ["Sourdough Bread", "Rye Bread", "Wholegrain Bread", "Seeded Bread"],
    ["", "Organic"],
    ["400g", "750g"],
    1.49,
    3.49,
    1,
    3,
    "loaf",
    None,
)

_add(
    "Fresh & Perishables",
    "Bakery & Pastry",
    "bakery",
    ["Baguette", "Ciabatta"],
    ["", "Rustic"],
    [""],
    0.49,
    1.29,
    1,
    4,
    "piece",
    None,
)

_add(
    "Fresh & Perishables",
    "Bakery & Pastry",
    "bakery",
    ["Dinner Rolls", "Pretzel Rolls"],
    [""],
    ["4-pack", "6-pack"],
    0.89,
    2.29,
    1,
    4,
    "pack",
    None,
)

_add(
    "Fresh & Perishables",
    "Bakery & Pastry",
    "bakery",
    ["Croissant Butter", "Croissant Almond", "Croissant Chocolate"],
    [""],
    [""],
    0.59,
    1.49,
    1,
    6,
    "piece",
    None,
)

_add(
    "Fresh & Perishables",
    "Bakery & Pastry",
    "bakery",
    ["Bretzel", "Laugenstange", "Laugenbrezel"],
    [""],
    [""],
    0.39,
    0.89,
    1,
    6,
    "piece",
    None,
)

_add(
    "Fresh & Perishables",
    "Bakery & Pastry",
    "bakery",
    ["Toast Bread White", "Toast Bread Whole Grain"],
    ["", "Organic"],
    ["500g", "750g"],
    0.99,
    2.49,
    1,
    3,
    "pack",
    None,
)

_add(
    "Fresh & Perishables",
    "Bakery & Pastry",
    "bakery",
    ["Berliner Doughnut", "Jam Doughnut"],
    [""],
    [""],
    0.59,
    1.29,
    1,
    6,
    "piece",
    [12, 1, 2],
)

_add(
    "Fresh & Perishables",
    "Bakery & Pastry",
    "bakery",
    ["Apple Strudel", "Cheesecake Slice", "Black Forest Cake Slice"],
    [""],
    [""],
    1.99,
    4.49,
    1,
    3,
    "piece",
    None,
)

_add(
    "Fresh & Perishables",
    "Bakery & Pastry",
    "bakery",
    ["Pumpernickel"],
    [""],
    ["250g", "500g"],
    1.49,
    2.99,
    1,
    3,
    "pack",
    None,
)

_add(
    "Fresh & Perishables",
    "Bakery & Pastry",
    "bakery",
    ["Weihnachtsstollen", "Lebkuchen"],
    [""],
    ["500g", "1kg"],
    3.99,
    9.99,
    1,
    3,
    "piece",
    [11, 12],
)

# -- Deli & Charcuterie --
_add(
    "Fresh & Perishables",
    "Deli & Charcuterie",
    "deli",
    ["Salami", "Pepperoni", "Chorizo"],
    ["Milano", "Spicy", "Thin Sliced"],
    ["80g", "100g"],
    1.49,
    3.99,
    1,
    3,
    "pack",
    None,
)

_add(
    "Fresh & Perishables",
    "Deli & Charcuterie",
    "deli",
    ["Cooked Ham", "Honey Roast Ham", "Smoked Ham"],
    ["Wafer Thin", "Thick Cut"],
    ["120g", "200g"],
    1.49,
    3.99,
    1,
    3,
    "pack",
    None,
)

_add(
    "Fresh & Perishables",
    "Deli & Charcuterie",
    "deli",
    ["Black Forest Ham", "Schwarzwälder Schinken"],
    [""],
    ["80g", "120g"],
    1.99,
    4.49,
    1,
    2,
    "pack",
    None,
)

_add(
    "Fresh & Perishables",
    "Deli & Charcuterie",
    "deli",
    ["Prosciutto Crudo", "Parma Ham", "Serrano Ham"],
    [""],
    ["70g", "100g"],
    2.49,
    5.99,
    1,
    2,
    "pack",
    None,
)

_add(
    "Fresh & Perishables",
    "Deli & Charcuterie",
    "deli",
    ["Leberwurst", "Teewurst", "Mettwurst"],
    ["", "Coarse"],
    ["125g", "200g"],
    1.29,
    3.49,
    1,
    3,
    "pack",
    None,
)

_add(
    "Fresh & Perishables",
    "Deli & Charcuterie",
    "deli",
    ["Hummus Original", "Hummus Roasted Red Pepper", "Hummus Beetroot"],
    [""],
    ["150g", "200g"],
    0.99,
    2.49,
    1,
    3,
    "pack",
    None,
)

_add(
    "Fresh & Perishables",
    "Deli & Charcuterie",
    "deli",
    ["Tzatziki", "Guacamole", "Taramasalata"],
    [""],
    ["150g", "200g"],
    0.99,
    2.99,
    1,
    3,
    "pack",
    None,
)

# ===========================================================================
# PACKAGED FOOD
# ===========================================================================

# -- Beverages: Soft Drinks --
_add(
    "Packaged Food",
    "Beverages",
    "beverages_soft",
    ["Cola", "Diet Cola", "Cola Zero"],
    [""],
    ["330ml can", "500ml bottle", "1.25L bottle", "2L bottle"],
    0.59,
    1.99,
    1,
    8,
    "bottle",
    None,
)

_add(
    "Packaged Food",
    "Beverages",
    "beverages_soft",
    ["Orange Soda", "Lemon Soda", "Grapefruit Soda", "Cherry Soda", "Elderflower"],
    ["", "Zero Sugar"],
    ["330ml can", "500ml bottle", "1.5L bottle"],
    0.49,
    1.79,
    1,
    6,
    "bottle",
    None,
)

_add(
    "Packaged Food",
    "Beverages",
    "beverages_soft",
    ["Still Water", "Sparkling Water", "Flavoured Water"],
    ["", "Mineral"],
    ["500ml", "750ml", "1.5L"],
    0.29,
    1.29,
    1,
    12,
    "bottle",
    None,
)

_add(
    "Packaged Food",
    "Beverages",
    "beverages_soft",
    [
        "Apple Juice",
        "Orange Juice",
        "Mango Juice",
        "Multivitamin Juice",
        "Cranberry Juice",
    ],
    ["", "100% Pure", "Organic"],
    ["200ml", "750ml", "1L"],
    0.79,
    2.99,
    1,
    4,
    "bottle",
    None,
)

_add(
    "Packaged Food",
    "Beverages",
    "beverages_soft",
    ["Energy Drink"],
    ["Classic", "Zero", "Watermelon", "Peach"],
    ["250ml can", "500ml can"],
    0.99,
    2.29,
    1,
    4,
    "can",
    None,
)

_add(
    "Packaged Food",
    "Beverages",
    "beverages_soft",
    ["Iced Tea Lemon", "Iced Tea Peach", "Iced Tea Green"],
    ["", "Zero"],
    ["500ml", "1.5L"],
    0.79,
    1.99,
    1,
    6,
    "bottle",
    None,
)

# -- Beverages: Alcohol --
_add(
    "Packaged Food",
    "Beverages",
    "beverages_alc",
    ["Lager Beer", "Wheat Beer", "Pilsner"],
    ["", "Alcohol-Free"],
    ["500ml can", "4×500ml pack", "6×500ml pack"],
    0.79,
    7.99,
    1,
    4,
    "pack",
    None,
)

_add(
    "Packaged Food",
    "Beverages",
    "beverages_alc",
    [
        "White Wine Riesling",
        "White Wine Grauburgunder",
        "Red Wine Merlot",
        "Red Wine Dornfelder",
        "Rosé Wine",
        "Sekt Brut",
    ],
    ["", "Organic", "Reserve"],
    ["0.75L"],
    4.49,
    12.99,
    1,
    3,
    "bottle",
    None,
)

# -- Coffee & Tea --
_add(
    "Packaged Food",
    "Beverages",
    "coffee_tea",
    ["Filter Coffee", "Espresso Beans", "Ground Coffee"],
    ["Classic", "Strong", "Mild", "Decaf"],
    ["200g", "250g", "500g"],
    3.49,
    9.99,
    1,
    3,
    "pack",
    None,
)

_add(
    "Packaged Food",
    "Beverages",
    "coffee_tea",
    ["Instant Coffee"],
    ["Classic", "Gold", "Decaf"],
    ["100g jar", "200g jar"],
    2.99,
    7.49,
    1,
    3,
    "jar",
    None,
)

_add(
    "Packaged Food",
    "Beverages",
    "coffee_tea",
    [
        "Earl Grey Tea",
        "English Breakfast Tea",
        "Green Tea",
        "Chamomile Tea",
        "Peppermint Tea",
        "Fruit Tea",
    ],
    ["", "Organic"],
    ["20 bags", "50 bags"],
    1.99,
    4.99,
    1,
    3,
    "box",
    None,
)

# -- Snacks: Chocolate & Candy --
_add(
    "Packaged Food",
    "Snacks & Confectionery",
    "snacks_choc",
    [
        "Milk Chocolate Bar",
        "Dark Chocolate Bar",
        "White Chocolate Bar",
        "Hazelnut Chocolate",
    ],
    ["100g", ""],
    [""],
    0.89,
    2.99,
    1,
    4,
    "bar",
    None,
)

_add(
    "Packaged Food",
    "Snacks & Confectionery",
    "snacks_choc",
    ["Chocolate Wafer", "Chocolate Fingers", "Chocolate Truffles", "Pralines Box"],
    ["", "Hazelnut", "Dark"],
    ["60g", "100g", "200g"],
    0.99,
    8.99,
    1,
    3,
    "pack",
    None,
)

_add(
    "Packaged Food",
    "Snacks & Confectionery",
    "snacks_choc",
    ["Hazelnut Cream Spread"],
    ["Original"],
    ["200g jar", "400g jar", "750g jar"],
    1.99,
    6.99,
    1,
    2,
    "jar",
    None,
)

_add(
    "Packaged Food",
    "Snacks & Confectionery",
    "snacks_candy",
    [
        "Gummy Bears",
        "Gummy Worms",
        "Foam Strawberries",
        "Sour Belts",
        "Liquorice",
        "Wine Gums",
    ],
    ["", "Sugar-Free", "Vegan"],
    ["100g", "175g", "200g", "500g"],
    0.79,
    2.99,
    1,
    4,
    "bag",
    None,
)

_add(
    "Packaged Food",
    "Snacks & Confectionery",
    "snacks_crisp",
    [
        "Potato Chips Original",
        "Potato Chips Paprika",
        "Potato Chips Sour Cream & Onion",
        "Potato Chips Salt & Vinegar",
    ],
    [""],
    ["100g", "175g", "200g"],
    1.09,
    2.49,
    1,
    4,
    "bag",
    None,
)

_add(
    "Packaged Food",
    "Snacks & Confectionery",
    "snacks_crisp",
    [
        "Tortilla Chips",
        "Popcorn Salted",
        "Popcorn Sweet",
        "Rice Cakes",
        "Pretzels Snack",
    ],
    ["", "Lightly Salted"],
    ["100g", "150g"],
    0.99,
    2.29,
    1,
    4,
    "bag",
    None,
)

_add(
    "Packaged Food",
    "Snacks & Confectionery",
    "snacks_biscuit",
    [
        "Butter Biscuits",
        "Shortbread",
        "Digestives",
        "Ginger Biscuits",
        "Sandwich Cookies",
    ],
    ["", "Chocolate Dipped"],
    ["150g", "200g", "400g"],
    0.99,
    2.99,
    1,
    4,
    "pack",
    None,
)

_add(
    "Packaged Food",
    "Snacks & Confectionery",
    "snacks_biscuit",
    ["Crackers", "Rice Crackers", "Water Biscuits"],
    ["", "Wholegrain", "Sesame"],
    ["150g", "200g"],
    0.99,
    2.49,
    1,
    4,
    "pack",
    None,
)

# -- Frozen Food --
_add(
    "Packaged Food",
    "Frozen Food",
    "frozen",
    [
        "Cheese Pizza",
        "Margherita Pizza",
        "Salami Pizza",
        "BBQ Chicken Pizza",
        "Vegetarian Pizza",
    ],
    ["Crispy", "Stone Baked"],
    ["320g", "425g"],
    2.49,
    5.49,
    1,
    3,
    "piece",
    None,
)

_add(
    "Packaged Food",
    "Frozen Food",
    "frozen",
    ["Oven Chips", "Crinkle Cut Chips", "Sweet Potato Fries", "Wedges", "Hash Browns"],
    [""],
    ["500g", "750g", "1kg"],
    1.49,
    3.99,
    1,
    4,
    "bag",
    None,
)

_add(
    "Packaged Food",
    "Frozen Food",
    "frozen",
    ["Fish Fingers", "Breaded Cod", "Breaded Prawns"],
    ["", "MSC"],
    ["300g", "400g"],
    2.49,
    4.99,
    1,
    3,
    "pack",
    None,
)

_add(
    "Packaged Food",
    "Frozen Food",
    "frozen",
    ["Chicken Nuggets", "Chicken Strips"],
    ["", "Southern Fried"],
    ["300g", "500g"],
    2.49,
    5.49,
    1,
    3,
    "pack",
    None,
)

_add(
    "Packaged Food",
    "Frozen Food",
    "frozen",
    [
        "Frozen Peas",
        "Frozen Spinach",
        "Frozen Mixed Veg",
        "Frozen Broccoli",
        "Frozen Sweetcorn",
    ],
    ["", "Organic"],
    ["500g", "750g"],
    0.99,
    2.49,
    1,
    4,
    "bag",
    None,
)

_add(
    "Packaged Food",
    "Frozen Food",
    "frozen",
    ["Vanilla Ice Cream", "Strawberry Ice Cream", "Chocolate Ice Cream"],
    ["", "Premium"],
    ["500ml", "1L"],
    2.49,
    7.99,
    1,
    3,
    "tub",
    None,
)

_add(
    "Packaged Food",
    "Frozen Food",
    "frozen",
    ["Frozen Berries Mix", "Frozen Mango Chunks", "Frozen Raspberries"],
    ["", "Organic"],
    ["500g", "750g"],
    1.99,
    3.99,
    1,
    3,
    "bag",
    None,
)

# -- Canned & Jarred --
_add(
    "Packaged Food",
    "Canned & Jarred",
    "canned",
    [
        "Sweetcorn",
        "Green Beans",
        "Mixed Vegetables",
        "Garden Peas",
        "Kidney Beans",
        "Chickpeas",
        "Cannellini Beans",
    ],
    [""],
    ["200g", "340g", "400g"],
    0.49,
    1.29,
    1,
    6,
    "can",
    None,
)

_add(
    "Packaged Food",
    "Canned & Jarred",
    "canned",
    ["Chopped Tomatoes", "Tomato Passata", "Tomato Purée"],
    ["", "Organic"],
    ["400g can", "500g jar"],
    0.39,
    1.29,
    2,
    8,
    "can",
    None,
)

_add(
    "Packaged Food",
    "Canned & Jarred",
    "canned",
    ["Tuna Chunks in Brine", "Tuna in Olive Oil", "Sardines in Oil"],
    [""],
    ["185g can"],
    0.89,
    2.49,
    2,
    6,
    "can",
    None,
)

_add(
    "Packaged Food",
    "Canned & Jarred",
    "canned",
    ["Baked Beans", "Tomato Soup", "Lentil Soup", "Vegetable Soup"],
    [""],
    ["400g"],
    0.69,
    1.99,
    1,
    6,
    "can",
    None,
)

_add(
    "Packaged Food",
    "Canned & Jarred",
    "canned",
    ["Strawberry Jam", "Raspberry Jam", "Apricot Jam", "Mixed Berry Jam"],
    ["", "Reduced Sugar", "Organic"],
    ["340g jar"],
    1.49,
    3.29,
    1,
    3,
    "jar",
    None,
)

_add(
    "Packaged Food",
    "Canned & Jarred",
    "canned",
    ["Gherkins", "Pickled Onions", "Sauerkraut", "Red Cabbage Pickled"],
    ["Classic", "Sweet"],
    ["350g jar"],
    0.89,
    2.49,
    1,
    3,
    "jar",
    None,
)

_add(
    "Packaged Food",
    "Canned & Jarred",
    "canned",
    ["Green Olives Pitted", "Black Olives Pitted", "Mixed Olives"],
    [""],
    ["200g jar"],
    1.19,
    2.99,
    1,
    3,
    "jar",
    None,
)

# -- Cereals & Breakfast --
_add(
    "Packaged Food",
    "Cereals & Breakfast",
    "cereals",
    ["Cornflakes", "Frosties", "Coco Pops", "Rice Krispies", "Special K"],
    ["", "Organic"],
    ["375g", "500g"],
    2.29,
    4.49,
    1,
    3,
    "box",
    None,
)

_add(
    "Packaged Food",
    "Cereals & Breakfast",
    "cereals",
    ["Muesli Fruit & Nut", "Granola Honey Almond", "Porridge Oats", "Bran Flakes"],
    ["", "Organic"],
    ["500g", "750g"],
    1.99,
    5.49,
    1,
    3,
    "bag",
    None,
)

_add(
    "Packaged Food",
    "Cereals & Breakfast",
    "cereals",
    ["Rye Crispbread", "Oat Crispbread"],
    ["", "Organic"],
    ["250g", "500g"],
    1.49,
    3.49,
    1,
    3,
    "pack",
    None,
)

# -- Pasta, Rice & Grains --
_add(
    "Packaged Food",
    "Pasta, Rice & Grains",
    "pasta_rice",
    [
        "Spaghetti",
        "Penne",
        "Fusilli",
        "Rigatoni",
        "Farfalle",
        "Tagliatelle",
        "Linguine",
    ],
    ["", "Whole Grain", "Organic"],
    ["400g", "500g"],
    0.59,
    2.49,
    1,
    6,
    "pack",
    None,
)

_add(
    "Packaged Food",
    "Pasta, Rice & Grains",
    "pasta_rice",
    [
        "Basmati Rice",
        "Long Grain Rice",
        "Brown Rice",
        "Arborio Risotto Rice",
        "Jasmine Rice",
    ],
    ["", "Organic"],
    ["500g", "1kg", "2kg"],
    0.89,
    3.99,
    1,
    4,
    "bag",
    None,
)

_add(
    "Packaged Food",
    "Pasta, Rice & Grains",
    "pasta_rice",
    ["Couscous", "Quinoa", "Bulgur Wheat", "Polenta", "Green Lentils", "Red Lentils"],
    ["", "Organic"],
    ["400g", "500g"],
    0.99,
    3.99,
    1,
    4,
    "bag",
    None,
)

# -- Condiments & Sauces --
_add(
    "Packaged Food",
    "Condiments & Sauces",
    "condiments",
    ["Tomato Ketchup", "Curry Ketchup", "Sweet Chilli Sauce"],
    ["", "Reduced Sugar"],
    ["250ml", "500ml", "570g"],
    0.99,
    3.99,
    1,
    3,
    "bottle",
    None,
)

_add(
    "Packaged Food",
    "Condiments & Sauces",
    "condiments",
    ["Mayonnaise", "Light Mayonnaise", "Garlic Aioli"],
    [""],
    ["200ml", "430ml"],
    0.99,
    2.99,
    1,
    3,
    "jar",
    None,
)

_add(
    "Packaged Food",
    "Condiments & Sauces",
    "condiments",
    ["Yellow Mustard", "Dijon Mustard", "Wholegrain Mustard"],
    [""],
    ["200ml tube", "300g jar"],
    0.79,
    2.49,
    1,
    3,
    "piece",
    None,
)

_add(
    "Packaged Food",
    "Condiments & Sauces",
    "condiments",
    [
        "Pasta Sauce Bolognese",
        "Pasta Sauce Arrabiata",
        "Pasta Sauce Pesto Rosso",
        "Pasta Sauce Carbonara",
    ],
    ["", "Organic"],
    ["350g jar"],
    1.29,
    3.49,
    1,
    4,
    "jar",
    None,
)

_add(
    "Packaged Food",
    "Condiments & Sauces",
    "condiments",
    ["Pesto Basilico", "Red Pesto", "Pesto Rosso"],
    ["", "Organic"],
    ["190g jar"],
    1.79,
    3.49,
    1,
    3,
    "jar",
    None,
)

_add(
    "Packaged Food",
    "Condiments & Sauces",
    "condiments",
    ["Extra Virgin Olive Oil", "Sunflower Oil", "Rapeseed Oil", "Coconut Oil"],
    ["", "Organic"],
    ["500ml", "1L"],
    2.49,
    8.99,
    1,
    3,
    "bottle",
    None,
)

_add(
    "Packaged Food",
    "Condiments & Sauces",
    "condiments",
    ["Soy Sauce", "Teriyaki Sauce", "Sweet Chilli"],
    ["Light", "Dark"],
    ["150ml", "250ml"],
    1.29,
    2.99,
    1,
    3,
    "bottle",
    None,
)

_add(
    "Packaged Food",
    "Condiments & Sauces",
    "condiments",
    ["Salt Fine", "Sea Salt Flakes", "Rock Salt"],
    ["", "Iodised"],
    ["200g", "500g"],
    0.29,
    1.99,
    1,
    4,
    "pack",
    None,
)

_add(
    "Packaged Food",
    "Condiments & Sauces",
    "condiments",
    [
        "Black Pepper Ground",
        "Mixed Peppercorns",
        "Paprika Sweet",
        "Paprika Smoked",
        "Cumin Ground",
        "Cinnamon Ground",
        "Mixed Herbs",
        "Thyme Dried",
        "Oregano Dried",
        "Basil Dried",
    ],
    [""],
    ["50g", "80g"],
    0.49,
    1.99,
    1,
    3,
    "pack",
    None,
)

# ===========================================================================
# HOUSEHOLD
# ===========================================================================

_add(
    "Household",
    "Cleaning Products",
    "cleaning",
    [
        "Washing-Up Liquid",
        "Dishwasher Tablets",
        "Kitchen Spray",
        "Bathroom Cleaner",
        "Toilet Cleaner",
        "Floor Cleaner",
        "Glass Cleaner",
    ],
    ["Lemon", "Original", "Anti-Bac", "Eco"],
    ["500ml", "750ml", "1L"],
    0.99,
    7.99,
    1,
    3,
    "bottle",
    None,
)

_add(
    "Household",
    "Laundry",
    "laundry",
    [
        "Bio Washing Liquid",
        "Non-Bio Washing Liquid",
        "Colour Washing Liquid",
        "Washing Pods",
    ],
    ["", "Sensitive", "Eco"],
    ["24 wash", "40 wash"],
    4.99,
    14.99,
    1,
    2,
    "pack",
    None,
)

_add(
    "Household",
    "Laundry",
    "laundry",
    ["Fabric Conditioner"],
    ["Spring Fresh", "Lavender", "Sensitive"],
    ["1L", "1.5L"],
    2.99,
    6.99,
    1,
    3,
    "bottle",
    None,
)

_add(
    "Household",
    "Paper & Hygiene",
    "paper",
    ["Toilet Roll"],
    ["2-ply", "3-ply", "Premium"],
    ["4-pack", "9-pack", "16-pack"],
    1.49,
    8.99,
    1,
    3,
    "pack",
    None,
)

_add(
    "Household",
    "Paper & Hygiene",
    "paper",
    ["Kitchen Towel"],
    ["", "XL"],
    ["2-pack", "4-pack"],
    1.49,
    4.99,
    1,
    3,
    "pack",
    None,
)

_add(
    "Household",
    "Paper & Hygiene",
    "paper",
    ["Facial Tissues"],
    ["", "Balsam", "Pocket"],
    ["80-pack", "100-pack", "9×pocket"],
    0.99,
    3.49,
    1,
    4,
    "pack",
    None,
)

_add(
    "Household",
    "Paper & Hygiene",
    "paper",
    ["Sanitary Pads Ultra", "Sanitary Pads Night", "Pantyliners"],
    [""],
    ["12-pack", "20-pack"],
    1.99,
    4.99,
    1,
    3,
    "pack",
    None,
)

_add(
    "Household",
    "Paper & Hygiene",
    "paper",
    [
        "Nappies Size 1",
        "Nappies Size 2",
        "Nappies Size 3",
        "Nappies Size 4",
        "Nappies Size 5",
    ],
    [""],
    ["24-pack", "44-pack"],
    5.99,
    14.99,
    1,
    2,
    "pack",
    None,
)

# ===========================================================================
# HEALTH & BEAUTY
# ===========================================================================

_add(
    "Health & Beauty",
    "Personal Care",
    "personal",
    ["Shampoo", "Conditioner", "2-in-1 Shampoo", "Dry Shampoo"],
    ["Normal", "Oily", "Damaged", "Colour Protect", "Anti-Dandruff", "Men"],
    ["200ml", "300ml", "400ml"],
    2.49,
    7.99,
    1,
    2,
    "bottle",
    None,
)

_add(
    "Health & Beauty",
    "Personal Care",
    "personal",
    ["Shower Gel", "Body Wash"],
    ["Fresh", "Moisturising", "Sensitive", "Men Deep Clean"],
    ["250ml", "500ml"],
    1.49,
    4.49,
    1,
    4,
    "bottle",
    None,
)

_add(
    "Health & Beauty",
    "Personal Care",
    "personal",
    ["Bar Soap"],
    ["Classic", "Moisturising", "Antibacterial"],
    ["90g", "4-pack"],
    0.49,
    2.99,
    1,
    4,
    "pack",
    None,
)

_add(
    "Health & Beauty",
    "Personal Care",
    "personal",
    ["Body Lotion", "Hand Cream", "Face Moisturiser"],
    ["", "SPF 30", "Sensitive"],
    ["75ml", "200ml", "400ml"],
    1.99,
    8.99,
    1,
    2,
    "bottle",
    None,
)

_add(
    "Health & Beauty",
    "Personal Care",
    "personal",
    ["Toothpaste"],
    ["Whitening", "Sensitive", "Kids", "Total Care"],
    ["75ml", "100ml"],
    1.49,
    3.99,
    1,
    3,
    "tube",
    None,
)

_add(
    "Health & Beauty",
    "Personal Care",
    "personal",
    ["Manual Toothbrush", "Interdental Brushes"],
    ["Soft", "Medium"],
    [""],
    0.99,
    3.49,
    1,
    3,
    "piece",
    None,
)

_add(
    "Health & Beauty",
    "Personal Care",
    "personal",
    ["Deodorant Roll-On", "Deodorant Spray", "Antiperspirant"],
    ["Original", "Sensitive", "48h", "Men"],
    ["150ml"],
    1.49,
    3.99,
    1,
    3,
    "piece",
    None,
)

_add(
    "Health & Beauty",
    "Personal Care",
    "personal",
    ["Disposable Razor", "Shaving Foam", "Aftershave Balm"],
    ["", "Sensitive", "Men"],
    [""],
    1.49,
    8.99,
    1,
    2,
    "piece",
    None,
)

_add(
    "Health & Beauty",
    "Personal Care",
    "personal",
    ["Tampons Regular", "Tampons Super"],
    [""],
    ["16-pack", "20-pack"],
    2.99,
    5.49,
    1,
    3,
    "pack",
    None,
)

_add(
    "Health & Beauty",
    "Cosmetics",
    "personal",
    ["Mascara", "Foundation", "Concealer", "Lipstick", "Nail Polish", "Eye Shadow"],
    [""],
    [""],
    4.99,
    16.99,
    1,
    1,
    "piece",
    None,
)

_add(
    "Health & Beauty",
    "Vitamins & Supplements",
    "pharma",
    [
        "Vitamin C 500mg",
        "Vitamin D3 1000 IU",
        "Vitamin B12",
        "Zinc Tablets",
        "Magnesium 300mg",
        "Iron Tablets",
        "Omega-3 Fish Oil Capsules",
        "Multivitamin Complete",
    ],
    ["", "High Strength"],
    ["30 tabs", "60 tabs", "90 tabs"],
    3.99,
    14.99,
    1,
    2,
    "box",
    None,
)

_add(
    "Health & Beauty",
    "Pharmacy OTC",
    "pharma",
    ["Paracetamol 500mg", "Ibuprofen 400mg", "Aspirin 300mg"],
    [""],
    ["16 tabs", "32 tabs"],
    2.49,
    5.99,
    1,
    2,
    "pack",
    None,
)

_add(
    "Health & Beauty",
    "Pharmacy OTC",
    "pharma",
    [
        "Antihistamine Tablets",
        "Cold & Flu Tablets",
        "Sore Throat Lozenges",
        "Cough Syrup",
    ],
    ["Day & Night"],
    [""],
    3.49,
    9.99,
    1,
    2,
    "pack",
    None,
)

_add(
    "Health & Beauty",
    "Pharmacy OTC",
    "pharma",
    ["Antiseptic Cream", "Wound Gel", "Plasters Assorted"],
    [""],
    [""],
    2.99,
    8.99,
    1,
    2,
    "piece",
    None,
)

# ===========================================================================
# NON-FOOD
# ===========================================================================

_add(
    "Non-Food",
    "Pet Supplies",
    "pet",
    ["Dry Cat Food", "Wet Cat Food Pouches", "Dry Dog Food", "Wet Dog Food"],
    ["Adult", "Senior", "Kitten", "Puppy"],
    ["400g", "1.5kg", "3kg"],
    2.99,
    12.99,
    1,
    3,
    "pack",
    None,
)

_add(
    "Non-Food",
    "Pet Supplies",
    "pet",
    ["Cat Litter Clumping", "Cat Litter Non-Clumping"],
    [""],
    ["5L", "10L"],
    3.99,
    9.99,
    1,
    2,
    "bag",
    None,
)

_add(
    "Non-Food",
    "Pet Supplies",
    "pet",
    ["Dog Treats", "Cat Treats"],
    ["Chicken", "Beef", "Fish"],
    ["100g", "200g"],
    1.49,
    4.99,
    1,
    3,
    "bag",
    None,
)

_add(
    "Non-Food",
    "Books & Stationery",
    "nonfood",
    ["Ballpoint Pen", "Felt Tip Pens", "Highlighters", "Pencil HB"],
    [""],
    ["single", "3-pack", "5-pack"],
    0.49,
    3.99,
    1,
    4,
    "pack",
    None,
)

_add(
    "Non-Food",
    "Books & Stationery",
    "nonfood",
    ["Notepad A4 Ruled", "Notepad A5 Ruled", "Sticky Notes"],
    [""],
    [""],
    0.99,
    3.49,
    1,
    3,
    "piece",
    None,
)

_add(
    "Non-Food",
    "Seasonal & Promotions",
    "nonfood",
    ["Christmas Stollen"],
    ["", "Marzipan", "Cranberry"],
    ["500g", "1kg"],
    4.99,
    11.99,
    1,
    2,
    "piece",
    [11, 12],
)

_add(
    "Non-Food",
    "Seasonal & Promotions",
    "nonfood",
    ["Easter Chocolate Egg", "Easter Chocolate Bunny"],
    ["", "Hollow"],
    ["100g", "200g"],
    1.99,
    6.99,
    1,
    3,
    "piece",
    [2, 3, 4],
)

_add(
    "Non-Food",
    "Seasonal & Promotions",
    "nonfood",
    ["BBQ Charcoal", "BBQ Briquettes", "Firelighters"],
    [""],
    ["3kg bag", "5kg bag"],
    3.99,
    9.99,
    1,
    3,
    "bag",
    [3, 4, 5, 6, 7, 8, 9],
)

_add(
    "Non-Food",
    "Seasonal & Promotions",
    "nonfood",
    ["Advent Calendar Chocolate", "Lebkuchen Box", "Christmas Gingerbread"],
    [""],
    ["200g", "400g"],
    3.99,
    9.99,
    1,
    3,
    "piece",
    [10, 11, 12],
)

_add(
    "Non-Food",
    "Seasonal & Promotions",
    "nonfood",
    ["Valentine's Rose Box", "Mother's Day Biscuit Tin"],
    [""],
    [""],
    4.99,
    14.99,
    1,
    2,
    "piece",
    [1, 2, 3, 5],
)

# ---------------------------------------------------------------------------
# Per-product popularity scores (Fix 2 — Zipf/Pareto distribution)
# ---------------------------------------------------------------------------
# In real retail, a few SKUs dominate volume (bananas, milk, bread) while
# most products are slow movers.  We assign each product a popularity
# multiplier using a Zipf-like curve within its subcategory.
#
# After all _add() calls have populated PRODUCTS, we rank products within
# each subcategory and assign: popularity = 1 / rank^0.6
# (exponent 0.6 gives a moderate 80/20 effect without making rare items
# completely invisible).

_PRODUCT_POPULARITY: dict = {}  # product_name -> float multiplier


def _compute_popularity():
    """Called once after catalogue is fully built."""
    from collections import defaultdict

    by_subcat = defaultdict(list)
    for i, p in enumerate(PRODUCTS):
        by_subcat[p[1]].append(i)  # group indices by subcategory

    pop_rng = _random.Random(99)  # deterministic, separate from brand rng
    for subcat, indices in by_subcat.items():
        # Shuffle within subcategory so popularity isn't tied to insertion order
        shuffled = list(indices)
        pop_rng.shuffle(shuffled)
        for rank, idx in enumerate(shuffled, start=1):
            name = PRODUCTS[idx][2]
            # Zipf-like: rank 1 → 1.0, rank 2 → 0.66, rank 10 → 0.25, rank 50 → 0.13
            _PRODUCT_POPULARITY[name] = 1.0 / (rank**0.6)


# ---------------------------------------------------------------------------
# Catalogue access API
# ---------------------------------------------------------------------------


def get_available_products(month: int):
    """Return (products, weights) for the given calendar month.

    Weight = subcategory_base_weight × per_product_popularity.
    This creates realistic Pareto distribution where a few SKUs per
    subcategory dominate sales volume.
    """
    # Ensure popularity is computed (idempotent after first call)
    if not _PRODUCT_POPULARITY:
        _compute_popularity()

    available, weights = [], []
    weight_map = {
        "Fruits": 10,
        "Vegetables": 10,
        "Meat & Poultry": 7,
        "Fish & Seafood": 4,
        "Dairy & Eggs": 9,
        "Bakery & Pastry": 7,
        "Deli & Charcuterie": 4,
        "Beverages": 8,
        "Snacks & Confectionery": 7,
        "Frozen Food": 4,
        "Canned & Jarred": 4,
        "Cereals & Breakfast": 3,
        "Pasta, Rice & Grains": 4,
        "Condiments & Sauces": 4,
        "Cleaning Products": 3,
        "Laundry": 2,
        "Paper & Hygiene": 3,
        "Personal Care": 3,
        "Cosmetics": 1,
        "Vitamins & Supplements": 1,
        "Pharmacy OTC": 1,
        "Pet Supplies": 2,
        "Books & Stationery": 1,
        "Seasonal & Promotions": 2,
    }
    for p in PRODUCTS:
        seasonal = p[10]
        if seasonal is None or month in seasonal:
            available.append(p)
            base_w = weight_map.get(p[1], 2)
            pop = _PRODUCT_POPULARITY.get(p[2], 0.5)
            weights.append(base_w * pop)
    return available, weights


def pick_product(rng, month: int) -> tuple:
    available, weights = get_available_products(month)
    return rng.choices(available, weights=weights, k=1)[0]


if __name__ == "__main__":
    print(f"Total SKUs : {len(PRODUCTS):,}")
    from collections import Counter

    cats = Counter(p[1] for p in PRODUCTS)
    for cat, n in sorted(cats.items()):
        print(f"  {cat:<32} {n:>4} SKUs")
    jan, _ = get_available_products(1)
    aug, _ = get_available_products(8)
    print(f"\nJanuary-available : {len(jan):,}")
    print(f"August-available  : {len(aug):,}")

    # Show popularity distribution for a sample subcategory
    print("\n  Popularity sample (Fruits — top 10 vs bottom 10):")
    fruit_pops = sorted(
        [
            (name, pop)
            for name, pop in _PRODUCT_POPULARITY.items()
            if any(p[2] == name and p[1] == "Fruits" for p in PRODUCTS)
        ],
        key=lambda x: -x[1],
    )
    for name, pop in fruit_pops[:10]:
        print(f"    TOP  {pop:.3f}  {name}")
    print("    ...")
    for name, pop in fruit_pops[-5:]:
        print(f"    LOW  {pop:.3f}  {name}")
