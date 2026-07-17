import json
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[2]
MASTER_DIR = PROJECT_ROOT / "master"


def _read_json(name: str) -> dict:
    with (MASTER_DIR / name).open(encoding="utf-8") as handle:
        return json.load(handle)


def test_store_master_count_and_keys_are_consistent():
    master = _read_json("store_master.json")
    stores = master["stores"]
    store_ids = [store["store_id"] for store in stores]

    assert len(stores) == master["total_stores"]
    assert len(store_ids) == len(set(store_ids))
    assert all(store["country_code"] == "DE" for store in stores)


def test_terminals_reference_known_stores_and_are_unique():
    stores = _read_json("store_master.json")["stores"]
    terminals = _read_json("terminal_master.json")["terminals"]
    store_ids = {store["store_id"] for store in stores}
    terminal_ids = [terminal["terminal_id"] for terminal in terminals]

    assert len(terminal_ids) == len(set(terminal_ids))
    assert {terminal["store_id"] for terminal in terminals}.issubset(store_ids)
    assert all(
        terminal["is_self_checkout"] == (terminal["terminal_type"] == "SELF_CHECKOUT")
        for terminal in terminals
    )


def test_raw_schema_has_unique_columns_and_required_line_item_keys():
    schema = _read_json("raw_schema.json")
    names = [column["name"] for column in schema["columns"]]

    assert len(names) == len(set(names))
    assert {
        "transaction_id",
        "basket_id",
        "record_hash",
        "order_date",
        "store_id",
        "product_id",
        "quantity",
        "net_revenue_eur",
    }.issubset(names)
