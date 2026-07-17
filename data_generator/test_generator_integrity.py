from __future__ import annotations

import csv
import json
import subprocess
import sys
import tempfile
import unittest
from collections import Counter, defaultdict
from datetime import datetime
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


class GeneratorIntegrityTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.temp_dir = tempfile.TemporaryDirectory()
        cls.output = Path(cls.temp_dir.name) / "raw"
        subprocess.run(
            [
                sys.executable,
                str(ROOT / "incremental.py"),
                "--records", "8000",
                "--customers", "2500",
                "--start-date", "2025-01-01",
                "--end-date", "2025-02-28",
                "--generation-date", "2025-03-01",
                "--output-dir", str(cls.output),
                "--master-dir", str(ROOT),
            ],
            cwd=ROOT,
            check=True,
            stdout=subprocess.DEVNULL,
        )

        cls.rows: list[dict[str, str]] = []
        for path in sorted((cls.output / "batches").glob("batch_*.csv")):
            with path.open(encoding="utf-8") as handle:
                cls.rows.extend(csv.DictReader(handle))

        with (cls.output / "fact_returns.csv").open(encoding="utf-8") as handle:
            cls.returns = list(csv.DictReader(handle))

        with (cls.output / "dim_customers.csv").open(encoding="utf-8") as handle:
            cls.customers = list(csv.DictReader(handle))

        with (cls.output / "dim_stores.csv").open(encoding="utf-8") as handle:
            cls.stores = list(csv.DictReader(handle))

        with (cls.output / "dim_products_scd2.csv").open(encoding="utf-8") as handle:
            cls.prices = list(csv.DictReader(handle))

    @classmethod
    def tearDownClass(cls) -> None:
        cls.temp_dir.cleanup()

    def test_transaction_ids_have_one_business_context(self) -> None:
        contexts: dict[str, set[tuple[str, str, str, str]]] = defaultdict(set)
        for row in self.rows:
            contexts[row["transaction_id"]].add(
                (
                    row["basket_id"],
                    row["store_id"],
                    row["order_date"],
                    row["customer_id"] or "WALKIN",
                )
            )
        self.assertFalse([txn for txn, values in contexts.items() if len(values) > 1])

    def test_duplicate_retries_are_exact_record_copies(self) -> None:
        hash_counts = Counter(row["record_hash"] for row in self.rows)
        retry_rows = [
            row for row in self.rows
            if "INFO:DUPLICATE_TXN" in row["data_quality_flag"]
        ]
        self.assertTrue(retry_rows)
        self.assertTrue(all(hash_counts[row["record_hash"]] >= 2 for row in retry_rows))

    def test_loyalty_cards_are_unique(self) -> None:
        cards = [row["loyalty_card_id"] for row in self.customers if row["loyalty_card_id"]]
        self.assertEqual(len(cards), len(set(cards)))

    def test_product_attributes_are_stable(self) -> None:
        attrs: dict[str, set[tuple[str, str]]] = defaultdict(set)
        for row in self.rows:
            attrs[row["product_id"]].add((row["brand"], row["is_private_label"]))
        self.assertFalse([pid for pid, values in attrs.items() if len(values) > 1])

    def test_valid_prices_match_effective_scd2_price(self) -> None:
        price_index: dict[str, list[tuple[datetime, datetime, float]]] = defaultdict(list)
        for row in self.prices:
            price_index[row["product_id"]].append(
                (
                    datetime.strptime(row["effective_from"], "%Y-%m-%d"),
                    datetime.strptime(row["effective_to"], "%Y-%m-%d"),
                    float(row["effective_price_eur"]),
                )
            )

        for row in self.rows:
            if "PRICE_" in row["data_quality_flag"]:
                continue
            order_date = datetime.strptime(row["order_date"], "%Y-%m-%d")
            matches = [
                price for start, end, price in price_index[row["product_id"]]
                if start <= order_date <= end
            ]
            self.assertEqual(len(matches), 1)
            self.assertAlmostEqual(float(row["unit_price_eur"]), matches[0], places=2)

    def test_returns_are_unique_and_never_over_refund(self) -> None:
        ids = [row["return_id"] for row in self.returns]
        self.assertEqual(len(ids), len(set(ids)))
        for row in self.returns:
            maximum = float(row["net_unit_price_eur"]) * int(row["return_quantity"])
            self.assertLessEqual(float(row["refund_amount_eur"]), maximum + 0.011)

    def test_source_system_is_stable_by_store(self) -> None:
        expected = {row["store_id"]: row["source_system"] for row in self.stores}
        for row in self.rows:
            self.assertEqual(row["source_system"], expected[row["store_id"]])

    def test_transactions_respect_store_hours_and_sunday_closure(self) -> None:
        hours = {
            row["store_id"]: json.loads(row["opening_hours"])
            for row in self.stores
        }
        for row in self.rows:
            order_date = datetime.strptime(row["order_date"], "%Y-%m-%d")
            self.assertNotEqual(order_date.weekday(), 6)
            opening = hours[row["store_id"]][order_date.strftime("%A").lower()]
            self.assertNotEqual(opening, "closed")
            start, end = opening.split("-")
            self.assertGreaterEqual(row["order_time"], start)
            self.assertLess(row["order_time"], end)

    def test_sales_statuses_do_not_encode_returns(self) -> None:
        self.assertLessEqual({row["order_status"] for row in self.rows}, {"Completed", "Voided"})


if __name__ == "__main__":
    unittest.main()