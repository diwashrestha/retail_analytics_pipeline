from datetime import datetime, timedelta
from random import Random

from generator import make_id, product_id_for, record_hash
from incremental import (
    LATE_ARRIVAL_FLAG,
    compute_daily_volumes,
    normalize_returns_inplace,
    schedule_late_arrival,
)
from price_history import PROMO_DURATION, build_intervals, pick_change_dates


def test_identifiers_are_reproducible():
    assert make_id(Random(10)) == make_id(Random(10))
    assert product_id_for("Test Product") == product_id_for("Test Product")
    assert record_hash("a", 1, None) == record_hash("a", 1, None)


def test_daily_volumes_reconcile_and_skip_sundays():
    volumes = compute_daily_volumes(
        datetime(2025, 1, 1), datetime(2025, 2, 28), 10_000, Random(10)
    )

    assert sum(volumes.values()) == 10_000
    assert volumes
    assert all(day.weekday() != 6 for day in volumes)
    assert all(volume > 0 for volume in volumes.values())


class _OneDayDelay:
    @staticmethod
    def choices(*_args, **_kwargs):
        return [1]


def test_late_arrival_moves_sunday_delivery_to_monday():
    basket = [{"ingestion_date": "2025-01-04", "data_quality_flag": "OK"}]

    delivery = schedule_late_arrival(
        basket,
        current_date=datetime(2025, 1, 4),
        end=datetime(2025, 1, 31),
        rng=_OneDayDelay(),
    )

    assert delivery == datetime(2025, 1, 6)
    assert basket[0]["ingestion_date"] == "2025-01-06"
    assert basket[0]["data_quality_flag"] == LATE_ARRIVAL_FLAG


def test_return_normalization_restores_positive_values():
    basket = [{"quantity": -2, "net_revenue_eur": -7.5, "order_status": "Returned"}]

    normalize_returns_inplace(basket)

    assert basket == [{
        "quantity": 2,
        "net_revenue_eur": 7.5,
        "order_status": "Completed",
    }]


def test_price_change_dates_respect_minimum_spacing():
    dates = pick_change_dates(
        Random(10), datetime(2025, 1, 1), datetime(2025, 12, 31), 12
    )

    assert all((current - previous).days >= 8 for previous, current in zip(dates, dates[1:]))


def test_scd2_intervals_are_contiguous_and_bounded():
    start = datetime(2025, 1, 1)
    end = datetime(2025, 12, 31)
    intervals = build_intervals(Random(10), 2.0, 5.0, start, end)

    assert intervals[0][0] == start
    assert intervals[-1][1] == end

    for previous, current in zip(intervals, intervals[1:]):
        assert current[0] == previous[1] + timedelta(days=1)

    for interval_start, interval_end, price, is_promo in intervals:
        assert interval_start <= interval_end
        assert price > 0
        if is_promo:
            assert (interval_end - interval_start).days + 1 <= PROMO_DURATION
