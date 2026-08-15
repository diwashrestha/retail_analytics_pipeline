from datetime import datetime
from pathlib import Path
from random import Random

import pytest

from data_generator.incremental import (
    compute_daily_volumes,
    date_ranges_overlap,
    validate_reset_path,
)


def test_non_overlapping_periods():
    from datetime import datetime

    assert not date_ranges_overlap(
        datetime(2026, 4, 1),
        datetime(2026, 4, 3),
        datetime(2026, 4, 4),
        datetime(2026, 4, 6),
    )


def test_overlapping_periods():
    from datetime import datetime

    assert date_ranges_overlap(
        datetime(2026, 4, 1),
        datetime(2026, 4, 3),
        datetime(2026, 4, 3),
        datetime(2026, 4, 5),
    )


@pytest.mark.parametrize(
    "path",
    [
        "/",
        "/Volumes",
        "/Workspace",
        "/dbfs",
    ],
)
def test_reset_rejects_dangerous_paths(path):
    with pytest.raises(ValueError):
        validate_reset_path(Path(path))


def test_reset_accepts_project_raw_directory():
    validate_reset_path(Path("data/raw"))


def test_reset_accepts_volume():
    validate_reset_path(Path("/Volumes/workspace/retail_dev_raw/retail_input"))


def test_demo_starting_on_sunday_uses_next_trading_day() -> None:
    volumes = compute_daily_volumes(
        datetime(2023, 1, 1),  # Sunday
        datetime(2023, 1, 3),
        100,
        Random(42),
    )

    dates = sorted(volumes)

    assert datetime(2023, 1, 1) not in dates
    assert dates[0] == datetime(2023, 1, 2)


def test_daily_volume_correction_preserves_final_trading_day() -> None:
    from datetime import datetime

    from data_generator.incremental import (
        compute_daily_volumes,
        get_rng,
    )

    rng = get_rng(
        42,
        "volumes:demo:2023-01-01:2026-03-31:100000",
    )

    volumes = compute_daily_volumes(
        datetime(2023, 1, 1),
        datetime(2026, 3, 31),
        100_000,
        rng,
    )

    assert sum(volumes.values()) == 100_000

    assert datetime(2026, 3, 31) in volumes

    assert volumes[datetime(2026, 3, 31)] > 0
