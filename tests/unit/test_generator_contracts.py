from pathlib import Path
from datetime import datetime
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
    validate_reset_path(
        Path(
            "/Volumes/workspace/"
            "retail_dev_raw/"
            "retail_input"
        )
    )

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