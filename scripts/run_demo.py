"""Run the reproducible local demo from generation through validation."""

from __future__ import annotations

import argparse
import subprocess
import sys
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[1]


def run(command: list[str]) -> None:
    print(f"\n$ {' '.join(command)}", flush=True)
    subprocess.run(command, cwd=PROJECT_ROOT, check=True)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run the local retail analytics demo")
    parser.add_argument("--records", type=int, default=50_000)
    parser.add_argument("--customers", type=int, default=10_000)
    parser.add_argument("--seed", type=int, default=10)
    parser.add_argument("--start-date", default="2025-01-01")
    parser.add_argument("--end-date", default="2025-03-31")
    parser.add_argument("--skip-generate", action="store_true")
    parser.add_argument("--skip-tests", action="store_true")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    python = sys.executable

    if not args.skip_generate:
        run([
            python,
            "data_generator/incremental.py",
            "--records", str(args.records),
            "--customers", str(args.customers),
            "--seed", str(args.seed),
            "--start-date", args.start_date,
            "--end-date", args.end_date,
        ])

    for script in (
        "pipeline/bronze/ingest_raw.py",
        "pipeline/silver/transform.py",
        "pipeline/gold/aggregate.py",
    ):
        run([python, script])

    if not args.skip_tests:
        run([python, "-m", "pytest", "pipeline/test.py", "-q"])

    print("\nDemo complete. Power BI-ready exports are in data/powerbi/.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
