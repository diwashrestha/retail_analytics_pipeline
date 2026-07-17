"""Test import setup for the script-oriented generator modules."""

import sys
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[1]
GENERATOR_DIR = PROJECT_ROOT / "data_generator"

for path in (PROJECT_ROOT, GENERATOR_DIR):
    value = str(path)
    if value not in sys.path:
        sys.path.insert(0, value)
