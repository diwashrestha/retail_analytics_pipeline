"""
progress.py — Zero-dependency progress bar for the Einkaufpark pipeline.

No pip install required — writes a carriage-return line to stderr. If tqdm
happens to be installed, ProgressBar transparently uses it for a nicer bar;
otherwise it falls back to the built-in renderer.

Usage:
    from progress import ProgressBar

    bar = ProgressBar(total=1016, unit="days", label="Generating batches")
    for i, day in enumerate(days):
        ...do work...
        bar.update(1, extra=f"{rows_written:,} rows")
    bar.close()

The `extra` string is appended to the bar — use it for a live row count,
current date, or anything else worth surfacing.
"""

from __future__ import annotations

import sys
import time

# Optional acceleration — used only if already present, never installed.
try:
    from tqdm import tqdm as _tqdm

    _HAS_TQDM = True
except ImportError:
    _HAS_TQDM = False


def _fmt_duration(seconds: float) -> str:
    """Human-readable duration: 5s, 3m12s, 1h04m."""
    seconds = int(seconds)
    if seconds < 60:
        return f"{seconds}s"
    if seconds < 3600:
        return f"{seconds // 60}m{seconds % 60:02d}s"
    return f"{seconds // 3600}h{(seconds % 3600) // 60:02d}m"


class _BuiltinBar:
    """Fallback progress bar — pure stdlib, writes \\r lines to stderr."""

    def __init__(self, total: int, unit: str, label: str, width: int = 28):
        self.total = max(total, 1)
        self.unit = unit
        self.label = label
        self.width = width
        self.done = 0
        self.start = time.monotonic()
        self._last_render = 0.0

    def update(self, n: int = 1, extra: str = "") -> None:
        self.done += n
        now = time.monotonic()
        # Throttle redraws to ~10/sec so we don't flood the terminal.
        if now - self._last_render < 0.1 and self.done < self.total:
            return
        self._last_render = now
        self._render(extra)

    def _render(self, extra: str) -> None:
        frac = min(self.done / self.total, 1.0)
        filled = int(self.width * frac)
        bar = "█" * filled + "░" * (self.width - filled)
        elapsed = time.monotonic() - self.start
        rate = self.done / elapsed if elapsed > 0 else 0
        eta = (self.total - self.done) / rate if rate > 0 else 0

        line = (
            f"\r  {self.label}: |{bar}| "
            f"{self.done:,}/{self.total:,} {self.unit} "
            f"({frac * 100:5.1f}%) "
            f"elapsed {_fmt_duration(elapsed)} "
            f"eta {_fmt_duration(eta)}"
        )
        if extra:
            line += f" — {extra}"
        # Pad to clear any leftover characters from a longer previous line.
        sys.stderr.write(line.ljust(110)[:110])
        sys.stderr.flush()

    def close(self) -> None:
        self._render("done")
        sys.stderr.write("\n")
        sys.stderr.flush()


class _TqdmBar:
    """Thin wrapper so tqdm and the builtin bar share one interface."""

    def __init__(self, total: int, unit: str, label: str):
        self._bar = _tqdm(
            total=total,
            unit=f" {unit}",
            desc=f"  {label}",
            dynamic_ncols=True,
            file=sys.stderr,
        )

    def update(self, n: int = 1, extra: str = "") -> None:
        if extra:
            self._bar.set_postfix_str(extra, refresh=False)
        self._bar.update(n)

    def close(self) -> None:
        self._bar.close()


def ProgressBar(total: int, unit: str = "items", label: str = "Progress"):
    """Return a progress bar. Uses tqdm if available, builtin otherwise.

    Both implementations expose the same two methods: update(n, extra) and
    close(). The caller doesn't need to know which one it got.
    """
    if _HAS_TQDM:
        return _TqdmBar(total, unit, label)
    return _BuiltinBar(total, unit, label)
