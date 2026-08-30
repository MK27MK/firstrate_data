from collections.abc import Callable, Generator
from contextlib import contextmanager
from itertools import count
from threading import Lock
from typing import Never

from tqdm import tqdm

# report that n more units of the tracked work have finished
type Advance = Callable[[int], None]

# Which terminal lines live bars hold, for the whole process. A transfer cannot
# tell whether it started inside a bundle sweep or alone, and tqdm needs
# `position` to keep two live bars off each other's line. Lines are claimed and
# released rather than counted: archives in flight do not finish in the order
# they started, so a depth counter would hand one line to two live bars.
_taken: set[int] = set()
_lock = Lock()
_silenced = False


def silence() -> None:
    """Draw no bars for the rest of the process."""
    global _silenced  # noqa: PLW0603 - one process-wide terminal, one switch.
    _silenced = True


def _claim() -> int:
    """Return the lowest line not currently claimed.

    Position 0 belongs to the sweep, which claims before any archive.
    """
    with _lock:
        position = next(line for line in count() if line not in _taken)
        _taken.add(position)
        return position


def _release(position: int) -> None:
    with _lock:
        _taken.discard(position)


@contextmanager
def track(label: str, total: int | None, unit: str) -> Generator[Advance]:
    """Draw one bar for one piece of work during the context.

    `total` is None when the size is not known up front -- a response that
    declares no Content-Length -- which means a bar without an ETA, not an
    error.
    """
    position = _claim()
    # Never: update() calls drive this bar. Nothing iterates over it.
    bar: tqdm[Never] = tqdm(
        total=total,
        desc=label,
        unit=unit,
        # bytes read best as MB/GB with a 1024 divisor. Cells stay cells.
        unit_scale=unit == "B",
        unit_divisor=1024,
        position=position,
        # The outermost bar is the summary worth keeping. The archive bars
        # below it are scrollback noise once their archive has landed.
        leave=position == 0,
        dynamic_ncols=True,
        # None means "off when stderr isn't a terminal": a bar redirected to
        # a log file is thousands of redraw frames nobody will read
        disable=True if _silenced else None,
    )

    def advance(units: int) -> None:
        bar.update(units)

    try:
        yield advance
    finally:
        bar.close()
        _release(position)
