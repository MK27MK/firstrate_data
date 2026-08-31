from collections.abc import Callable, Generator
from contextlib import contextmanager
from typing import Never

from tqdm import tqdm

# report that n more units of the tracked work have finished
type Advance = Callable[[int], None]


@contextmanager
def track(label: str, total: int | None, unit: str) -> Generator[Advance]:
    """Draw one bar for one piece of work during the context.

    `total` is None when the size is not known up front -- a response that
    declares no Content-Length -- which means a bar without an ETA, not an
    error.
    """
    # Never: update() calls drive this bar. Nothing iterates over it.
    bar: tqdm[Never] = tqdm(
        total=total,
        desc=label,
        unit=unit,
        # bytes read best as MB/GB with a 1024 divisor. Cells stay cells.
        unit_scale=unit == "B",
        unit_divisor=1024,
        dynamic_ncols=True,
        # None means "off when stderr isn't a terminal": a bar redirected to
        # a log file is thousands of redraw frames nobody will read
        disable=None,
    )

    def advance(units: int) -> None:
        bar.update(units)

    try:
        yield advance
    finally:
        bar.close()
