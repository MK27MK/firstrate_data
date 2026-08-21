from collections.abc import Callable, Generator
from contextlib import AbstractContextManager, contextmanager
from itertools import count
from threading import Lock
from typing import Never, Protocol

from tqdm import tqdm

# report that n more units of the tracked work have finished
type Advance = Callable[[int], None]


class ProgressReporter(Protocol):
    """Where the loader reports progress to.

    One method serves every level. A byte bar over a single archive and a
    cell bar over an hours-long sweep differ only in their unit and their
    total.

    The loader reports what it's doing and how far along it's come. Whether
    that gets drawn, and how, isn't the loader's business, which keeps a
    library from writing to stderr on a whim.
    """

    def track(
        self,
        label: str,
        total: int | None,
        unit: str,
    ) -> AbstractContextManager[Advance]:
        """Track one piece of work during the context.

        ``total`` is None when the size isn't known up front -- a response that
        declares no Content-Length -- which means a bar without an ETA, not an
        error.
        """
        ...


class NullProgress:
    """Reports nothing. The default, so importing the loader never draws."""

    # label, total, and unit meet ProgressReporter.track's signature.
    # This implementation reports nothing.
    @contextmanager
    def track(self, label: str, total: int | None, unit: str) -> Generator[Advance]:  # noqa: ARG002
        yield lambda _: None


class TqdmProgress:
    """Nested tqdm bars: the sweep on top, the archives in flight beneath it.

    A bar claims its own line instead of having a caller assign one. A
    transfer can't tell whether it started inside a bundle sweep or alone,
    and tqdm needs ``position`` to keep two live bars off each other's line.

    It claims and releases lines instead of counting them. Archives in
    flight don't finish in the order they started, so a depth counter would
    hand the same line to two live bars once one outlived a later one.
    """

    def __init__(self) -> None:
        self._taken: set[int] = set()
        self._lock = Lock()

    def _claim(self) -> int:
        """Return the lowest line not currently claimed.

        Position 0 belongs to the sweep, which claims before any archive.
        """
        with self._lock:
            position = next(line for line in count() if line not in self._taken)
            self._taken.add(position)
            return position

    def _release(self, position: int) -> None:
        with self._lock:
            self._taken.discard(position)

    @contextmanager
    def track(self, label: str, total: int | None, unit: str) -> Generator[Advance]:
        position = self._claim()
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
            disable=None,
        )

        def advance(units: int) -> None:
            bar.update(units)

        try:
            yield advance
        finally:
            bar.close()
            self._release(position)
