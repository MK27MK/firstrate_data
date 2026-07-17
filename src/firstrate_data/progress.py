from collections.abc import Callable, Generator
from contextlib import AbstractContextManager, contextmanager
from typing import Never, Protocol

from tqdm import tqdm

# report that n more units of the tracked work are done
type Advance = Callable[[int], None]


class ProgressReporter(Protocol):
    """Where the loader reports progress to.

    One method rather than one per level: a byte bar over a single archive and a
    cell bar over an hours-long sweep differ only in their unit and their total.
    The loader says what it is doing and how far along it is; whether that gets
    drawn, and how, is not the loader's business -- which is also what keeps a
    library from writing to stderr because it felt like it.
    """

    def track(
        self, label: str, total: int | None, unit: str
    ) -> AbstractContextManager[Advance]:
        """Track one piece of work for the duration of the context.

        ``total`` is None when the size is not known up front -- a response that
        declares no Content-Length -- which means a bar without an ETA, not an
        error.
        """
        ...


class NullProgress:
    """Reports nothing. The default, so importing the loader never draws."""

    @contextmanager
    def track(self, label: str, total: int | None, unit: str) -> Generator[Advance]:
        yield lambda _: None


class TqdmProgress:
    """Nested tqdm bars: the sweep on top, the archive in flight beneath it.

    Depth decides which line a bar owns, rather than callers passing a position:
    ``_get`` cannot know whether it was called inside a bundle sweep or on its
    own, and tqdm needs ``position`` to keep two live bars off each other's line.
    """

    def __init__(self) -> None:
        self._depth = 0

    @contextmanager
    def track(self, label: str, total: int | None, unit: str) -> Generator[Advance]:
        # Never: this bar is driven by update() calls, never iterated over
        bar: tqdm[Never] = tqdm(
            total=total,
            desc=label,
            unit=unit,
            # bytes want to read as MB/GB against a 1024 divisor; cells are cells
            unit_scale=unit == "B",
            unit_divisor=1024,
            position=self._depth,
            # the outermost bar is the summary worth keeping; the archive bars
            # below it are scrollback noise once their archive has landed
            leave=self._depth == 0,
            dynamic_ncols=True,
            # None means "off when stderr is not a terminal": a bar redirected to
            # a log file is thousands of redraw frames nobody will read
            disable=None,
        )
        self._depth += 1

        def advance(units: int) -> None:
            bar.update(units)

        try:
            yield advance
        finally:
            self._depth -= 1
            bar.close()
