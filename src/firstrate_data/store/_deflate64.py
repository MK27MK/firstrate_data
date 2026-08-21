"""Deflate64 archives: the compression the vendor uses and ``zipfile`` refuses.

FirstRate builds its larger archives with Deflate64 (method 9). It carries 1550
of the 1552 entries in the 3.3 GiB delisted 1min archive, while every archive
small enough for the ingest to swallow uses method 8, deflate. CPython ships no
Deflate64 decoder, so ``extractall`` raises ``NotImplementedError: That
compression method is not supported`` on an archive that's otherwise sound.

The failure lands at ingest rather than at download, which earns Deflate64 a
module of its own. The method lives in each entry's header, not in the central
directory, so an archive streams, verifies, and hashes cleanly. Only when the
ingest opens an entry does the archive turn out to be unreadable, long after
the fetch spent its bandwidth.

See
``docs/notes/firstrate-api/large-archives-are-deflate64-which-python-cannot-read.md``.
"""

import zipfile

import inflate64

# what the vendor writes and ``zipfile.compressor_names`` already knows how to
# name, having only ever declined to decode it
DEFLATE64 = 9


class Deflate64Decompressor:
    """An inflate64 decoder in the shape ``ZipExtFile`` drives.

    ``zipfile`` special-cases the two methods it streams incrementally and sends
    everything else down one generic path: ``decompress(data)`` per read, and
    ``eof`` to know when to stop. That's the whole contract -- no
    ``unconsumed_tail``, no ``flush`` -- which is why this is a shim rather than
    a decompressor.
    """

    def __init__(self) -> None:
        self._inflater = inflate64.Inflater()

    def decompress(self, data: bytes) -> bytes:
        return self._inflater.inflate(data)

    @property
    def eof(self) -> bool:
        return self._inflater.eof


def install() -> None:
    """Teach ``zipfile`` to read Deflate64, for every caller in the process.

    Idempotent, and safe to call from anywhere: it only ever adds a method that
    ``zipfile`` refuses to decode, so every call that already works keeps its
    behavior.

    Global because the alternative isn't: ``ZipFile.extractall`` opens an
    archive, builds its own ``ZipExtFile`` per entry, and takes no decompressor
    argument, so no seam exists to pass one through.
    """
    stock = zipfile._get_decompressor  # type: ignore[attr-defined]
    if getattr(stock, "_reads_deflate64", False):
        return

    def _get_decompressor(compress_type: int) -> object:
        if compress_type == DEFLATE64:
            return Deflate64Decompressor()
        return stock(compress_type)

    _get_decompressor._reads_deflate64 = True  # type: ignore[attr-defined]
    zipfile._get_decompressor = _get_decompressor  # type: ignore[attr-defined]
