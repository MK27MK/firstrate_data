"""The compression the vendor actually ships its large archives in.

The delisted 1min archive arrived as 1550 Deflate64 entries beside 2 stored
ones, and CPython's ``zipfile`` decodes neither Deflate64 nor anything else it
was not built for. It cost a 3.3 GiB download to find out, because the method is
recorded per entry and not in the central directory: every check the transport
makes passes, and the archive is only found to be unreadable once the ingest
opens an entry.

So what these pin is the ingest reading such an archive at all, and the
``zipfile`` patch that lets it staying invisible to everything else.
"""

import binascii
import io
import zipfile

import inflate64
import pytest

from firstrate_data.store.store import Store, _deflate64
from tests.conftest import BARS, Spool, bars_archive, listed_request, payload_name


def deflate64_archive(payloads: dict[str, str]) -> bytes:
    """Build a vendor archive compressed with Deflate64 and declared as such.

    Entries are written through the raw handle rather than with ``writestr``,
    which re-compresses with deflate -- the stdlib can no more produce this
    archive than read one.
    """
    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, "w") as zipped:
        assert zipped.fp is not None
        for name, text in payloads.items():
            raw = text.encode()
            deflater = inflate64.Deflater()
            compressed = deflater.deflate(raw) + deflater.flush()

            info = zipfile.ZipInfo(name)
            info.compress_type = _deflate64.DEFLATE64
            info.file_size = len(raw)
            info.compress_size = len(compressed)
            info.CRC = binascii.crc32(raw) & 0xFFFFFFFF
            info.header_offset = zipped.fp.tell()

            zipped.fp.write(info.FileHeader())
            zipped.fp.write(compressed)
            zipped.filelist.append(info)
            zipped.NameToInfo[info.filename] = info
            zipped.start_dir = zipped.fp.tell()
    return buffer.getvalue()


class TestTheVendorsCompression:
    def test_the_stdlib_alone_cannot_read_it(self) -> None:
        """Pin the failure this module exists to remove.

        A stdlib that grows a Deflate64 decoder shows up here as a redundant
        patch rather than going unnoticed.
        """
        with pytest.raises(NotImplementedError):
            zipfile._check_compression(_deflate64.DEFLATE64)  # type: ignore[attr-defined]

    def test_the_archive_really_is_deflate64(self) -> None:
        """Guards the fixture, not the code: an archive that quietly fell back
        to deflate would let every test below pass without the patch.

        """
        archive = deflate64_archive({payload_name("AAPL"): BARS})

        with zipfile.ZipFile(io.BytesIO(archive)) as opened:
            methods = {entry.compress_type for entry in opened.infolist()}

        assert methods == {_deflate64.DEFLATE64}

    def test_the_ingest_reads_it(self, store: Store, spool: Spool) -> None:
        archive = deflate64_archive(
            {payload_name("AAPL"): BARS, payload_name("AMZN"): BARS},
        )

        ingested = store.ingest_bars(spool(archive), listed_request())

        assert ingested.tickers == 2
        assert ingested.rows == 6
        assert ingested.rejected == 0

    def test_deflate_archives_still_read(self, store: Store, spool: Spool) -> None:
        """The patch adds a method and changes none, which is what makes it safe
        to install globally and never uninstall.

        """
        ingested = store.ingest_bars(spool(bars_archive("AAPL")), listed_request())

        assert ingested.tickers == 1
        assert ingested.rows == 3


class TestInstallingIsRepeatable:
    def test_installing_twice_leaves_one_layer(self) -> None:
        """Called per unzip, so a wrapper stacked per call would grow a chain as
        long as the sweep.

        """
        _deflate64.install()
        once = zipfile._get_decompressor  # type: ignore[attr-defined]
        _deflate64.install()

        assert zipfile._get_decompressor is once  # type: ignore[attr-defined]

    def test_the_stock_methods_are_untouched(self) -> None:
        _deflate64.install()

        assert zipfile._get_decompressor(zipfile.ZIP_STORED) is None  # type: ignore[attr-defined]
        assert hasattr(
            zipfile._get_decompressor(zipfile.ZIP_DEFLATED),  # type: ignore[attr-defined]
            "unconsumed_tail",
        )
        with pytest.raises(NotImplementedError):
            zipfile._get_decompressor(99)  # type: ignore[attr-defined]
