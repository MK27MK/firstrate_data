"""The connection settings a store-sized ingest depends on.

Defaults that are fine for a scratch query are not fine for a COPY over
hundreds of gigabytes, and getting them wrong fails in ways that look like
something else -- a full boot disk, or an OOM three hours in.
"""

from pathlib import Path

from firstrate_data.store.store import Store


class TestSpillConfiguration:
    def test_spills_inside_the_store_not_the_boot_disk(self, tmp_path: Path) -> None:
        """DuckDB's default temp dir is ``.tmp`` in the working directory.

        The store is on an external disk with room for it; the machine running
        this generally is not, so an unset temp directory means an ingest large
        enough to spill takes the boot disk down with it.
        """
        store = Store(tmp_path)

        setting = store._connection.sql(
            "SELECT current_setting('temp_directory')",
        ).fetchone()

        assert setting is not None
        assert Path(setting[0]).is_relative_to(tmp_path)

    def test_does_not_preserve_insertion_order(self, tmp_path: Path) -> None:
        """Row order across a partitioned COPY is not something the store promises.

        Holding it costs memory proportional to the input, which is the
        difference between an ingest that spills politely and one that dies.
        """
        store = Store(tmp_path)

        setting = store._connection.sql(
            "SELECT current_setting('preserve_insertion_order')",
        ).fetchone()

        assert setting is not None
        assert setting[0] is False

    def test_the_store_directory_does_not_have_to_exist_yet(
        self,
        tmp_path: Path,
    ) -> None:
        absent = tmp_path / "not-yet"

        store = Store(absent)

        assert store._connection.sql("SELECT 1").fetchone() == (1,)
