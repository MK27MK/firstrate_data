"""The harness that decides the worker count has to be right about the shape of
a curve, so a test asserts its reading of one rather than eyeballing it.
"""

from pathlib import Path

from firstrate_data.scripts.benchmark import (
    MIB,
    Measurement,
    disk_write_throughput,
    main,
    plateau,
)


def measured(workers: int, megabytes_per_second: float) -> Measurement:
    """One row of a sweep, at a given rate."""
    seconds = 10.0
    return Measurement(
        workers=workers,
        archives=workers,
        downloaded=int(megabytes_per_second * MIB * seconds),
        seconds=seconds,
        reading=seconds * workers,
        writing=0.0,
    )


class TestReadingThePlateau:
    def test_it_names_the_cheapest_setting_that_reaches_the_top(self) -> None:
        """The fastest setting is often 1 percent over a much cheaper one, and a
        worker that buys 1 percent still costs a connection and a spool slot.
        """
        curve = [
            measured(1, 36.0),
            measured(2, 71.0),
            measured(4, 94.5),
            measured(6, 87.7),
            measured(8, 96.0),
        ]

        assert plateau(curve) == 4

    def test_a_curve_that_never_flattens_names_its_last_setting(self) -> None:
        curve = [measured(1, 10.0), measured(2, 20.0), measured(4, 40.0)]

        assert plateau(curve) == 4

    def test_an_empty_sweep_names_nothing(self) -> None:
        assert plateau([]) == 0


class TestTheSocketDiskSplit:
    def test_time_on_the_socket_is_reported_as_a_share_of_busy_time(self) -> None:
        """The number that says whether more workers could still help."""
        network_bound = Measurement(4, 4, 100, 10.0, reading=39.0, writing=1.0)
        disk_bound = Measurement(4, 4, 100, 10.0, reading=10.0, writing=30.0)

        assert network_bound.socket_share == 0.975
        assert disk_bound.socket_share == 0.25


def test_the_disk_probe_measures_the_directory_it_is_given(tmp_path: Path) -> None:
    rate = disk_write_throughput(tmp_path / "spool", 4 * MIB)

    assert rate > 0
    # the probe is the harness's own scratch and must not outlive it
    assert list((tmp_path / "spool").iterdir()) == []


def test_the_local_sweep_runs_end_to_end(tmp_path: Path) -> None:
    """A whole sweep, small enough for a test suite: the harness is the thing that
    has to work before any number it prints means anything.
    """
    exit_status = main(
        [
            "--target",
            "local",
            "--workers",
            "1,2",
            "--size-mib",
            "1",
            "--archives",
            "2",
            "--rate-mbps",
            "0",
            "--link-mbps",
            "0",
            "--spool-dir",
            str(tmp_path),
        ],
    )

    assert exit_status == 0
    # every archive it fetched, it also cleaned up
    assert list((tmp_path / "bench-spool").iterdir()) == []
