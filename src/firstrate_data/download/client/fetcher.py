import hashlib
import http
import json
import os
import threading
import time
import zipfile
from dataclasses import dataclass, field
from pathlib import Path

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from firstrate_data.download.progress import NullProgress, ProgressReporter
from firstrate_data.download.requests import Request

# 1 MiB: large enough that a multi-GB archive isn't paid for one syscall at a
# time, small enough that a progress bar still moves on a slow line
CHUNK_SIZE = 1 << 20

# a rate limit and the transient 5xx family. A 4xx is the request being wrong,
# and repeating it won't make it right.
_RETRYABLE_STATUS = (429, 500, 502, 503, 504)
_CLIENT_ERROR = http.HTTPStatus.BAD_REQUEST
_SERVER_ERROR = http.HTTPStatus.INTERNAL_SERVER_ERROR

# connecting succeeds fast or not at all. A body, though, arrives at whatever
# rate the link gives, so the read budget is per-chunk and generous
_TIMEOUT = (30, 120)

# a partial carries no proof of what it contains, so its filename marks it as
# such and it never looks like a finished archive to anything that lists the
# spool
_PARTIAL_SUFFIX = ".partial"


class IncompleteDownload(requests.RequestException):
    """The body that arrived was not the body that was promised."""


class _StalePartialError(Exception):
    """The bytes on disk cannot be built on, and nothing is wrong with the link.

    Not a transport failure: there is no point backing off before retrying, and
    it must not spend one of the attempts a genuinely flaky link needs.
    """


@dataclass(frozen=True, slots=True)
class Fetched:
    """One archive, on disk and verified. Carries no bytes: never resident."""

    path: Path
    size: int
    sha256: str
    seconds: float
    # >1 means the transfer was interrupted and picked back up
    attempts: int
    # how much of `size` came from a previous attempt rather than this one
    resumed_from: int
    # Where the transfer's time went, summed over attempts: blocked on the
    # socket, and blocked on the spool disk. Which of the two dominates is the
    # whole question when a download is not going as fast as the link should,
    # and it cannot be answered from outside this loop.
    reading: float = 0.0
    writing: float = 0.0


@dataclass(slots=True)
class _Meter:
    """Where a transfer's time goes, accumulated across its attempts.

    Mutated rather than returned, so an attempt that dies halfway is still
    charged for the time it spent.
    """

    reading: float = 0.0
    # spooling and hashing together: everything the transfer did that was not
    # waiting on the socket, which is the comparison the split exists to make
    writing: float = 0.0
    # the archive's hash, accumulated as it is written. Hashing here rather than
    # in a pass of its own is what keeps a 400 GB bundle from being read twice.
    digest: "hashlib._Hash" = field(default_factory=hashlib.sha256)


def _describe(request: Request) -> str:
    """Describe a request as one line of bar label.

    ``data_file stock/full/1min/adj_split/A``.
    """
    return f"{request.endpoint} {'/'.join(request.to_params().values())}"


def _spool_name(request: Request) -> str:
    """Build a filename that is a function of the request.

    Two processes asking for the same archive land on the same partial, so the
    second process can finish the work the first one started.

    """
    parts = [request.endpoint, *request.to_params().values()]
    safe = ("".join(c if c.isalnum() or c in "-." else "_" for c in p) for p in parts)
    return "_".join(safe)


def _fingerprint(response: requests.Response, total: int | None) -> dict[str, object]:
    """Identify the body a partial was cut from.

    The vendor builds these archives per request, so "the same URL" does not mean
    "the same bytes" across days. Appending today's response to yesterday's
    partial would splice two archives together, and a splice is not reliably
    caught by a zip's own structure -- so the partial records what it came from
    and refuses to grow from anything else.
    """
    return {
        "total": total,
        "etag": response.headers.get("ETag"),
        "last_modified": response.headers.get("Last-Modified"),
    }


class ArchiveFetcher:
    """Fetches archives to a spool directory, and small bodies straight to text.

    Safe to call from many threads.

    Each thread gets its own ``requests.Session`` -- Sessions are not thread-safe
    -- with a connection pool behind it, so the TCP connection is reused across
    the archives one worker fetches in sequence.
    """

    def __init__(  # noqa: PLR0913 - each parameter configures a distinct, unrelated concern.
        self,
        base_url: str,
        user_id: str,
        spool: Path,
        *,
        max_workers: int = 4,
        chunk_size: int = CHUNK_SIZE,
        attempts: int = 5,
        progress: ProgressReporter | None = None,
    ) -> None:
        self._base_url = base_url.rstrip("/")
        self._user_id = user_id
        self._spool = spool
        self._chunk_size = chunk_size
        self._attempts = attempts
        self._progress = NullProgress() if progress is None else progress

        self._local = threading.local()
        # every session this fetcher has handed out, so close() can reach the
        # ones living in other threads' locals
        self._sessions: list[requests.Session] = []
        self._sessions_lock = threading.Lock()
        self._pool_size = max_workers

        self._spool.mkdir(parents=True, exist_ok=True)

    # ------------------------------------------------------------------
    # public
    # ------------------------------------------------------------------

    @property
    def spool(self) -> Path:
        """Where archives land, and where an interrupted one waits to be resumed."""
        return self._spool

    def fetch(
        self,
        request: Request,
        label: str | None = None,
        name: str | None = None,
    ) -> Fetched:
        """Fetch one archive into the spool and return where it landed.

        Retries and resumes are the same code path: a transfer that died at byte
        N is a transfer that should ask for byte N onwards, whether it died from
        a reset connection or a 503.

        Parameters
        ----------
        request : Request
            What to ask for. The URL is rebuilt from this on every attempt rather
            than captured once, so a vendor that answers with a signed redirect
            gets to re-sign it each time instead of expiring mid-retry.
        label : str | None
            What to call this transfer on a progress bar. Defaults to a one-line
            description of the request.
        name : str | None
            Spool filename. Defaults to one derived from the request, which is
            what lets a re-run finish a partial the previous run left behind.

        Returns
        -------
        Fetched
            Where the archive is, how big it is, and what it hashes to. The
            archive itself is on disk and never in memory.

        """
        label = _describe(request) if label is None else label
        destination = self._spool / (name or _spool_name(request))
        partial = destination.with_name(destination.name + _PARTIAL_SUFFIX)
        started = time.monotonic()
        resumed_from = 0
        meter = _Meter()

        attempt = 0
        while attempt < self._attempts:
            attempt += 1
            try:
                resumed_from = self._attempt(request, partial, label, meter)
            except _StalePartialError:
                _discard(partial)
                # the bytes were the problem, not the link, so this costs a
                # request but not an attempt -- and it can happen at most once,
                # there being nothing left on disk to be stale the second time
                attempt -= 1
                continue
            except requests.HTTPError as refused:
                self._handle_http_error(refused, attempt, label)
                continue
            except (requests.RequestException, OSError) as interrupted:
                self._wait_or_give_up(attempt, label, interrupted)
                continue

            size = self._verified(partial, label)
            partial.replace(destination)
            _meta_path(partial).unlink(missing_ok=True)
            return Fetched(
                path=destination,
                size=size,
                sha256=meter.digest.hexdigest(),
                seconds=time.monotonic() - started,
                attempts=attempt,
                resumed_from=resumed_from,
                reading=meter.reading,
                writing=meter.writing,
            )

        msg = "unreachable: the loop either returns or raises"
        raise AssertionError(msg)

    def _handle_http_error(
        self,
        refused: requests.HTTPError,
        attempt: int,
        label: str,
    ) -> None:
        # a 4xx is the request being wrong, and asking again will not make it
        # right. The adapter has already retried the statuses that are worth
        # retrying, 429 among them, before we see this. `is not None`, not
        # truthiness: a Response is falsy exactly when it carries an error
        # status, which is every response reaching here
        status = refused.response.status_code if refused.response is not None else 0
        if _CLIENT_ERROR <= status < _SERVER_ERROR:
            raise refused
        self._wait_or_give_up(attempt, label, refused)

    def _wait_or_give_up(self, attempt: int, label: str, cause: Exception) -> None:
        if attempt == self._attempts:
            msg = f"{label}: gave up after {attempt} attempts"
            raise IncompleteDownload(msg) from cause
        # 2, 4, 8... capped: a link that just dropped a multi-GB transfer is
        # not helped by being asked again immediately
        time.sleep(min(2**attempt, 60))

    def read(self, request: Request) -> str:
        """Fetch one endpoint's body as text, without touching the spool.

        For the endpoints that answer with a value rather than an archive: a
        date, or a listing of a few thousand lines.

        Parameters
        ----------
        request : Request
            What to ask for.

        Returns
        -------
        str
            The body, decoded.

        Raises
        ------
        requests.HTTPError
            If the endpoint refused the request.

        """
        # no spool, no resume, no progress bar: a body this size either arrives
        # or is asked for again, and the session's own Retry already does that
        with self._session().get(
            f"{self._base_url}/{request.endpoint}",
            params={**request.to_params(), "userid": self._user_id},
            timeout=_TIMEOUT,
        ) as response:
            response.raise_for_status()
            return response.text

    def close(self) -> None:
        """Close every session this fetcher opened, in whichever thread opened it."""
        with self._sessions_lock:
            for session in self._sessions:
                session.close()
            self._sessions.clear()

    # ------------------------------------------------------------------
    # one attempt
    # ------------------------------------------------------------------

    def _attempt(
        self,
        request: Request,
        partial: Path,
        label: str,
        meter: _Meter,
    ) -> int:
        """Stream the body into `partial`, resuming it if the server allows.

        Returns how many bytes were already there and were kept. Raises whatever
        the transport raises; the caller decides whether to try again.
        """
        already, recorded = self._resumable(partial)
        headers = {"Range": f"bytes={already}-"} if already else {}

        with self._session().get(
            f"{self._base_url}/{request.endpoint}",
            params={**request.to_params(), "userid": self._user_id},
            headers=headers,
            timeout=_TIMEOUT,
            stream=True,
        ) as response:
            if response.status_code == http.HTTPStatus.REQUESTED_RANGE_NOT_SATISFIABLE:
                # we asked to start past the end of the body, so the partial is
                # longer than what is being served and cannot be part of it
                msg = f"{label}: offset {already} is past the end"
                raise _StalePartialError(msg)

            response.raise_for_status()

            keep, total, fingerprint = self._resume_plan(
                response,
                partial,
                already,
                recorded,
                label,
            )
            _write_meta(partial, fingerprint)

            # seeded from the bytes already on disk when resuming, so the hash is
            # of the whole archive rather than of this attempt's share of it
            meter.digest = _hashed_prefix(partial, keep)

            self._stream_body(response, partial, keep, total, meter, label)

            if total is not None and partial.stat().st_size != total:
                msg = f"{label}: {partial.stat().st_size} bytes of a declared {total}"
                raise IncompleteDownload(
                    msg,
                )

        return keep

    def _resume_plan(
        self,
        response: requests.Response,
        partial: Path,
        already: int,
        recorded: dict[str, object] | None,
        label: str,
    ) -> tuple[int, int | None, dict[str, object]]:
        """How many bytes of `partial` to keep, the body's total, and its fingerprint.

        Raises `_StalePartialError` if the bytes on disk cannot be trusted to be a
        prefix of the body this response is serving.
        """
        # a body the transport un-gzips for us is not the body on the wire, so
        # neither its length nor a byte offset into it means anything
        encoded = "Content-Encoding" in response.headers
        # A Range counts bytes of the *encoded* entity; what lands on disk is
        # what the stream decoded. The two offsets are not the same number,
        # and the tail of a gzip stream is not itself a gzip stream -- so a
        # resumed encoded body cannot be decoded at all and every retry fails
        # the same way. Restart instead.
        resumed = response.status_code == http.HTTPStatus.PARTIAL_CONTENT
        keep = already if resumed and not encoded else 0
        total = _declared_total(response, keep) if not encoded else None
        fingerprint = _fingerprint(response, total)

        if keep and recorded is not None and recorded != fingerprint:
            # the server honoured the Range, but into a different body than
            # the one these bytes were cut from -- appending would splice two
            # archives, and a splice is not reliably caught by a zip's own
            # structure, so it would reach the ingest looking plausible
            msg = f"{label}: partial predates the served body"
            raise _StalePartialError(msg)

        if already and not keep:
            # 200 to a Range request: the server served the whole body again.
            # Not an error -- these archives are generated per request, so a
            # server may have nothing to seek into.
            partial.unlink(missing_ok=True)

        return keep, total, fingerprint

    def _stream_body(  # noqa: PLR0913, PLR0917 - each argument is a distinct input to the copy.
        self,
        response: requests.Response,
        partial: Path,
        keep: int,
        total: int | None,
        meter: _Meter,
        label: str,
    ) -> None:
        with self._progress.track(label, total, "B") as advance:
            advance(keep)
            with partial.open("ab" if keep else "wb") as spooled:
                mark = time.monotonic()
                for chunk in response.iter_content(chunk_size=self._chunk_size):
                    arrived = time.monotonic()
                    meter.reading += arrived - mark
                    spooled.write(chunk)
                    meter.digest.update(chunk)
                    mark = time.monotonic()
                    meter.writing += mark - arrived
                    advance(len(chunk))
                # the bytes are ours to promise only once they are the
                # kernel's, not merely the buffer's
                spooled.flush()
                os.fsync(spooled.fileno())
                meter.writing += time.monotonic() - mark

    def _resumable(self, partial: Path) -> tuple[int, dict[str, object] | None]:
        """How many bytes of `partial` may be built on, and what they came from.

        None of them, unless the partial recorded its source: an unlabelled
        partial could be anything, and a resume is only sound when the bytes
        ahead of the offset are known to belong to the same body as the bytes
        behind it.
        """
        if not partial.exists():
            return 0, None

        recorded = _read_meta(partial)
        if recorded is None:
            _discard(partial)
            return 0, None

        return partial.stat().st_size, recorded

    # ------------------------------------------------------------------
    # verification
    # ------------------------------------------------------------------

    def _verified(self, partial: Path, label: str) -> int:
        """Return the partial's size, once it has been shown to be an archive."""
        size = partial.stat().st_size
        if size == 0:
            msg = f"{label}: empty body"
            raise IncompleteDownload(msg)

        # a metafile is allowed to arrive as a bare CSV, so "not a zip" is not by
        # itself a failure -- but a body that claims to be one must open
        if zipfile.is_zipfile(partial):
            try:
                # the central directory, not testzip(): reading the index catches
                # a truncated or spliced archive, and does not pay to decompress
                # tens of gigabytes to learn it
                with zipfile.ZipFile(partial) as opened:
                    if not opened.namelist():
                        msg = f"{label}: archive holds no entries"
                        raise IncompleteDownload(msg)
            except zipfile.BadZipFile as damaged:
                msg = f"{label}: {damaged}"
                raise IncompleteDownload(msg) from damaged

        return size

    # ------------------------------------------------------------------
    # sessions
    # ------------------------------------------------------------------

    def _session(self) -> requests.Session:
        session: requests.Session | None = getattr(self._local, "session", None)
        if session is not None:
            return session

        session = requests.Session()
        adapter = HTTPAdapter(
            # one host, one thread: a single pooled connection per thread is
            # exactly what a worker fetching archives in sequence reuses
            pool_connections=1,
            pool_maxsize=self._pool_size,
            max_retries=Retry(
                total=3,
                backoff_factor=1,
                status_forcelist=_RETRYABLE_STATUS,
                allowed_methods=frozenset({"GET"}),
                respect_retry_after_header=True,
            ),
        )
        session.mount("http://", adapter)
        session.mount("https://", adapter)

        self._local.session = session
        with self._sessions_lock:
            self._sessions.append(session)
        return session


# ----------------------------------------------------------------------
# the partial's record of where it came from
# ----------------------------------------------------------------------


def _hashed_prefix(partial: Path, keep: int) -> "hashlib._Hash":
    """Compute a sha256 of the first `keep` bytes of `partial`, to carry on updating.

    Only a resumed transfer pays this read, and it pays it against the local
    disk rather than against the link -- which is the trade the whole partial
    mechanism exists to make.
    """
    digest = hashlib.sha256()
    if not keep:
        return digest

    with partial.open("rb") as spooled:
        remaining = keep
        while remaining > 0 and (block := spooled.read(min(CHUNK_SIZE, remaining))):
            digest.update(block)
            remaining -= len(block)

    return digest


def _meta_path(partial: Path) -> Path:
    return partial.with_name(partial.name + ".meta")


def _discard(partial: Path) -> None:
    """Forget a partial entirely -- its bytes and its claim about their source."""
    partial.unlink(missing_ok=True)
    _meta_path(partial).unlink(missing_ok=True)


def _write_meta(partial: Path, fingerprint: dict[str, object]) -> None:
    _meta_path(partial).write_text(json.dumps(fingerprint), encoding="utf-8")


def _read_meta(partial: Path) -> dict[str, object] | None:
    meta = _meta_path(partial)
    if not meta.exists():
        return None
    try:
        return json.loads(meta.read_text(encoding="utf-8"))
    except (json.JSONDecodeError, UnicodeDecodeError):
        return None


def _declared_total(response: requests.Response, keep: int) -> int | None:
    """Return the full size of the body, however the response chose to say it.

    ``Content-Length`` is the remainder when the response is a 206, so a resumed
    transfer reads its total out of ``Content-Range`` instead. Absent from both
    means a chunked response: a bar without an ETA, not a failure.
    """
    if content_range := response.headers.get("Content-Range"):
        _, _, total = content_range.partition("/")
        if total.strip().isdigit():
            return int(total)

    declared = response.headers.get("Content-Length")
    return int(declared) + keep if declared is not None else None
