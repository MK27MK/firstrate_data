"""A stand-in FirstRate endpoint that can misbehave on purpose.

This module runs a real server rather than a mocked transport, because the
transport itself is what's under test. Resume, retry and concurrency are
agreements between a client and a server, and a mock at that seam would test
only the client's own assumptions. The server takes no credentials.
"""

import gzip
import threading
from collections.abc import Generator
from dataclasses import dataclass, field
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import ParseResult, parse_qs, urlparse


@dataclass
class Asked:
    """One request the vendor received."""

    endpoint: str
    params: dict[str, list[str]]
    range_header: str | None


@dataclass
class Vendor:
    """One endpoint, and the ways it can let a client down.

    Every knob here corresponds to something a real large-file download has to
    survive: a connection that dies partway, a server that ignores Range, and a
    transient 503.
    """

    payload: bytes

    # what ``meta_file`` serves, the endpoint answering with corporate actions
    # rather than bars. A bare headerless CSV, which the store also accepts.
    metafile: bytes = b"AAPL,2020-08-31,4.0\n"

    # the two endpoints that answer with a value rather than an archive
    last_update: bytes = b"2026-07-31"
    ticker_listing: bytes = (
        b"SPX,S&P 500 Index,2005-01-03,2026-07-31\n"
        b"NDX,Nasdaq 100 Index,2005-01-03,2026-07-31\n"
    )

    # whether a Range request draws 206 and the tail, or goes ignored for 200 and
    # the whole body -- a client must handle both
    honour_range: bool = True
    # serve the body gzip-encoded. Range then applies to the *encoded* entity,
    # as RFC 9110 says it does, while the client's stream decodes on the way in
    gzip_encoded: bool = False
    # cut the connection after this many bytes of body, for the next
    # `aborts_remaining` requests
    abort_after: int | None = None
    aborts_remaining: int = 0
    # answer 503 this many times before serving anything
    fails_remaining: int = 0
    # answer 404 to any request carrying these query parameters, so a test can
    # fail one named cell of a sweep while its siblings succeed
    refuse: dict[str, str] = field(default_factory=dict)
    # hold each request open this long, so concurrency is observable at all
    dwell: float = 0.0

    url: str = ""
    asked: list[Asked] = field(default_factory=list)
    in_flight: int = 0
    peak_in_flight: int = 0
    _lock: threading.Lock = field(default_factory=threading.Lock)

    def _enter(self) -> None:
        with self._lock:
            self.in_flight += 1
            self.peak_in_flight = max(self.peak_in_flight, self.in_flight)

    def _leave(self) -> None:
        with self._lock:
            self.in_flight -= 1

    def _claim_abort(self) -> int | None:
        """Take this request's turn to abort, and return the byte count to cut at."""
        with self._lock:
            if self.aborts_remaining <= 0 or self.abort_after is None:
                return None
            self.aborts_remaining -= 1
            return self.abort_after

    def _claim_failure(self) -> bool:
        with self._lock:
            if self.fails_remaining <= 0:
                return False
            self.fails_remaining -= 1
            return True

    def _body_for(self, path: str) -> bytes:
        """Pick the body one endpoint answers with, or the archive when unnamed."""
        for endpoint, body in (
            ("meta_file", self.metafile),
            ("last_update", self.last_update),
            ("ticker_listing", self.ticker_listing),
        ):
            if path.endswith(endpoint):
                return body
        return self.payload


def _refused(vendor: Vendor, requested: ParseResult) -> bool:
    asked_for = parse_qs(requested.query)
    return bool(vendor.refuse) and all(
        asked_for.get(key) == [value] for key, value in vendor.refuse.items()
    )


def _send_early_failure(
    handler: BaseHTTPRequestHandler,
    vendor: Vendor,
    requested: ParseResult,
) -> bool:
    """Send a 503 or 404 when this vendor owes this request a failure.

    Return True after sending such a response. The caller serves the body
    otherwise.
    """
    if vendor._claim_failure():
        _send_empty(handler, 503)
        return True
    if _refused(vendor, requested):
        _send_empty(handler, 404)
        return True
    return False


def _send_empty(handler: BaseHTTPRequestHandler, status: int) -> None:
    handler.send_response(status)
    handler.send_header("Content-Length", "0")
    handler.end_headers()


def _send_range_headers(
    handler: BaseHTTPRequestHandler,
    wanted: str | None,
    *,
    honour_range: bool,
    body: bytes,
) -> int | None:
    """Send the status and Content-Range headers, and return the offset into `body`.

    Return None when the requested range starts past the end of `body`. The
    caller still owes Content-Length and end_headers().
    """
    if not (wanted and honour_range):
        handler.send_response(200)
        return 0
    start = int(wanted.removeprefix("bytes=").rstrip("-"))
    if start >= len(body):
        handler.send_response(416)
        handler.send_header("Content-Range", f"bytes */{len(body)}")
        return None
    handler.send_response(206)
    handler.send_header("Content-Range", f"bytes {start}-{len(body) - 1}/{len(body)}")
    return start


def _serve(handler: BaseHTTPRequestHandler, vendor: Vendor) -> None:
    requested = urlparse(handler.path)
    wanted = handler.headers.get("Range")
    vendor.asked.append(
        Asked(requested.path.lstrip("/"), parse_qs(requested.query), wanted),
    )

    if _send_early_failure(handler, vendor, requested):
        return

    body = vendor._body_for(requested.path)
    if vendor.gzip_encoded:
        body = gzip.compress(body)
    start = _send_range_headers(
        handler, wanted, honour_range=vendor.honour_range, body=body
    )
    if start is None:
        handler.send_header("Content-Length", "0")
        handler.end_headers()
        return

    tail = body[start:]
    if vendor.gzip_encoded:
        handler.send_header("Content-Encoding", "gzip")
    handler.send_header("Content-Length", str(len(tail)))
    handler.end_headers()
    _write_body(handler, vendor, tail)


def _write_body(handler: BaseHTTPRequestHandler, vendor: Vendor, tail: bytes) -> None:
    if (cut := vendor._claim_abort()) is not None:
        # a declared length the body doesn't deliver: exactly what a dropped
        # connection looks like from the client's side
        handler.wfile.write(tail[:cut])
        handler.wfile.flush()
        handler.close_connection = True
        return

    if vendor.dwell:
        threading.Event().wait(vendor.dwell)
    handler.wfile.write(tail)


def _handler(vendor: Vendor) -> type[BaseHTTPRequestHandler]:
    class Handler(BaseHTTPRequestHandler):
        protocol_version = "HTTP/1.1"

        def do_GET(self) -> None:
            vendor._enter()
            try:
                _serve(self, vendor)
            finally:
                vendor._leave()

        def log_message(self, format: str, *args: object) -> None:  # noqa: A002 - overrides BaseHTTPRequestHandler.log_message, whose signature the standard library fixes
            """Stay quiet: a test suite isn't a request log."""

    return Handler


def serving(vendor: Vendor) -> Generator[Vendor]:
    """Run `vendor` on a loopback port until the generator closes."""
    http = ThreadingHTTPServer(("127.0.0.1", 0), _handler(vendor))
    threading.Thread(target=http.serve_forever, daemon=True).start()
    vendor.url = f"http://127.0.0.1:{http.server_port}"
    try:
        yield vendor
    finally:
        http.shutdown()
        http.server_close()
