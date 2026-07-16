# Requests as objects, and a Catalog that understands them

Every helper between `download_*` and the network re-listed the same four or five
parameters and passed them on — `download_historical_bars` → `_fetch_and_persist_historical_bars`
→ `_get` and `write_raw_bars`, each naming `period`, `timeframe`, `adjustment`,
`ticker_range` again. The drilling was not merely noise; two defects fell out of it.

The first is the ticker_range clobber. `get_bars_path` computed the store key from four
parameters and `write_raw_bars` appended the fifth, `ticker_range`, afterwards: the key was
split across two methods, one of them blind to a field. So every letter of the full archive
resolved to one folder and each range's download deleted the one before it. Commit `c4ac9dd`
passed the field through, which fixed the instance and left the shape. The second: `_get`
took `endpoint: str`, a free-form string, and two of its four call sites had the wrong one —
delisted bars asked `data_file` for a dataset that lives at `delisted_data_file`, and futures
contracts asked `meta_file` instead of `futures_contract`. Both typechecked. Both were wrong
only against the docs, which is the worst place for a bug to be discoverable.

So a request is now an object: a frozen dataclass holding its own fields, its `endpoint` as a
`ClassVar`, and a `to_params()` that renders its own query string. `_get` takes the request
instead of a string and a dict, which makes the second class of bug unrepresentable rather
than fixed — the same move as ADR 0003, one layer up. `BarsRequest` is generic on the
adjustment for the reason the base is (ADR 0002), so the equities/futures split survives all
the way down to the transport rather than dissolving into `dict[str, str]` at the first
private call.

The `Catalog` takes those requests and **stays domain-aware**: it imports the query-parameter
enums and keeps one `write_raw_*` and one `get_*_path` per request shape. The alternative was
a blind catalog — `write(bytes, key: tuple[str, ...])`, each loader building its own key —
which severs the dependency on the enums entirely and collapses four write methods into one.
That was reopened once and rejected on the second pass too: a catalog naturally grows a read
side later (cf. Nautilus's `ParquetDataCatalog`), and "where does a delisted archive live" is
a question the store should answer once, not one every caller answers for itself. The accepted
cost is real — the enums stay imported here, and a new endpoint touches two files. The gain is
that the key is derived in one place from the whole request, so the clobber shape cannot
recur: `get_bars_path(request)` sees `ticker_range` because it sees everything.

Requests are **internal**. Public `download_*` keeps its loose, per-asset-type parameters:
ADR 0002 exists precisely to make those signatures differ where the API differs, and handing
callers a request object to populate would undo it. The drilling this removes was always in
the private chain.

Consequences: `DelistedRequest.kind` decides "archive or update" once, and both the wire
parameter name (`archive_number` vs `update`) and the store's path segment read it — the same
`isinstance` XOR previously appeared in `catalog.py` and `stock.py`, free to drift. The
`_fetch_and_persist_*` helpers survive only where reuse is real: bars (both loaders) and
metafiles (three callers across two classes). Contracts and delisted bars build a request,
call `_get`, and call the catalog inline — a private helper with one caller that only
forwards its arguments is the ceremony this decision exists to remove.
