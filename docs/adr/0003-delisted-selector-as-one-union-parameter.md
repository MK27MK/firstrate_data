# Delisted selector as one union parameter

`delisted_data_file` slices the delisted dataset with two parameters the docs both
call *optional*: `archive_number` (1–5, the pre-2026 history) and `update`
(`week` | `year`, the 2026+ history). Each one's description says it "does not need
to be used if the other is used" — so they are neither independent nor genuinely
optional: **exactly one applies**. The docs never define what passing both, or
neither, means.

Mirroring the API 1:1 would give
`download_delisted_historical_data(archive_number=None, update=None, ...)` plus a
runtime `ValueError` when the count of non-`None` arguments isn't 1 — a check that
exists only because the signature permits states the endpoint doesn't have. Instead
the two values are separate enums (`DelistedArchive`, `DelistedUpdate`) and the
method takes **one required union-typed argument**, `selector: DelistedArchive |
DelistedUpdate`. It dispatches on the runtime type to pick the query parameter name.
"Both" and "neither" become unwritable rather than merely rejected, and the caller
gets one obviously-required decision instead of two optional-looking ones.

Rejected: two methods (`download_delisted_archive` / `download_delisted_update`),
which also makes illegal states unrepresentable but splits one endpoint across two
methods, breaking the one-method-per-endpoint shape the rest of the loader keeps.

**The archive count is a guess.** The docs list accepted values `1, 2, 3, 4, 5` and,
one sentence later, call them "the four historical (pre-2026) archives". We follow
the accepted values and ship five, because the two errors are not symmetric: a fifth
archive that doesn't exist fails loudly on the first request, whereas omitting a
fifth that does exist silently costs a fifth of the delisted history — and
`download_stocks_complete` would report a clean run. Confirm against the live
endpoint once a real userid is on hand.

Consequences: the endpoint is stock-only (no other asset type's docs page has it) and
takes no `type` parameter, so the method lives on `FirstRateStocks`, not
`FirstRateEquities`, and builds its own params and target around `_fetch_archive` —
the same escape hatch `download_contracts` uses, since `_historical_data_query`
hardcodes `data_file`, `type`, and a `period` that delisted has no concept of.
