# Hive-partitioned Parquet, and vintages that are never merged

ADR 0004 said a catalog "naturally grows a read side later" and kept the store domain-aware
against that day. This is that day. `nautilus_trader`'s `ParquetDataCatalog` was evaluated
first, at v1.230.0, installed and measured rather than read: it writes OHLCV as
`fixed_size_binary[16]` (little-endian i128, scale in schema metadata), so DuckDB sees `BLOB`
and `avg(close)` does not resolve — the bytes are readable only through NT's Rust
deserializer. Its `Bar` path identifier is `str(bar_type)`, whose `PriceType` enum is closed,
so `adjustment` has nowhere to live but a forked symbol. Measured on 1M bars: 2.5x the disk,
58x slower to aggregate. It is a backtest feed, not an analytics engine. Rejected — neither
the dependency nor an upstream contribution.

So the read side is ours: `raw/` CSV ingested into Hive-partitioned Parquet, queried through
DuckDB, which is the one dependency this adds. It covers both halves — `read_csv` parses the
`.txt` and `COPY ... PARTITION_BY` writes the tree — so pyarrow buys nothing and stays out.

## The store is derived; `raw/` is the truth

`raw/` is never deleted and never rewritten in place. Parquet is a projection: `rm -rf` it and
`sync()` rebuilds it with no network. CSV→zstd Parquet runs 5-10x, so the second copy costs
~10-20% of an archive already in the hundreds of GB — cheap enough that irreversible parsing
is not worth buying. The manifest is likewise a cache, rebuildable by scanning.

That premise obliges two changes to `raw/`, because as ADR 0004 left it, it could not carry
the weight. First, `_unzip_and_write` wipes and replaces per key, and the key includes
`period` — so `raw/stock/week/...` holds only the newest week, and six months of ingested
increments would exist in Parquet and nowhere else. Rebuild would silently lose them and the
"derived" claim would be false. Second, nothing recorded *when* an archive was fetched, and
mtimes cannot stand in: `extractall` takes `.txt` mtimes from the zip's own `date_time`, so
they are the vendor's, while the directory's is ours — two semantics in one tree, and neither
survives a `cp`.

Both are fixed by putting the fetch date in the path. Every raw download now lands under a
vintage segment (`.../adj_split/A/2026-01-05/`), and retention is one number: keep one date
for `full`, keep every date for the increments. That asymmetry is honest — a `full` is
hundreds of GB and a week of 1min bars is a rounding error — and it is a policy over a uniform
layout, not a second code path. It also removes a real window: dated directories are written
beside the old one and the old one is dropped only on success, where today's staging swap
deletes `target` before `replace` lands.

`get_bars_path` therefore takes `(request, vintage)`. The vintage does not go on `BarsRequest`:
CONTEXT.md defines a request as the parameters identifying *a slice of the dataset*, and when
we asked identifies neither a slice nor the dataset. A request stays exactly the wire
contract. A required second argument is not ADR 0004's clobber returning — that defect was a
key computed from a *subset* by a method blind to a field; `--strict` cannot let this one be
omitted. A `_vintage.json` sidecar rides along, and earns its place not as the date (the path
has that) but as the record of *which request* produced a directory, so `sync()` reads
provenance instead of inverting a path with regexes.

## Vintages are replaced, never merged

The reframe that "period is overlapping vintages of the same bars" is right about the key and
wrong about the arithmetic. Adjusted prices are not append-only: every corporate action
rewrites all history before it. Append January's `adj_split` full to July's `week` across a
4:1 split and the seam carries an artificial 4x gap — silent, and indistinguishable from a
crash. A newer `full` therefore *replaces* an older one; it does not extend it.

So the rule is per-adjustment, which nothing before this stated. `UNADJUSTED` is append-only
and increments always apply. For `adj_split`/`adj_splitdiv` an increment applies only if the
splits/dividends metafile shows no action for that ticker since the partition's basis date;
otherwise the partition is stale and wants a fresh `full`. The metafiles stop being trivia and
become the guard. Deriving adjusted series from `UNADJUSTED` instead was considered and
rejected: `data_file` serves `UNADJUSTED` at `1min` and `1day` only (`delisted_data_file` at
`1min` only), so 5min/30min/1hour would have to be re-aggregated by us — and per the source's
own shape, intraday equity bars span 04:00-20:00 and omit zero-volume minutes, so that rollup
is not the vendor's bar and would not match.

Each vintage writes its own file: `FILENAME_PATTERN` is keyed on it. This is not tidiness.
Measured, `COPY ... PARTITION_BY (ticker), OVERWRITE_OR_IGNORE` reuses `data_0.parquet`, so an
increment overwrote a full and 100 rows became 10 with no error — the ticker_range clobber of
ADR 0004, reincarnated one layer down. Distinct names append correctly (110 rows) and leave
the vintage legible in the filename.

## The layout

```
{asset_type}/{dataset}/{adjustment}/{timeframe}/{ticker}/v{vintage}_{i}.parquet
```

`dataset` is `listed|delisted` for stocks, `continuous` for futures, and `contract` when
individual contracts arrive. It exists at that level because the depth must be uniform:
measured, a glob mixing a stock tree that has `delisted=` with a futures tree that does not
fails with `Binder Error: Hive partition mismatch`, and `union_by_name` does not rescue it.
It also settles what CONTEXT.md and the reframe disagreed about. Delisted data *is* a separate
dataset — its own endpoint, no period, no ticker_range — and it is *also* the same universe at
rest. A Hive key is a column that costs no bytes, so it is both: one glob answers without
survivorship bias by default, `WHERE dataset='listed'` prunes (measured: 1 file of 3), and a
ticker reused by a later company stays distinguishable from its dead namesake.

The download-side key evaporates here, as it should: `period` is a vintage, `ticker_range` is a
download partition, and the delisted selectors (`archive_number`, `update`) are slices of one
fetch. Individual contracts stay out of v1 — a contract has no adjustment, since adjustment
exists to erase roll jumps and a contract is the thing being stitched, and `contin_UNadj` is
still the continuous construction. Forcing `adjustment=none` would be NT's crime with our own
hands. `dataset=contract` is the seam when it is time.

Schema is uniform at seven columns: `ts, open, high, low, close, volume, open_interest`, the
last NULL wherever the source omits it. Divergent schemas are the alternative and they are a
trap — measured, a glob whose first file is a 6-column stock silently drops `open_interest`
(8 columns, 1M rows, no error) while a futures-first glob keeps it (9 columns), and
`sum(open_interest)` fails outright without `union_by_name`. The schema would depend on
filesystem enumeration order. Uniformity costs 0.043% (435 bytes per 500k rows under zstd);
correctness that hangs on every call site remembering a flag is the thing ADR 0004 exists to
refuse.

`ts` is stored exactly as delivered, naive. FirstRate's docs do not state the bars' timezone
and we have not verified it, and converting naive local time to UTC cannot resolve the
repeated hour at the DST fall-back — an hour equities sleep through but futures trade. Writing
a guess would corrupt silently and break the promise that Parquet is a faithful projection of
`raw/`. Any timezone is a view, with a policy its caller chooses.

## Reading

`sync()` is the whole update surface: it scans `raw/`, reads sidecars, diffs against the
manifest, and ingests what is missing in vintage order — full first, increments after, with
the splits guard. It is idempotent, so it is also the rebuild, and also the cure for a Ctrl-C.
It is what makes the ~400GB already on disk ingestable at all, which a download-time hook
never could. Coupling ingest into `download_*` would re-fuse the transport and the store that
ADR 0004 just separated, and would still need reconciliation after any interruption.

Reads return a lazy `DuckDBPyRelation` from `stock_bars(...)` / `futures_bars(...)`, plus
`bars()` across the tree. The selectors are typed and they *build the glob* — they are not a
`WHERE`. That is the measurement that shaped the whole API: at 3000 partitions a fine slice
costs 291ms through `**/*.parquet` + `WHERE ticker=...` and 0.3ms through a narrow glob, ~970x,
because pruning happens after enumeration and the tree will hold ~150k leaves. ADR 0004's
domain-aware catalog is now justified in reading, by measurement rather than anticipation: a
blind store cannot build the glob that makes fine-grained access fast.

A relation and not rows: rows would be `list[Bar]`, which is what we refused in NT, and 400GB
does not fit in a list. The caller gets the engine.

The asset type is in the method name, not a parameter, so `(FUTURES, EquitiesAdjustment.SPLIT)`
is unrepresentable — ADR 0001's move applied to methods, and ADR 0004 already accepted this
price in writing. On the read side the nonsense pairing is worse than ADR 0002's server-side
error: no server sees it, the glob simply misses, and an empty relation reads as "no data".
`@overload` on `Literal[AssetType.STOCK]` was tried and typechecks the nonsense away, but it
also rejects a legitimate call passing an `AssetType` variable — and the cure is a
`# type: ignore`, which in this codebase is the signal that a design drifted.

## Consequences

The manifest is one row per partition-vintage (keys, basis date, coverage, row count),
rewritten wholesale, holding nothing that a scan could not recover — its job is that a scan
costs 291ms and `sync()` should not pay it per call. `duckdb` joins the core dependencies;
`nautilus_trader` stays an extra, for a `to_nautilus()` that is a conversion and never a
re-point. The archive already on disk needs a one-off migration — rename each raw directory
under a date derived from its mtime and write its sidecar — before the first `sync()`; mtime
is weak evidence, which is exactly why it is a one-time, inspectable step and not the design.
`Catalog` stays one class, since CONTEXT.md already says the store is the one thing that knows
its layout; if the raw side and the query side stop sharing a reason to change, `RawStore` is
the seam.

Untested claims, deliberately: that FirstRate's adjusted history is rewritten by corporate
actions is how adjusted data works everywhere, but it is *falsifiable here* by pulling two
fulls across a known split and diffing, and the whole vintage rule stands on it. The bar
timezone is unverified. `UNADJUSTED`'s legal timeframes differ per endpoint and cannot live on
the enum, so a read asking for `5min UNADJUSTED` returns an empty relation rather than an
error — a per-method guard, as in writing.
