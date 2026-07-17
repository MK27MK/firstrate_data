# Hive-partitioned Parquet, and snapshots that are never merged

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

Both are fixed by putting the fetch date in the path. Every raw download now lands as a dated
snapshot (`.../adj_split/A/2026-01-05/`), and retention is one number: keep one date for
`full`, keep every date for the increments. That asymmetry is honest — a `full` is
hundreds of GB and a week of 1min bars is a rounding error — and it is a policy over a uniform
layout, not a second code path. It also removes a real window: dated directories are written
beside the old one and the old one is dropped only on success, where today's staging swap
deletes `target` before `replace` lands.

`get_bars_path` therefore takes `(request, snapshot_date)`. The date does not go on
`BarsRequest`: CONTEXT.md defines a request as the parameters identifying *a slice of the
dataset*, and when we asked identifies neither a slice nor the dataset. A request stays exactly
the wire contract. A required second argument is not ADR 0004's clobber returning — that
defect was a key computed from a *subset* by a method blind to a field; `--strict` cannot let
this one be omitted. A `_snapshot.json` record rides along in the directory, and earns its
place not as the date (the path has that) but as the record of *which request* produced the
directory, so `sync()` reads provenance instead of inverting a path with regexes.

## Snapshots are replaced, never merged

The reframe that "period is overlapping snapshots of the same bars" is right about the key and
wrong about the arithmetic. Adjusted prices are not append-only: every corporate action
rewrites all history before it. Append January's `adj_split` full to July's `week` across a
4:1 split and the seam carries an artificial 4x gap — silent, and indistinguishable from a
crash. A newer `full` therefore *replaces* an older one; it does not extend it.

So the rule is per-adjustment, which nothing before this stated. `UNADJUSTED` is append-only
and increments always apply. For `adj_split`/`adj_splitdiv` an increment applies only if the
splits/dividends metafile shows no action for that ticker since the partition's basis date;
otherwise the partition is stale and wants a fresh `full`. The metafiles stop being trivia and
become the guard. (Measured since, and the guard does not survive it: the metafile leads the
restatement by an unpredictable per-ticker interval, so it cannot time the refetch. See "The
restatement lags the metafile" below — the snapshot rule stands, this guard does not.)
Deriving adjusted series from `UNADJUSTED` instead was considered and
rejected: `data_file` serves `UNADJUSTED` at `1min` and `1day` only (`delisted_data_file` at
`1min` only), so 5min/30min/1hour would have to be re-aggregated by us — and per the source's
own shape, intraday equity bars span 04:00-20:00 and omit zero-volume minutes, so that rollup
is not the vendor's bar and would not match.

Each snapshot writes its own file: `FILENAME_PATTERN` is keyed on its date. This is not
tidiness. Measured, `COPY ... PARTITION_BY (ticker), OVERWRITE_OR_IGNORE` reuses
`data_0.parquet`, so an increment overwrote a full and 100 rows became 10 with no error — the
ticker_range clobber of ADR 0004, reincarnated one layer down. Distinct names append correctly
(110 rows) and leave the snapshot date legible in the filename.

## The layout

```
{asset_type}/{dataset}/{adjustment}/{timeframe}/{ticker}/{snapshot_date}_{i}.parquet
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

The download-side key evaporates here, as it should: `period` names a snapshot's scope,
`ticker_range` is a download partition, and the delisted selectors (`archive_number`,
`update`) are slices of one
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

`sync()` is the whole update surface: it scans `raw/`, reads the snapshot records, diffs
against the manifest, and ingests what is missing in date order — full first, increments
after, with the splits guard. It is idempotent, so it is also the rebuild, and also the cure
for a Ctrl-C.
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

The manifest is one row per partition-snapshot (keys, basis date, coverage, row count),
rewritten wholesale, holding nothing that a scan could not recover — its job is that a scan
costs 291ms and `sync()` should not pay it per call. `duckdb` joins the core dependencies;
`nautilus_trader` stays an extra, for a `to_nautilus()` that is a conversion and never a
re-point. The archive already on disk needs a one-off migration — rename each raw directory
under a date derived from its mtime and write its snapshot record — before the first
`sync()`; mtime
is weak evidence, which is exactly why it is a one-time, inspectable step and not the design.
`Catalog` stays one class, since CONTEXT.md already says the store is the one thing that knows
its layout; if the raw side and the query side stop sharing a reason to change, `RawStore` is
the seam.

Still untested: the bar timezone. `UNADJUSTED`'s legal timeframes differ per endpoint and
cannot live on the enum, so a read asking for `5min UNADJUSTED` returns an empty relation
rather than an error — a per-method guard, as in writing.

## The snapshot assumption, tested

The claim the whole snapshot rule stands on — that FirstRate rewrites adjusted history when a
corporate action lands — was recorded above as deliberately untested. It has now been tested.
It holds, and the test found a second thing that the splits guard above does not survive
unamended.

The intended test was to diff a fresh `full` against an older one on disk. That was not
available: `DATA_PATH` holds no archive, so there is no older snapshot to diff against. The
substitute needs no archive and is stronger, because it reads the restatement out of a single
download: an adjusted bar whose price encodes an action dated *after* that bar is a bar that
cannot have carried the same price before the action. All figures below are one download,
`period=full`, `timeframe=1day`, `ticker_range=X`, fetched 2026-07-17 into a temp dir.

`XAIR` has two splits on record, `2025-07-14` and `2026-07-13`, each `0.05` — 1:20 reverse,
twice. Across its 1800 bars the ratio `adj_split/UNADJUSTED` takes exactly three values and
steps exactly twice, on precisely those two dates and nowhere else: 400 before `2025-07-14`, 20
until `2026-07-13`, 1 after. The bar for `2019-05-08`, seven years before the later split,
reads `5.875` unadjusted and `2350.0` adjusted — a factor of 400, which is 20 × 20. Twenty of
that came from a split effective four days before the download. The same bar therefore read
`117.5` at any point between the two splits, and `5.875` before either: one bar, three
different adjusted closes in thirteen months. Snapshots of `adj_split` are not comparable, and
appending one to another across `2026-07-13` would splice a 20x cliff into `XAIR`'s 2019.

Dividends do the same, more finely. `XOM` carries 107 dividends and one split (`2001-07-19`,
2:1). The ratio `adj_splitdiv/adj_split` steps 89 times over 6672 bars and every step lands on
a dividend ex-date. Its `2015-06-15` bar reads `52.7103` today against `83.72` on `adj_split`,
a cumulative factor of `0.6296`; the `2026-02-12` dividend alone rescaled all prior history by
`0.99337876`, so that same 2015 bar read `53.0616` before February and `52.7103` after.

The control holds. `XOM`'s `UNADJUSTED/adj_split` ratio has exactly one step above 0.1% in 6672
bars — `2001-07-19`, x0.5, its only split — and not one of its 107 dividends perturbs it.
`UNADJUSTED` is not a function of the corporate-action set; the adjusted series are. What that
control does *not* establish is that the vendor never revises raw prices over time, which is a
different claim and stays untested for want of an archive to diff. If raw bars are ever found
to move, `UNADJUSTED`'s append-only rule above is what breaks, not this section.

## The restatement lags the metafile, per ticker

The same download refutes the premise the splits guard rests on — that a metafile action since
the basis date implies the partition has been restated. The two are not synchronised.

`XXII` split `2026-06-12` at `0.05`. Its unadjusted series shows the mechanical jump, `0.316`
on `2026-06-11` to `6.545` on `2026-06-12`. Its `adj_split` series shows *the same jump*: on
`2026-06-11` `adj_split` equals `UNADJUSTED` equals `0.316`. Thirty-five days on, the split has
not been applied, and `XXII`'s adjusted history today carries exactly the artificial 20x seam
this ADR cites as the reason never to merge — shipped by the vendor, inside a single `full`.
Its previous split, `2026-01-26`, *is* applied, so the file was regenerated somewhere between
the two. `XAIR`'s `2026-07-13` split, four days old, is applied. The lag is per-ticker, not a
constant. Dividends behave the same way: of 14 `X` payers whose series cover their last
recorded ex-date, 7 are current and 7 lag by 33 to 121 days, clustered near one quarter —
`XOM`'s `2026-05-15` dividend is in the metafile and absent from its prices.

So the metafile *leads* the restatement by an unpredictable interval, and the guard's
`action since basis date -> refetch the full` is wrong in both directions. It fires while the
vendor's own `full` is still unrestated, so the refetch returns the same numbers and re-arms
nothing; and when the restatement does land, no metafile row changes, so the guard stays
silent and the partition is stale for good. A guard keyed on the metafile cannot see the event
it needs to see. Detecting restatement means comparing the vendor's bytes against what we
already hold — which is a decision this ADR has not made, and is the open question it leaves.
That `XXII` is mid-flight right now makes it cheap to settle: its adjusted 2019 history will
change under a re-download, with no metafile row moving to announce it.
