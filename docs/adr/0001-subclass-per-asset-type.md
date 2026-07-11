# Subclass per asset type instead of an `asset_type` parameter

FirstRate's API is uniform across instrument types (stock, etf, index, fx, …)
but the *applicable* operations differ: only stocks/ETFs have splits and
dividends, and only they are meaningfully split/dividend-adjustable. Rather than
thread an `asset_type` argument through every method and validate combinations at
runtime, `FirstRateData` is a base holding the shared machinery (http fetch,
in-memory unzip, clean-and-replace persistence, managed `_directory`) and each
instrument type is a subclass that fixes `_asset_type` as a class attribute
(e.g. `FirstRateStocks`).

Consequences: call sites are simpler and self-documenting (`FirstRateStocks(...)`
carries the type), and a subclass can expose only the methods that apply — so
`download_splits`/`download_dividends` live on stock/ETF loaders and never appear
where they'd be nonsense. The cost is one small class per instrument type;
new types are added lazily as needed rather than all up front.
