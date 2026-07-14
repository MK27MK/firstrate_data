# Generic base parametrised by the adjustment enum

The first cut of `FirstRateData` assumed FirstRate's API was uniform across
instrument types and pushed every `data_file` parameter into the shared base:
a stock-shaped `adjustment` and a `ticker_range`. Adding futures proved the
assumption false. Futures `data_file` accepts **no `ticker_range` at all**
(`period=full` needs nothing extra), and its `adjustment` values are a disjoint
set (`contin_*`, describing roll-date adjustment of a stitched series, not
corporate actions). The shared base had baked in one asset type's shape.

So the base is now generic on the adjustment enum —
`FirstRateData[AdjustmentT: StrEnum](ABC)` — and each loader binds it
(`FirstRateEquities(FirstRateData[EquitiesAdjustment])`,
`FirstRateFutures(FirstRateData[ContinuousFuturesAdjustment])`). The abstract
`download_historical_data(period, timeframe, adjustment: AdjustmentT)`
deliberately **omits** `ticker_range`; `FirstRateEquities` *adds* it as an
optional parameter in its override. Adding an optional parameter is a widening,
so the override still accepts every call the base signature allows and the whole
thing type-checks under `mypy --strict` with no ignores.

Alternatives rejected: typing the abstract method `(*args, **kwargs)` — keeps the
ABC's teeth but abandons type-checking of the arguments that actually vary; and a
single concrete method in the base taking the union of both signatures — which
would hand every futures caller a `ticker_range` the API does not have, and let
them pass `adj_splitdiv` to a futures loader, trading a compile-time error for a
server-side one.

Consequences: the base holds only what is genuinely shared (HTTP fetch, unzip,
clean-and-replace persistence into the managed directory) and knows nothing about
`ticker_range` beyond wiring it through when a subclass passes one — the *rules*
for it live in `FirstRateEquities`, which is the only family that has one. When
adding an asset type, subclass `FirstRateData[<ItsAdjustmentEnum>]` and implement
`download_historical_data` with only the parameters its own docs page lists.
Resist the pull to re-uniformise the base.
