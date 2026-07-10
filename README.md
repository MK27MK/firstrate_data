# firstrate-data

Lightweight FastAPI wrapper around the [FirstRate Data API](https://firstratedata.com/about/api-docs). Futures only for now.

## Setup

```bash
cp .env.example .env   # add your FIRSTRATE_USERID
uv run fastapi dev app/main.py
```

Docs at <http://127.0.0.1:8000/docs>.

## Futures endpoints

| Route | FirstRate call | Returns |
|-------|----------------|---------|
| `GET /futures/data` | `/data_file` | zip of csv |
| `GET /futures/meta` | `/meta_file` | zip of csv (splits/dividends) |
| `GET /futures/last-update` | `/last_update` | plain text date |
| `GET /futures/tickers` | `/ticker_listing` | csv text |

Query params are validated as enums (`period`, `timeframe`, `adjustment`, `metafile_type`);
`ticker_range` (A–Z) is required when `period=full`.

```bash
curl "http://127.0.0.1:8000/futures/data?period=day&timeframe=1day&adjustment=adj_split" -OJ
```
