# stock-pipeline

Dagster-orchestrated ETL for Indian stock market data. First slice: daily EOD
equity from Kite, written to local parquet. Symbol universe is read from the
shared trading-system Postgres.

## Architecture

```
stock_pipeline/
├── __init__.py              Definitions() — only wiring file
├── core/                    shared across data types
│   ├── partitions.py        date axis + equity_symbols dynamic pool
│   ├── db.py                PostgresResource (read-only)
│   ├── models.py            SQLAlchemy mapping of `instruments`
│   └── sources/
│       ├── base.py          BaseSource protocol
│       └── kite.py          KiteSource + FakeKiteSource
└── daily_eod/
    ├── assets.py            raw_daily -> processed_daily -> daily_parquet
    └── sensor.py            syncs equity_symbols from Postgres
```

Intraday, F&O, Dhan, Upstox, S3, Lean — deferred. Each lands as an additive
change alongside `daily_eod/` with its own sensor and schedule.

## Setup

```bash
cd /home/mrrobot/Projects/data-pipeline
uv venv
source .venv/bin/activate
uv pip install -e .

cp .env.example .env
# Edit .env — set DATABASE_URL to your Postgres.
# Leave KITE_API_KEY unset to run with FakeKiteSource.

./run.sh
```

Open http://localhost:3000.

## Try it

1. **Watch the sensor tick.** Go to Sensors → `equity_symbols_sync`. Within 30s
   it registers symbols from the curated list that exist in `instruments` as
   NSE/EQ.
2. **Materialize one partition.** Assets → `daily_parquet` → Materialize →
   pick a `(date, symbol)` cell. All three assets run for that slice.
   Check `data/equity/<SYMBOL>/<DATE>.parquet`.
3. **Materialize all.** Top-right "Materialize all" — fans out across every
   registered (date, symbol) pair in parallel.
4. **Rerun one cell.** Only that cell recomputes. This is why the pipeline is
   partitioned.

## Source selection

`build_source()` in `core/sources/kite.py` picks real vs. fake at load time:
- `KITE_API_KEY` set → real `KiteSource` via `kiteconnect`.
- Unset → `FakeKiteSource` generates deterministic data keyed off
  `instrument_token`. Full DAG exercises the real code path (DB lookup,
  asset graph, parquet write) without network calls.

## Universe filter

`CURATED_UNIVERSE` in `daily_eod/sensor.py` is a hardcoded list for v1.
Replace with a query against a dedicated table (`active_equity`,
`nifty500`, or F&O underlyings) once the filter stabilises.

## What's next

| Next step             | Change                                                    |
|-----------------------|-----------------------------------------------------------|
| Daily schedule        | Add `ScheduleDefinition` in `daily_eod/schedule.py`       |
| Intraday data type    | New `intraday/` group; reuse `KiteSource.fetch_intraday`  |
| F&O data type         | New `fno/` group; `fno_instruments` dynamic pool          |
| S3 destination        | New terminal asset `daily_s3` sharing `processed_daily`   |
| Lean destination      | New terminal asset `daily_lean`                           |
| Retries               | `retry_policy=` on `raw_daily` once real failures emerge  |
| IOManagers            | Replace inline parquet writes when >2 destinations exist  |


### Data
- Daily
  - Stock
  - FnO
- Intraday
  - Stock
  - FnO

### Format
- Parquet
- Lean Compatiable
