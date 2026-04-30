# stock-pipeline

Dagster-orchestrated ETL for Indian stock market data. Pluggable
**sources** (Kite, CSV) feed a shared asset graph that fans out to
pluggable **destinations** (local parquet, QuantConnect LEAN). One
partition per `(symbol)` for equity, per `(option_underlying)` for
options. Symbol/contract universe is synced from the shared
trading-system Postgres.

## Layout

```
stock_pipeline/
├── __init__.py              Definitions() — only wiring file
├── core/
│   ├── partitions.py        equity_symbols, option_contracts (dynamic)
│   ├── tags.py              run-tag keys + defaults (source/storage/dates)
│   ├── db.py                PostgresResource (read-only)
│   ├── models.py            SQLAlchemy mapping of `instruments`
│   ├── sources/             input adapters
│   │   ├── base.py            Protocol
│   │   ├── kite.py            KiteSource (Zerodha)
│   │   └── csv_source.py      CsvSource (pre-downloaded CSVs)
│   └── destinations/        output adapters
│       ├── base.py            Destination protocol
│       ├── local.py           LocalStorage (parquet)
│       └── lean.py            LeanStorage (QuantConnect zip format)
├── daily_eq/                daily equity (1 partition = 1 symbol)
├── daily_op/                daily options (1 partition = 1 underlying)
├── intraday_eq/             minute equity (1 partition = 1 symbol)
└── intraday_op/             minute options (1 partition = 1 underlying)
```

Each group has the same shape: `raw_*` → `processed_*` → `*_parquet`,
plus a `sensor.py` that registers partition keys from Postgres.

## Setup

```bash
cd /home/mrrobot/Projects/data-pipeline
uv venv && source .venv/bin/activate
uv pip install -e .
cp .env.example .env       # set DATABASE_URL, CSV_ROOT_DIR, ...
./run.sh                   # = dagster dev with DAGSTER_HOME=.dagster_home
```

Open <http://localhost:3000>.

## Run-tag matrix

Every group reads the same four tags off `context.run.tags`:

| tag          | values                  | default                            |
|--------------|-------------------------|------------------------------------|
| `source`     | `kite` \| `csv`         | `kite` (currently no-op stub)      |
| `storage`    | `local` \| `lean`       | `local`                            |
| `start_date` | `YYYY-MM-DD`            | `2000-01-01` daily, `2015-02-02` intraday |
| `end_date`   | `YYYY-MM-DD`            | today                              |

Status:

| Group        | csv | kite | local | lean |
|--------------|-----|------|-------|------|
| daily_eq     | ✓   | stub | ✓     | ✓    |
| daily_op     | ✓   | stub | ✓     | ✓    |
| intraday_eq  | ✓   | stub | ✓     | ✓    |
| intraday_op  | ✓   | stub | ✓     | ✓    |

Pre-baked test jobs live in `test.py` (one per group×source×storage
cell). Open Dagster UI → **Jobs** → pick a `test_*` job → **Materialize**
on a partition. Tags come from the job definition, no Launchpad edits.

## Output paths

```
storage=local
  data/daily/eq/{symbol}.parquet
  data/daily/op/{underlying}/{contract}.parquet
  data/intraday/eq/{symbol}/{date}.parquet
  data/intraday/op/{underlying}/{date}/{contract}.parquet

storage=lean
  lean_data/equity/{country}/daily/{symbol}.zip
  lean_data/equity/{country}/minute/{symbol}/{YYYYMMDD}_trade.zip
  lean_data/option/{country}/daily/{ul}_{YYYY}_{trade|openinterest}_american.zip
  lean_data/option/{country}/minute/{ul}/{YYYYMMDD}_{trade|quote|openinterest}_american.zip
```

For LEAN backtests, copy `lean_data/equity/` and `lean_data/option/`
into your LEAN clone's `Data/` folder.

---

# How to extend

Three common shapes: **add a source**, **add a destination**, **add a flow**.

## Add a new source

A source is a `ConfigurableResource` that returns a `pd.DataFrame`
matching the lowercase schema each group expects.

1. **Create the file.** `stock_pipeline/core/sources/my_source.py`:

   ```python
   from datetime import date
   import pandas as pd
   from dagster import ConfigurableResource

   class MySource(ConfigurableResource):
       api_key: str

       def fetch_daily_eq_range(
           self, symbol: str, instrument_token: int,
           from_date: date, to_date: date,
       ) -> pd.DataFrame:
           # return columns: date, open, high, low, close, volume
           ...

       # Implement only the methods your flows need:
       #   fetch_daily_eq_range, fetch_intraday_eq_range,
       #   fetch_daily_op_range, fetch_intraday_op_range
   ```

2. **Wire it in `stock_pipeline/__init__.py`:**

   ```python
   resources={
       ...,
       "mysrc": MySource(api_key=EnvVar("MY_API_KEY")),
   }
   ```

3. **Add the tag value.** In each `*/assets.py` `raw_*` function,
   extend the source dispatch:

   ```python
   if source not in ("kite", "csv", "mysrc"):
       raise ValueError(...)
   ...
   elif source == "mysrc":
       return mysrc.fetch_daily_eq_range(symbol=..., ...)
   ```

   Add `mysrc: MySource` to the asset's parameter list. Dagster
   injects it by name.

4. **Run with `source=mysrc`** as a run tag.

## Add a new destination

A destination wraps a backend (filesystem, S3, GCS, ...) and exposes
read/write methods. Two flavours exist:

- **Format-agnostic** (parquet to anywhere) — subclass `Destination`
  in `core/destinations/base.py`. Implement `read(rel_path) -> df`,
  `write(df, rel_path) -> None`, `exists(rel_path) -> bool`. Used by
  `local`-flavoured branches.
- **Format-specific** (LEAN zip, custom binary) — write a
  `ConfigurableResource` from scratch with bespoke methods like
  `write_daily_equity(df, *, symbol)`. See `lean.py` for the template.

Steps:

1. Create `stock_pipeline/core/destinations/my_dest.py`. For S3:

   ```python
   import pandas as pd
   from stock_pipeline.core.destinations.base import Destination

   class S3Storage(Destination):
       bucket: str
       prefix: str = ""

       def read(self, rel_path: str) -> pd.DataFrame:
           # s3 get → bytes → pd.read_parquet(BytesIO(...))
           ...

       def write(self, df: pd.DataFrame, rel_path: str) -> None:
           # df.to_parquet(BytesIO(...)) → s3 put
           ...

       def exists(self, rel_path: str) -> bool: ...
   ```

2. Wire it in `__init__.py`:

   ```python
   "s3": S3Storage(bucket=os.getenv("S3_BUCKET"), prefix="pipeline/"),
   ```

3. Extend each group's `_VALID_STORAGES` and add a dispatch branch in
   the write asset (`*_parquet`). Look at how `daily_eq/assets.py`
   handles `storage=lean` for the pattern: inject the resource as a
   parameter, branch on the tag, call your method.

4. Run with `storage=s3`.

## Add a new flow (new data type / interval)

Mirror the structure of `daily_eq/`. Pick a partition shape first:
one partition per `(symbol)`, per `(underlying)`, per `(date, symbol)`,
or whatever the data grain is.

1. **Create the group folder:**

   ```
   stock_pipeline/my_flow/
   ├── __init__.py     (empty)
   ├── assets.py       raw_X → processed_X → X_parquet
   └── sensor.py       registers partition keys
   ```

2. **Pick or define a partition** in `core/partitions.py`. Reuse
   `equity_symbols` or `option_contracts` if the universe matches; add
   a new `DynamicPartitionsDefinition` if not.

3. **Write the assets.** Three-stage shape, all sharing the same
   partition def and `group_name`:

   ```python
   from dagster import asset, AssetExecutionContext
   from stock_pipeline.core.partitions import equity_symbols
   from stock_pipeline.core.tags import (
       TAG_SOURCE, TAG_STORAGE, TAG_START_DATE, TAG_END_DATE,
       DEFAULT_SOURCE, DEFAULT_STORAGE, DEFAULT_START_DATE,
   )

   GROUP = "my_flow"
   _VALID_STORAGES = ("local", "lean")

   @asset(partitions_def=equity_symbols, group_name=GROUP)
   def raw_my(context, kite, csv):
       # read run tags, dispatch on source, return df
       ...

   @asset(partitions_def=equity_symbols, group_name=GROUP)
   def processed_my(context, raw_my):
       # transform
       return raw_my

   @asset(partitions_def=equity_symbols, group_name=GROUP)
   def my_parquet(context, processed_my, local, lean):
       # branch on storage tag, write
       ...
   ```

4. **Write the sensor.** Copies a pattern from
   `daily_eq/sensor.py` — query Postgres for the universe, diff
   against current partition keys, register additions:

   ```python
   from dagster import sensor, SensorEvaluationContext
   from stock_pipeline.core.partitions import equity_symbols

   @sensor(name="my_universe_sync", default_status=...)
   def my_universe_sync(context: SensorEvaluationContext):
       new = set(query_db()) - set(
           context.instance.get_dynamic_partitions("equity_symbols")
       )
       for k in new:
           context.instance.add_dynamic_partitions("equity_symbols", [k])
       return SkipReason(...)
   ```

5. **Wire into `__init__.py`:**

   ```python
   from stock_pipeline.my_flow import assets as my_flow_assets
   from stock_pipeline.my_flow.sensor import my_universe_sync

   defs = Definitions(
       assets=load_assets_from_modules([..., my_flow_assets]),
       sensors=[..., my_universe_sync],
       resources={...},
   )
   ```

6. **Add a test job** in `test.py`:

   ```python
   _my = AssetSelection.groups("my_flow")
   test_my_csv_local = define_asset_job(
       name="test_my_csv_local", selection=_my,
       tags={TAG_SOURCE: "csv", TAG_STORAGE: "local",
             TAG_START_DATE: "2024-01-01", TAG_END_DATE: "2024-01-31"},
   )
   test_jobs.append(test_my_csv_local)
   ```

7. Restart `dagster dev`. Sensor registers partitions. Open the new
   `test_my_*` job, pick a partition, materialize.

## Conventions

- **Empty input is fine** — every `raw_*` returns an empty
  `pd.DataFrame()` if the source has nothing; downstream stages
  short-circuit on `df.empty`.
- **Helper columns are underscore-prefixed** (`_underlying`,
  `_contract`, `_trading_date`) — set in `raw_*`, read in
  `*_parquet` for path routing, dropped before the on-disk write.
- **Re-runs are safe.** Daily flows merge+dedupe by date; intraday
  flows overwrite per-day files; LEAN does read-modify-write at zip
  entry granularity.

## Where things live

- Run-tag keys & defaults: `core/tags.py`
- Partition definitions: `core/partitions.py`
- DB session: `core/db.py` (read-only `instruments` access)
- Test jobs: `test.py`
- Architecture deep-dive: `architecture.md`


## TODO
- Add comments in asset flows
- Make flow easy to read
