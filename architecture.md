# Architecture

Dagster-orchestrated ETL for Indian equities and F&O options data, with
pluggable sources (Kite, CSV) and pluggable destinations (local parquet,
QuantConnect LEAN). Designed to add new data types and storage backends
as additive changes — no rewrites of the existing groups.

---

## 1. High-level picture

```
                 ┌─────────────────┐
                 │  Postgres        │
                 │  (instruments,   │
                 │   symphony_...)  │
                 └────────┬─────────┘
                          │  read-only via PostgresResource
                          ▼
              ┌─────────────────────────────┐
              │    Dynamic-partition         │
              │    sensors (every 10–30s)    │
              │  ─────────────────────────   │
              │  equity_symbols_sync         │
              │  option_contracts_sync       │
              └──────────────┬──────────────┘
                             │ register / remove
                             ▼
            ┌────────────────────────────────────────┐
            │   Dynamic partitions                    │
            │   ───────────────────                   │
            │   equity_symbols   ("RELIANCE", ...)    │
            │   option_contracts ("NIFTY", ...)       │
            └─────┬────────────────────────────┬─────┘
                  │                            │
                  │ partition_key fans out    │
                  ▼                            ▼
  ┌──────────────────────────────────────────────────────────┐
  │                     Asset graph                           │
  │  ────────────────────────────────────────────────────────│
  │   raw_*    ──▶   processed_*    ──▶   *_parquet (write)   │
  │   (fetch)         (pass-through,        (storage dispatch │
  │                    schema-stable         on tag, write to │
  │                    hook)                 local | lean)    │
  └────────────┬───────────────────────────────────┬─────────┘
               │                                    │
               │ uses                               │ uses
               ▼                                    ▼
     ┌──────────────────┐               ┌──────────────────────┐
     │  Sources          │               │  Destinations         │
     │  ────────────     │               │  ─────────────        │
     │  KiteSource       │               │  LocalStorage         │
     │  CsvSource        │               │  LeanStorage          │
     │  (FakeKite)       │               │  (S3, Drive — future) │
     └──────────────────┘               └──────────────────────┘
```

Every group has the same three-stage shape: **raw → processed →
write**. `processed` is a deliberate hook for future per-row
transforms (corp-action adjustments, schema validation) that today is a
pass-through. The write stage is the only place that knows about
storage; everything upstream is destination-agnostic.

---

## 2. Repository layout

```
data-pipeline/
├── stock_pipeline/                    Dagster package — all assets live here
│   ├── __init__.py                    Definitions() — only wiring file
│   │
│   ├── core/                          Shared across all groups
│   │   ├── partitions.py              equity_symbols, option_contracts
│   │   ├── tags.py                    Tag keys + universal defaults
│   │   ├── db.py                      PostgresResource (read-only)
│   │   ├── models.py                  SQLAlchemy mapping (Instrument,
│   │   │                              SymphonyInstruments)
│   │   ├── sources/
│   │   │   ├── base.py                BasedSource Protocol
│   │   │   ├── kite.py                KiteSource + FakeKiteSource
│   │   │   └── csv_source.py          File-backed source (offline replays,
│   │   │                              streaming PyArrow reader for the
│   │   │                              multi-GB intraday op file)
│   │   └── destinations/
│   │       ├── base.py                Destination protocol (read/write/exists)
│   │       ├── local.py               LocalStorage  → parquet on disk
│   │       └── lean.py                LeanStorage   → QuantConnect LEAN tree
│   │
│   ├── daily_eq/                      Group: daily equity EOD bars
│   │   ├── assets.py                  raw_daily → processed_daily → daily_parquet
│   │   └── sensor.py                  equity_symbols_sync (NSE/EQ from DB)
│   │
│   ├── daily_op/                      Group: daily option contracts (per underlying)
│   │   ├── assets.py                  raw_option_daily → processed → option_daily_parquet
│   │   └── sensor.py                  option_contracts_sync (NFO-OPT from DB)
│   │
│   ├── intraday_eq/                   Group: minute equity bars
│   │   └── assets.py                  raw_intraday → processed → intraday_parquet
│   │
│   └── intraday_op/                   Group: minute option contracts (multi-contract)
│       └── assets.py                  raw_intraday_op → processed → intraday_op_parquet
│
├── csv_to_lean.ipynb                  One-shot converter for a 17 GB unsorted
│                                      multi-contract NIFTY option CSV → LEAN.
│                                      Uses Polars batched reader + spill staging
│                                      + atomic checkpoints. Bypasses Dagster.
│
├── transform.py                       Stdlib-only CLI converter for a folder of
│                                      per-contract option CSVs → LEAN.
│
├── daily_equity_transformer.py        Stdlib-only CLI converter for daily equity
│                                      CSVs → LEAN equity zip.
│
├── dagster.yaml                       Instance config (run coordinator, per-symbol
│                                      concurrency limit).
├── run.sh                             Local startup helper.
├── pyproject.toml                     Package + deps.
├── .env.example                       DATABASE_URL, KITE_*, CSV_ROOT_DIR,
│                                      LEAN_STORAGE_DIR, ENV, TRACKED_OPTION_UNDERLYINGS
└── README.md
```

---

## 3. The five core abstractions

### 3.1 Sources (`core/sources/`)

A source is a typed adapter over a market-data backend. Every source
implements one method per `(instrument_type, interval)` cell —
`fetch_daily_eq_range`, `fetch_intraday_eq_range`,
`fetch_daily_op_range`, `fetch_intraday_op_range`. All return a
`pandas.DataFrame` in a fixed lowercase schema so call sites never branch
on source type.

| Source            | Backed by                | Notes                                        |
|-------------------|--------------------------|----------------------------------------------|
| `KiteSource`      | `kiteconnect` HTTP API   | Real Zerodha calls. Daily token refresh.     |
| `FakeKiteSource`  | Deterministic generator  | Same shape as Kite; activated when `KITE_API_KEY` is unset. Lets you exercise the full DAG offline. |
| `CsvSource`       | Local filesystem (`CSV_ROOT_DIR`) | Pre-downloaded CSVs. The `intraday_op` reader uses PyArrow streaming + per-batch date prefix filter to handle multi-GB files without OOM. |

**Source selection** is per-run via the `source` tag (`kite` or `csv`).
Kite paths in option flows are stubbed today (`source=kite` returns an
empty frame for `daily_op` / `intraday_op`); the live-data pull is
deferred until equity flows are stable.

### 3.2 Destinations (`core/destinations/`)

A destination is a write sink. The base `Destination` protocol is
deliberately minimal — `read`, `write`, `exists` over relative paths —
and `LocalStorage` implements all three. Higher-shape destinations like
`LeanStorage` add **higher-level emitters** instead of forcing a
per-file primitive:

| Destination     | Output                                        | Methods                                                  |
|-----------------|-----------------------------------------------|----------------------------------------------------------|
| `LocalStorage`  | `data/{group_path}/...parquet`                | `read(rel)`, `write(df, rel)`, `exists(rel)`             |
| `LeanStorage`   | `lean_data/{option,equity}/{country}/...zip`  | `write_minute_option_day(df, *, underlying, date_str)` (option-minute), `write_daily_equity(df, *, symbol)` (equity-daily) |

Why two patterns? LEAN's grouping doesn't fit one-DataFrame-per-path:
options bundle every contract for a date into one zip, daily equity has a
per-symbol zip with read-modify-write merge semantics. Pushing those
into `write(df, rel_path)` would smuggle intent into the path string.
Instead, `LeanStorage` exposes named methods, and the write-stage asset
dispatches on `storage` tag.

**Storage selection** is per-run via the `storage` tag (`local` |
`lean`). Each group keeps a `_VALID_STORAGES` whitelist that grows as
new destinations are wired in; an unknown value raises before any work
is done.

### 3.3 Partitions (`core/partitions.py`)

```python
equity_symbols   = DynamicPartitionsDefinition(name="equity_symbols")
option_contracts = DynamicPartitionsDefinition(name="option_contracts")
```

Two dynamic partition pools, shared across every group that needs
them:

- `equity_symbols` — used by `daily_eq` and `intraday_eq`. Populated by
  `equity_symbols_sync` (NSE/EQ rows joined Kite ↔ Symphony).
- `option_contracts` — used by `daily_op` and `intraday_op`. Populated
  by `option_contracts_sync` (NFO-OPT, narrowed to
  `TRACKED_OPTION_UNDERLYINGS` — defaults to NIFTY/BANKNIFTY/FINNIFTY/SENSEX).

The partition key for **option** flows is the **underlying symbol**
(e.g. `NIFTY`), not a contract trading symbol. This is intentional:
`intraday_op` reads a single per-underlying multi-GB CSV that already
fans out to thousands of contracts, so partitioning by underlying is the
right granularity.

Sensors register/remove partitions based on what's currently in
Postgres. `option_contracts_sync` caps at `_BATCH_PER_TICK = 2000` per
tick to avoid sensor-tick timeouts on first-time backfills.

### 3.4 Tags (`core/tags.py`)

Run tags are how every group reads runtime parameters off
`context.run.tags`. Tag keys live centrally so the strings don't drift
across groups:

| Tag          | Values             | Default                          | Effect                                  |
|--------------|--------------------|----------------------------------|-----------------------------------------|
| `source`     | `kite` \| `csv`    | `kite`                           | Which source adapter to call            |
| `start_date` | `YYYY-MM-DD`       | `2000-01-01` (intraday: `2015-02-02`) | Inclusive lower bound on fetch range    |
| `end_date`   | `YYYY-MM-DD`       | `today`                          | Inclusive upper bound                   |
| `storage`    | `local` \| `lean`  | `local`                          | Which destination to write to           |

Asset modules may override a default (e.g. intraday flows tighten
`start_date` to `2015-02-02` because the universal `2000-01-01` would
iterate years of empty range).

### 3.5 Postgres (`core/db.py`, `core/models.py`)

The pipeline reads (never writes) from a shared trading-system
Postgres. Two ORM tables matter:

- `Instrument` (Kite-ingested instrument list) — provides
  `instrument_token` for KiteConnect's historical API; matched on
  `tradingsymbol` + `exchange` + `instrument_type`.
- `SymphonyInstruments` — used to constrain the universe to instruments
  that exist on both Kite and Symphony; joined on
  `Instrument.exchange_token == SymphonyInstruments.scrip_code`.

`PostgresResource` wraps an SQLAlchemy engine cached at module scope
(engines are expensive and thread-safe; `ConfigurableResource` is a
Pydantic model so it can't hold the engine itself).

---

## 4. The four data groups

All four follow the same `raw → processed → write` shape with
group-specific specifics.

### 4.1 `daily_eq` — daily equity EOD bars

| Aspect          | Value                                              |
|-----------------|----------------------------------------------------|
| Partition       | `equity_symbols` (one per NSE/EQ tradingsymbol)    |
| Source          | Kite or CSV (`{root}/daily/eq/{SYMBOL}.csv`)       |
| DataFrame shape | `symbol, date, open, high, low, close, volume`     |
| Local output    | `data/daily/eq/{symbol}.parquet` (read-modify-write merge by date) |
| LEAN output     | `lean_data/equity/{country}/daily/{symbol}.zip` containing `{symbol}.csv` (read-modify-write merge) |
| Sensor          | `equity_symbols_sync` (NSE/EQ from Postgres)       |

Both storage paths preserve full history: a narrow re-run merges the
new range into the existing file rather than overwriting it.

### 4.2 `daily_op` — daily option contracts (per underlying)

| Aspect          | Value                                                       |
|-----------------|-------------------------------------------------------------|
| Partition       | `option_contracts`, partition key = underlying (e.g. `NIFTY`) |
| Source          | CSV only today (`{root}/daily/op/{UNDERLYING}/*.csv`, one file per contract) |
| DataFrame shape | `datetime, open, high, low, close, volume, oi, strike, expiry, option_type, lot_size, _underlying, _contract` |
| Local output    | `data/daily/op/{underlying}/{contract}.parquet` (per-contract; merge-deduped by `datetime`) |
| LEAN output     | _Pending_                                                   |
| Sensor          | `option_contracts_sync`                                     |

The contract identifier follows the convention
`{UNDERLYING}_{STRIKE}_{TYPE}_{DD}_{MON}_{YY}` (e.g.
`NIFTY_19750_CE_01_FEB_24`).

### 4.3 `intraday_eq` — minute equity bars

| Aspect          | Value                                                |
|-----------------|------------------------------------------------------|
| Partition       | `equity_symbols`                                     |
| Source          | Kite or CSV (`{root}/intraday/eq/{SYMBOL}.csv`)      |
| DataFrame shape | `symbol, date, open, high, low, close, volume, _trading_date` |
| Local output    | `data/intraday/eq/{symbol}/{trading_date}.parquet` (one file per trading day) |
| LEAN output     | _Pending_                                            |
| Sensor          | Reuses `equity_symbols_sync`                         |

### 4.4 `intraday_op` — minute option contracts (multi-contract per underlying)

| Aspect          | Value                                                    |
|-----------------|----------------------------------------------------------|
| Partition       | `option_contracts`, partition key = underlying           |
| Source          | CSV (`{root}/intraday/op/{UNDERLYING}.csv`, one multi-GB file with all contracts of that underlying interleaved). PyArrow streaming reader + per-batch ISO-prefix date filter. |
| DataFrame shape | `datetime, contract, open, high, low, close, volume, oi, strike, expiry, option_type, lot_size, _underlying, _trading_date` |
| Local output    | `data/intraday/op/{underlying}/{trading_date}/{contract}.parquet` (one parquet per (contract, day)) |
| LEAN output     | `lean_data/option/{country}/minute/{symbol}/{YYYYMMDD}_trade_{style}.zip`, `..._openinterest_{style}.zip`, plus `lean_data/option/{country}/universes/{symbol}/{YYYYMMDD}.csv` |
| Sensor          | `option_contracts_sync`                                  |

The CSV is too large to load whole; the source streams it in PyArrow
batches and applies a string-prefix filter on `DateTime[:10]` so RAM
peak scales with the matching slice, not the file. The `contract`
column is synthesized at read time from the typed columns
(`Symbol`/`Strike`/`OptionType`/`Expiry`) instead of using the CSV's
`Ticker` value, so the output naming is independent of how the source
file labels rows.

---

## 5. Storage formats

### 5.1 Local parquet

Plain pandas `to_parquet`. Schema-stable per group. Safe to read with
`pd.read_parquet` or `pyarrow`. Read-modify-write merging happens in
the asset, not in `LocalStorage`.

### 5.2 LEAN — minute options (`LeanStorage.write_minute_option_day`)

```
{base_dir}/option/{country}/minute/{symbol}/
    {YYYYMMDD}_trade_{style}.zip          ← inner: per-contract CSVs (time_ms,o,h,l,c,v)
    {YYYYMMDD}_openinterest_{style}.zip   ← inner: per-contract CSVs (time_ms,oi)
{base_dir}/option/{country}/universes/{symbol}/
    {YYYYMMDD}.csv                        ← header + blank underlying row + per-contract daily aggregates
```

Encoding rules (shared with `csv_to_lean.ipynb` and `transform.py`):

- **Prices** stored as integers in deci-cents (`price × 10_000`,
  rounded). Filenames carry the deci-cent strike for sortability;
  universe rows carry the plain integer strike for readability.
- **Time** as `time_ms` (milliseconds since midnight, start-of-bar).
- **Right** as `C` / `P` (mapped from `CE` / `PE`).
- **Expiry** as `YYYYMMDD`.
- **Volume** as integer; **OI** as integer, written only when
  positive (LEAN treats `0` as missing).

The universe file's first data row is the underlying's daily bar.
`LeanStorage` writes a blank row there to avoid coupling to a specific
source for the spot — populate post-hoc if your strategy queries
`Underlying.Price`.

### 5.3 LEAN — daily equity (`LeanStorage.write_daily_equity`)

```
{base_dir}/equity/{country}/daily/{symbol}.zip
    {symbol}.csv      (one line per day, no header)
    YYYYMMDD 00:00,o_deci,h_deci,l_deci,c_deci,volume
```

Read-modify-write merge: existing rows are decoded back from the zip,
concatenated with incoming, deduped by `date` (incoming wins), sorted,
re-zipped. Re-running with a narrow range never destroys old history —
matches the parquet path's safety property.

---

## 6. Concurrency and run management

`dagster.yaml` configures a `QueuedRunCoordinator` with one
`tag_concurrency_limit`:

```yaml
- key: "dagster/partition"
  value:
    applyLimitPerUniqueValue: true
  limit: 1
```

`dagster/partition` is auto-applied by Dagster with the partition key
as its value. With `applyLimitPerUniqueValue: true`, this means
"at most 1 concurrent run per unique partition value". Two runs for
`RELIANCE` queue behind each other; `RELIANCE` and `INFY` fan out
freely. This prevents the read-modify-write race on
`data/daily/eq/{symbol}.parquet` and on the LEAN equity zip.

Different groups (e.g. a `daily_eq` run and an `intraday_eq` run)
sharing the same partition key still serialize against each other
under this rule — usually fine, since both paths touch
`data/.../{symbol}.*` and would race anyway.

---

## 7. Out-of-pipeline tooling

Three CLI / notebook utilities live at repo root for ad-hoc LEAN
conversions outside the Dagster flow:

| Tool                          | Input                                       | Best for                                  |
|-------------------------------|---------------------------------------------|-------------------------------------------|
| `csv_to_lean.ipynb`           | One giant unsorted multi-contract CSV (e.g. 17 GB NIFTY.csv) | RAM-bound first-time conversion. Polars batched reader + per-date staging spill + atomic checkpoint for resumability. |
| `transform.py`                | Folder of per-contract option CSVs          | Stdlib-only ad-hoc conversion. Appends to existing zips so it's safe to run incrementally. |
| `daily_equity_transformer.py` | Folder of daily equity CSVs                 | Stdlib-only one-shot daily-equity → LEAN. The Dagster `daily_eq` `storage=lean` path supersedes it for production use. |

The encoding logic in all three is identical to what `LeanStorage`
emits — they double as ground-truth references when debugging the
Dagster path.

---

## 8. Environment and config

`.env` keys:

| Var                          | Purpose                                                         | Default if unset                 |
|------------------------------|-----------------------------------------------------------------|----------------------------------|
| `DATABASE_URL`               | Read-only Postgres for instruments tables                       | _required_                       |
| `KITE_API_KEY`, `KITE_ACCESS_TOKEN` | Live Zerodha auth                                       | If unset → `FakeKiteSource`      |
| `CSV_ROOT_DIR`               | Root for CSV-source files                                       | `CustomSource`                   |
| `LOCAL_STORAGE_DIR`          | Root for LocalStorage parquet writes                            | `data`                           |
| `LEAN_STORAGE_DIR`           | Root for LeanStorage zip writes                                 | `lean_data`                      |
| `LEAN_COUNTRY`               | LEAN tree country segment                                       | `india`                          |
| `LEAN_OPTION_STYLE`          | LEAN filename style token                                       | `american`                       |
| `ENV`                        | `dev` caps sensor universes at 5 partitions                     | `prod`                           |
| `TRACKED_OPTION_UNDERLYINGS` | Comma-list of underlyings for `option_contracts_sync`           | `NIFTY,BANKNIFTY,FINNIFTY,SENSEX`|
| `DAGSTER_HOME`               | Dagster instance dir                                            | `./.dagster_home` (set by `run.sh`) |

---

## 9. How to extend

### Add a new data group

1. Create `stock_pipeline/{group_name}/assets.py` with three assets:
   `raw_*`, `processed_*`, `*_parquet`. Mirror the shape of an existing
   group with similar shape (daily-style → look at `daily_eq`;
   per-underlying option-style → look at `intraday_op`).
2. Add the corresponding `fetch_*` method on `KiteSource` and
   `CsvSource` (and `FakeKiteSource` if you want offline runs).
3. If the partition pool doesn't already exist, define it in
   `core/partitions.py` and add a `*_sync` sensor in the new group dir.
4. Wire the assets and sensor into `stock_pipeline/__init__.py`.

### Add a new storage backend

1. Add a class under `core/destinations/` implementing `Destination`'s
   `read`/`write`/`exists` (or higher-level emitters if the shape
   demands it, like `LeanStorage`).
2. Wire as a resource in `stock_pipeline/__init__.py`.
3. In each write-stage asset that should support it, extend
   `_VALID_STORAGES` and the dispatch branch.

### Add a new source

1. Add a class under `core/sources/` implementing the same `fetch_*`
   methods as `KiteSource` / `CsvSource`. Returning an empty frame for
   unsupported `(instrument_type, interval)` cells is the convention.
2. Wire as a resource in `stock_pipeline/__init__.py`.
3. Extend the `source` tag whitelist and the dispatch in each `raw_*`
   asset that should accept it.

---

## 10. Implementation status

Implemented (commit / roadmap snapshot at the time of this doc):

| Group         | `source=csv` | `source=kite`     | `storage=local` | `storage=lean` |
|---------------|--------------|-------------------|-----------------|----------------|
| `daily_eq`    | ✓            | stub (no-op)      | ✓               | ✓              |
| `daily_op`    | ✓            | stub (no-op)      | ✓               | ✓              |
| `intraday_eq` | ✓            | ✓                 | ✓               | _pending_      |
| `intraday_op` | ✓            | stub (no-op)      | ✓               | ✓              |

The `option_contracts_sync` sensor cap-batching (2000/tick) and the
`tag_concurrency_limits` rule (one run per partition value) are both
production-side guarantees the pipeline now relies on; deferred work
includes wiring `storage=lean` for `intraday_eq`,
populating the LEAN universe row's underlying bar, and turning the
Kite stubs into real fetches for option flows.
