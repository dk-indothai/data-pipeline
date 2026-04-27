"""CSV-backed source adapter.

Reads pre-downloaded OHLCV from a directory laid out as:

    {root_dir}/daily/eq/{SYMBOL}.csv
    {root_dir}/daily/fut/{SYMBOL}.csv
    {root_dir}/daily/call/{SYMBOL}.csv
    {root_dir}/daily/put/{SYMBOL}.csv
    {root_dir}/intraday/eq/{SYMBOL}.csv
    {root_dir}/intraday/op/{UNDERLYING}.csv  (multi-contract, fan out by Ticker)

CSV schema: `date,open,high,low,close,volume` (plus `oi` when present).
`date` is parsed leniently — the timestamp's calendar date is matched
against the requested `on`, so both naive (`2000-01-03T05:30:00.000`)
and tz-aware (`2015-02-02 09:15:00+05:30`) formats work.

Daily methods return one row per day with `date` as an ISO date string.
Intraday methods preserve the full timestamp as an ISO datetime string.

Return shape matches KiteSource so call sites don't branch on source type.
"""

from datetime import date
from pathlib import Path

import pandas as pd
import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.csv as pacsv
from dagster import ConfigurableResource, get_dagster_logger

# Rename map for legacy option CSVs that use the public output-schema casing
# directly. Applied with rename(), so missing keys are no-ops — safe for
# lowercase-schema files too. Extra columns (Symbol, Strike, ...) get dropped
# when the method projects to the final column list.
_OP_COLUMN_NORMALIZE = {
    "DateTime": "datetime",
    "Open": "open",
    "High": "high",
    "Low": "low",
    "Close": "close",
    "Volume": "volume",
    "Open Interest": "oi",
    "Strike": "strike",
    "Expiry": "expiry",
    "OptionType": "option_type",
    "lot size": "lot_size",
    # Ticker holds the contract trading symbol (e.g. BANKNIFTY24D1910000CE)
    # in multi-contract intraday CSVs. daily_op's per-contract files don't
    # need it (filename is the contract) and project it out anyway.
    "Ticker": "contract",
}


class CsvSource(ConfigurableResource):
    """File-backed source for offline replays and tests.

    `root_dir` may be absolute or relative to Dagster's working directory.
    """

    root_dir: str = "CustomSource"

    def _path(self, bucket: str, symbol: str) -> Path:
        return Path(self.root_dir) / "daily" / bucket / f"{symbol}.csv"

    def _fetch(self, symbol: str, on: date, bucket: str, with_oi: bool) -> pd.DataFrame:
        path = self._path(bucket, symbol)
        log = get_dagster_logger()
        if not path.exists():
            log.warning(f"CsvSource: no file at {path} for {symbol}")
            return pd.DataFrame()

        df = pd.read_csv(path)
        df["date"] = pd.to_datetime(df["date"], utc=False, format="mixed")
        mask = df["date"].dt.date == on
        df = df.loc[mask].copy()
        if df.empty:
            log.warning(f"CsvSource: no rows for {symbol} on {on} in {path}")
            return pd.DataFrame()

        df["symbol"] = symbol
        df["date"] = on.isoformat()
        cols = ["symbol", "date", "open", "high", "low", "close", "volume"]
        if with_oi and "oi" in df.columns:
            cols.append("oi")
        return df[cols]

    def fetch_daily_eq(
        self, symbol: str, instrument_token: int, on: date
    ) -> pd.DataFrame:
        return self._fetch(symbol, on, bucket="eq", with_oi=False)

    def fetch_daily_eq_range(
        self,
        symbol: str,
        instrument_token: int,
        from_date: date,
        to_date: date,
    ) -> pd.DataFrame:
        path = self._path("eq", symbol)
        log = get_dagster_logger()
        if not path.exists():
            log.warning(f"CsvSource: no file at {path} for {symbol}")
            return pd.DataFrame()

        df = pd.read_csv(path)
        df["date"] = pd.to_datetime(df["date"], utc=False, format="mixed")
        mask = (df["date"].dt.date >= from_date) & (df["date"].dt.date <= to_date)
        df = df.loc[mask].copy()
        if df.empty:
            log.warning(
                f"CsvSource: no rows for {symbol} in [{from_date}, {to_date}] in {path}"
            )
            return pd.DataFrame()

        df["symbol"] = symbol
        df["date"] = df["date"].dt.date.astype(str)
        return df[["symbol", "date", "open", "high", "low", "close", "volume"]]

    def fetch_intraday_eq_range(
        self,
        symbol: str,
        instrument_token: int,
        from_date: date,
        to_date: date,
    ) -> pd.DataFrame:
        # Read the per-symbol CSV once and filter to the whole range.
        # instrument_token is unused here but kept to match KiteSource.
        path = Path(self.root_dir) / "intraday" / "eq" / f"{symbol}.csv"
        log = get_dagster_logger()
        if not path.exists():
            log.warning(f"CsvSource: no file at {path} for {symbol}")
            return pd.DataFrame()

        df = pd.read_csv(path)
        # `format="mixed"` handles both naive and tz-aware timestamps.
        df["date"] = pd.to_datetime(df["date"], utc=False, format="mixed")
        mask = (df["date"].dt.date >= from_date) & (df["date"].dt.date <= to_date)
        df = df.loc[mask].copy()
        if df.empty:
            log.warning(
                f"CsvSource: no intraday rows for {symbol} in [{from_date}, {to_date}]"
            )
            return pd.DataFrame()

        df["symbol"] = symbol
        # Store full timestamp as string so parquet round-trips cleanly
        # across pandas versions; downstream slices first 10 chars for date.
        df["date"] = df["date"].astype(str)
        return df[["symbol", "date", "open", "high", "low", "close", "volume"]]

    def fetch_daily_op_range(
        self,
        contract: str,
        underlying: str,
        instrument_token: int,
        from_date: date,
        to_date: date,
    ) -> pd.DataFrame:
        # Layout: {root}/daily/op/{UNDERLYING}/{CONTRACT}.csv. The underlying
        # subdir keeps the options tree navigable even with 50k+ contracts.
        # instrument_token is unused here but kept to match KiteSource.
        path = Path(self.root_dir) / "daily" / "op" / underlying / f"{contract}.csv"
        log = get_dagster_logger()
        if not path.exists():
            log.warning(f"CsvSource: no file at {path} for {contract}")
            return pd.DataFrame()

        df = pd.read_csv(path)
        # Option CSVs ship with the output-schema casing (DateTime, Open, ...).
        # Normalize to the lowercase raw schema the asset stage expects.
        df = df.rename(columns=_OP_COLUMN_NORMALIZE)
        if "datetime" not in df.columns:
            log.warning(
                f"CsvSource: {path} missing `datetime`/`DateTime` column "
                f"(found: {list(df.columns)})"
            )
            return pd.DataFrame()

        df["datetime"] = pd.to_datetime(df["datetime"], utc=False, format="mixed")
        mask = (df["datetime"].dt.date >= from_date) & (
            df["datetime"].dt.date <= to_date
        )
        df = df.loc[mask].copy()
        if df.empty:
            log.warning(
                f"CsvSource: no op rows for {contract} in [{from_date}, {to_date}]"
            )
            return pd.DataFrame()

        df["datetime"] = df["datetime"].astype(str)

        # Normalize expiry to YYYY-MM-DD so parquet round-trips cleanly across
        # re-runs. Rows where parsing fails keep their original string value.
        if "expiry" in df.columns:
            parsed = pd.to_datetime(df["expiry"], errors="coerce", format="mixed")
            df["expiry"] = parsed.dt.strftime("%Y-%m-%d").fillna(
                df["expiry"].astype(str)
            )

        cols = ["datetime", "open", "high", "low", "close", "volume"]
        for opt in ("oi", "strike", "expiry", "option_type", "lot_size"):
            if opt in df.columns:
                cols.append(opt)
        return df[cols]

    def fetch_intraday_op_range(
        self,
        underlying: str,
        from_date: date,
        to_date: date,
    ) -> pd.DataFrame:
        # Layout: {root}/intraday/op/{UNDERLYING}.csv. One file per
        # underlying carrying every contract's minute candles — the asset
        # fans out by `contract` at write time.
        #
        # These files run into multiple GB. `pd.read_csv` would load and
        # type-infer the whole thing before we filter, blowing both RAM
        # and wall time. We stream batches through pyarrow's CSV reader
        # and apply the date filter as a cheap prefix comparison on the
        # raw `DateTime` string (the ISO format `YYYY-MM-DD HH:MM:SS` is
        # lexicographically ordered on its first 10 chars), so RAM peak
        # scales with the matching slice — not the file.
        path = Path(self.root_dir) / "intraday" / "op" / f"{underlying}.csv"
        log = get_dagster_logger()
        if not path.exists():
            log.warning(f"CsvSource: no file at {path} for {underlying}")
            return pd.DataFrame()

        from_str = from_date.isoformat()
        to_str = to_date.isoformat()
        # - DateTime: string so the per-batch filter is a prefix compare
        #   (no timestamp parsing on rejected rows).
        # - Numeric columns: float64 explicitly. Pyarrow infers per-batch;
        #   if the first batch happens to be all-int (e.g. lot size = 50)
        #   it locks int64, then a later `25.0` blows up the read. Float
        #   handles both shapes uniformly.
        convert_opts = pacsv.ConvertOptions(
            column_types={
                "DateTime": pa.string(),
                "Strike": pa.float64(),
                "Open": pa.float64(),
                "High": pa.float64(),
                "Low": pa.float64(),
                "Close": pa.float64(),
                "Volume": pa.float64(),
                "Open Interest": pa.float64(),
                "lot size": pa.float64(),
            },
        )

        matching: list[pa.RecordBatch] = []
        with pacsv.open_csv(str(path), convert_options=convert_opts) as reader:
            schema_names = reader.schema.names
            if "DateTime" not in schema_names:
                log.warning(
                    f"CsvSource: {path} missing `DateTime` column "
                    f"(found: {schema_names})"
                )
                return pd.DataFrame()
            if "Ticker" not in schema_names:
                log.warning(
                    f"CsvSource: {path} missing `Ticker` column "
                    f"(found: {schema_names}) — cannot fan out by contract"
                )
                return pd.DataFrame()
            for batch in reader:
                date_part = pc.utf8_slice_codeunits(batch.column("DateTime"), 0, 10)
                mask = pc.and_(
                    pc.greater_equal(date_part, pa.scalar(from_str)),
                    pc.less_equal(date_part, pa.scalar(to_str)),
                )
                kept = batch.filter(mask)
                if kept.num_rows:
                    matching.append(kept)

        if not matching:
            log.warning(
                f"CsvSource: no intraday op rows for {underlying} "
                f"in [{from_date}, {to_date}]"
            )
            return pd.DataFrame()

        df = pa.Table.from_batches(matching).to_pandas()
        df = df.rename(columns=_OP_COLUMN_NORMALIZE)
        # Already filtered + already string; no re-parse needed.
        df["datetime"] = df["datetime"].astype(str)

        if "expiry" in df.columns:
            parsed = pd.to_datetime(df["expiry"], errors="coerce", format="mixed")
            df["expiry"] = parsed.dt.strftime("%Y-%m-%d").fillna(
                df["expiry"].astype(str)
            )

        # Override the Ticker-derived `contract` with the daily_op naming
        # convention: {UNDERLYING}_{STRIKE}_{TYPE}_{DD}_{MON}_{YY}. Uses
        # the function arg `underlying` (single value per call) rather
        # than the CSV's `Symbol` column, so a stray row with a wrong
        # Symbol can't corrupt the filename.
        if {"strike", "expiry", "option_type"}.issubset(df.columns):
            exp_dt = pd.to_datetime(df["expiry"], format="%Y-%m-%d", errors="coerce")
            strike_int = df["strike"].astype(float).astype("Int64").astype(str)
            df["contract"] = (
                underlying
                + "_" + strike_int
                + "_" + df["option_type"].astype(str)
                + "_" + exp_dt.dt.day.map("{:02d}".format)
                + "_" + exp_dt.dt.strftime("%b").str.upper()
                + "_" + exp_dt.dt.year.mod(100).map("{:02d}".format)
            )

        cols = ["datetime", "contract", "open", "high", "low", "close", "volume"]
        for opt in ("oi", "strike", "expiry", "option_type", "lot_size"):
            if opt in df.columns:
                cols.append(opt)
        return df[cols]

    def fetch_daily_fut(
        self, symbol: str, instrument_token: int, on: date
    ) -> pd.DataFrame:
        return self._fetch(symbol, on, bucket="fut", with_oi=True)

    def fetch_daily_call(
        self, symbol: str, instrument_token: int, on: date
    ) -> pd.DataFrame:
        return self._fetch(symbol, on, bucket="call", with_oi=True)

    def fetch_daily_put(
        self, symbol: str, instrument_token: int, on: date
    ) -> pd.DataFrame:
        return self._fetch(symbol, on, bucket="put", with_oi=True)
