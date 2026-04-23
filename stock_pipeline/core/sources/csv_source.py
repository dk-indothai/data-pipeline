"""CSV-backed source adapter.

Reads pre-downloaded OHLCV from a directory laid out as:

    {root_dir}/daily/eq/{SYMBOL}.csv
    {root_dir}/daily/fut/{SYMBOL}.csv
    {root_dir}/daily/call/{SYMBOL}.csv
    {root_dir}/daily/put/{SYMBOL}.csv

CSV schema: `date,open,high,low,close,volume` (plus `oi` when present).
`date` is parsed leniently — the timestamp's calendar date is matched
against the requested `on`, so both naive (`2000-01-03T05:30:00.000`)
and tz-aware (`2015-02-02 09:15:00+05:30`) formats work.

Return shape matches KiteSource so call sites don't branch on source type.
"""
from datetime import date
from pathlib import Path

import pandas as pd
from dagster import ConfigurableResource, get_dagster_logger


class CsvSource(ConfigurableResource):
    """File-backed source for offline replays and tests.

    `root_dir` may be absolute or relative to Dagster's working directory.
    """

    root_dir: str = "CustomSource"

    def _path(self, bucket: str, symbol: str) -> Path:
        return Path(self.root_dir) / "daily" / bucket / f"{symbol}.csv"

    def _fetch(
        self, symbol: str, on: date, bucket: str, with_oi: bool
    ) -> pd.DataFrame:
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
