"""Kite Connect source adapter.

Kite's historical endpoint is the same for equity, futures, and options —
only `oi` flips between False (equity) and True (F&O). The four protocol
methods exist so call sites read naturally; internally they funnel through
one helper.
"""
from datetime import date, timedelta

import pandas as pd
from dagster import ConfigurableResource, get_dagster_logger
from kiteconnect import KiteConnect

# Kite's historical_data caps daily candles at 2000 days per call.
_DAILY_CHUNK_DAYS = 2000


class KiteSource(ConfigurableResource):
    """api_key and access_token are editable per-run from the Launchpad.

    access_token expires daily (Zerodha regenerates it via OAuth at ~6am IST) —
    override it in the Launchpad or rotate it in .env and restart `dagster dev`.
    """

    api_key: str
    access_token: str

    def _client(self) -> KiteConnect:
        if not self.api_key or not self.access_token:
            raise ValueError(
                "KiteSource: api_key and access_token are required. "
                "Set KITE_API_KEY and KITE_ACCESS_TOKEN in .env, or override "
                "them in the Launchpad."
            )
        kite = KiteConnect(api_key=self.api_key)
        kite.set_access_token(self.access_token)
        return kite

    def _fetch(
        self, symbol: str, instrument_token: int, on: date, with_oi: bool
    ) -> pd.DataFrame:
        raw = self._client().historical_data(
            instrument_token=instrument_token,
            from_date=on,
            to_date=on,
            interval="day",
            oi=with_oi,
        )
        if not raw:
            get_dagster_logger().warning(f"Kite returned no data for {symbol} on {on}")
            return pd.DataFrame()

        df = pd.DataFrame(raw)
        df["symbol"] = symbol
        df["date"] = on.isoformat()
        cols = ["symbol", "date", "open", "high", "low", "close", "volume"]
        if with_oi and "oi" in df.columns:
            cols.append("oi")
        return df[cols]

    def fetch_daily_eq(
        self, symbol: str, instrument_token: int, on: date
    ) -> pd.DataFrame:
        return self._fetch(symbol, instrument_token, on, with_oi=False)

    def fetch_daily_eq_range(
        self,
        symbol: str,
        instrument_token: int,
        from_date: date,
        to_date: date,
    ) -> pd.DataFrame:
        client = self._client()
        log = get_dagster_logger()
        frames: list[pd.DataFrame] = []
        cursor = from_date
        while cursor <= to_date:
            chunk_end = min(
                cursor + timedelta(days=_DAILY_CHUNK_DAYS - 1), to_date
            )
            raw = client.historical_data(
                instrument_token=instrument_token,
                from_date=cursor,
                to_date=chunk_end,
                interval="day",
                oi=False,
            )
            if raw:
                frames.append(pd.DataFrame(raw))
            cursor = chunk_end + timedelta(days=1)

        if not frames:
            log.warning(
                f"Kite returned no data for {symbol} in [{from_date}, {to_date}]"
            )
            return pd.DataFrame()

        df = pd.concat(frames, ignore_index=True)
        df["symbol"] = symbol
        df["date"] = pd.to_datetime(df["date"]).dt.date.astype(str)
        return df[["symbol", "date", "open", "high", "low", "close", "volume"]]

    def fetch_daily_fut(
        self, symbol: str, instrument_token: int, on: date
    ) -> pd.DataFrame:
        return self._fetch(symbol, instrument_token, on, with_oi=True)

    def fetch_daily_call(
        self, symbol: str, instrument_token: int, on: date
    ) -> pd.DataFrame:
        return self._fetch(symbol, instrument_token, on, with_oi=True)

    def fetch_daily_put(
        self, symbol: str, instrument_token: int, on: date
    ) -> pd.DataFrame:
        return self._fetch(symbol, instrument_token, on, with_oi=True)
