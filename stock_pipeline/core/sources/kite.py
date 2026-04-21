"""Kite Connect source adapter."""
from datetime import date

import pandas as pd
from dagster import ConfigurableResource, get_dagster_logger
from kiteconnect import KiteConnect


class KiteSource(ConfigurableResource):
    """api_key and access_token are editable per-run from the Launchpad.

    access_token expires daily (Zerodha regenerates it via OAuth at ~6am IST) —
    override it in the Launchpad or rotate it in .env and restart `dagster dev`.
    """

    api_key: str
    access_token: str

    def fetch_daily(
        self, symbol: str, instrument_token: int, on: date
    ) -> pd.DataFrame:
        if not self.api_key or not self.access_token:
            raise ValueError(
                "KiteSource: api_key and access_token are required. "
                "Set KITE_API_KEY and KITE_ACCESS_TOKEN in .env, or override "
                "them in the Launchpad."
            )

        kite = KiteConnect(api_key=self.api_key)
        kite.set_access_token(self.access_token)
        raw = kite.historical_data(
            instrument_token=instrument_token,
            from_date=on,
            to_date=on,
            interval="day",
        )
        if not raw:
            get_dagster_logger().warning(f"Kite returned no data for {symbol} on {on}")
            return pd.DataFrame()

        df = pd.DataFrame(raw)
        df["symbol"] = symbol
        df["date"] = on.isoformat()
        return df[["symbol", "date", "open", "high", "low", "close", "volume"]]
