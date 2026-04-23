"""Protocol for broker source adapters.

Every source must implement one fetch method per (instrument_type, interval)
cell we care about. Daily methods below; intraday twins go next to them when
the intraday pipeline is added.
"""

from datetime import date
from typing import Protocol, runtime_checkable

import pandas as pd


@runtime_checkable
class BasedSource(Protocol):
    def fetch_daily_eq(
        self, symbol: str, instrument_token: int, on: date
    ) -> pd.DataFrame: ...

    def fetch_daily_fut(
        self, symbol: str, instrument_token: int, on: date
    ) -> pd.DataFrame: ...

    def fetch_daily_call(
        self, symbol: str, instrument_token: int, on: date
    ) -> pd.DataFrame: ...

    def fetch_daily_put(
        self, symbol: str, instrument_token: int, on: date
    ) -> pd.DataFrame: ...
