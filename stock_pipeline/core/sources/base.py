"""Protocol for broker source adapters."""
from datetime import date
from typing import Protocol, runtime_checkable

import pandas as pd


@runtime_checkable
class BaseSource(Protocol):
    """Minimal interface for daily EOD data.

    Extend with fetch_intraday / fetch_fno when those data types land. Each
    source decides which methods it can implement; assets type-hint against
    this protocol so the concrete resource can be swapped at Definitions load.
    """

    def fetch_daily(
        self,
        symbol: str,
        instrument_token: int,
        on: date,
    ) -> pd.DataFrame: ...
