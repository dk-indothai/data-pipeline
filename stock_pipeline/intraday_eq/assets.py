"""Intraday equity pipeline: fetch minute candles over a date range.

One partition is one symbol; one run processes every weekday in
[start_date, end_date] and writes a separate parquet per trading day.

Tags:
  source        "kite" | "csv"         default: kite
  start_date    YYYY-MM-DD             default: 2015-02-02 (earliest CSV)
  end_date      YYYY-MM-DD             default: today

Output: data/intraday/equity/{SYMBOL}/{DATE}.parquet (one file per date
that had data; empty days are skipped).
"""

from datetime import date as date_cls

import pandas as pd
from dagster import AssetExecutionContext, asset
from sqlalchemy import select

from stock_pipeline.core.db import PostgresResource
from stock_pipeline.core.destinations.base import DEFAULT_STORAGE, TAG_STORAGE
from stock_pipeline.core.destinations.local import LocalStorage
from stock_pipeline.core.models import Instrument
from stock_pipeline.core.partitions import equity_symbols
from stock_pipeline.core.sources.csv_source import CsvSource
from stock_pipeline.core.sources.kite import KiteSource
from stock_pipeline.daily_eq.assets import (
    DEFAULT_SOURCE,
    TAG_END_DATE,
    TAG_SOURCE,
    TAG_START_DATE,
)

GROUP = "intraday_eq"

# Whitelist grows as more Destination impls land (s3, drive_local, ...).
_VALID_STORAGES = ("local",)

# Earliest date present in the intraday CSVs; daily_eod's 2000-01-01 default
# is too wide and would iterate ~15 years of weekends for nothing.
DEFAULT_INTRADAY_START_DATE = "2015-02-02"

# Column carrying the trading day; added to the raw frame so downstream assets
# don't need to re-parse the minute-level timestamp to group by date.
TRADING_DATE_COL = "trading_date"


def _resolve_tags(tags: dict[str, str]) -> tuple[str, date_cls, date_cls]:
    """Pull (source, start_date, end_date) from run tags, with defaults."""
    source = tags.get(TAG_SOURCE, DEFAULT_SOURCE)
    if source not in ("kite", "csv"):
        raise ValueError(
            f"tag '{TAG_SOURCE}'='{source}' invalid — must be 'kite' or 'csv'"
        )
    start = date_cls.fromisoformat(
        tags.get(TAG_START_DATE, DEFAULT_INTRADAY_START_DATE)
    )
    end_tag = tags.get(TAG_END_DATE)
    end = date_cls.fromisoformat(end_tag) if end_tag else date_cls.today()
    if end < start:
        raise ValueError(f"end_date {end} < start_date {start}")
    return source, start, end


@asset(partitions_def=equity_symbols, group_name=GROUP)
def raw_intraday(
    context: AssetExecutionContext,
    kite: KiteSource,
    csv: CsvSource,
    db: PostgresResource,
) -> pd.DataFrame:
    symbol = context.partition_key
    source, start, end = _resolve_tags(context.run.tags)

    # Kite's historical API requires instrument_token, not tradingsymbol.
    with db.session() as s:
        token = s.execute(
            select(Instrument.instrument_token)
            .where(Instrument.tradingsymbol == symbol)
            .where(Instrument.exchange == "NSE")
            .where(Instrument.instrument_type == "EQ")
        ).scalar_one_or_none()

    if token is None:
        raise ValueError(
            f"{symbol} not found in instruments as NSE/EQ — "
            f"check the sensor or the universe filter."
        )

    # Both sources expose the same fetch_intraday_eq_range signature (duck-
    # typed via core/sources/base.py protocol); one range call, then the
    # terminal asset splits by trading date at write time.
    src = kite if source == "kite" else csv
    context.log.info(
        f"Fetching intraday {symbol} (token={token}) [{start}, {end}] via {source}"
    )
    df = src.fetch_intraday_eq_range(
        symbol=symbol, instrument_token=token, from_date=start, to_date=end
    )
    if df.empty:
        context.log.warning(f"No intraday data for {symbol} in [{start}, {end}]")
        return df

    # First 10 chars of the ISO timestamp are the YYYY-MM-DD slot — avoids
    # reparsing the full datetime on a multi-million-row frame.
    df[TRADING_DATE_COL] = df["date"].str[:10]
    return df


@asset(partitions_def=equity_symbols, group_name=GROUP)
def processed_intraday(
    context: AssetExecutionContext, raw_intraday: pd.DataFrame
) -> pd.DataFrame:
    # Pass-through for now. Put tz normalization, schema coercion, halt-day
    # flagging, and split-adjusted-close derivation here when needed.
    if raw_intraday.empty:
        context.log.warning("Upstream empty — nothing to process")
        return raw_intraday
    return raw_intraday.copy()


@asset(partitions_def=equity_symbols, group_name=GROUP)
def intraday_parquet(
    context: AssetExecutionContext,
    processed_intraday: pd.DataFrame,
    local: LocalStorage,
) -> None:
    if processed_intraday.empty:
        context.log.warning("Empty DataFrame — skipping write")
        return

    storage = context.run.tags.get(TAG_STORAGE, DEFAULT_STORAGE)
    if storage not in _VALID_STORAGES:
        raise ValueError(
            f"tag '{TAG_STORAGE}'='{storage}' invalid — must be one of {_VALID_STORAGES}"
        )
    dest = local  # only local impl today; dispatch grows when S3/Drive land.

    symbol = context.partition_key

    # Split by trading_date and write one parquet per day. Writes overwrite
    # atomically — re-materializing a range refreshes every file in scope.
    written = 0
    for trading_date, group in processed_intraday.groupby(TRADING_DATE_COL):
        rel = f"intraday/eq/{symbol}/{trading_date}.parquet"
        dest.write(group.drop(columns=[TRADING_DATE_COL]), rel)
        context.log.info(f"Wrote {rel} ({len(group)} rows)")
        written += 1

    context.log.info(f"{symbol}: wrote {written} parquet files via storage={storage}")
