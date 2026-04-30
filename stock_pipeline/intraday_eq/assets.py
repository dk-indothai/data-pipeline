"""Intraday equity pipeline: fetch minute candles over a date range.

One partition is one symbol; one run processes every weekday in
[start_date, end_date] and writes a separate parquet (or LEAN zip) per
trading day.

Tags:
  source        "kite" | "csv"         default: kite (kite is no-op stub)
  start_date    YYYY-MM-DD             default: 2015-02-02 (earliest CSV)
  end_date      YYYY-MM-DD             default: today
  storage       "local" | "lean"       default: local

Output:
  storage=local  data/intraday/eq/{symbol}/{date}.parquet
  storage=lean   lean_data/equity/{country}/minute/{symbol}/{YYYYMMDD}_trade.zip
"""

from datetime import date as date_cls

import pandas as pd
from dagster import AssetExecutionContext, asset
from sqlalchemy import select

from stock_pipeline.core.db import PostgresResource
from stock_pipeline.core.destinations.lean import LeanStorage
from stock_pipeline.core.destinations.local import LocalStorage
from stock_pipeline.core.models import Instrument
from stock_pipeline.core.partitions import equity_symbols
from stock_pipeline.core.sources.csv_source import CsvSource
from stock_pipeline.core.sources.kite import KiteSource
from stock_pipeline.core.tags import (
    DEFAULT_SOURCE,
    DEFAULT_STORAGE,
    TAG_END_DATE,
    TAG_SOURCE,
    TAG_START_DATE,
    TAG_STORAGE,
)

GROUP = "intraday_eq"

# Whitelist grows as more Destination impls land (s3, drive_local, ...).
_VALID_STORAGES = ("local", "lean")

# Earliest date present in the intraday CSVs; daily_eod's 2000-01-01 default
# is too wide and would iterate ~15 years of weekends for nothing.
DEFAULT_INTRADAY_START_DATE = "2015-02-02"

# Helper column carrying the trading day; added in raw, read in the write
# asset for path routing, dropped before parquet serialization. Underscore
# prefix matches daily_op's `_underlying`/`_contract` convention.
_TRADING_DATE_COL = "_trading_date"


@asset(partitions_def=equity_symbols, group_name=GROUP)
def raw_intraday(
    context: AssetExecutionContext,
    kite: KiteSource,
    csv: CsvSource,
    db: PostgresResource,
) -> pd.DataFrame:
    symbol = context.partition_key
    tags = context.run.tags

    source = tags.get(TAG_SOURCE, DEFAULT_SOURCE)
    if source not in ("kite", "csv"):
        raise ValueError(
            f"tag '{TAG_SOURCE}'='{source}' invalid — must be 'kite' or 'csv'"
        )

    if source == "kite":
        context.log.info(
            f"source=kite for {symbol} — skipping (not implemented for intraday eq)"
        )
        return pd.DataFrame()
    elif source == "csv":
        start_tag = tags.get(TAG_START_DATE, DEFAULT_INTRADAY_START_DATE)
        end_tag = tags.get(TAG_END_DATE)
        try:
            from_date = date_cls.fromisoformat(start_tag)
            to_date = date_cls.fromisoformat(end_tag) if end_tag else date_cls.today()
        except ValueError as e:
            raise ValueError(
                f"bad date tag — start_date={start_tag!r} end_date={end_tag!r}: {e}"
            ) from e
        if to_date < from_date:
            raise ValueError(f"end_date {to_date} < start_date {from_date}")

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
                f"{symbol} not found in instruments as NSE/EQ tradingsymbol — "
                f"check the sensor or the universe filter."
            )

        context.log.info(
            f"Fetching intraday {symbol} (token={token}) "
            f"[{from_date}, {to_date}] via csv"
        )
        df = csv.fetch_intraday_eq_range(
            symbol=symbol,
            instrument_token=token,
            from_date=from_date,
            to_date=to_date,
        )
        if df.empty:
            context.log.warning(
                f"No intraday data for {symbol} in [{from_date}, {to_date}]"
            )
            return df

        # First 10 chars of the ISO timestamp are the YYYY-MM-DD slot — avoids
        # reparsing the full datetime on a multi-million-row frame.
        df[_TRADING_DATE_COL] = df["date"].str[:10]
        return df
    else:
        context.log.warning(f"unsupported source: {source}")
        return pd.DataFrame()


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
    lean: LeanStorage,
) -> None:
    if processed_intraday.empty:
        context.log.warning("Empty DataFrame — skipping write")
        return

    storage = context.run.tags.get(TAG_STORAGE, DEFAULT_STORAGE)
    if storage not in _VALID_STORAGES:
        raise ValueError(
            f"tag '{TAG_STORAGE}'='{storage}' invalid — must be one of {_VALID_STORAGES}"
        )

    symbol = context.partition_key

    if storage == "lean":
        # Per-day zips at equity/{country}/minute/{symbol}/. LeanStorage
        # owns its own overwrite semantics, so this branch stays tight.
        written = 0
        for trading_date, group in processed_intraday.groupby(
            _TRADING_DATE_COL, sort=False
        ):
            n = lean.write_minute_equity_day(
                group.drop(columns=[_TRADING_DATE_COL]),
                symbol=symbol,
                date_str=trading_date,
            )
            context.log.info(f"LEAN: wrote {n} file(s) for {symbol} on {trading_date}")
            written += n
        context.log.info(
            f"{symbol}: wrote {written} LEAN file(s) via storage={storage}"
        )
        return
    elif storage == "local":
        # storage == "local": one parquet per trading day. Writes overwrite
        # atomically — re-materializing a range refreshes every file in scope.
        written = 0
        for trading_date, group in processed_intraday.groupby(_TRADING_DATE_COL):
            rel = f"intraday/eq/{symbol}/{trading_date}.parquet"
            local.write(group.drop(columns=[_TRADING_DATE_COL]), rel)
            context.log.info(f"Wrote {rel} ({len(group)} rows)")
            written += 1

        context.log.info(
            f"{symbol}: wrote {written} parquet files via storage={storage}"
        )
    else:
        context.log.warning(f"unsupported storage: {storage}")
