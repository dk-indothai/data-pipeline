"""Daily options pipeline: one partition per option contract tradingsymbol.

Each run fetches OHLCV + OI for [start_date, end_date], enriches with
contract metadata (strike, expiry, option_type, lot_size) from the
instruments table, and writes a merged+deduped parquet at
  daily/op/{UNDERLYING}/{CONTRACT}.parquet

Output columns (fixed schema, exact casing):
  DateTime, Strike, Expiry, OptionType, Open, High, Low, Close, Volume,
  Open Interest, lot size

Tags:
  source        "kite" | "csv"         default: kite
  start_date    YYYY-MM-DD             default: 2000-01-01
  end_date      YYYY-MM-DD             default: today
  storage       "local"                default: local
"""

from datetime import date as date_cls

import pandas as pd
from dagster import AssetExecutionContext, asset
from sqlalchemy import select

from stock_pipeline.core.db import PostgresResource
from stock_pipeline.core.destinations.base import DEFAULT_STORAGE, TAG_STORAGE
from stock_pipeline.core.destinations.local import LocalStorage
from stock_pipeline.core.models import Instrument
from stock_pipeline.core.partitions import option_contracts
from stock_pipeline.core.sources.csv_source import CsvSource
from stock_pipeline.core.sources.kite import KiteSource
from stock_pipeline.daily_eq.assets import (
    DEFAULT_SOURCE,
    DEFAULT_START_DATE,
    TAG_END_DATE,
    TAG_SOURCE,
    TAG_START_DATE,
)

GROUP = "daily_op"

# Whitelist grows as more Destination impls land.
_VALID_STORAGES = ("local",)

# Helper column — set in raw, read in the write asset for path routing,
# dropped before parquet serialization. Leading underscore marks it private.
_UNDERLYING_COL = "_underlying"

# Raw-column → output-column rename map. Preserves the user's exact casing
# (note: "Open Interest" has a space, "lot size" is lowercase with a space).
_RENAME = {
    "date": "DateTime",
    "strike": "Strike",
    "expiry": "Expiry",
    "option_type": "OptionType",
    "open": "Open",
    "high": "High",
    "low": "Low",
    "close": "Close",
    "volume": "Volume",
    "oi": "Open Interest",
    "lot_size": "lot size",
}
_OUTPUT_COLS = list(_RENAME.values())


@asset(partitions_def=option_contracts, group_name=GROUP)
def raw_option_daily(
    context: AssetExecutionContext,
    kite: KiteSource,
    csv: CsvSource,
    db: PostgresResource,
) -> pd.DataFrame:
    contract = context.partition_key
    tags = context.run.tags

    source = tags.get(TAG_SOURCE, DEFAULT_SOURCE)
    if source not in ("kite", "csv"):
        raise ValueError(
            f"tag '{TAG_SOURCE}'='{source}' invalid — must be 'kite' or 'csv'"
        )

    from_date = date_cls.fromisoformat(tags.get(TAG_START_DATE, DEFAULT_START_DATE))
    end_tag = tags.get(TAG_END_DATE)
    to_date = date_cls.fromisoformat(end_tag) if end_tag else date_cls.today()

    # Fetch the full Instrument row so we can attach strike/expiry/type/lot
    # to every OHLCV row without a second DB hit in processed_option_daily.
    with db.session() as s:
        inst = s.execute(
            select(Instrument)
            .where(Instrument.tradingsymbol == contract)
            .where(Instrument.instrument_type.in_(("CE", "PE")))
        ).scalar_one_or_none()

    if inst is None:
        raise ValueError(
            f"{contract} not found in instruments as CE/PE — "
            f"check the sensor or the universe filter."
        )
    if inst.name is None:
        raise ValueError(
            f"{contract} has null `name` in instruments — "
            f"cannot route output path without an underlying."
        )

    src = kite if source == "kite" else csv
    context.log.info(
        f"Fetching op daily {contract} (token={inst.instrument_token}, "
        f"name={inst.name}) [{from_date}, {to_date}] via {source}"
    )
    df = src.fetch_daily_op_range(
        contract=contract,
        underlying=inst.name,
        instrument_token=inst.instrument_token,
        from_date=from_date,
        to_date=to_date,
    )
    context.log.info(
        f"fetched {len(df)} rows for {contract} via {source} in [{from_date}, {to_date}]"
    )
    if df.empty:
        return df

    # Attach contract metadata — broadcast across all bars (constant per run).
    df["strike"] = inst.strike
    df["expiry"] = inst.expiry.isoformat() if inst.expiry else None
    df["option_type"] = inst.instrument_type
    df["lot_size"] = inst.lot_size
    df[_UNDERLYING_COL] = inst.name

    # OI may be missing from legacy CSVs; materialize an empty column so
    # the rename + output-projection in the next asset has something to hit.
    if "oi" not in df.columns:
        df["oi"] = pd.NA

    return df


@asset(partitions_def=option_contracts, group_name=GROUP)
def processed_option_daily(
    context: AssetExecutionContext, raw_option_daily: pd.DataFrame
) -> pd.DataFrame:
    # Rename to the user-visible schema. The helper column `_underlying`
    # is not in _RENAME so it passes through for the write asset to read.
    if raw_option_daily.empty:
        context.log.warning("Upstream empty — nothing to process")
        return raw_option_daily
    return raw_option_daily.rename(columns=_RENAME).copy()


@asset(partitions_def=option_contracts, group_name=GROUP)
def option_daily_parquet(
    context: AssetExecutionContext,
    processed_option_daily: pd.DataFrame,
    local: LocalStorage,
) -> None:
    if processed_option_daily.empty:
        context.log.warning("Empty DataFrame — skipping write")
        return

    storage = context.run.tags.get(TAG_STORAGE, DEFAULT_STORAGE)
    if storage not in _VALID_STORAGES:
        raise ValueError(
            f"tag '{TAG_STORAGE}'='{storage}' invalid — must be one of {_VALID_STORAGES}"
        )
    dest = local

    contract = context.partition_key
    # Underlying is constant within a contract's frame; take from first row.
    underlying = processed_option_daily[_UNDERLYING_COL].iloc[0]
    fresh = processed_option_daily.drop(columns=[_UNDERLYING_COL])[_OUTPUT_COLS]

    rel = f"daily/op/{underlying}/{contract}.parquet"

    # Merge + dedupe by DateTime. Re-runs with overlapping ranges preserve
    # old rows and update any that changed (e.g. corp-action adjustments later).
    existing = dest.read(rel)
    if not existing.empty:
        combined = pd.concat([existing, fresh], ignore_index=True)
        combined = combined.drop_duplicates(subset=["DateTime"], keep="last")
        combined = combined.sort_values("DateTime").reset_index(drop=True)
    else:
        combined = fresh
    dest.write(combined, rel)
    context.log.info(f"Wrote {rel} ({len(combined)} rows) via storage={storage}")
