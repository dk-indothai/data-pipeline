"""Intraday options pipeline: one partition per option underlying.

Each run scans `{csv.root_dir}/intraday/op/{UNDERLYING}.csv` — a single
multi-contract minute-candle file — filters to [start_date, end_date],
then fans out by (contract, trading_date) and writes one parquet per
contract per trading day at:
  intraday/op/{UNDERLYING}/{DATE}/{CONTRACT}.parquet

The CSV carries per-row metadata (Strike, Expiry, OptionType, lot size,
Ticker, ...), so no DB lookup is performed. `Ticker` is the contract
trading symbol; the source adapter renames it to `contract`.

Tags:
  source        "kite" | "csv"          default: kite  (kite is no-op for now)
  start_date    YYYY-MM-DD              default: 2015-02-02 (earliest CSV)
  end_date      YYYY-MM-DD              default: today
  storage       "local"                 default: local
"""

from datetime import date as date_cls

import pandas as pd
from dagster import AssetExecutionContext, asset

from stock_pipeline.core.destinations.local import LocalStorage
from stock_pipeline.core.partitions import option_contracts
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

GROUP = "intraday_op"

# Whitelist grows as more Destination impls land (s3, drive_local, ...).
_VALID_STORAGES = ("local",)

# Earliest date present in the intraday CSV corpus; the universal default
# of 2000-01-01 would scan years of empty range for nothing.
DEFAULT_INTRADAY_START_DATE = "2015-02-02"

# Helper columns — set in raw, read in the write asset for path routing,
# dropped before parquet serialization. Underscore prefix matches the
# `_underlying`/`_contract` convention used by daily_op.
_UNDERLYING_COL = "_underlying"
_TRADING_DATE_COL = "_trading_date"
# `contract` is a public column (sourced from the CSV's Ticker), kept in
# the written parquet so each per-contract file is self-describing.
_CONTRACT_COL = "contract"


@asset(partitions_def=option_contracts, group_name=GROUP)
def raw_intraday_op(
    context: AssetExecutionContext,
    kite: KiteSource,
    csv: CsvSource,
) -> pd.DataFrame:
    underlying = context.partition_key
    tags = context.run.tags

    source = tags.get(TAG_SOURCE, DEFAULT_SOURCE)
    if source not in ("kite", "csv"):
        raise ValueError(
            f"tag '{TAG_SOURCE}'='{source}' invalid — must be 'kite' or 'csv'"
        )

    if source == "kite":
        context.log.info(
            f"source=kite for {underlying} — skipping (not implemented for intraday op)"
        )
        return pd.DataFrame()

    from_date = date_cls.fromisoformat(
        tags.get(TAG_START_DATE, DEFAULT_INTRADAY_START_DATE)
    )
    end_tag = tags.get(TAG_END_DATE)
    to_date = date_cls.fromisoformat(end_tag) if end_tag else date_cls.today()
    if to_date < from_date:
        raise ValueError(f"end_date {to_date} < start_date {from_date}")

    df = csv.fetch_intraday_op_range(
        underlying=underlying, from_date=from_date, to_date=to_date
    )
    if df.empty:
        context.log.warning(
            f"no rows for {underlying} in [{from_date}, {to_date}]"
        )
        return df

    df[_UNDERLYING_COL] = underlying
    # First 10 chars of the ISO timestamp are the YYYY-MM-DD slot — avoids
    # reparsing the full datetime on a multi-million-row frame.
    df[_TRADING_DATE_COL] = df["datetime"].str[:10]

    context.log.info(
        f"fetched {len(df)} rows across {df[_CONTRACT_COL].nunique()} contracts "
        f"for {underlying} via csv in [{from_date}, {to_date}]"
    )
    return df


@asset(partitions_def=option_contracts, group_name=GROUP)
def processed_intraday_op(
    context: AssetExecutionContext, raw_intraday_op: pd.DataFrame
) -> pd.DataFrame:
    # Pass-through stage. Reserved as a hook for future per-row processing
    # (corp-action adjustments, schema validation, etc.) without disturbing
    # the asset graph.
    if raw_intraday_op.empty:
        context.log.warning("Upstream empty — nothing to process")
        return raw_intraday_op
    return raw_intraday_op.copy()


@asset(partitions_def=option_contracts, group_name=GROUP)
def intraday_op_parquet(
    context: AssetExecutionContext,
    processed_intraday_op: pd.DataFrame,
    local: LocalStorage,
) -> None:
    if processed_intraday_op.empty:
        context.log.warning("Empty DataFrame — skipping write")
        return

    storage = context.run.tags.get(TAG_STORAGE, DEFAULT_STORAGE)
    if storage not in _VALID_STORAGES:
        raise ValueError(
            f"tag '{TAG_STORAGE}'='{storage}' invalid — must be one of {_VALID_STORAGES}"
        )
    dest = local

    underlying = processed_intraday_op[_UNDERLYING_COL].iloc[0]

    # Fan out: one parquet per (contract, trading_date) under this
    # underlying. Writes overwrite atomically — re-materializing a range
    # refreshes every file in scope without merge/dedupe overhead.
    written = 0
    for (contract, trading_date), group in processed_intraday_op.groupby(
        [_CONTRACT_COL, _TRADING_DATE_COL], sort=False
    ):
        rel = f"intraday/op/{underlying}/{trading_date}/{contract}.parquet"
        dest.write(
            group.drop(columns=[_UNDERLYING_COL, _TRADING_DATE_COL]),
            rel,
        )
        context.log.info(f"Wrote {rel} ({len(group)} rows)")
        written += 1

    context.log.info(
        f"{underlying}: wrote {written} parquet files via storage={storage}"
    )
