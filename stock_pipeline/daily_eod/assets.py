"""Daily EOD pipeline: fetch -> process -> write parquet.

Partition: (date x equity_symbol). The equity_symbol pool is synced from
the shared `instruments` table by daily_eod.sensor.
"""

from datetime import date as date_cls
from pathlib import Path

import pandas as pd
from dagster import AssetExecutionContext, ResourceParam, asset
from sqlalchemy import select

from stock_pipeline.core.db import PostgresResource
from stock_pipeline.core.models import Instrument
from stock_pipeline.core.partitions import daily_equity_partitions
from stock_pipeline.core.sources.base import BaseSource

DATA_DIR = Path("data")
GROUP = "daily_eod"


@asset(partitions_def=daily_equity_partitions, group_name=GROUP)
def raw_daily(
    context: AssetExecutionContext,
    kite: ResourceParam[BaseSource],
    db: PostgresResource,
) -> pd.DataFrame:
    keys = context.partition_key.keys_by_dimension
    d = date_cls.fromisoformat(keys["date"])
    symbol = keys["symbol"]

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

    context.log.info(f"Fetching {symbol} (token={token}) for {d}")
    return kite.fetch_daily(symbol=symbol, instrument_token=token, on=d)


@asset(partitions_def=daily_equity_partitions, group_name=GROUP)
def processed_daily(
    context: AssetExecutionContext, raw_daily: pd.DataFrame
) -> pd.DataFrame:
    if raw_daily.empty:
        context.log.warning("Upstream empty — nothing to process")
        return raw_daily

    df = raw_daily.copy()
    return df


@asset(partitions_def=daily_equity_partitions, group_name=GROUP)
def daily_parquet(
    context: AssetExecutionContext, processed_daily: pd.DataFrame
) -> None:
    if processed_daily.empty:
        context.log.warning("Empty DataFrame — skipping write")
        return

    keys = context.partition_key.keys_by_dimension
    out = DATA_DIR / "equity" / keys["symbol"] / f"{keys['date']}.parquet"
    out.parent.mkdir(parents=True, exist_ok=True)
    processed_daily.to_parquet(out, index=False)
    context.log.info(f"Wrote {out}")
