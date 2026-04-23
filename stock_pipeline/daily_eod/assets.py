"""Daily EOD pipeline: fetch -> process -> write parquet.

Partition: (date x equity_symbol). The equity_symbol pool is synced from
the shared `instruments` table by daily_eod.sensor.
"""

from datetime import date as date_cls
from pathlib import Path

import pandas as pd
from dagster import AssetExecutionContext, asset
from sqlalchemy import select

from stock_pipeline.core.db import PostgresResource
from stock_pipeline.core.models import Instrument
from stock_pipeline.core.partitions import equity_symbols
from stock_pipeline.core.sources.csv_source import CsvSource
from stock_pipeline.core.sources.kite import KiteSource

DATA_DIR = Path("data")
GROUP = "daily_eod"

# Run tags override these — set via Launchpad or Backfill dialog.
TAG_SOURCE = "source"
TAG_START_DATE = "start_date"
TAG_END_DATE = "end_date"

DEFAULT_SOURCE = "kite"
DEFAULT_START_DATE = "2000-01-01"


def _write_with_dedupe(df: pd.DataFrame, out: Path) -> int:
    """Merge df into a per-symbol parquet, deduped by date. Returns row count."""
    if df.empty:
        return 0
    out.parent.mkdir(parents=True, exist_ok=True)
    if out.exists():
        existing = pd.read_parquet(out)
        combined = pd.concat([existing, df], ignore_index=True)
        combined = combined.drop_duplicates(subset=["date"], keep="last")
        combined = combined.sort_values("date").reset_index(drop=True)
    else:
        combined = df
    combined.to_parquet(out, index=False)
    return len(combined)


@asset(partitions_def=equity_symbols, group_name=GROUP)
def raw_daily(
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

    from_date = date_cls.fromisoformat(tags.get(TAG_START_DATE, DEFAULT_START_DATE))
    end_tag = tags.get(TAG_END_DATE)
    to_date = date_cls.fromisoformat(end_tag) if end_tag else date_cls.today()

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

    src = kite if source == "kite" else csv
    context.log.info(
        f"Fetching {symbol} (token={token}) for [{from_date}, {to_date}] via {source}"
    )
    return src.fetch_daily_eq_range(
        symbol=symbol, instrument_token=token, from_date=from_date, to_date=to_date
    )


@asset(partitions_def=equity_symbols, group_name=GROUP)
def processed_daily(
    context: AssetExecutionContext, raw_daily: pd.DataFrame
) -> pd.DataFrame:
    if raw_daily.empty:
        context.log.warning("Upstream empty — nothing to process")
        return raw_daily

    df = raw_daily.copy()
    return df


@asset(partitions_def=equity_symbols, group_name=GROUP)
def daily_parquet(
    context: AssetExecutionContext, processed_daily: pd.DataFrame
) -> None:
    if processed_daily.empty:
        context.log.warning("Empty DataFrame — skipping write")
        return

    symbol = context.partition_key
    out = DATA_DIR / "daily" / "eq" / f"{symbol}.parquet"
    rows = _write_with_dedupe(processed_daily, out)
    context.log.info(f"Wrote {out} ({rows} rows)")
