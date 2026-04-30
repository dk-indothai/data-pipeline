"""Daily options pipeline: one partition per option underlying.

Each run scans `{csv.root_dir}/daily/op/{UNDERLYING}/*.csv`, loads every
contract file under that folder for [start_date, end_date], and writes a
merged+deduped parquet at
  daily/op/{UNDERLYING}/{CONTRACT}.parquet
for each contract found.

The CSVs already carry per-row metadata (Strike, Expiry, OptionType, lot
size, ...), so no DB lookup is performed.

Output columns (lowercase raw schema):
  datetime, open, high, low, close, volume, oi, strike, expiry,
  option_type, lot_size

Tags:
  source        "kite" | "csv"         default: kite  (kite is no-op for now)
  start_date    YYYY-MM-DD             default: 2000-01-01
  end_date      YYYY-MM-DD             default: today
  storage       "local" | "lean"       default: local
"""

from datetime import date as date_cls
from pathlib import Path

import pandas as pd
from dagster import AssetExecutionContext, asset

from stock_pipeline.core.destinations.lean import LeanStorage
from stock_pipeline.core.destinations.local import LocalStorage
from stock_pipeline.core.partitions import option_contracts
from stock_pipeline.core.sources.csv_source import CsvSource
from stock_pipeline.core.sources.kite import KiteSource
from stock_pipeline.core.tags import (
    DEFAULT_SOURCE,
    DEFAULT_START_DATE,
    DEFAULT_STORAGE,
    TAG_END_DATE,
    TAG_SOURCE,
    TAG_START_DATE,
    TAG_STORAGE,
)

GROUP = "daily_op"

# Whitelist grows as more Destination impls land.
_VALID_STORAGES = ("local", "lean")

# Helper columns — set in raw, read in the write asset for path routing,
# dropped before parquet serialization. Leading underscore marks them private.
_UNDERLYING_COL = "_underlying"
_CONTRACT_COL = "_contract"


@asset(partitions_def=option_contracts, group_name=GROUP)
def raw_option_daily(
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
            f"source=kite for {underlying} — skipping (not implemented for per-underlying flow)"
        )
        return pd.DataFrame()
    elif source == "csv":
        start_tag = tags.get(TAG_START_DATE, DEFAULT_START_DATE)
        end_tag = tags.get(TAG_END_DATE)
        try:
            from_date = date_cls.fromisoformat(start_tag)
            to_date = date_cls.fromisoformat(end_tag) if end_tag else date_cls.today()
        except ValueError as e:
            raise ValueError(
                f"bad date tag — start_date={start_tag!r} end_date={end_tag!r}: {e}"
            ) from e

        op_dir = Path(csv.root_dir) / "daily" / "op" / underlying
        if not op_dir.exists():
            context.log.warning(f"no folder at {op_dir} for {underlying}")
            return pd.DataFrame()

        contract_files = sorted(op_dir.glob("*.csv"))
        if not contract_files:
            context.log.warning(f"no contract CSVs under {op_dir}")
            return pd.DataFrame()

        frames: list[pd.DataFrame] = []
        for path in contract_files:
            contract = path.stem
            df = csv.fetch_daily_op_range(
                contract=contract,
                underlying=underlying,
                instrument_token=0,
                from_date=from_date,
                to_date=to_date,
            )
            if df.empty:
                continue
            # OI may be missing from legacy CSVs; materialize an empty column so
            # the rename + output-projection downstream has something to hit.
            if "oi" not in df.columns:
                df["oi"] = pd.NA
            df[_CONTRACT_COL] = contract
            df[_UNDERLYING_COL] = underlying
            frames.append(df)

        if not frames:
            context.log.warning(
                f"no rows for {underlying} in [{from_date}, {to_date}] "
                f"across {len(contract_files)} contract files"
            )
            return pd.DataFrame()

        combined = pd.concat(frames, ignore_index=True)
        context.log.info(
            f"fetched {len(combined)} rows across {len(frames)} contracts "
            f"for {underlying} via csv in [{from_date}, {to_date}]"
        )
        return combined
    else:
        context.log.warning(f"unsupported source: {source}")
        return pd.DataFrame()


@asset(partitions_def=option_contracts, group_name=GROUP)
def processed_option_daily(
    context: AssetExecutionContext, raw_option_daily: pd.DataFrame
) -> pd.DataFrame:
    # Pass-through stage. Reserved as a hook for future per-row processing
    # (corp-action adjustments, schema validation, etc.) without disturbing
    # the asset graph.
    if raw_option_daily.empty:
        context.log.warning("Upstream empty — nothing to process")
        return raw_option_daily
    return raw_option_daily.copy()


@asset(partitions_def=option_contracts, group_name=GROUP)
def option_daily_parquet(
    context: AssetExecutionContext,
    processed_option_daily: pd.DataFrame,
    local: LocalStorage,
    lean: LeanStorage,
) -> None:
    if processed_option_daily.empty:
        context.log.warning("Empty DataFrame — skipping write")
        return

    storage = context.run.tags.get(TAG_STORAGE, DEFAULT_STORAGE)
    if storage not in _VALID_STORAGES:
        raise ValueError(
            f"tag '{TAG_STORAGE}'='{storage}' invalid — must be one of {_VALID_STORAGES}"
        )

    underlying = processed_option_daily[_UNDERLYING_COL].iloc[0]

    if storage == "lean":
        # LeanStorage rebuilds contract identity from the typed
        # strike/expiry/option_type columns, so the helper columns
        # are dropped before handoff.
        n = lean.write_daily_option(
            processed_option_daily.drop(columns=[_UNDERLYING_COL, _CONTRACT_COL]),
            underlying=underlying,
        )
        context.log.info(
            f"LEAN: wrote {n} zip(s) for {underlying} via storage={storage}"
        )
        return
    elif storage == "local":
        # storage == "local": one parquet per contract under this
        # underlying. Merge+dedupe by DateTime so re-runs with overlapping
        # ranges preserve old rows and update any that changed (e.g.
        # corp-action adjustments later).
        dest = local
        for contract, group in processed_option_daily.groupby(
            _CONTRACT_COL, sort=False
        ):
            fresh = group.drop(columns=[_UNDERLYING_COL, _CONTRACT_COL])
            rel = f"daily/op/{underlying}/{contract}.parquet"
            existing = dest.read(rel)
            if not existing.empty:
                combined = pd.concat([existing, fresh], ignore_index=True)
                combined = combined.drop_duplicates(subset=["datetime"], keep="last")
                combined = combined.sort_values("datetime").reset_index(drop=True)
            else:
                combined = fresh
            dest.write(combined, rel)
            context.log.info(
                f"Wrote {rel} ({len(combined)} rows) via storage={storage}"
            )
    else:
        context.log.warning(f"unsupported storage: {storage}")
