"""Asset jobs for daily_eod. Preset tags act as defaults; editable per
run from the Launchpad (Tags section) and per backfill from the Backfill
dialog's Tags section."""

from dagster import AssetSelection, define_asset_job

from stock_pipeline.daily_eq.assets import (
    TAG_END_DATE,
    TAG_SOURCE,
    TAG_START_DATE,
)

_SELECTION = AssetSelection.keys("raw_daily", "processed_daily", "daily_parquet")


daily_equity = define_asset_job(
    name="daily_equity",
    selection=_SELECTION,
    tags={
        "job_kind": "backfill",
        TAG_SOURCE: "kite",
        TAG_START_DATE: "2015-01-01",
        TAG_END_DATE: "2024-12-31`",
    },
)
