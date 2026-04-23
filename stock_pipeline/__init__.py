"""Top-level wiring. This is the only file that builds Definitions."""

import os

from dagster import Definitions, EnvVar, load_assets_from_modules

from stock_pipeline.core.db import PostgresResource
from stock_pipeline.core.sources.csv_source import CsvSource
from stock_pipeline.core.sources.kite import KiteSource
from stock_pipeline.daily_eod import assets as daily_eod_assets
from stock_pipeline.daily_eod.sensor import equity_symbols_sync
from stock_pipeline.intraday_eq import assets as intraday_eq_assets

# os.getenv (not EnvVar) so CSV_ROOT_DIR stays optional — falls back to the
# class default when unset. Dagster auto-loads .env from the working dir.
defs = Definitions(
    assets=load_assets_from_modules([daily_eod_assets, intraday_eq_assets]),
    sensors=[
        equity_symbols_sync,
    ],
    resources={
        "kite": KiteSource(
            api_key=EnvVar("KITE_API_KEY"),
            access_token=EnvVar("KITE_ACCESS_TOKEN"),
        ),
        "csv": CsvSource(root_dir=os.getenv("CSV_ROOT_DIR", "CustomSource")),
        "db": PostgresResource(url=EnvVar("DATABASE_URL")),
    },
)
