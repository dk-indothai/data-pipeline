"""Top-level wiring. This is the only file that builds Definitions."""

from dagster import Definitions, EnvVar, load_assets_from_modules

from stock_pipeline.core.db import PostgresResource
from stock_pipeline.core.sources.csv_source import CsvSource
from stock_pipeline.core.sources.kite import KiteSource
from stock_pipeline.daily_eod import assets as daily_eod_assets
from stock_pipeline.daily_eod.jobs import daily_equity
from stock_pipeline.daily_eod.sensor import equity_symbols_sync

defs = Definitions(
    assets=load_assets_from_modules([daily_eod_assets]),
    jobs=[daily_equity],
    sensors=[
        equity_symbols_sync,
    ],
    resources={
        "kite": KiteSource(
            api_key=EnvVar("KITE_API_KEY"),
            access_token=EnvVar("KITE_ACCESS_TOKEN"),
        ),
        "csv": CsvSource(),
        "db": PostgresResource(url=EnvVar("DATABASE_URL")),
    },
)
