"""Sync NSE equity symbols from Postgres into Dagster's dynamic partition store.

Pulls every `instruments` row where exchange='NSE' and instrument_type='EQ'.
NSE has ~5000+ listed equities — in dev (ENV=dev) the query is capped at 5
so the partition grid stays small. Narrow further (F&O underlyings only,
Nifty 500, a curated `active_equity` table) once the filter stabilises.
"""

import os

from dagster import (
    DefaultSensorStatus,
    SensorEvaluationContext,
    SensorResult,
    SkipReason,
    sensor,
)
from sqlalchemy import select

from stock_pipeline.core.db import PostgresResource
from stock_pipeline.core.models import Instrument, SymphonyInstruments
from stock_pipeline.core.partitions import equity_symbols

IS_DEV = os.getenv("ENV", "prod").lower() == "dev"
DEV_SYMBOL_LIMIT = 5


@sensor(
    name="equity_symbols_sync",
    minimum_interval_seconds=10,
    default_status=DefaultSensorStatus.RUNNING,
)
def equity_symbols_sync(context: SensorEvaluationContext, db: PostgresResource):
    query = (
        select(SymphonyInstruments.name)
        .join(
            Instrument,
            Instrument.exchange_token == SymphonyInstruments.scrip_code,
        )
        .where(Instrument.segment == "NSE")
        .where(Instrument.instrument_type == "EQ")
        .where(SymphonyInstruments.name.is_not(None))
    )
    if IS_DEV:
        query = query.limit(DEV_SYMBOL_LIMIT)

    with db.session() as s:
        found = set(s.execute(query).scalars().all())

    context.log.info(
        f"[SENSOR] universe from DB: {len(found)} symbols "
        f"(ENV={'dev' if IS_DEV else 'prod'})"
    )

    if not found:
        return SkipReason("no NSE/EQ rows in instruments — check DB or ingest job")

    existing = set(context.instance.get_dynamic_partitions(equity_symbols.name))
    to_add = found - existing
    if not to_add:
        return SkipReason(f"all {len(found)} symbols already registered")

    context.log.info(f"Registering {len(to_add)} new symbols")
    return SensorResult(
        dynamic_partitions_requests=[equity_symbols.build_add_request(list(to_add))]
    )
