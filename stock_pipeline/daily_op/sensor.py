"""Sync NFO/BFO option contracts from Postgres into Dagster's dynamic
partition store. Filter: rows present in both Kite `instruments` and
Symphony, segment in NSE / NFO-OPT. In dev (ENV=dev) the query is
capped at 5.
"""

import os
from datetime import date

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
from stock_pipeline.core.partitions import option_contracts

IS_DEV = os.getenv("ENV", "prod").lower() == "dev"
DEV_CONTRACT_LIMIT = 5

# Cap per tick. Registering tens of thousands in one shot overruns the
# sensor tick timeout and Dagster silently drops the whole batch. Smaller
# chunks land reliably; the sensor just runs more ticks until done.
_BATCH_PER_TICK = 2000


@sensor(
    name="option_contracts_sync",
    minimum_interval_seconds=30,
    default_status=DefaultSensorStatus.RUNNING,
)
def option_contracts_sync(context: SensorEvaluationContext, db: PostgresResource):
    # Inner-join Symphony so the universe is constrained to instruments
    # that exist on both Kite (instruments) and Symphony — link is
    # Instrument.exchange_token == SymphonyInstruments.scrip_code.
    query = (
        select(SymphonyInstruments.name)
        .join(
            Instrument,
            Instrument.exchange_token == SymphonyInstruments.scrip_code,
        )
        .where(Instrument.segment.in_(("NSE", "NFO-OPT")))
        .where(SymphonyInstruments.name.is_not(None))
    )
    if IS_DEV:
        query = query.limit(DEV_CONTRACT_LIMIT)

    with db.session() as s:
        # Narrow to str here — `is_not(None)` filter rules out None at runtime
        # but the ORM still types the column as `str | None`.
        found: set[str] = {
            ts for ts in s.execute(query).scalars().all() if ts is not None
        }

    context.log.info(
        f"[SENSOR] option contracts from DB: {len(found)} "
        f"(ENV={'dev' if IS_DEV else 'prod'})"
    )

    if not found:
        return SkipReason(
            "no NFO/BFO CE/PE rows with expiry >= today — check DB or ingest job"
        )

    existing = set(context.instance.get_dynamic_partitions(option_contracts.name))
    to_add = found - existing
    # Partitions present locally but no longer in the filtered DB query
    # (e.g. expired contracts that fell off `expiry >= today`). Without this,
    # the partition set grows monotonically even when the DB doesn't change.
    to_remove = existing - found

    if not to_add and not to_remove:
        return SkipReason(f"partition set in sync with DB ({len(found)} contracts)")

    # Batch both directions at the same per-tick cap for tick-timeout safety.
    # Deterministic ordering via sorted() so repeated ticks target the same
    # prefix/suffix rather than shuffling under partial progress.
    requests = []
    log_parts = []

    if to_add:
        add_batch = sorted(to_add)[:_BATCH_PER_TICK]
        requests.append(option_contracts.build_add_request(add_batch))
        log_parts.append(f"+{len(add_batch)}/{len(to_add)}")

    if to_remove:
        rm_batch = sorted(to_remove)[:_BATCH_PER_TICK]
        requests.append(option_contracts.build_delete_request(rm_batch))
        log_parts.append(f"-{len(rm_batch)}/{len(to_remove)}")

    context.log.info(
        f"option_contracts sync: {', '.join(log_parts)} "
        f"(cap={_BATCH_PER_TICK}/tick, found={len(found)}, existing={len(existing)})"
    )
    return SensorResult(dynamic_partitions_requests=requests)
