"""Sync NFO/BFO option contracts from Postgres into Dagster's dynamic
partition store. Filter: non-expired CE/PE in NFO-OPT or BFO-OPT, narrowed
to the tracked underlyings (TRACKED_OPTION_UNDERLYINGS env var). In dev
(ENV=dev) the query is also capped at 5.
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
from stock_pipeline.core.models import Instrument
from stock_pipeline.core.partitions import option_contracts

IS_DEV = os.getenv("ENV", "prod").lower() == "dev"
DEV_CONTRACT_LIMIT = 5

# Default underlyings: the major index options (covers >90% of liquid
# options volume on NFO). Override via TRACKED_OPTION_UNDERLYINGS=A,B,C
# in .env to pick a different set.
_DEFAULT_UNDERLYINGS = ("NIFTY", "BANKNIFTY", "FINNIFTY", "SENSEX")

_env_raw = os.getenv("TRACKED_OPTION_UNDERLYINGS", "").strip()
TRACKED_UNDERLYINGS: tuple[str, ...] = (
    tuple(s.strip() for s in _env_raw.split(",") if s.strip())
    if _env_raw
    else _DEFAULT_UNDERLYINGS
)

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
    query = (
        select(Instrument.tradingsymbol)
        .where(Instrument.segment.in_(("NFO-OPT", "BFO-OPT")))
        .where(Instrument.instrument_type.in_(("CE", "PE")))
        .where(Instrument.name.in_(TRACKED_UNDERLYINGS))
        .where(Instrument.expiry >= date.today())
        .where(Instrument.tradingsymbol.is_not(None))
        .order_by(Instrument.tradingsymbol)
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
        f"(underlyings={list(TRACKED_UNDERLYINGS)}, "
        f"ENV={'dev' if IS_DEV else 'prod'})"
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
