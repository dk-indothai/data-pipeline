"""Test jobs — one per (group, source, storage) cell.

Usage:
    Dagster UI → Jobs → pick a job → Launchpad → Materialize.
    Partition keys must already exist (run the sensors first).

Each job is a thin asset-job over the group's three assets, with run
tags hard-coded to the cell under test. Launching it materializes the
chosen partition with those tags — no Launchpad config edits needed.

Wire into Definitions by importing `test_jobs` from this module
(see bottom of file for the list).
"""

from dagster import AssetSelection, define_asset_job

from stock_pipeline.core.tags import (
    TAG_END_DATE,
    TAG_SOURCE,
    TAG_START_DATE,
    TAG_STORAGE,
)

# ---------------------------------------------------------------------------
# daily_eq
# ---------------------------------------------------------------------------

_daily_eq = AssetSelection.groups("daily_eq")

test_daily_eq_csv_local = define_asset_job(
    name="test_daily_eq_csv_local",
    selection=_daily_eq,
    tags={
        TAG_SOURCE: "csv",
        TAG_STORAGE: "local",
        TAG_START_DATE: "2000-01-01",
        TAG_END_DATE: "2025-12-31",
    },
)

test_daily_eq_csv_lean = define_asset_job(
    name="test_daily_eq_csv_lean",
    selection=_daily_eq,
    tags={
        TAG_SOURCE: "csv",
        TAG_STORAGE: "lean",
        TAG_START_DATE: "2000-01-01",
        TAG_END_DATE: "2025-12-31",
    },
)

test_daily_eq_kite_stub = define_asset_job(
    name="test_daily_eq_kite_stub",
    selection=_daily_eq,
    tags={TAG_SOURCE: "kite"},
)

# ---------------------------------------------------------------------------
# daily_op
# ---------------------------------------------------------------------------

_daily_op = AssetSelection.groups("daily_op")

test_daily_op_csv_local = define_asset_job(
    name="test_daily_op_csv_local",
    selection=_daily_op,
    tags={
        TAG_SOURCE: "csv",
        TAG_STORAGE: "local",
        TAG_START_DATE: "2016-01-01",
        TAG_END_DATE: "2020-12-31",
    },
)

test_daily_op_csv_lean = define_asset_job(
    name="test_daily_op_csv_lean",
    selection=_daily_op,
    tags={
        TAG_SOURCE: "csv",
        TAG_STORAGE: "lean",
        TAG_START_DATE: "2016-01-01",
        TAG_END_DATE: "2020-12-31",
    },
)

test_daily_op_kite_stub = define_asset_job(
    name="test_daily_op_kite_stub",
    selection=_daily_op,
    tags={TAG_SOURCE: "kite"},
)

# ---------------------------------------------------------------------------
# intraday_eq
# ---------------------------------------------------------------------------

_intraday_eq = AssetSelection.groups("intraday_eq")

test_intraday_eq_csv_local = define_asset_job(
    name="test_intraday_eq_csv_local",
    selection=_intraday_eq,
    tags={
        TAG_SOURCE: "csv",
        TAG_STORAGE: "local",
        TAG_START_DATE: "2015-01-01",
        TAG_END_DATE: "2024-12-31",
    },
)

test_intraday_eq_csv_lean = define_asset_job(
    name="test_intraday_eq_csv_lean",
    selection=_intraday_eq,
    tags={
        TAG_SOURCE: "csv",
        TAG_STORAGE: "lean",
        TAG_START_DATE: "2015-01-01",
        TAG_END_DATE: "2024-12-31",
    },
)

test_intraday_eq_kite_stub = define_asset_job(
    name="test_intraday_eq_kite_stub",
    selection=_intraday_eq,
    tags={TAG_SOURCE: "kite"},
)

# ---------------------------------------------------------------------------
# intraday_op
# ---------------------------------------------------------------------------

_intraday_op = AssetSelection.groups("intraday_op")

test_intraday_op_csv_local = define_asset_job(
    name="test_intraday_op_csv_local",
    selection=_intraday_op,
    tags={
        TAG_SOURCE: "csv",
        TAG_STORAGE: "local",
        TAG_START_DATE: "2015-01-01",
        TAG_END_DATE: "2024-12-31",
    },
)

test_intraday_op_csv_lean = define_asset_job(
    name="test_intraday_op_csv_lean",
    selection=_intraday_op,
    tags={
        TAG_SOURCE: "csv",
        TAG_STORAGE: "lean",
        TAG_START_DATE: "2015-01-01",
        TAG_END_DATE: "2024-12-31",
    },
)

test_intraday_op_kite_stub = define_asset_job(
    name="test_intraday_op_kite_stub",
    selection=_intraday_op,
    tags={TAG_SOURCE: "kite"},
)

# ---------------------------------------------------------------------------
# Export
# ---------------------------------------------------------------------------

test_jobs = [
    test_daily_eq_csv_local,
    test_daily_eq_csv_lean,
    test_daily_eq_kite_stub,
    test_daily_op_csv_local,
    test_daily_op_csv_lean,
    test_daily_op_kite_stub,
    test_intraday_eq_csv_local,
    test_intraday_eq_csv_lean,
    test_intraday_eq_kite_stub,
    test_intraday_op_csv_local,
    test_intraday_op_csv_lean,
    test_intraday_op_kite_stub,
]
