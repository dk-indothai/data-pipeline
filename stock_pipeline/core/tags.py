"""Centralized run-tag keys and their defaults.

Every group (daily_eq, intraday_eq, daily_op, ...) reads the same tags
off `context.run.tags` to parameterize a run. Keeping the keys here
prevents string drift (e.g. "start_date" vs "startDate" vs "from_date")
and makes the set of recognized tags discoverable from one file.

Asset-specific overrides (e.g. intraday's later start_date default) stay
in their own modules — only the tag keys and universal defaults live here.
"""

# Source selection: "kite" | "csv". Picks which fetcher the asset uses.
TAG_SOURCE = "source"
DEFAULT_SOURCE = "kite"

# Fetch window for historical backfills. Both inclusive, ISO YYYY-MM-DD.
TAG_START_DATE = "start_date"
TAG_END_DATE = "end_date"

# Universal default for start_date when the tag is unset. Groups may
# override with a tighter default in their own module (intraday_eq, for
# example, uses 2015-02-02 — the earliest date in its CSV corpus).
DEFAULT_START_DATE = "2000-01-01"

# Write destination: "local" | (future) "s3", "drive_local".
TAG_STORAGE = "storage"
DEFAULT_STORAGE = "local"
