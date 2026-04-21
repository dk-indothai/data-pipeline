"""Partition definitions shared across all data-type groups."""
from dagster import (
    DailyPartitionsDefinition,
    DynamicPartitionsDefinition,
    MultiPartitionsDefinition,
)

daily = DailyPartitionsDefinition(start_date="2026-04-14")

equity_symbols = DynamicPartitionsDefinition(name="equity_symbols")

daily_equity_partitions = MultiPartitionsDefinition(
    {"date": daily, "symbol": equity_symbols}
)
