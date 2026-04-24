"""Partition definitions shared across all data-type groups."""

from dagster import DynamicPartitionsDefinition

equity_symbols = DynamicPartitionsDefinition(name="equity_symbols")

# One partition per option contract (tradingsymbol like NIFTY24D1910000CE).
# Synced from NFO/BFO option instruments by daily_op.sensor.
option_contracts = DynamicPartitionsDefinition(name="option_contracts")
