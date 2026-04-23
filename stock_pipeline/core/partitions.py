"""Partition definitions shared across all data-type groups."""

from dagster import DynamicPartitionsDefinition

equity_symbols = DynamicPartitionsDefinition(name="equity_symbols")
