"""Destination protocol for write sinks.

Storage backends read/write parquet dataframes at relative paths. Concrete
impls (LocalStorage, later S3Storage, DriveLocalStorage) own their own
root/bucket/credentials; callers pass rel_paths like
"daily/eq/RELIANCE.parquet". Storage never knows about "daily" or "intraday".
"""

import pandas as pd
from dagster import ConfigurableResource

# Run-tag key selecting which destination to write to.
TAG_STORAGE = "storage"
DEFAULT_STORAGE = "local"


class Destination(ConfigurableResource):
    def read(self, rel_path: str) -> pd.DataFrame:
        """Read parquet at rel_path. Return empty DataFrame if missing."""
        raise NotImplementedError

    def write(self, df: pd.DataFrame, rel_path: str) -> None:
        """Write df as parquet at rel_path, overwriting any existing file."""
        raise NotImplementedError

    def exists(self, rel_path: str) -> bool:
        raise NotImplementedError
