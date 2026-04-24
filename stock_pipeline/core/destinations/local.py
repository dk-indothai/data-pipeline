"""Local filesystem destination."""

from pathlib import Path

import pandas as pd

from stock_pipeline.core.destinations.base import Destination


class LocalStorage(Destination):
    # Root for all rel_paths; override via LOCAL_STORAGE_DIR env var
    # at resource instantiation time (see stock_pipeline/__init__.py).
    base_dir: str = "data"

    def _abs(self, rel_path: str) -> Path:
        return Path(self.base_dir) / rel_path

    def read(self, rel_path: str) -> pd.DataFrame:
        path = self._abs(rel_path)
        if not path.exists():
            return pd.DataFrame()
        return pd.read_parquet(path)

    def write(self, df: pd.DataFrame, rel_path: str) -> None:
        path = self._abs(rel_path)
        path.parent.mkdir(parents=True, exist_ok=True)
        df.to_parquet(path, index=False)

    def exists(self, rel_path: str) -> bool:
        return self._abs(rel_path).exists()
