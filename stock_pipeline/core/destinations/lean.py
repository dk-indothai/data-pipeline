"""QuantConnect LEAN destination for minute option data.

Emits the on-disk shape LEAN expects for an option backtest:

    {base_dir}/option/{country}/minute/{symbol}/
        {YYYYMMDD}_trade_{style}.zip          (inner: per-contract CSVs)
        {YYYYMMDD}_openinterest_{style}.zip
    {base_dir}/option/{country}/universes/{symbol}/
        {YYYYMMDD}.csv

LEAN's grouping is per-(date, tick_type) — one zip per trading day
carrying every contract — so the per-file `write(df, rel_path)`
primitive that LocalStorage uses doesn't fit. This class adds a
higher-level emitter, `write_minute_option_day`, that takes one day's
multi-contract DataFrame in the natural pipeline shape and produces the
trade zip + OI zip + universe CSV.

Reusable: any flow producing a minute-bar option DataFrame with the
columns documented on `write_minute_option_day` can route output here
by selecting `storage=lean`.

Conversion logic (deci-cent prices, time_ms, contract filename
encoding, universe row layout) follows the reference notebook
`csv_to_lean.ipynb` at repo root.
"""

import io
import zipfile
from pathlib import Path

import pandas as pd
from dagster import get_dagster_logger

from stock_pipeline.core.destinations.base import Destination

# LEAN encodes prices as integers in deci-cents (raw price × 1e4).
_DECI_CENT_MULTIPLIER = 10_000

# CE/PE → LEAN's single-letter `right` code.
_RIGHT_FROM_OPTION_TYPE = {"CE": "C", "PE": "P"}
# `right` → long form used in the inner CSV filename.
_LONG_FROM_RIGHT = {"C": "call", "P": "put"}


class LeanStorage(Destination):
    """LEAN minute option + universe writer.

    Config:
      base_dir       Root of the LEAN data tree (default "lean_data").
      country        Country segment under `option/` (default "india").
      option_style   Filename token, e.g. "american" / "european"
                     (default "american"). Purely cosmetic — LEAN reads
                     it from the path; pick whatever your LEAN config
                     expects.
    """

    base_dir: str = "lean_data"
    country: str = "india"
    option_style: str = "american"

    def write_minute_option_day(
        self,
        df: pd.DataFrame,
        *,
        underlying: str,
        date_str: str,
    ) -> int:
        """Emit one trading day's multi-contract minute data in LEAN format.

        Args:
            df: rows for `underlying` on `date_str`. Required columns:
                `datetime` (ISO string "YYYY-MM-DD HH:MM:SS[...]"),
                `open`, `high`, `low`, `close`, `volume`, `strike`,
                `expiry` ("YYYY-MM-DD"), `option_type` ("CE"/"PE").
                Optional: `oi`. Extra columns are ignored.
            underlying: e.g. "NIFTY". Used as `symbol` (lowercased) in
                the LEAN path.
            date_str: trading date — accepts ISO ("YYYY-MM-DD") or
                compact ("YYYYMMDD"). LEAN paths use compact.

        Returns: number of files written (0–3 per call).
        """
        if df.empty:
            return 0

        log = get_dagster_logger()
        prepared = self._prepare(df)
        if prepared.empty:
            log.warning(
                f"LeanStorage: no usable rows for {underlying} on {date_str} "
                f"after derivation (missing strike/expiry/option_type, or "
                f"no row mapped to a valid CE/PE)"
            )
            return 0

        symbol = underlying.lower()
        date_compact = date_str.replace("-", "")

        minute_dir = (
            Path(self.base_dir) / "option" / self.country / "minute" / symbol
        )
        universe_dir = (
            Path(self.base_dir) / "option" / self.country / "universes" / symbol
        )
        minute_dir.mkdir(parents=True, exist_ok=True)
        universe_dir.mkdir(parents=True, exist_ok=True)

        written = 0

        # Trade zip — only rows with a usable Open (LEAN inner CSV writes
        # full OHLCV; a NULL Open implies the row was OI-only on the
        # source side).
        trade_df = prepared[prepared["o_deci"].notna()]
        if not trade_df.empty:
            zpath = minute_dir / f"{date_compact}_trade_{self.option_style}.zip"
            self._write_minute_zip(
                zpath,
                trade_df,
                date_compact=date_compact,
                symbol=symbol,
                tick_type="trade",
                content_cols=[
                    "time_ms",
                    "o_deci",
                    "h_deci",
                    "l_deci",
                    "c_deci",
                    "volume_int",
                ],
            )
            written += 1

        # Open-interest zip — only rows with positive OI.
        oi_df = prepared[prepared["oi_int"].notna() & (prepared["oi_int"] > 0)]
        if not oi_df.empty:
            zpath = (
                minute_dir / f"{date_compact}_openinterest_{self.option_style}.zip"
            )
            self._write_minute_zip(
                zpath,
                oi_df,
                date_compact=date_compact,
                symbol=symbol,
                tick_type="openinterest",
                content_cols=["time_ms", "oi_int"],
            )
            written += 1

        # Universe CSV — daily OHLCV + last OI per contract. Underlying
        # row (first line after header) is left blank: we don't carry the
        # underlying's daily bar through this path, and merging it in
        # would couple the destination to a specific source. Populate
        # post-hoc if your LEAN run needs it.
        universe_path = universe_dir / f"{date_compact}.csv"
        self._write_universe(universe_path, prepared)
        written += 1

        return written

    # ------------------------------------------------------------------
    # Internals
    # ------------------------------------------------------------------

    def _prepare(self, df: pd.DataFrame) -> pd.DataFrame:
        """Add LEAN-encoded columns; drop rows with no contract identity."""
        required = {"strike", "expiry", "option_type", "datetime"}
        if not required.issubset(df.columns):
            return df.iloc[0:0]

        out = df.copy()

        # time_ms: start-of-minute milliseconds from midnight.
        # Slice positions are stable for both "YYYY-MM-DD HH:MM:SS" and
        # "YYYY-MM-DDTHH:MM:SS[+TZ]" — the source already relies on this
        # invariant for date filtering.
        hh = pd.to_numeric(out["datetime"].str[11:13], errors="coerce")
        mm = pd.to_numeric(out["datetime"].str[14:16], errors="coerce")
        out["time_ms"] = ((hh * 3600 + mm * 60) * 1000).astype("Int64")

        out["strike_deci"] = (
            pd.to_numeric(out["strike"], errors="coerce")
            .mul(_DECI_CENT_MULTIPLIER)
            .round()
            .astype("Int64")
        )
        out["expiry_str"] = (
            out["expiry"].astype(str).str.replace("-", "", regex=False)
        )
        out["right"] = out["option_type"].map(_RIGHT_FROM_OPTION_TYPE)

        for src, dst in (
            ("open", "o_deci"),
            ("high", "h_deci"),
            ("low", "l_deci"),
            ("close", "c_deci"),
        ):
            if src in out.columns:
                out[dst] = (
                    pd.to_numeric(out[src], errors="coerce")
                    .mul(_DECI_CENT_MULTIPLIER)
                    .round()
                    .astype("Int64")
                )
            else:
                out[dst] = pd.array([pd.NA] * len(out), dtype="Int64")

        if "volume" in out.columns:
            out["volume_int"] = (
                pd.to_numeric(out["volume"], errors="coerce")
                .round()
                .astype("Int64")
            )
        else:
            out["volume_int"] = pd.array([pd.NA] * len(out), dtype="Int64")

        if "oi" in out.columns:
            out["oi_int"] = (
                pd.to_numeric(out["oi"], errors="coerce").round().astype("Int64")
            )
        else:
            out["oi_int"] = pd.array([pd.NA] * len(out), dtype="Int64")

        # A contract is keyed in LEAN by (strike_deci, expiry_str, right);
        # any row that can't produce all three is unaddressable.
        out = out.dropna(subset=["strike_deci", "right", "time_ms"])
        out = out[out["expiry_str"].str.len() == 8]
        return out

    def _write_minute_zip(
        self,
        zpath: Path,
        df: pd.DataFrame,
        *,
        date_compact: str,
        symbol: str,
        tick_type: str,
        content_cols: list[str],
    ) -> None:
        """One zip; one inner CSV per (strike, expiry, right) tuple."""
        sorted_df = df.sort_values(
            ["strike_deci", "expiry_str", "right", "time_ms"], kind="mergesort"
        )
        with zipfile.ZipFile(zpath, "w", zipfile.ZIP_DEFLATED) as zf:
            for (strike_deci, expiry_str, right), group in sorted_df.groupby(
                ["strike_deci", "expiry_str", "right"], sort=False
            ):
                long_right = _LONG_FROM_RIGHT[right]
                inner = (
                    f"{date_compact}_{symbol}_minute_{tick_type}"
                    f"_{self.option_style}_{long_right}_{int(strike_deci)}"
                    f"_{expiry_str}.csv"
                )
                buf = io.StringIO()
                group[content_cols].to_csv(buf, header=False, index=False)
                zf.writestr(inner, buf.getvalue())

    def _write_universe(self, path: Path, df: pd.DataFrame) -> None:
        """Daily aggregates: first/max/min/last by (strike, expiry, right)."""
        # Sort so groupby first/last are chronological.
        ordered = df.sort_values("time_ms", kind="mergesort")
        agg = (
            ordered.groupby(
                ["strike_deci", "expiry_str", "right"], sort=False
            )
            .agg(
                first_o=("o_deci", "first"),
                max_h=("h_deci", "max"),
                min_l=("l_deci", "min"),
                last_c=("c_deci", "last"),
                total_v=("volume_int", "sum"),
                last_oi=("oi_int", "last"),
            )
            .reset_index()
            .sort_values(["expiry_str", "strike_deci", "right"])
        )

        header = (
            "#expiry,strike,right,open,high,low,close,volume,open_interest,"
            "implied_volatility,delta,gamma,vega,theta,rho\n"
        )
        # Underlying row (LEAN convention places it first). Left blank
        # here — see class docstring.
        underlying_row = ",,,,,,,,,,,,,\n"

        with open(path, "w") as f:
            f.write(header)
            f.write(underlying_row)
            for _, r in agg.iterrows():
                strike_int = int(r["strike_deci"]) // _DECI_CENT_MULTIPLIER
                o = self._fmt_deci(r["first_o"])
                h = self._fmt_deci(r["max_h"])
                l = self._fmt_deci(r["min_l"])
                c = self._fmt_deci(r["last_c"])
                v = "" if pd.isna(r["total_v"]) else str(int(r["total_v"]))
                oi = (
                    ""
                    if pd.isna(r["last_oi"]) or int(r["last_oi"]) <= 0
                    else str(int(r["last_oi"]))
                )
                f.write(
                    f"{r['expiry_str']},{strike_int},{r['right']},"
                    f"{o},{h},{l},{c},{v},{oi},,,,,,\n"
                )

    @staticmethod
    def _fmt_deci(val) -> str:
        if pd.isna(val):
            return ""
        return f"{int(val) / _DECI_CENT_MULTIPLIER:.4f}"
