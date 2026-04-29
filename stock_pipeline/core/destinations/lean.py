"""QuantConnect LEAN destination for option-minute and equity-daily data.

Emits the on-disk shapes LEAN expects:

    # Minute options (write_minute_option_day)
    {base_dir}/option/{country}/minute/{symbol}/
        {YYYYMMDD}_trade_{style}.zip          (inner: per-contract CSVs)
        {YYYYMMDD}_openinterest_{style}.zip
    {base_dir}/option/{country}/universes/{symbol}/
        {YYYYMMDD}.csv

    # Daily equity (write_daily_equity)
    {base_dir}/equity/{country}/daily/{symbol}.zip
        {symbol}.csv                          (one line per day, no header)

The two resolutions live on the same class so any pipeline group can
take `lean: LeanStorage` as a resource and dispatch on a `storage=lean`
run tag — the right emitter is just a method call away.

Higher-level emitters (`write_minute_option_day`, `write_daily_equity`)
exist instead of the per-file `write(df, rel_path)` primitive that
`LocalStorage` uses, because LEAN's groupings (per-date for options,
per-symbol-with-merge for equity) don't fit one-DataFrame-per-path.

Conversion logic (deci-cent prices, time_ms, contract filename
encoding, universe row layout) follows the reference notebook
`csv_to_lean.ipynb` at repo root; daily-equity encoding follows
`daily_equity_transformer.py`.
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

    def write_daily_equity(
        self,
        df: pd.DataFrame,
        *,
        symbol: str,
    ) -> int:
        """Emit one symbol's full daily history in LEAN equity format.

        Args:
            df: rows with columns `date` ("YYYY-MM-DD"), `open`,
                `high`, `low`, `close`, `volume`. Extra columns are
                ignored.
            symbol: e.g. "RELIANCE". Lowercased for the LEAN path.

        Behavior: read-modify-write. If `{symbol}.zip` already exists
        at the target path, the existing inner CSV is parsed back into
        a frame and merged with `df`; rows are deduped by `date`
        (incoming wins) and sorted ascending before rewrite. This
        matches the parquet-side merge semantics of `daily_eq`'s local
        path so a narrow re-run never destroys old history.

        Returns: 1 on successful write, 0 if the merged frame is empty.
        """
        log = get_dagster_logger()
        sym = symbol.lower()
        out_dir = Path(self.base_dir) / "equity" / self.country / "daily"
        out_dir.mkdir(parents=True, exist_ok=True)
        zpath = out_dir / f"{sym}.zip"
        inner_name = f"{sym}.csv"

        incoming = self._normalize_daily_eq(df)
        existing = self._read_daily_equity_zip(zpath, inner_name)

        if existing is not None and not existing.empty:
            merged = pd.concat([existing, incoming], ignore_index=True)
            # `keep="last"` → incoming row wins on `date` overlap.
            merged = merged.drop_duplicates(subset=["date"], keep="last")
            merged = merged.sort_values("date").reset_index(drop=True)
        else:
            merged = incoming.sort_values("date").reset_index(drop=True)

        if merged.empty:
            log.warning(
                f"LeanStorage: nothing to write for {symbol} "
                f"(empty incoming frame and no existing zip)"
            )
            return 0

        buf = io.StringIO()
        for r in merged.itertuples(index=False):
            date_compact = str(r.date).replace("-", "")
            o = int(round(float(r.open) * _DECI_CENT_MULTIPLIER))
            h = int(round(float(r.high) * _DECI_CENT_MULTIPLIER))
            l = int(round(float(r.low) * _DECI_CENT_MULTIPLIER))
            c = int(round(float(r.close) * _DECI_CENT_MULTIPLIER))
            v = int(float(r.volume))
            buf.write(f"{date_compact} 00:00,{o},{h},{l},{c},{v}\n")

        with zipfile.ZipFile(zpath, "w", zipfile.ZIP_DEFLATED) as zf:
            zf.writestr(inner_name, buf.getvalue())

        return 1

    def write_daily_option(
        self,
        df: pd.DataFrame,
        *,
        underlying: str,
    ) -> int:
        """Emit one underlying's daily option history in LEAN format.

        Args:
            df: per-(contract, date) rows. Required columns:
                `datetime` (ISO "YYYY-MM-DD ..." or "YYYY-MM-DD"),
                `open`, `high`, `low`, `close`, `volume`, `strike`,
                `expiry` ("YYYY-MM-DD"), `option_type` ("CE"/"PE").
                Optional: `oi`. Extra columns are ignored.
            underlying: e.g. "NIFTY". Lowercased for the LEAN path.

        Layout (verified against Lean/Data/option/usa/daily/):
            {base}/option/{country}/daily/
                {ul}_{YYYY}_trade_{style}.zip
                {ul}_{YYYY}_openinterest_{style}.zip
            Inner entry per contract:
                {ul}_trade_{style}_{call|put}_{strikeDeci}_{expiryYYYYMMDD}.csv

        Behavior: read-modify-write per (underlying, year) zip.
        Existing rows are decoded and merged with incoming by
        `(entry_name, date)` — incoming wins. Years not present in
        `df` are not touched.

        Returns: number of zip files written across all touched
        years (trade + OI; OI is skipped when no positive-OI rows
        landed in that year).
        """
        if df.empty:
            return 0

        log = get_dagster_logger()
        prepared = self._prepare_daily_op(df)
        if prepared.empty:
            log.warning(
                f"LeanStorage: no usable rows for {underlying} after "
                f"derivation (missing strike/expiry/option_type, or "
                f"no row mapped to a valid CE/PE)"
            )
            return 0

        ul = underlying.lower()
        out_dir = Path(self.base_dir) / "option" / self.country / "daily"
        out_dir.mkdir(parents=True, exist_ok=True)

        written = 0
        for year_str, year_df in prepared.groupby("year_str", sort=False):
            written += self._write_daily_option_year(
                out_dir=out_dir,
                ul=ul,
                year_str=year_str,
                year_df=year_df,
            )
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

    @staticmethod
    def _normalize_daily_eq(df: pd.DataFrame) -> pd.DataFrame:
        """Project to the daily-equity columns and force `date` to ISO str.

        Drops everything except `date,open,high,low,close,volume`. Any
        missing required column raises — these are bugs in the upstream
        asset, not data quality issues to swallow.
        """
        if df.empty:
            return df.iloc[0:0][[]]
        required = ["date", "open", "high", "low", "close", "volume"]
        missing = [c for c in required if c not in df.columns]
        if missing:
            raise ValueError(
                f"LeanStorage.write_daily_equity: missing columns {missing}; "
                f"got {list(df.columns)}"
            )
        out = df[required].copy()
        out["date"] = out["date"].astype(str)
        return out

    @staticmethod
    def _read_daily_equity_zip(
        zpath: Path, inner_name: str
    ) -> pd.DataFrame | None:
        """Decode an existing LEAN daily-equity zip back into a frame.

        Returns None if the zip doesn't exist; an empty frame if the
        inner CSV is empty. Encoded prices are divided back by the
        deci-cent multiplier so the merge step works in the same units
        as the incoming frame.
        """
        if not zpath.exists():
            return None
        with zipfile.ZipFile(zpath, "r") as zf:
            try:
                raw = zf.read(inner_name).decode()
            except KeyError:
                return None
        if not raw.strip():
            return pd.DataFrame(
                columns=["date", "open", "high", "low", "close", "volume"]
            )
        rows = []
        for line in raw.splitlines():
            # Format: "YYYYMMDD 00:00,o,h,l,c,v" — split on "," and the
            # leading " " is part of the date field.
            parts = line.split(",")
            if len(parts) != 6:
                continue
            date_compact = parts[0].split(" ", 1)[0]
            iso = f"{date_compact[0:4]}-{date_compact[4:6]}-{date_compact[6:8]}"
            rows.append(
                {
                    "date": iso,
                    "open": int(parts[1]) / _DECI_CENT_MULTIPLIER,
                    "high": int(parts[2]) / _DECI_CENT_MULTIPLIER,
                    "low": int(parts[3]) / _DECI_CENT_MULTIPLIER,
                    "close": int(parts[4]) / _DECI_CENT_MULTIPLIER,
                    "volume": int(parts[5]),
                }
            )
        return pd.DataFrame(rows)

    def _prepare_daily_op(self, df: pd.DataFrame) -> pd.DataFrame:
        """Daily-option analog of `_prepare`.

        Adds `year_str`, `date_compact`, `strike_deci`, `expiry_str`,
        `right`, deci-cent O/H/L/C, integer V/OI. Drops rows missing
        any column needed to build a contract entry.
        """
        required = {"strike", "expiry", "option_type", "datetime"}
        if not required.issubset(df.columns):
            return df.iloc[0:0]

        out = df.copy()
        out["datetime"] = out["datetime"].astype(str)
        out["year_str"] = out["datetime"].str[0:4]
        out["date_compact"] = out["datetime"].str[0:10].str.replace(
            "-", "", regex=False
        )

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

        out = out.dropna(subset=["strike_deci", "right"])
        out = out[out["expiry_str"].str.len() == 8]
        out = out[out["date_compact"].str.len() == 8]
        out = out[out["year_str"].str.len() == 4]
        return out

    def _write_daily_option_year(
        self,
        *,
        out_dir: Path,
        ul: str,
        year_str: str,
        year_df: pd.DataFrame,
    ) -> int:
        """Write the trade and (if present) OI zip for one year.

        Read-modify-write: existing entries' rows are loaded keyed by
        date and merged with incoming. Returns the count of zips
        actually written (1 or 2).
        """
        written = 0
        style = self.option_style

        # ---- Trade zip ----
        trade_rows = year_df[year_df["o_deci"].notna()]
        if not trade_rows.empty:
            zpath = out_dir / f"{ul}_{year_str}_trade_{style}.zip"
            existing = self._read_daily_option_zip(zpath, tick_type="trade")
            entries: dict[str, dict[str, str]] = existing or {}

            for (strike_deci, expiry_str, right), group in trade_rows.groupby(
                ["strike_deci", "expiry_str", "right"], sort=False
            ):
                long_right = _LONG_FROM_RIGHT[right]
                entry = (
                    f"{ul}_trade_{style}_{long_right}"
                    f"_{int(strike_deci)}_{expiry_str}.csv"
                )
                bucket = entries.setdefault(entry, {})
                for r in group.itertuples(index=False):
                    line = (
                        f"{r.date_compact} 00:00,"
                        f"{int(r.o_deci)},{int(r.h_deci)},"
                        f"{int(r.l_deci)},{int(r.c_deci)},"
                        f"{int(r.volume_int) if not pd.isna(r.volume_int) else 0}"
                    )
                    bucket[r.date_compact] = line  # incoming wins

            self._flush_daily_option_zip(zpath, entries)
            written += 1

        # ---- OI zip ----
        oi_rows = year_df[year_df["oi_int"].notna() & (year_df["oi_int"] > 0)]
        if not oi_rows.empty:
            zpath = out_dir / f"{ul}_{year_str}_openinterest_{style}.zip"
            existing = self._read_daily_option_zip(zpath, tick_type="openinterest")
            entries = existing or {}

            for (strike_deci, expiry_str, right), group in oi_rows.groupby(
                ["strike_deci", "expiry_str", "right"], sort=False
            ):
                long_right = _LONG_FROM_RIGHT[right]
                entry = (
                    f"{ul}_openinterest_{style}_{long_right}"
                    f"_{int(strike_deci)}_{expiry_str}.csv"
                )
                bucket = entries.setdefault(entry, {})
                for r in group.itertuples(index=False):
                    line = f"{r.date_compact} 00:00,{int(r.oi_int)}"
                    bucket[r.date_compact] = line

            self._flush_daily_option_zip(zpath, entries)
            written += 1

        return written

    @staticmethod
    def _read_daily_option_zip(
        zpath: Path, *, tick_type: str
    ) -> dict[str, dict[str, str]] | None:
        """Decode an existing daily-option zip into {entry: {date: line}}.

        Used by the read-modify-write merge so re-runs don't drop
        previously-written days. Lines are kept as-is — they get
        re-emitted verbatim if the date isn't overwritten by incoming.
        `tick_type` is unused today but accepted so the call sites
        document what they're loading.
        """
        if not zpath.exists():
            return None
        out: dict[str, dict[str, str]] = {}
        with zipfile.ZipFile(zpath, "r") as zf:
            for name in zf.namelist():
                raw = zf.read(name).decode()
                bucket: dict[str, str] = {}
                for line in raw.splitlines():
                    if not line.strip():
                        continue
                    # First field is "YYYYMMDD HH:mm" — date is the
                    # leading 8 chars of that field.
                    date_compact = line.split(",", 1)[0].split(" ", 1)[0]
                    if len(date_compact) != 8:
                        continue
                    bucket[date_compact] = line
                if bucket:
                    out[name] = bucket
        return out

    @staticmethod
    def _flush_daily_option_zip(
        zpath: Path, entries: dict[str, dict[str, str]]
    ) -> None:
        """Write `{entry: {date: line}}` to a fresh zip, ascending."""
        with zipfile.ZipFile(zpath, "w", zipfile.ZIP_DEFLATED) as zf:
            for entry_name in sorted(entries.keys()):
                bucket = entries[entry_name]
                body = "\n".join(bucket[d] for d in sorted(bucket.keys())) + "\n"
                zf.writestr(entry_name, body)
