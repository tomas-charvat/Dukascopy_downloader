"""Reading the local store into DataFrames and exporting (CSV / Parquet)."""
from __future__ import annotations

from datetime import datetime
from pathlib import Path
from typing import Optional

import pandas as pd

from .decode import resample_candles, ticks_to_candles
from .storage import Store
from .timeframes import normalize_tf, parse_date


def _dt(v, end=False) -> Optional[datetime]:
    if v is None:
        return None
    if isinstance(v, (datetime, pd.Timestamp)):
        return pd.Timestamp(v).tz_localize("UTC").to_pydatetime() if pd.Timestamp(v).tzinfo is None \
            else pd.Timestamp(v).tz_convert("UTC").to_pydatetime()
    return parse_date(str(v), end=end)


def load(data_dir, code: str, tf: str = "m1", start=None, end=None, side: str = "BID",
         resample: Optional[str] = None, tz: Optional[str] = None, drop_flat: bool = False) -> pd.DataFrame:
    """Load stored data.

    side: BID | ASK | MID | BOTH  (MID = (bid+ask)/2, BOTH = bid_* and ask_* columns)
    resample: pandas rule, e.g. '5min', '15min', '1h', '4h', '1D' (candles; for ticks builds candles)
    tz: convert the 'time' column, e.g. 'Europe/Prague' (data is stored in UTC)
    drop_flat: drop candles with zero volume (Dukascopy fills inactive minutes with flat candles)
    """
    tf = normalize_tf(tf)
    store = Store(data_dir)
    s, e = _dt(start), _dt(end, end=True)
    side = (side or "BID").upper()

    def localize(d):
        if tz and not d.empty:
            d = d.copy()
            d["time"] = d["time"].dt.tz_convert(tz)  # resampling below then aligns to local time
        return d

    if tf == "tick":
        df = localize(store.read("tick", code, None, s, e))
        if resample:
            if side in ("BID", "ASK"):
                df = ticks_to_candles(df, resample, side.lower())
            else:
                df = _combine(ticks_to_candles(df, resample, "bid"), ticks_to_candles(df, resample, "ask"), side)
    else:
        def one(sd):
            d = store.read(tf, code, sd, s, e)
            if drop_flat and not d.empty:
                d = d[d["volume"] > 0]
            d = localize(d)
            if resample and not d.empty:
                d = resample_candles(d, resample)
            return d

        if side in ("BID", "ASK"):
            df = one(side)
        elif side in ("MID", "BOTH"):
            df = _combine(one("BID"), one("ASK"), side)
        else:
            raise ValueError("side must be BID, ASK, MID or BOTH")
    return df.reset_index(drop=True)


def _combine(b: pd.DataFrame, a: pd.DataFrame, side: str) -> pd.DataFrame:
    m = b.merge(a, on="time", how="inner", suffixes=("_bid", "_ask"))
    if side == "BOTH":
        return m
    out = pd.DataFrame({"time": m["time"]})
    for c in ("open", "high", "low", "close"):
        out[c] = ((m[f"{c}_bid"] + m[f"{c}_ask"]) / 2).round(8)
    out["volume"] = m["volume_bid"]
    out["spread"] = (m["close_ask"] - m["close_bid"]).round(8)
    return out


def export(data_dir, code: str, tf: str, out_path, start=None, end=None, side="BID",
           resample=None, tz=None, drop_flat=False, fmt: Optional[str] = None,
           time_format: Optional[str] = None) -> int:
    df = load(data_dir, code, tf, start, end, side, resample, tz, drop_flat)
    out = Path(out_path)
    out.parent.mkdir(parents=True, exist_ok=True)
    fmt = fmt or ("parquet" if out.suffix == ".parquet" else "csv")
    if fmt == "parquet":
        df.to_parquet(out, index=False)
    else:
        d = df.copy()
        if time_format:
            d["time"] = d["time"].dt.strftime(time_format)
        else:
            d["time"] = d["time"].dt.strftime("%Y-%m-%d %H:%M:%S.%f" if tf == "tick" and not resample
                                              else "%Y-%m-%d %H:%M:%S")
        compression = "gzip" if out.name.endswith(".gz") else None
        d.to_csv(out, index=False, compression=compression)
    return len(df)
