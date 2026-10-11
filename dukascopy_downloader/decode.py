"""Decoding of the delta-encoded JSON returned by the jetta API.

Candles::

    {"timestamp": 1704153600000, "multiplier": 1e-5, "shift": 60000,
     "open": 1.10366, "high": ..., "low": ..., "close": ...,
     "times": [0, 1, 1, ...], "opens": [0, 5, 3, ...], "highs": [...],
     "lows": [...], "closes": [...], "volumes": [102.37, ...]}

    time[i]  = timestamp + cumsum(times)[i] * shift          (ms, UTC)
    open[i]  = (round(open / multiplier) + cumsum(opens)[i]) * multiplier
    ... the same for high / low / close; volumes are plain values.

Ticks::

    {"timestamp": ..., "multiplier": 1e-5, "bid": .., "ask": ..,
     "times": [...ms deltas...], "bids": [...], "asks": [...],
     "bidVolumes": [...], "askVolumes": [...]}
"""
from __future__ import annotations

import math
from datetime import datetime
from typing import Optional

import numpy as np
import pandas as pd

CANDLE_COLUMNS = ["time", "open", "high", "low", "close", "volume"]
TICK_COLUMNS = ["time", "bid", "ask", "bid_volume", "ask_volume"]


class DataError(ValueError):
    """Response is structurally invalid (will be retried)."""


def _decimals(multiplier: float) -> int:
    if multiplier <= 0:
        raise DataError(f"invalid multiplier {multiplier}")
    d = -math.log10(multiplier)
    if abs(d - round(d)) < 1e-9:
        return max(0, int(round(d)))
    return 10


def _times(j: dict, n: int, shift: int) -> np.ndarray:
    deltas = np.asarray(j["times"], dtype=np.int64)
    if len(deltas) != n:
        raise DataError("times length mismatch")
    if n > 1 and (deltas[1:] <= 0).any():
        raise DataError("timestamps are not strictly increasing")
    return int(j["timestamp"]) + np.cumsum(deltas) * int(shift)


def _series(j: dict, base_key: str, delta_key: str, n: int, mult: float, dec: int) -> np.ndarray:
    deltas = np.asarray(j[delta_key], dtype=np.int64)
    if len(deltas) != n:
        raise DataError(f"{delta_key} length mismatch")
    base = int(round(float(j[base_key]) / mult))
    return np.round((base + np.cumsum(deltas)) * mult, dec)


def _to_time(ms: np.ndarray) -> pd.Series:
    return pd.Series(pd.to_datetime(ms, unit="ms", utc=True))


def _check_range(ms: np.ndarray, start: Optional[datetime], end: Optional[datetime]) -> None:
    if len(ms) == 0 or start is None or end is None:
        return
    lo, hi = int(start.timestamp() * 1000), int(end.timestamp() * 1000)
    if ms[0] < lo or ms[-1] >= hi:
        raise DataError(f"timestamps outside of requested range {start} - {end}")


def empty_candles() -> pd.DataFrame:
    df = pd.DataFrame({c: pd.Series(dtype="float64") for c in CANDLE_COLUMNS})
    df["time"] = pd.Series(pd.to_datetime([], utc=True))
    return df


def empty_ticks() -> pd.DataFrame:
    df = pd.DataFrame({c: pd.Series(dtype="float64") for c in TICK_COLUMNS})
    df["time"] = pd.Series(pd.to_datetime([], utc=True))
    return df


def decode_candles(j: dict, start: Optional[datetime] = None, end: Optional[datetime] = None) -> pd.DataFrame:
    try:
        n = len(j.get("times") or [])
        if n == 0:
            return empty_candles()
        mult = float(j["multiplier"])
        dec = _decimals(mult)
        ms = _times(j, n, j.get("shift") or 60000)
        _check_range(ms, start, end)
        vols = np.asarray(j["volumes"], dtype="float64")
        if len(vols) != n:
            raise DataError("volumes length mismatch")
        df = pd.DataFrame({
            "time": _to_time(ms),
            "open": _series(j, "open", "opens", n, mult, dec),
            "high": _series(j, "high", "highs", n, mult, dec),
            "low": _series(j, "low", "lows", n, mult, dec),
            "close": _series(j, "close", "closes", n, mult, dec),
            "volume": vols,
        })
    except (KeyError, TypeError) as e:
        raise DataError(f"malformed candle response: {e!r}") from e
    return df


def decode_ticks(j: dict, start: Optional[datetime] = None, end: Optional[datetime] = None) -> pd.DataFrame:
    try:
        times = j.get("times") or []
        n = len(times)
        if n == 0:
            return empty_ticks()
        mult = float(j["multiplier"])
        dec = _decimals(mult)
        deltas = np.asarray(times, dtype=np.int64)
        if (deltas[1:] < 0).any():  # several ticks may share one millisecond
            raise DataError("tick timestamps decreasing")
        ms = int(j["timestamp"]) + np.cumsum(deltas) * int(j.get("shift") or 1)
        _check_range(ms, start, end)
        bv = np.asarray(j["bidVolumes"], dtype="float64")
        av = np.asarray(j["askVolumes"], dtype="float64")
        if len(bv) != n or len(av) != n:
            raise DataError("volume length mismatch")
        df = pd.DataFrame({
            "time": _to_time(ms),
            "bid": _series(j, "bid", "bids", n, mult, dec),
            "ask": _series(j, "ask", "asks", n, mult, dec),
            "bid_volume": bv,
            "ask_volume": av,
        })
    except (KeyError, TypeError) as e:
        raise DataError(f"malformed tick response: {e!r}") from e
    return df


def ohlc_violations(df: pd.DataFrame) -> int:
    """Number of candles where high/low do not envelope open/close."""
    if df.empty:
        return 0
    bad = (df["low"] > df[["open", "close"]].min(axis=1) + 1e-12) | \
          (df["high"] < df[["open", "close"]].max(axis=1) - 1e-12) | (df["low"] > df["high"])
    return int(bad.sum())


def resample_candles(df: pd.DataFrame, rule: str) -> pd.DataFrame:
    """Aggregate OHLCV candles (UTC 'time' column) to a coarser timeframe."""
    if df.empty:
        return empty_candles()
    g = df.set_index("time").resample(rule, label="left", closed="left")
    out = g.agg({"open": "first", "high": "max", "low": "min", "close": "last", "volume": "sum"})
    out = out.dropna(subset=["open"]).reset_index()
    return out[CANDLE_COLUMNS]


def ticks_to_candles(df: pd.DataFrame, rule: str, price: str = "bid") -> pd.DataFrame:
    """Build candles from ticks (price = 'bid' | 'ask'); volume = sum of tick volume."""
    if df.empty:
        return empty_candles()
    s = df.set_index("time")
    g = s[price].resample(rule, label="left", closed="left")
    out = g.ohlc()
    out["volume"] = s[f"{price}_volume"].resample(rule, label="left", closed="left").sum()
    out = out.dropna(subset=["open"]).reset_index()
    return out[CANDLE_COLUMNS]
