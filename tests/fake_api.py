"""Offline fake of the jetta API used by unit tests (same JSON encoding as the real one)."""
from __future__ import annotations

import re
import zlib
import threading
from datetime import datetime, timedelta, timezone

import numpy as np
import pandas as pd

from dukascopy_downloader.client import ApiError, TooLateError, UnknownInstrumentError
from dukascopy_downloader.decode import DataError

UTC = timezone.utc
MULT = 1e-5


def encode_candles(df: pd.DataFrame, timestamp_ms: int, shift: int, mult: float = MULT) -> dict:
    """Encode integer-price candles (columns time_ms, o, h, l, c, v) like the API does."""
    if df.empty:
        return {"timestamp": timestamp_ms, "multiplier": mult, "open": 0, "high": 0, "low": 0, "close": 0,
                "shift": shift, "times": [], "opens": [], "highs": [], "lows": [], "closes": [], "volumes": []}
    t = (df["time_ms"].to_numpy() - timestamp_ms) // shift
    out = {"timestamp": timestamp_ms, "multiplier": mult, "shift": shift,
           "times": np.diff(t, prepend=0).tolist(), "volumes": df["v"].round(2).tolist()}
    for k, col in (("open", "o"), ("high", "h"), ("low", "l"), ("close", "c")):
        a = df[col].to_numpy(dtype=np.int64)
        out[k] = float(a[0] * mult)
        out[k + "s"] = np.diff(a, prepend=a[0]).tolist()
    return out


def fake_minutes(day: datetime, code: str, side: str) -> pd.DataFrame:
    """Deterministic minute candles for one UTC day (no data on Saturdays)."""
    if day.weekday() == 5:
        return pd.DataFrame(columns=["time_ms", "o", "h", "l", "c", "v"])
    seed = zlib.crc32(f"{code}{day.date().isoformat()}".encode())
    rng = np.random.default_rng(seed)
    n = 1440
    base = 110000 + int(rng.integers(-500, 500))
    c = base + np.cumsum(rng.integers(-3, 4, n))
    o = np.concatenate([[base], c[:-1]])
    h = np.maximum(o, c) + rng.integers(0, 3, n)
    l = np.minimum(o, c) - rng.integers(0, 3, n)
    spread = 2 if side == "ASK" else 0
    t0 = int(day.timestamp() * 1000)
    return pd.DataFrame({"time_ms": t0 + np.arange(n) * 60000, "o": o + spread, "h": h + spread,
                         "l": l + spread, "c": c + spread, "v": np.round(rng.random(n) * 100, 2)})


def _agg(df: pd.DataFrame, period_ms: int) -> pd.DataFrame:
    if df.empty:
        return df
    g = df.groupby(df["time_ms"] // period_ms * period_ms)
    out = pd.DataFrame({"o": g["o"].first(), "h": g["h"].max(), "l": g["l"].min(),
                        "c": g["c"].last(), "v": g["v"].sum()})
    return out.reset_index().rename(columns={"index": "time_ms"})


class FakeClient:
    def __init__(self, now: datetime, fail_urls=(), codes=("EUR-USD",)):
        self.now = now
        self.fail_urls = set(fail_urls)
        self.codes = set(codes)
        self.request_count = 0
        self.paths = []
        self._lock = threading.Lock()

    def close(self):
        pass

    def get_json(self, path, parse=None):
        with self._lock:
            self.request_count += 1
            self.paths.append(path)
        if path in self.fail_urls:
            raise ApiError(500, "boom", path)
        j = self._serve(path)
        return parse(j) if parse else j

    def _serve(self, path):
        if path == "/v1/instruments":
            return {"instruments": [
                {"code": "EUR-USD", "name": "EUR/USD", "groupId": 18, "rating": 10, "description": "Euro vs US Dollar"},
                {"code": "XAU-USD", "name": "XAU/USD", "groupId": 19, "rating": 9, "description": "Gold vs US Dollar"},
                {"code": "USA500.IDX-USD", "name": "USA500.IDX/USD", "groupId": 21, "rating": 8, "description": "S&P 500"},
                {"code": "BTC-USD", "name": "BTC/USD", "groupId": 9, "rating": 7, "description": "Bitcoin"},
                {"code": "BTC-EUR", "name": "BTC/EUR", "groupId": 9, "rating": 6, "description": "Bitcoin"},
            ], "groups": [
                {"id": 3, "code": "FX", "parentId": None, "description": "Forex"},
                {"id": 18, "code": "FX_MAJORS", "parentId": 3, "description": "Majors"},
                {"id": 19, "code": "FX_METALS", "parentId": 3, "description": "Metals"},
                {"id": 5, "code": "IDX", "parentId": None, "description": "Indices"},
                {"id": 21, "code": "IDX_AMERICA", "parentId": 5, "description": "America"},
                {"id": 9, "code": "VCCY", "parentId": None, "description": "Crypto"},
            ]}
        m = re.match(r"^/v1/instruments/(.+)$", path)
        if m:
            if m.group(1) not in self.codes:
                raise UnknownInstrumentError(400, "Unknown instrument", path)
            return {"code": m.group(1), "pipValue": 1e-4, "histories": [
                {"period": "TICK", "from": int(datetime(2020, 1, 1, tzinfo=UTC).timestamp() * 1000)},
                {"period": "MINUTE", "from": int(datetime(2020, 1, 1, tzinfo=UTC).timestamp() * 1000)},
                {"period": "HOUR", "from": int(datetime(2020, 1, 1, tzinfo=UTC).timestamp() * 1000)},
                {"period": "DAY", "from": int(datetime(2020, 1, 1, tzinfo=UTC).timestamp() * 1000)}]}
        m = re.match(r"^/v1/candles/(minute|hour|day)/([^/]+)/(BID|ASK)/(\d+)(?:/(\d+))?(?:/(\d+))?$", path)
        if not m:
            raise ApiError(404, "not found", path)
        period, code, side, y, mo, d = m.groups()
        if code not in self.codes:
            raise UnknownInstrumentError(400, "Unknown instrument", path)
        y = int(y)
        if period == "minute":
            start = datetime(y, int(mo), int(d), tzinfo=UTC)
            end = start + timedelta(days=1)
        elif period == "hour":
            start = datetime(y, int(mo), 1, tzinfo=UTC)
            end = (start + timedelta(days=32)).replace(day=1)
        else:
            start = datetime(y, 1, 1, tzinfo=UTC)
            end = datetime(y + 1, 1, 1, tzinfo=UTC)
        if end > self.now:
            raise TooLateError(400, "From time is too late", path)
        days = []
        cur = start
        while cur < end:
            days.append(fake_minutes(cur, code, side))
            cur += timedelta(days=1)
        mins = pd.concat(days, ignore_index=True) if days else pd.DataFrame()
        ts = int(start.timestamp() * 1000)
        if period == "minute":
            return encode_candles(mins, ts, 60000)
        if period == "hour":
            return encode_candles(_agg(mins, 3600000), ts, 3600000)
        return encode_candles(_agg(mins, 86400000), ts, 86400000)


def corrupt(j: dict) -> dict:
    j = dict(j)
    j["opens"] = j["opens"][:-1]
    return j


__all__ = ["FakeClient", "encode_candles", "fake_minutes", "DataError", "corrupt"]
