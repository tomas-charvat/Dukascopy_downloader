"""Timeframes, date helpers and download-chunk planning.

The Dukascopy jetta API serves data in fixed, calendar aligned chunks (all UTC):

    tick  -> 1 hour   /v1/ticks/{code}/{Y}/{M}/{D}/{H}
    m1    -> 1 day    /v1/candles/minute/{code}/{side}/{Y}/{M}/{D}
    h1    -> 1 month  /v1/candles/hour/{code}/{side}/{Y}/{M}
    d1    -> 1 year   /v1/candles/day/{code}/{side}/{Y}

A chunk is served only after its period has fully ended (otherwise the API
answers 400 "From time is too late").  For h1 / d1 the still running month /
year is therefore *derived* from lower timeframes (m1 days, h1 months).
"""
from __future__ import annotations

import re
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Iterator, List, Optional

UTC = timezone.utc

TIMEFRAMES = ("tick", "m1", "h1", "d1")

_TF_ALIASES = {
    "tick": "tick", "ticks": "tick", "t": "tick",
    "m1": "m1", "1m": "m1", "1min": "m1", "min": "m1", "minute": "m1", "minutes": "m1",
    "h1": "h1", "1h": "h1", "60m": "h1", "hour": "h1", "hours": "h1", "hourly": "h1",
    "d1": "d1", "1d": "d1", "day": "d1", "days": "d1", "daily": "d1",
}

# API period name used in instrument "histories"
TF_HISTORY_PERIOD = {"tick": "TICK", "m1": "MINUTE", "h1": "HOUR", "d1": "DAY"}

# pandas resample rule used when deriving a timeframe from a lower one
TF_RULE = {"m1": "1min", "h1": "1h", "d1": "1D"}


def normalize_tf(tf: str) -> str:
    key = str(tf).strip().lower()
    if key not in _TF_ALIASES:
        raise ValueError(f"Unknown timeframe '{tf}'. Use one of: tick, m1, h1, d1")
    return _TF_ALIASES[key]


# --------------------------------------------------------------------------- dates

def utcnow() -> datetime:
    return datetime.now(UTC)


def to_utc(dt: datetime) -> datetime:
    if dt.tzinfo is None:
        return dt.replace(tzinfo=UTC)
    return dt.astimezone(UTC)


_DATE_RE = re.compile(r"^(\d{4})(?:-(\d{1,2})(?:-(\d{1,2})(?:[t ](\d{1,2})(?::(\d{2}))?)?)?)?$")


def parse_date(text: str, end: bool = False) -> datetime:
    """Parse '2015', '2015-03', '2015-03-02', '2015-03-02T14', 'today', 'now'.

    With ``end=True`` the value is treated as an *inclusive* end, i.e. the
    returned datetime is the exclusive end of that period
    ('2015' -> 2016-01-01, '2015-03' -> 2015-04-01, '2015-03-02' -> 2015-03-03).
    """
    t = str(text).strip().lower()
    now = utcnow()
    if t == "now":
        return now
    if t == "today":
        d = floor_dt(now, "day")
        return d + timedelta(days=1) if end else d
    if t == "yesterday":
        d = floor_dt(now, "day") - timedelta(days=1)
        return d + timedelta(days=1) if end else d
    m = _DATE_RE.match(t)
    if not m:
        raise ValueError(f"Cannot parse date '{text}'. Use YYYY, YYYY-MM, YYYY-MM-DD or YYYY-MM-DDTHH")
    y, mo, d, h, mi = m.groups()
    y = int(y)
    if mo is None:
        start = datetime(y, 1, 1, tzinfo=UTC)
        return datetime(y + 1, 1, 1, tzinfo=UTC) if end else start
    mo = int(mo)
    if d is None:
        start = datetime(y, mo, 1, tzinfo=UTC)
        return add_months(start, 1) if end else start
    d = int(d)
    if h is None:
        start = datetime(y, mo, d, tzinfo=UTC)
        return start + timedelta(days=1) if end else start
    start = datetime(y, mo, d, int(h), int(mi or 0), tzinfo=UTC)
    return start  # exact time -> exclusive end as given


def add_months(dt: datetime, n: int) -> datetime:
    total = dt.year * 12 + (dt.month - 1) + n
    return dt.replace(year=total // 12, month=total % 12 + 1, day=1)


def floor_dt(dt: datetime, unit: str) -> datetime:
    dt = to_utc(dt)
    if unit == "hour":
        return dt.replace(minute=0, second=0, microsecond=0)
    if unit == "day":
        return dt.replace(hour=0, minute=0, second=0, microsecond=0)
    if unit == "month":
        return dt.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
    if unit == "year":
        return dt.replace(month=1, day=1, hour=0, minute=0, second=0, microsecond=0)
    raise ValueError(unit)


def ceil_dt(dt: datetime, unit: str) -> datetime:
    f = floor_dt(dt, unit)
    if f == to_utc(dt):
        return f
    return _step(f, unit)


def _step(dt: datetime, unit: str) -> datetime:
    if unit == "hour":
        return dt + timedelta(hours=1)
    if unit == "day":
        return dt + timedelta(days=1)
    if unit == "month":
        return add_months(dt, 1)
    if unit == "year":
        return dt.replace(year=dt.year + 1)
    raise ValueError(unit)


# --------------------------------------------------------------------------- chunks

# API chunk unit for each timeframe and the storage partition unit
CHUNK_UNIT = {"tick": "hour", "m1": "day", "h1": "month", "d1": "year"}
PART_UNIT = {"tick": "day", "m1": "month", "h1": "year", "d1": None}  # None = single file


@dataclass(frozen=True)
class Chunk:
    """One API request: timeframe ``tf`` covering [start, end) in UTC."""
    tf: str
    start: datetime
    end: datetime

    @property
    def key(self) -> str:
        s = self.start
        if self.tf == "tick":
            return s.strftime("%Y-%m-%dT%H")
        if self.tf == "m1":
            return s.strftime("%Y-%m-%d")
        if self.tf == "h1":
            return s.strftime("%Y-%m")
        return s.strftime("%Y")

    def url_path(self, code: str, side: Optional[str]) -> str:
        s = self.start
        if self.tf == "tick":
            return f"/v1/ticks/{code}/{s.year}/{s.month}/{s.day}/{s.hour}"
        side = (side or "BID").upper()
        if self.tf == "m1":
            return f"/v1/candles/minute/{code}/{side}/{s.year}/{s.month}/{s.day}"
        if self.tf == "h1":
            return f"/v1/candles/hour/{code}/{side}/{s.year}/{s.month}"
        return f"/v1/candles/day/{code}/{side}/{s.year}"


def iter_chunks(tf: str, start: datetime, end: datetime) -> Iterator[Chunk]:
    """Calendar aligned API chunks of ``tf`` that overlap [start, end)."""
    unit = CHUNK_UNIT[tf]
    cur = floor_dt(start, unit)
    end = to_utc(end)
    while cur < end:
        nxt = _step(cur, unit)
        yield Chunk(tf, cur, nxt)
        cur = nxt


def part_key(tf: str, dt: datetime) -> str:
    unit = PART_UNIT[tf]
    if unit is None:
        return "all"
    if unit == "day":
        return dt.strftime("%Y-%m-%d")
    if unit == "month":
        return dt.strftime("%Y-%m")
    return dt.strftime("%Y")


def part_range(tf: str, key: str):
    """(start, end) of a storage partition key, or (None, None) for 'all'."""
    unit = PART_UNIT[tf]
    if unit is None or key == "all":
        return None, None
    start = parse_date(key)
    return start, _step(start, unit)


@dataclass(frozen=True)
class Job:
    """Planned download: an API chunk + where its data goes."""
    chunk: Chunk
    target_tf: str        # timeframe of the series the data is stored in
    part: str             # storage partition key
    native: bool          # True = chunk.tf == target_tf (cacheable, final)
    rule: Optional[str]   # resample rule when derived from a lower timeframe


def plan_jobs(tf: str, start: datetime, end: datetime, now: Optional[datetime] = None) -> List[Job]:
    """Plan API requests needed to cover [start, end) for timeframe ``tf``.

    Native chunks that already ended are requested directly.  Chunks that have
    not ended yet are, for h1/d1, derived from lower timeframe chunks that did
    end.  tick / m1 chunks that did not end are skipped (not served yet).
    """
    now = to_utc(now or utcnow())
    start, end = to_utc(start), to_utc(end)
    jobs: List[Job] = []
    for c in iter_chunks(tf, start, end):
        pk = part_key(tf, c.start)
        if c.end <= now:
            jobs.append(Job(c, tf, pk, True, None))
            continue
        if tf in ("tick", "m1"):
            continue
        limit = min(c.end, end, now)
        if tf == "h1":
            for d in iter_chunks("m1", c.start, limit):
                if d.end <= now:
                    jobs.append(Job(d, tf, pk, False, TF_RULE["h1"]))
        elif tf == "d1":
            for m in iter_chunks("h1", c.start, limit):
                if m.end <= now:
                    jobs.append(Job(m, tf, pk, False, TF_RULE["d1"]))
                else:
                    for d in iter_chunks("m1", m.start, min(m.end, limit)):
                        if d.end <= now:
                            jobs.append(Job(d, tf, pk, False, TF_RULE["d1"]))
    return jobs
