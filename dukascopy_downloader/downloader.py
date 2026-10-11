"""High level, resumable, parallel downloader."""
from __future__ import annotations

import logging
import sys
import threading
import time
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Sequence, Tuple

import pandas as pd

from .client import DukascopyClient, TooLateError, UnknownInstrumentError
from .decode import decode_candles, decode_ticks, empty_candles, empty_ticks, resample_candles
from .instruments import Instruments
from .storage import Store
from .timeframes import Job, normalize_tf, parse_date, plan_jobs, to_utc, utcnow

log = logging.getLogger("dukascopy_downloader")

SIDES = ("BID", "ASK")


def normalize_sides(side) -> Tuple[str, ...]:
    if side is None:
        return ("BID",)
    if isinstance(side, str):
        s = side.strip().upper()
        if s in ("BOTH", "ALL", "BIDASK"):
            return SIDES
        side = [x for x in s.replace(",", " ").split() if x]
    out = []
    for s in side:
        s = s.upper()
        if s not in SIDES:
            raise ValueError(f"side must be BID, ASK or BOTH (got {s})")
        if s not in out:
            out.append(s)
    return tuple(out)


@dataclass
class SeriesResult:
    code: str
    tf: str
    side: Optional[str]
    start: Optional[datetime] = None
    end: Optional[datetime] = None
    planned: int = 0
    cached: int = 0
    fetched: int = 0
    not_yet_published: int = 0
    rows_written: int = 0
    seconds: float = 0.0
    errors: List[Tuple[str, str]] = field(default_factory=list)

    @property
    def ok(self) -> bool:
        return not self.errors

    def summary(self) -> str:
        side = self.side or "-"
        s = (f"{self.code:<18} {self.tf:<4} {side:<4} requests={self.fetched:<6} cached={self.cached:<6} "
             f"rows={self.rows_written:<9} {self.seconds:6.1f}s")
        if self.not_yet_published:
            s += f"  not-yet-published={self.not_yet_published}"
        if self.errors:
            s += f"  ERRORS={len(self.errors)}"
        return s


class _Progress:
    def __init__(self, total: int, desc: str, enabled: bool):
        self.total, self.desc, self.n = total, desc, 0
        self._bar = None
        self._last = 0.0
        self.enabled = enabled and total > 0 and sys.stderr.isatty()
        if self.enabled:
            try:
                from tqdm import tqdm
                self._bar = tqdm(total=total, desc=desc, unit="req", leave=False, dynamic_ncols=True)
            except ImportError:
                self._bar = None

    def update(self, k: int = 1) -> None:
        self.n += k
        if not self.enabled:
            return
        if self._bar is not None:
            self._bar.update(k)
            return
        now = time.monotonic()
        if now - self._last > 1.0 or self.n == self.total:
            self._last = now
            pct = 100.0 * self.n / self.total
            sys.stderr.write(f"\r  {self.desc}: {self.n}/{self.total} ({pct:5.1f}%)")
            sys.stderr.flush()

    def close(self) -> None:
        if not self.enabled:
            return
        if self._bar is not None:
            self._bar.close()
        else:
            sys.stderr.write("\r" + " " * 80 + "\r")
            sys.stderr.flush()


class Downloader:
    """Download Dukascopy history into a local store.

    >>> d = Downloader("data")
    >>> d.download(["EURUSD", "XAUUSD"], tf="m1", start="2020-01-01", sides="both")
    """

    def __init__(self, data_dir: str = "data", workers: int = 6, rate_limit: float = 8.0,
                 max_retries: int = 8, fmt: Optional[str] = None,
                 client: Optional[DukascopyClient] = None, settle_minutes: int = 60,
                 progress: bool = True):
        self.store = Store(data_dir, fmt)
        self.workers = max(1, int(workers))
        self.client = client or DukascopyClient(rate_limit=rate_limit, max_retries=max_retries,
                                                pool_size=max(16, self.workers * 2))
        self.instruments = Instruments(self.client, self.store.meta_dir)
        self.settle = timedelta(minutes=settle_minutes)
        self.progress = progress
        self._stop = threading.Event()
        self.now = utcnow  # injectable clock (tests)

    # ------------------------------------------------------------------ symbols
    def resolve_symbols(self, symbols: Iterable[str] = (), groups: Iterable[str] = ()) -> List[str]:
        codes: List[str] = []
        if isinstance(symbols, str):
            symbols = [symbols]
        if isinstance(groups, str):
            groups = [groups]
        for s in symbols or ():
            for part in str(s).replace(",", " ").split():
                c = self.instruments.resolve(part)
                if c not in codes:
                    codes.append(c)
        for g in groups or ():
            items = self.instruments.in_group(g)
            if not items:
                raise KeyError(f"Unknown or empty group '{g}'. See: duka groups")
            for i in items:
                if i["code"] not in codes:
                    codes.append(i["code"])
        return codes

    # ------------------------------------------------------------------ fetching
    def _fetch(self, code: str, side: Optional[str], job: Job):
        if self._stop.is_set():
            raise RuntimeError("cancelled")
        c = job.chunk
        if c.tf == "tick":
            parse = lambda j: decode_ticks(j, c.start, c.end)  # noqa: E731
        else:
            parse = lambda j: decode_candles(j, c.start, c.end)  # noqa: E731
        try:
            df = self.client.get_json(c.url_path(code, side), parse)
            final = True
        except TooLateError:
            df = empty_ticks() if c.tf == "tick" else empty_candles()
            final = False
        if job.rule and not df.empty:
            df = resample_candles(df, job.rule)
        return df, final

    def download_series(self, code: str, tf: str = "m1", start=None, end=None,
                        side: Optional[str] = "BID", force: bool = False) -> SeriesResult:
        tf = normalize_tf(tf)
        side = None if tf == "tick" else (side or "BID").upper()
        res = SeriesResult(code, tf, side)
        t0 = time.monotonic()
        now = self.now()

        hs = self.instruments.history_start(code, tf)
        start = max(to_utc(_as_dt(start)), hs) if start is not None else hs
        end = min(to_utc(_as_dt(end, True)), now) if end is not None else now
        res.start, res.end = start, end
        if start >= end:
            log.info("%s %s %s: nothing to download (range %s - %s)", code, tf, side or "", start, end)
            return res

        jobs = plan_jobs(tf, start, end, now)
        done = set() if force else self.store.load_manifest(tf, code, side)
        todo = [j for j in jobs if not (j.native and j.chunk.key in done)]
        res.planned, res.cached = len(jobs), len(jobs) - len(todo)
        if not todo:
            res.seconds = time.monotonic() - t0
            return res

        remaining: Dict[str, int] = defaultdict(int)
        for j in todo:
            remaining[j.part] += 1
        buckets: Dict[str, List[pd.DataFrame]] = defaultdict(list)
        new_complete: Dict[str, List[str]] = defaultdict(list)
        complete = set(done)
        settle_limit = now - self.settle
        desc = f"{code} {tf} {side or ''}".strip()
        prog = _Progress(len(todo), desc, self.progress)

        self._stop.clear()
        ex = ThreadPoolExecutor(max_workers=self.workers)
        futs = {ex.submit(self._fetch, code, side, j): j for j in todo}
        try:
            for fut in as_completed(futs):
                j = futs[fut]
                try:
                    df, final = fut.result()
                    res.fetched += 1
                    if not df.empty:
                        buckets[j.part].append(df)
                    if not final:
                        res.not_yet_published += 1
                    elif j.native and j.chunk.end <= settle_limit:
                        new_complete[j.part].append(j.chunk.key)
                except UnknownInstrumentError:
                    raise
                except Exception as e:  # noqa: BLE001 - recorded, retried next run
                    res.errors.append((j.chunk.key, str(e)))
                    log.error("%s %s %s chunk %s failed: %s", code, tf, side or "", j.chunk.key, e)
                prog.update()
                remaining[j.part] -= 1
                if remaining[j.part] == 0:
                    frames = buckets.pop(j.part, [])
                    new = pd.concat(frames, ignore_index=True) if frames else None
                    keys = new_complete.pop(j.part, [])
                    if new is not None or keys:
                        res.rows_written += len(new) if new is not None else 0
                        self.store.write_part(tf, code, side, j.part, new)
                    if keys:
                        complete.update(keys)
                        self.store.save_manifest(tf, code, side, complete)
        except BaseException:
            self._stop.set()
            ex.shutdown(wait=False, cancel_futures=True)
            prog.close()
            raise
        ex.shutdown(wait=True)
        prog.close()
        res.seconds = time.monotonic() - t0
        return res

    def download(self, symbols: Iterable[str] = (), tf: str = "m1", start=None, end=None,
                 sides="BID", groups: Iterable[str] = (), force: bool = False,
                 on_result=None) -> List[SeriesResult]:
        tf = normalize_tf(tf)
        codes = self.resolve_symbols(symbols, groups)
        if not codes:
            raise ValueError("no symbols given")
        side_list: Sequence[Optional[str]] = (None,) if tf == "tick" else normalize_sides(sides)
        results = []
        for code in codes:
            for side in side_list:
                try:
                    r = self.download_series(code, tf, start, end, side, force)
                except UnknownInstrumentError as e:
                    r = SeriesResult(code, tf, side, errors=[("instrument", str(e))])
                results.append(r)
                log.info(r.summary())
                if on_result:
                    on_result(r)
        return results

    def update(self, tf: Optional[str] = None, symbols: Iterable[str] = (), on_result=None) -> List[SeriesResult]:
        """Bring every locally stored series up to date (only new chunks are requested)."""
        tfs = {normalize_tf(tf)} if tf else None
        codes = set(self.resolve_symbols(symbols)) if symbols else None
        results = []
        for s in self.store.list_series():
            if (tfs and s["tf"] not in tfs) or (codes and s["code"] not in codes):
                continue
            keys = self.store.load_manifest(s["tf"], s["code"], s["side"])
            if keys:
                ps = min(parse_date(k) for k in keys)
            else:
                df = self.store.read(s["tf"], s["code"], s["side"])
                if df.empty:
                    continue
                ps = df["time"].iloc[0].to_pydatetime()
            r = self.download_series(s["code"], s["tf"], ps, None, s["side"])
            results.append(r)
            log.info(r.summary())
            if on_result:
                on_result(r)
        return results

    # ------------------------------------------------------------------ reading
    def load(self, symbol: str, tf: str = "m1", start=None, end=None, side: str = "BID") -> pd.DataFrame:
        from .export import load as _load
        return _load(self.store.root, self.instruments.resolve(symbol), tf, start, end, side)

    def close(self) -> None:
        self.client.close()


def _as_dt(v, end: bool = False) -> datetime:
    if isinstance(v, datetime):
        return v
    if isinstance(v, pd.Timestamp):
        return v.to_pydatetime()
    from .timeframes import parse_date
    return parse_date(str(v), end=end)
