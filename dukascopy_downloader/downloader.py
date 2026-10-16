"""High level, resumable, parallel downloader."""
from __future__ import annotations

import logging
import sys
import threading
import time
from collections import defaultdict, deque
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


class _RateWindow:
    """Requests per second averaged over the last ``window`` seconds (shared across series)."""

    def __init__(self, window: float = 600.0):
        self.window = window
        self.n = 0
        self._samples: deque = deque()  # (monotonic time, cumulative count)
        self._lock = threading.Lock()

    def add(self, k: int = 1) -> None:
        now = time.monotonic()
        with self._lock:
            self.n += k
            if self._samples and now - self._samples[-1][0] < 1.0:
                self._samples[-1] = (self._samples[-1][0], self.n)  # at most one sample per second
            else:
                self._samples.append((now, self.n))
            while len(self._samples) > 2 and self._samples[1][0] <= now - self.window:
                self._samples.popleft()

    def rate(self) -> Optional[float]:
        with self._lock:
            if len(self._samples) < 2:
                return None
            (t0, n0), (t1, n1) = self._samples[0], self._samples[-1]
        return (n1 - n0) / (t1 - t0) if t1 - t0 >= 5.0 and n1 > n0 else None  # too noisy before 5 s


def _fmt_eta(seconds: float) -> str:
    m, s = divmod(int(round(seconds)), 60)
    h, m = divmod(m, 60)
    return f"{h}:{m:02d}:{s:02d}" if h else f"{m:02d}:{s:02d}"


class _Progress:
    """Progress bar whose ETA uses the request rate of the last 10 minutes, so it doesn't jump."""

    def __init__(self, total: int, desc: str, enabled: bool, rate: Optional[_RateWindow] = None):
        self.total, self.desc, self.n = total, desc, 0
        self.rate = rate or _RateWindow()
        self._bar = None
        self._last = 0.0
        self.enabled = enabled and total > 0 and sys.stderr.isatty()
        if self.enabled:
            try:
                from tqdm import tqdm
                self._bar = tqdm(total=total, desc=desc, unit="req", leave=False, dynamic_ncols=True,
                                 bar_format="{desc}: {percentage:3.0f}%|{bar}| {n_fmt}/{total_fmt} "
                                            "[{elapsed}{postfix}]")  # tqdm prefixes postfix with ", "
                self._bar.set_postfix_str(self._eta(), refresh=False)
            except ImportError:
                self._bar = None

    def _eta(self) -> str:
        r = self.rate.rate()
        return f"ETA {_fmt_eta((self.total - self.n) / r)}, {r:.1f} req/s" if r else "ETA ?"

    def update(self, k: int = 1) -> None:
        self.n += k
        self.rate.add(k)
        if not self.enabled:
            return
        now = time.monotonic()
        refresh = now - self._last > 1.0 or self.n == self.total
        if refresh:
            self._last = now
        if self._bar is not None:
            if refresh:
                self._bar.set_postfix_str(self._eta(), refresh=False)
            self._bar.update(k)
            return
        if refresh:
            pct = 100.0 * self.n / self.total
            sys.stderr.write(f"\r  {self.desc}: {self.n}/{self.total} ({pct:5.1f}%)  {self._eta()}   ")
            sys.stderr.flush()

    def close(self) -> None:
        if not self.enabled:
            return
        if self._bar is not None:
            self._bar.close()
        else:
            sys.stderr.write("\r" + " " * 100 + "\r")
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
        self.store.category_of = self.instruments.category_path
        self.settle = timedelta(minutes=settle_minutes)
        self.progress = progress
        self._rate = _RateWindow()  # request rate over the last 10 min, for the ETA
        self._stop = threading.Event()
        self.now = utcnow  # injectable clock (tests)

    # ------------------------------------------------------------------ symbols
    def resolve_symbols(self, symbols: Iterable[str] = (), groups: Iterable[str] = (),
                        all_instruments: bool = False, exclude: Iterable[str] = ()) -> List[str]:
        """Symbols + whole groups (+ the whole universe with ``all_instruments``) minus ``exclude``.

        ``exclude`` items may be group codes (FX_CROSSES, STCK_CFD, US, ...) or symbols.
        """
        codes: List[str] = []
        if isinstance(symbols, str):
            symbols = [symbols]
        if isinstance(groups, str):
            groups = [groups]
        if isinstance(exclude, str):
            exclude = [exclude]
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
        if all_instruments:
            inst = self.instruments
            ordered = sorted(inst.all, key=lambda i: (inst.category_path(i["code"]), -(i.get("rating") or 0)))
            for i in ordered:
                if i["code"] not in codes:
                    codes.append(i["code"])
        drop = set()
        for e in exclude or ():
            for part in str(e).replace(",", " ").split():
                items = self.instruments.in_group(part)
                if items:
                    drop.update(i["code"] for i in items)
                else:
                    drop.add(self.instruments.resolve(part))
        return [c for c in codes if c not in drop]

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

        if not self.instruments.details(code).get("histories"):
            log.info("%s: the API publishes no history for this instrument - skipped", code)
            res.seconds = time.monotonic() - t0
            return res
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
        prog = _Progress(len(todo), desc, self.progress, self._rate)

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
                 on_result=None, all_instruments: bool = False,
                 exclude: Iterable[str] = ()) -> List[SeriesResult]:
        tf = normalize_tf(tf)
        codes = self.resolve_symbols(symbols, groups, all_instruments, exclude)
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
                except Exception as e:  # noqa: BLE001 - one bad instrument must not stop a big batch
                    log.error("%s %s %s failed: %s", code, tf, side or "", e)
                    r = SeriesResult(code, tf, side, errors=[("series", str(e))])
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
