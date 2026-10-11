"""End-to-end test against the live API: N symbols from every category.

For every symbol it downloads (into a temporary store) and validates:

* m1 BID + ASK for the last 7 days of the previous month
* h1 BID for the previous month (native)    -> must equal m1 aggregated to 1h
* d1 BID previous year (native) + current year (derived from h1/m1)
                                            -> days inside the window must equal m1 aggregated to 1D
* ticks for one hour inside the window      -> bid <= ask, 1-min closes vs m1
* a second run of the m1 download must be served from cache (0 requests)
"""
from __future__ import annotations

import logging
import shutil
import sys
import tempfile
import time
from dataclasses import dataclass, field
from datetime import timedelta
from typing import List, Optional

import numpy as np
import pandas as pd

from .decode import ohlc_violations, resample_candles, ticks_to_candles
from .downloader import Downloader
from .instruments import CATEGORIES
from .timeframes import add_months, floor_dt, utcnow

log = logging.getLogger("dukascopy_downloader")


@dataclass
class SymbolReport:
    category: str
    code: str
    stats: dict = field(default_factory=dict)
    failures: List[str] = field(default_factory=list)
    warnings: List[str] = field(default_factory=list)

    @property
    def ok(self) -> bool:
        return not self.failures


def _compare(a: pd.DataFrame, b: pd.DataFrame, tol_price: float):
    """Compare two candle frames on the same timestamps -> (error or None, warning or None).

    Dukascopy's own aggregates occasionally carry a different *open* than the first
    minute of the period (verified directly on the API, e.g. EUR-USD 2026-09-24 03:00),
    while high/low/close/volume match - such open-only differences are reported as warnings.
    """
    if a.empty and b.empty:
        return None, None
    m = a.merge(b, on="time", how="outer", suffixes=("_a", "_b"), indicator=True)
    only = m[m["_merge"] != "both"]
    if len(only):
        return f"{len(only)} candle times differ (e.g. {only['time'].iloc[0]})", None
    for c in ("high", "low", "close"):
        d = (m[f"{c}_a"] - m[f"{c}_b"]).abs().max()
        if d > tol_price:
            return f"{c} differs by {d}", None
    va, vb = m["volume_a"], m["volume_b"]
    rel = ((va - vb).abs() / np.maximum(1e-12, vb.abs())).max()
    if rel > 1e-4:
        return f"volume differs (rel {rel:.2e})", None
    bad_open = m[(m["open_a"] - m["open_b"]).abs() > tol_price]
    if len(bad_open):
        return None, (f"open differs in {len(bad_open)}/{len(m)} candles (Dukascopy data quirk), "
                      f"e.g. {bad_open['time'].iloc[0]}")
    return None, None


def pick_symbols(dl: Downloader, per_category: int, window_start) -> List[tuple]:
    picked = []
    for group, label in CATEGORIES:
        n = 0
        for inst in dl.instruments.in_group(group):
            if n >= per_category:
                break
            try:
                hs = dl.instruments.history_start(inst["code"], "m1")
            except Exception as e:  # noqa: BLE001
                log.warning("skip %s: %s", inst["code"], e)
                continue
            if hs < window_start - timedelta(days=400):  # needs >1y history
                picked.append((label, inst["code"]))
                n += 1
        if n < per_category:
            log.warning("category %s: only %d instruments with enough history", group, n)
    return picked


def test_symbol(dl: Downloader, category: str, code: str, now=None) -> SymbolReport:
    rep = SymbolReport(category, code)
    now = now or utcnow()
    month0 = floor_dt(now, "month")
    pm_start = add_months(month0, -1)
    w_end = month0
    w_start = w_end - timedelta(days=7)
    year0 = floor_dt(now, "year")
    py_start = year0.replace(year=year0.year - 1)
    inst = dl.instruments.details(code)
    pip = float(inst.get("pipValue") or 1e-5)
    tol = pip / 1000.0  # well below one point

    def run(tf, s, e, side):
        r = dl.download_series(code, tf, s, e, side)
        for k, msg in r.errors:
            rep.failures.append(f"{tf} {side or ''} chunk {k}: {msg}")
        return r

    # --- m1 bid/ask
    run("m1", w_start, w_end, "BID")
    run("m1", w_start, w_end, "ASK")
    bid = dl.store.read("m1", code, "BID", w_start, w_end)
    ask = dl.store.read("m1", code, "ASK", w_start, w_end)
    rep.stats["m1_bid"] = len(bid)
    rep.stats["m1_ask"] = len(ask)
    if bid.empty or ask.empty:
        rep.failures.append("no m1 data in window")
        return rep
    for name, df in (("bid", bid), ("ask", ask)):
        v = ohlc_violations(df)
        if v:
            rep.failures.append(f"m1 {name}: {v} OHLC violations")
        if not df["time"].is_monotonic_increasing or df["time"].duplicated().any():
            rep.failures.append(f"m1 {name}: timestamps not unique/increasing")
    m = bid.merge(ask, on="time", suffixes=("_b", "_a"))
    frac = float((m["close_a"] >= m["close_b"]).mean()) if len(m) else 0.0
    rep.stats["ask>=bid"] = f"{frac:.1%}"
    if frac < 0.99:
        rep.failures.append(f"ask >= bid only in {frac:.1%} of minutes")

    # --- h1 native vs m1 aggregated
    run("h1", pm_start, w_end, "BID")
    h1 = dl.store.read("h1", code, "BID", w_start, w_end)
    rep.stats["h1"] = len(h1)
    err, warn = _compare(resample_candles(bid, "1h"), h1, tol)
    rep.stats["h1==m1"] = "DIFF" if err else ("OK*" if warn else "OK")
    if err:
        rep.failures.append(f"h1 vs m1->1h: {err}")
    if warn:
        rep.warnings.append(f"h1 vs m1->1h: {warn}")

    # --- d1 previous year (native) + current year (derived)
    run("d1", py_start, now, "BID")
    d1 = dl.store.read("d1", code, "BID", py_start, None)
    rep.stats["d1_prev_year"] = int(((d1["time"] >= pd.Timestamp(py_start)) & (d1["time"] < pd.Timestamp(year0))).sum())
    rep.stats["d1_this_year"] = int((d1["time"] >= pd.Timestamp(year0)).sum())
    if rep.stats["d1_prev_year"] == 0:
        rep.failures.append("no d1 data for previous year")
    if rep.stats["d1_this_year"] == 0 and now - year0 > timedelta(days=7):
        rep.failures.append("no derived d1 data for current year")
    d1w = d1[(d1["time"] >= pd.Timestamp(w_start)) & (d1["time"] < pd.Timestamp(w_end))]
    err, warn = _compare(resample_candles(bid, "1D"), d1w, tol)
    rep.stats["d1==m1"] = "DIFF" if err else ("OK*" if warn else "OK")
    for msg in (err, warn):
        if msg:
            rep.warnings.append(f"d1 vs m1->1D: {msg}")

    # --- ticks for one hour
    hour = bid["time"].iloc[len(bid) // 2].floor("h").to_pydatetime()
    run("tick", hour, hour + timedelta(hours=1), None)
    ticks = dl.store.read("tick", code, None, hour, hour + timedelta(hours=1))
    rep.stats["ticks"] = len(ticks)
    if ticks.empty:
        rep.failures.append(f"no ticks in {hour:%Y-%m-%d %H}:00")
    else:
        tf_ok = float((ticks["ask"] >= ticks["bid"]).mean())
        if tf_ok < 0.99:
            rep.failures.append(f"tick ask >= bid only in {tf_ok:.1%}")
        tc = ticks_to_candles(ticks, "1min", "bid")
        mm = tc.merge(bid, on="time", suffixes=("_t", "_m"))
        if len(mm):
            match = float(((mm["close_t"] - mm["close_m"]).abs() <= tol).mean())
            rep.stats["tick~m1"] = f"{match:.0%}"
            if match < 0.9:
                rep.warnings.append(f"tick-built 1min closes match m1 only in {match:.0%}")

    # --- resume: second run must not hit the network
    before = dl.client.request_count
    r = dl.download_series(code, "m1", w_start, w_end, "BID")
    rep.stats["resume_requests"] = dl.client.request_count - before
    if r.fetched:
        rep.failures.append(f"resume: {r.fetched} chunks downloaded again")
    return rep


def run_selftest(per_category: int = 2, symbols: Optional[List[str]] = None, data_dir: Optional[str] = None,
                 keep: bool = False, workers: int = 6, rate_limit: float = 8.0, printer=print) -> bool:
    tmp = data_dir or tempfile.mkdtemp(prefix="duka_selftest_")
    dl = Downloader(tmp, workers=workers, rate_limit=rate_limit, progress=False)
    t0 = time.monotonic()
    try:
        now = utcnow()
        w_start = floor_dt(now, "month") - timedelta(days=7)
        if symbols:
            targets = [("custom", c) for c in dl.resolve_symbols(symbols)]
        else:
            targets = pick_symbols(dl, per_category, w_start)
        printer(f"Selftest: {len(targets)} symbols, window {w_start:%Y-%m-%d} .. "
                f"{floor_dt(now, 'month'):%Y-%m-%d} (UTC), store: {tmp} [{dl.store.fmt}]")
        reports = []
        for cat, code in targets:
            try:
                rep = test_symbol(dl, cat, code, now)
            except Exception as e:  # noqa: BLE001
                rep = SymbolReport(cat, code, failures=[f"exception: {e!r}"])
            reports.append(rep)
            st = rep.stats
            printer(f"  [{'OK  ' if rep.ok else 'FAIL'}] {cat:<30} {code:<18} "
                    f"m1={st.get('m1_bid', 0)}/{st.get('m1_ask', 0)} h1={st.get('h1', 0)}({st.get('h1==m1', '-')}) "
                    f"d1={st.get('d1_prev_year', 0)}+{st.get('d1_this_year', 0)}({st.get('d1==m1', '-')}) "
                    f"ticks={st.get('ticks', 0)} ask>=bid={st.get('ask>=bid', '-')} tick~m1={st.get('tick~m1', '-')} "
                    f"resume_req={st.get('resume_requests', '-')}")
            for f in rep.failures:
                printer(f"         FAIL: {f}")
            for w in rep.warnings:
                printer(f"         warn: {w}")
            sys.stdout.flush()
        ok = sum(r.ok for r in reports)
        printer(f"Result: {ok}/{len(reports)} symbols OK, {dl.client.request_count} HTTP requests, "
                f"{time.monotonic() - t0:.0f}s")
        return ok == len(reports) and len(reports) > 0
    finally:
        dl.close()
        if not keep and data_dir is None:
            shutil.rmtree(tmp, ignore_errors=True)
