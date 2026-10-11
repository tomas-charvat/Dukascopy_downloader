"""Command line interface:  duka <command> ...   (or: python -m dukascopy_downloader <command> ...)"""
from __future__ import annotations

import argparse
import logging
import os
import sys
from logging.handlers import RotatingFileHandler
from pathlib import Path

from . import __version__

DEFAULT_DATA_DIR = os.environ.get("DUKA_DATA_DIR", "data")

EPILOG = """examples:
  duka groups
  duka instruments --group FX_MAJORS
  duka instruments --search gold
  duka info EURUSD
  duka download EURUSD XAUUSD --from 2015 --side both
  duka download --group FX_MAJORS --tf m1 --from 2014-01-01
  duka download BTCUSD --tf tick --from 2024-03-01 --to 2024-03-01
  duka update
  duka status
  duka export EURUSD --from 2020 --to 2024 --resample 15min --tz Europe/Prague -o eurusd_m15.csv
  duka selftest
"""


def _setup_logging(data_dir: str, verbose: int) -> None:
    root = logging.getLogger("dukascopy_downloader")
    root.setLevel(logging.DEBUG)
    root.handlers.clear()
    ch = logging.StreamHandler(sys.stderr)
    ch.setLevel(logging.DEBUG if verbose > 1 else logging.INFO if verbose else logging.WARNING)
    ch.setFormatter(logging.Formatter("%(levelname)s %(message)s"))
    root.addHandler(ch)
    try:
        log_dir = Path(data_dir) / "_logs"
        log_dir.mkdir(parents=True, exist_ok=True)
        fh = RotatingFileHandler(log_dir / "duka.log", maxBytes=5_000_000, backupCount=3, encoding="utf-8")
        fh.setLevel(logging.INFO)
        fh.setFormatter(logging.Formatter("%(asctime)s %(levelname)s %(message)s"))
        root.addHandler(fh)
    except OSError:
        pass


def _downloader(a, progress=True):
    from .downloader import Downloader
    return Downloader(a.data_dir, workers=a.workers, rate_limit=a.rate, max_retries=a.retries,
                      progress=progress and not a.no_progress)


def _print_results(results) -> int:
    print()
    print(f"{'symbol':<18} {'tf':<4} side")
    for r in results:
        print(r.summary())
        for k, msg in r.errors[:5]:
            print(f"    error {k}: {msg}")
        if len(r.errors) > 5:
            print(f"    ... {len(r.errors) - 5} more errors (see data/_logs/duka.log)")
    failed = [r for r in results if r.errors]
    if failed:
        print(f"\n{len(failed)} series had errors - run the same command again to retry only the missing chunks.")
        return 1
    print("\nDone.")
    return 0


# ---------------------------------------------------------------------------- commands

def cmd_groups(a) -> int:
    d = _downloader(a, False)
    rows = d.instruments.group_tree()
    by_parent = {}
    for g in rows:
        by_parent.setdefault(g.get("parentId"), []).append(g)

    def show(pid, indent):
        for g in sorted(by_parent.get(pid, []), key=lambda x: (x.get("sortOrder") or 99, x["code"])):
            print(f"{'  ' * indent}{g['code']:<{24 - 2 * indent}} {g['count']:>5}  {g['description']}")
            show(g["id"], indent + 1)

    print(f"{'GROUP':<24} {'COUNT':>5}  DESCRIPTION")
    show(None, 0)
    print("\nUse e.g.:  duka instruments --group FX_MAJORS   |   duka download --group FX_MAJORS")
    return 0


def cmd_instruments(a) -> int:
    d = _downloader(a, False)
    if a.refresh:
        d.instruments.load(refresh=True)
    items = d.instruments.search(a.search, a.group)
    for i in items[: a.limit]:
        print(f"{i['code']:<20} {i.get('name', ''):<20} {i.get('platformGroupId') or '':<12} {i.get('description', '')}")
    print(f"\n{len(items)} instruments" + (f" (showing {a.limit})" if len(items) > a.limit else ""))
    return 0


def cmd_info(a) -> int:
    from datetime import datetime, timezone
    d = _downloader(a, False)
    for s in a.symbols:
        code = d.instruments.resolve(s)
        i = d.instruments.details(code, refresh=a.refresh)
        print(f"{i['code']}  ({i.get('name')})  {i.get('description')}")
        print(f"  group: {i.get('groupCode')}  pip: {i.get('pipValue')}  price scale: {i.get('priceScale')}  "
              f"timezone: {i.get('defaultTimezone')}")
        for h in i.get("histories", []):
            t = datetime.fromtimestamp(h["from"] / 1000, timezone.utc) if h.get("from") is not None else None
            print(f"  history {h['period']:<7} from {t:%Y-%m-%d %H:%M} UTC" if t else f"  history {h['period']}: -")
        for tf in ("tick", "m1", "h1", "d1"):
            print(f"  downloader start {tf:<4}: {d.instruments.history_start(code, tf):%Y-%m-%d %H:%M} UTC")
        print()
    return 0


def cmd_download(a) -> int:
    from .timeframes import normalize_tf, parse_date
    d = _downloader(a)
    tf = normalize_tf(a.tf)
    if tf == "tick" and not a.start:
        print("For ticks please give --from (tick history is huge: 1 request per hour).", file=sys.stderr)
        return 2
    start = parse_date(a.start) if a.start else None
    end = parse_date(a.end, end=True) if a.end else None
    codes = d.resolve_symbols(a.symbols, a.group)
    s_txt = f"{start:%Y-%m-%d}" if start else "start of history"
    e_txt = f"{end:%Y-%m-%d %H:%M} (excl.)" if end else "now"
    print(f"Downloading {len(codes)} symbol(s)  tf={tf}  side={a.side.upper()}  {s_txt} -> {e_txt}  "
          f"into {Path(a.data_dir).resolve()} [{d.store.fmt}]", flush=True)
    results = d.download(codes, tf, start, end, a.side, force=a.force,
                         on_result=lambda r: print(r.summary(), flush=True))
    return _print_results(results)


def cmd_update(a) -> int:
    d = _downloader(a)
    results = d.update(a.tf, a.symbols, on_result=lambda r: print(r.summary(), flush=True))
    if not results:
        print("Nothing stored yet - use `duka download` first.")
        return 0
    return _print_results(results)


def cmd_status(a) -> int:
    from .storage import Store
    st = Store(a.data_dir)
    series = st.list_series()
    if not series:
        print(f"No data in {Path(a.data_dir).resolve()}")
        return 0
    print(f"{'tf':<5}{'category':<20}{'symbol':<20}{'side':<6}{'files':>6}{'rows':>12}{'MB':>9}  first -> last (UTC)")
    for s in series:
        i = st.series_summary(s["tf"], s["code"], s["side"])
        f = f"{i['first']:%Y-%m-%d %H:%M}" if i["first"] is not None else "-"
        l = f"{i['last']:%Y-%m-%d %H:%M}" if i["last"] is not None else "-"
        print(f"{i['tf']:<5}{i['category']:<20}{i['code']:<20}{i['side']:<6}{i['files']:>6}{i['rows']:>12}{i['size_mb']:>9}  {f} -> {l}")
    return 0


def cmd_export(a) -> int:
    from .export import export
    from .instruments import Instruments
    from .client import DukascopyClient
    from .storage import Store
    st = Store(a.data_dir)
    code = Instruments(DukascopyClient(), st.meta_dir).resolve(a.symbol)
    out = a.output or f"{code}_{a.tf}{'_' + a.resample if a.resample else ''}_{a.side.lower()}.csv"
    n = export(a.data_dir, code, a.tf, out, a.start, a.end, a.side, a.resample, a.tz,
               a.drop_flat, time_format=a.time_format)
    print(f"Wrote {n} rows -> {Path(out).resolve()}")
    return 0 if n else 1


def cmd_selftest(a) -> int:
    from .selftest import run_selftest
    ok = run_selftest(per_category=a.per_category, symbols=a.symbols or None,
                      data_dir=a.test_dir, keep=a.keep, workers=a.workers, rate_limit=a.rate)
    return 0 if ok else 1


# ---------------------------------------------------------------------------- parser

def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(prog="duka", description="Dukascopy historical data downloader (jetta API)",
                                epilog=EPILOG, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--version", action="version", version=f"%(prog)s {__version__}")
    common = argparse.ArgumentParser(add_help=False)
    common.add_argument("--data-dir", default=DEFAULT_DATA_DIR, help="data folder (default: ./data or $DUKA_DATA_DIR)")
    common.add_argument("-w", "--workers", type=int, default=6, help="parallel requests (default 6)")
    common.add_argument("--rate", type=float, default=8.0, help="max requests per second (default 8)")
    common.add_argument("--retries", type=int, default=8, help="retries per request (default 8)")
    common.add_argument("--no-progress", action="store_true", help="disable progress bar")
    common.add_argument("-v", "--verbose", action="count", default=0)
    sub = p.add_subparsers(dest="cmd", required=True)

    s = sub.add_parser("groups", parents=[common], help="list instrument groups/categories")
    s.set_defaults(func=cmd_groups)

    s = sub.add_parser("instruments", parents=[common], help="list/search instruments")
    s.add_argument("--group", "-g", help="group code, e.g. FX_MAJORS, FX, CMD_ENERGY, IDX, VCCY")
    s.add_argument("--search", "-s", help="text to search in code/name/description")
    s.add_argument("--limit", type=int, default=200)
    s.add_argument("--refresh", action="store_true", help="re-download the instrument list")
    s.set_defaults(func=cmd_instruments)

    s = sub.add_parser("info", parents=[common], help="instrument details and available history")
    s.add_argument("symbols", nargs="+")
    s.add_argument("--refresh", action="store_true")
    s.set_defaults(func=cmd_info)

    s = sub.add_parser("download", aliases=["dl"], parents=[common], help="download data (resumable)")
    s.add_argument("symbols", nargs="*", help="e.g. EURUSD XAUUSD USA500.IDX BTCUSD")
    s.add_argument("--group", "-g", action="append", default=[], help="download a whole group (repeatable)")
    s.add_argument("--tf", "-t", default="m1", help="tick | m1 | h1 | d1 (default m1)")
    s.add_argument("--from", dest="start", help="YYYY | YYYY-MM | YYYY-MM-DD (default: start of history)")
    s.add_argument("--to", dest="end", help="inclusive end date (default: now)")
    s.add_argument("--side", default="BID", help="BID | ASK | BOTH (default BID)")
    s.add_argument("--force", action="store_true", help="ignore cache and download again")
    s.set_defaults(func=cmd_download)

    s = sub.add_parser("update", parents=[common], help="update all downloaded series to now")
    s.add_argument("symbols", nargs="*", help="limit to these symbols")
    s.add_argument("--tf", "-t", help="limit to one timeframe")
    s.set_defaults(func=cmd_update)

    s = sub.add_parser("status", parents=[common], help="show what is downloaded")
    s.set_defaults(func=cmd_status)

    s = sub.add_parser("export", parents=[common], help="export stored data to CSV/Parquet")
    s.add_argument("symbol")
    s.add_argument("--tf", "-t", default="m1")
    s.add_argument("--from", dest="start")
    s.add_argument("--to", dest="end")
    s.add_argument("--side", default="BID", help="BID | ASK | MID | BOTH")
    s.add_argument("--resample", "-r", help="pandas rule: 5min, 15min, 1h, 4h, 1D ...")
    s.add_argument("--tz", help="output timezone, e.g. Europe/Prague (default UTC)")
    s.add_argument("--drop-flat", action="store_true", help="drop zero-volume candles")
    s.add_argument("--time-format", help="strftime format for the time column")
    s.add_argument("-o", "--output", help="output file (.csv, .csv.gz or .parquet)")
    s.set_defaults(func=cmd_export)

    s = sub.add_parser("selftest", parents=[common], help="live test: N symbols from every category")
    s.add_argument("symbols", nargs="*", help="test only these symbols")
    s.add_argument("--per-category", type=int, default=2)
    s.add_argument("--keep", action="store_true", help="keep the temporary data folder")
    s.add_argument("--test-dir", help="use this folder instead of a temporary one")
    s.set_defaults(func=cmd_selftest)
    return p


def main(argv=None) -> int:
    p = build_parser()
    a = p.parse_args(argv)
    if a.cmd in ("download", "dl") and not a.symbols and not a.group:
        p.error("give symbols or --group")
    _setup_logging(a.data_dir, a.verbose)
    try:
        return a.func(a)
    except KeyboardInterrupt:
        print("\nInterrupted - already downloaded data is saved, run the command again to continue.",
              file=sys.stderr)
        return 130
    except (KeyError, ValueError) as e:
        print(f"Error: {e.args[0] if e.args else e}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    sys.exit(main())
