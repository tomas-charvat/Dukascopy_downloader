"""Local storage: partitioned Parquet (or gzip CSV fallback) files + resume manifests.

Layout (all times UTC)::

    data/
      _meta/instruments.json, instruments/<CODE>.json, store.json
      _logs/duka.log
      <tf>/<category path>/<CODE>/...      category path = readable group names, e.g. Forex/Majors
      m1/Forex/Majors/EUR-USD/BID/2024-01.parquet     (one file per month)
      m1/Forex/Majors/EUR-USD/BID/_manifest.json      (API chunks downloaded & final)
      h1/Forex/Majors/EUR-USD/BID/2024.parquet        (one file per year)
      d1/Forex/Majors/EUR-USD/BID/all.parquet         (single file)
      tick/Forex/Majors/EUR-USD/2024-01-02.parquet    (one file per day, bid+ask)

Series are found by scanning ``<tf>/``, so readers need no category info. A store with
``category_of`` set (the Downloader) moves series found elsewhere (e.g. the old flat
``<tf>/<CODE>`` layout) into their category folder on first access.
"""
from __future__ import annotations

import json
import logging
import os
import re
import shutil
import time
from datetime import datetime
from pathlib import Path
from typing import Callable, Dict, Iterable, Iterator, List, Optional

import pandas as pd

from .decode import empty_candles, empty_ticks
from .timeframes import parse_date, part_range, to_utc, utcnow

log = logging.getLogger(__name__)
TIMEFRAMES = ("tick", "m1", "h1", "d1")
SIDES = ("BID", "ASK")


def parquet_available() -> bool:
    try:
        import pyarrow  # noqa: F401
        return True
    except ImportError:
        try:
            import fastparquet  # noqa: F401
            return True
        except ImportError:
            return False


def _atomic_replace(tmp: Path, dst: Path) -> None:
    for attempt in range(10):  # Windows: target may be briefly locked (antivirus, viewer)
        try:
            os.replace(tmp, dst)
            return
        except PermissionError:
            if attempt == 9:
                raise
            time.sleep(0.5 * (attempt + 1))


class Store:
    FORMATS = {"parquet": ".parquet", "csv": ".csv.gz"}

    def __init__(self, root: os.PathLike, fmt: Optional[str] = None,
                 category_of: Optional[Callable[[str], str]] = None):
        self.root = Path(root)
        self.category_of = category_of  # code -> "Forex/Majors"; None = read-only lookup
        self._code_dirs: Dict[tuple, Path] = {}
        self.meta_dir = self.root / "_meta"
        cfg_path = self.meta_dir / "store.json"
        cfg = {}
        if cfg_path.exists():
            try:
                cfg = json.loads(cfg_path.read_text(encoding="utf-8"))
            except ValueError:
                cfg = {}
        if fmt is None:
            fmt = cfg.get("format") or ("parquet" if parquet_available() else "csv")
        if fmt not in self.FORMATS:
            raise ValueError(f"unknown storage format {fmt}")
        if fmt == "parquet" and not parquet_available():
            raise RuntimeError("Parquet storage requires 'pyarrow' (pip install pyarrow)")
        self.fmt = fmt
        self.ext = self.FORMATS[fmt]
        if cfg.get("format") != fmt:
            self.meta_dir.mkdir(parents=True, exist_ok=True)
            cfg_path.write_text(json.dumps({"format": fmt}), encoding="utf-8")

    # ------------------------------------------------------------------ paths
    def _is_code_dir(self, tf: str, d: Path) -> bool:
        if tf == "tick":
            return (d / "_manifest.json").exists() or any(
                p.is_file() and (p.name.endswith(".parquet") or p.name.endswith(".csv.gz")) for p in d.iterdir())
        return any((d / s).is_dir() for s in SIDES)

    def _iter_code_dirs(self, tf: str) -> Iterator[Path]:
        """All series folders under ``<tf>/`` at any category depth."""
        base = self.root / tf
        if not base.is_dir():
            return
        stack = [base]
        while stack:
            d = stack.pop()
            for c in sorted(d.iterdir(), reverse=True):
                if not c.is_dir() or c.name.startswith("_"):
                    continue
                if self._is_code_dir(tf, c):
                    yield c
                else:
                    stack.append(c)

    def _category_dir(self, tf: str, code: str) -> Path:
        try:
            cat = self.category_of(code) or ""
        except Exception as e:  # noqa: BLE001 - unknown code / offline catalogue
            log.warning("no category for %s (%s), using Others", code, e)
            cat = ""
        parts = [re.sub(r"[^A-Za-z0-9._-]+", "_", x).strip("_") for x in cat.split("/")]
        parts = [x for x in parts if x] or ["Others"]
        return self.root.joinpath(tf, *parts)

    def code_dir(self, tf: str, code: str) -> Path:
        key = (tf, code)
        if key in self._code_dirs:
            return self._code_dirs[key]
        found = next((d for d in self._iter_code_dirs(tf) if d.name == code), None)
        if self.category_of is None:
            if found is None:  # nothing stored; don't cache so a later write is picked up
                return self.root / tf / code
            self._code_dirs[key] = found
            return found
        want = self._category_dir(tf, code) / code
        if found is not None and found != want and not want.exists():
            want.parent.mkdir(parents=True, exist_ok=True)
            shutil.move(str(found), str(want))
            log.info("moved %s -> %s", found, want)
            parent = found.parent
            while parent != self.root / tf and parent.is_dir() and not any(parent.iterdir()):
                parent.rmdir()
                parent = parent.parent
        self._code_dirs[key] = want
        return want

    def category(self, tf: str, code: str) -> str:
        d = self.code_dir(tf, code)
        rel = d.parent.relative_to(self.root / tf)
        return rel.as_posix() if str(rel) != "." else ""

    def series_dir(self, tf: str, code: str, side: Optional[str]) -> Path:
        d = self.code_dir(tf, code)
        return d if tf == "tick" else d / (side or "BID").upper()

    def part_path(self, tf: str, code: str, side: Optional[str], part: str) -> Path:
        return self.series_dir(tf, code, side) / f"{part}{self.ext}"

    def list_parts(self, tf: str, code: str, side: Optional[str]) -> List[Path]:
        d = self.series_dir(tf, code, side)
        if not d.exists():
            return []
        files = [p for p in d.iterdir() if p.is_file() and not p.name.startswith("_")
                 and (p.name.endswith(".parquet") or p.name.endswith(".csv.gz"))]
        return sorted(files, key=lambda p: p.name)

    @staticmethod
    def part_name(p: Path) -> str:
        return p.name.replace(".parquet", "").replace(".csv.gz", "")

    # ------------------------------------------------------------------ manifest
    def _manifest_path(self, tf, code, side) -> Path:
        return self.series_dir(tf, code, side) / "_manifest.json"

    def load_manifest(self, tf: str, code: str, side: Optional[str]) -> set:
        p = self._manifest_path(tf, code, side)
        if not p.exists():
            return set()
        try:
            return set(json.loads(p.read_text(encoding="utf-8")).get("complete", []))
        except ValueError:
            return set()  # corrupted manifest -> everything is re-checked

    def save_manifest(self, tf: str, code: str, side: Optional[str], complete: Iterable[str]) -> None:
        p = self._manifest_path(tf, code, side)
        p.parent.mkdir(parents=True, exist_ok=True)
        tmp = p.with_name(p.name + ".tmp")
        tmp.write_text(json.dumps({"complete": sorted(complete), "updated": utcnow().isoformat()}),
                       encoding="utf-8")
        _atomic_replace(tmp, p)

    # ------------------------------------------------------------------ io
    def _read_file(self, p: Path, tick: bool) -> pd.DataFrame:
        if p.name.endswith(".parquet"):
            df = pd.read_parquet(p)
        else:
            df = pd.read_csv(p, compression="gzip")
        if df.empty:
            return empty_ticks() if tick else empty_candles()
        df["time"] = pd.to_datetime(df["time"], utc=True)
        return df

    def _write_file(self, df: pd.DataFrame, p: Path) -> None:
        p.parent.mkdir(parents=True, exist_ok=True)
        tmp = p.with_name(p.name + ".tmp")
        if self.fmt == "parquet":
            df.to_parquet(tmp, index=False)
        else:
            out = df.copy()
            out["time"] = out["time"].dt.strftime("%Y-%m-%dT%H:%M:%S.%fZ")
            out.to_csv(tmp, index=False, compression="gzip")
        _atomic_replace(tmp, p)

    def write_part(self, tf: str, code: str, side: Optional[str], part: str, new: pd.DataFrame) -> int:
        """Merge ``new`` rows into a partition (new rows win). Returns row count."""
        tick = tf == "tick"
        p = self.part_path(tf, code, side, part)
        new = new if new is not None else (empty_ticks() if tick else empty_candles())
        frames = []
        # existing file in any format (e.g. format was switched)
        for ext in self.FORMATS.values():
            q = p.with_name(part + ext)
            if q.exists():
                old = self._read_file(q, tick)
                if tick and not new.empty and not old.empty:
                    # ticks have no unique key -> replace whole hours present in `new`
                    hours = set(new["time"].dt.floor("h"))
                    old = old[~old["time"].dt.floor("h").isin(hours)]
                frames.append(old)
        frames.append(new)
        frames = [f for f in frames if not f.empty]
        if not frames:
            df = empty_ticks() if tick else empty_candles()
        else:
            df = pd.concat(frames, ignore_index=True)
            if not tick:
                df = df.drop_duplicates(subset="time", keep="last")
            df = df.sort_values("time", kind="stable").reset_index(drop=True)
        self._write_file(df, p)
        for ext in self.FORMATS.values():  # drop leftovers in the other format
            q = p.with_name(part + ext)
            if q != p and q.exists():
                q.unlink()
        return len(df)

    def read(self, tf: str, code: str, side: Optional[str] = None,
             start: Optional[datetime] = None, end: Optional[datetime] = None) -> pd.DataFrame:
        tick = tf == "tick"
        if isinstance(start, str):
            start = parse_date(start)
        if isinstance(end, str):
            end = parse_date(end, end=True)
        start = to_utc(pd.Timestamp(start).to_pydatetime()) if start is not None else None
        end = to_utc(pd.Timestamp(end).to_pydatetime()) if end is not None else None
        frames = []
        for p in self.list_parts(tf, code, side):
            ps, pe = part_range(tf, self.part_name(p))
            if ps is not None and ((end and ps >= end) or (start and pe <= start)):
                continue
            frames.append(self._read_file(p, tick))
        frames = [f for f in frames if not f.empty]
        if not frames:
            return empty_ticks() if tick else empty_candles()
        df = pd.concat(frames, ignore_index=True).sort_values("time", kind="stable")
        if start is not None:
            df = df[df["time"] >= pd.Timestamp(start)]
        if end is not None:
            df = df[df["time"] < pd.Timestamp(end)]
        return df.reset_index(drop=True)

    # ------------------------------------------------------------------ status
    def list_series(self) -> List[Dict]:
        out = []
        if not self.root.exists():
            return out
        for tf in TIMEFRAMES:
            for code_dir in self._iter_code_dirs(tf):
                cat = code_dir.parent.relative_to(self.root / tf).as_posix()
                cat = "" if cat == "." else cat
                sides = [None] if tf == "tick" else [s for s in SIDES if (code_dir / s).is_dir()]
                for side in sides:
                    out.append({"tf": tf, "category": cat, "code": code_dir.name, "side": side})
        return sorted(out, key=lambda x: (TIMEFRAMES.index(x["tf"]), x["category"], x["code"], x["side"] or ""))

    def series_summary(self, tf: str, code: str, side: Optional[str]) -> Dict:
        parts = self.list_parts(tf, code, side)
        info = {"tf": tf, "category": self.category(tf, code), "code": code, "side": side or "-",
                "files": len(parts),
                "rows": 0, "first": None, "last": None,
                "size_mb": round(sum(p.stat().st_size for p in parts) / 1e6, 2)}
        if not parts:
            return info
        rows = 0
        for p in parts:
            if p.name.endswith(".parquet"):
                try:
                    import pyarrow.parquet as pq
                    rows += pq.ParquetFile(p).metadata.num_rows
                    continue
                except Exception:
                    pass
            rows += len(self._read_file(p, tf == "tick"))
        info["rows"] = rows
        for p in parts:
            df = self._read_file(p, tf == "tick")
            if not df.empty:
                info["first"] = df["time"].iloc[0]
                break
        for p in reversed(parts):
            df = self._read_file(p, tf == "tick")
            if not df.empty:
                info["last"] = df["time"].iloc[-1]
                break
        return info
