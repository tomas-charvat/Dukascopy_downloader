"""Local storage: partitioned Parquet (or gzip CSV fallback) files + resume manifests.

Layout (all times UTC)::

    data/
      _meta/instruments.json, instruments/<CODE>.json, store.json
      _logs/duka.log
      m1/EUR-USD/BID/2024-01.parquet       (one file per month)
      m1/EUR-USD/BID/_manifest.json        (API chunks downloaded & final)
      h1/EUR-USD/BID/2024.parquet          (one file per year)
      d1/EUR-USD/BID/all.parquet           (single file)
      tick/EUR-USD/2024-01-02.parquet      (one file per day, bid+ask)
"""
from __future__ import annotations

import json
import os
import time
from datetime import datetime
from pathlib import Path
from typing import Dict, Iterable, List, Optional

import pandas as pd

from .decode import empty_candles, empty_ticks
from .timeframes import parse_date, part_range, to_utc, utcnow


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

    def __init__(self, root: os.PathLike, fmt: Optional[str] = None):
        self.root = Path(root)
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
    def series_dir(self, tf: str, code: str, side: Optional[str]) -> Path:
        d = self.root / tf / code
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
        for tf_dir in sorted(self.root.iterdir()):
            tf = tf_dir.name
            if tf not in ("tick", "m1", "h1", "d1") or not tf_dir.is_dir():
                continue
            for code_dir in sorted(tf_dir.iterdir()):
                if not code_dir.is_dir():
                    continue
                sides = [None] if tf == "tick" else sorted(d.name for d in code_dir.iterdir() if d.is_dir())
                for side in sides:
                    out.append({"tf": tf, "code": code_dir.name, "side": side})
        return out

    def series_summary(self, tf: str, code: str, side: Optional[str]) -> Dict:
        parts = self.list_parts(tf, code, side)
        info = {"tf": tf, "code": code, "side": side or "-", "files": len(parts),
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
