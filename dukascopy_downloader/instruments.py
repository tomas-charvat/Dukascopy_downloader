"""Instrument catalogue: listing, symbol resolution, groups/categories, history start."""
from __future__ import annotations

import json
import os
import re
import time
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional

from .client import DukascopyClient
from .timeframes import TF_HISTORY_PERIOD, UTC, floor_dt

# Categories used by `selftest` (group code -> human label)
CATEGORIES = [
    ("FX_MAJORS", "Forex - majors"),
    ("FX_CROSSES", "Forex - crosses"),
    ("FX_METALS", "Forex - metals (gold/silver)"),
    ("CMD_ENERGY", "Commodities - energy"),
    ("CMD_METALS", "Commodities - metals"),
    ("CMD_AGRICULTURAL", "Commodities - agricultural"),
    ("IDX", "Indices"),
    ("BND_CFD", "Bonds"),
    ("STCK_CFD", "Stocks"),
    ("ETF_CFD", "ETF"),
    ("VCCY", "Crypto"),
]


def _norm(s: str) -> str:
    return re.sub(r"[^A-Z0-9]", "", str(s).upper())


class Instruments:
    LIST_PATH = "/v1/instruments"

    def __init__(self, client: DukascopyClient, cache_dir: os.PathLike, max_age_days: float = 7):
        self.client = client
        self.cache_dir = Path(cache_dir)
        self.max_age = max_age_days * 86400
        self._list: Optional[dict] = None
        self._details: Dict[str, dict] = {}

    # ------------------------------------------------------------------ cache
    def _cached(self, path: Path, url: str, refresh: bool) -> dict:
        if not refresh and path.exists() and time.time() - path.stat().st_mtime < self.max_age:
            try:
                return json.loads(path.read_text(encoding="utf-8"))
            except (OSError, ValueError):
                pass
        try:
            data = self.client.get_json(url)
        except Exception:
            if path.exists():  # offline fallback to a stale cache
                return json.loads(path.read_text(encoding="utf-8"))
            raise
        path.parent.mkdir(parents=True, exist_ok=True)
        tmp = path.with_suffix(".tmp")
        tmp.write_text(json.dumps(data), encoding="utf-8")
        os.replace(tmp, path)
        return data

    def load(self, refresh: bool = False) -> dict:
        if self._list is None or refresh:
            self._list = self._cached(self.cache_dir / "instruments.json", self.LIST_PATH, refresh)
        return self._list

    @property
    def all(self) -> List[dict]:
        return self.load()["instruments"]

    @property
    def groups(self) -> List[dict]:
        return self.load().get("groups", [])

    def details(self, code: str, refresh: bool = False) -> dict:
        if code not in self._details or refresh:
            safe = re.sub(r"[^A-Za-z0-9._-]", "_", code)
            self._details[code] = self._cached(
                self.cache_dir / "instruments" / f"{safe}.json", f"/v1/instruments/{code}", refresh)
        return self._details[code]

    # ------------------------------------------------------------------ lookup
    def resolve(self, symbol: str) -> str:
        """'eurusd', 'EUR/USD', 'EUR-USD', 'xauusd', 'USA500.IDX' -> API code."""
        s = str(symbol).strip()
        codes = {i["code"] for i in self.all}
        if s in codes:
            return s
        n = _norm(s)
        by_code = {_norm(i["code"]): i["code"] for i in self.all}
        if n in by_code:
            return by_code[n]
        by_name = {_norm(i.get("name", "")): i["code"] for i in self.all}
        if n in by_name:
            return by_name[n]
        cands = sorted({c for k, c in by_code.items() if k.startswith(n)})
        if len(cands) == 1:
            return cands[0]
        if cands:
            raise KeyError(f"Symbol '{symbol}' is ambiguous: {', '.join(cands[:15])}")
        raise KeyError(f"Unknown symbol '{symbol}'. Try: duka instruments --search {symbol}")

    def _group_ids(self, group: str) -> set:
        g = str(group).upper()
        ids = {x["id"] for x in self.groups if str(x["code"]).upper() == g or str(x["id"]) == g}
        changed = True
        while changed:  # include child groups
            new = {x["id"] for x in self.groups if x.get("parentId") in ids} - ids
            changed = bool(new)
            ids |= new
        return ids

    def in_group(self, group: str) -> List[dict]:
        g = str(group).upper()
        ids = self._group_ids(g)
        res = [i for i in self.all if i.get("groupId") in ids
               or str(i.get("platformGroupId") or "").upper() == g
               or str(i.get("webGroupId") or "").upper() == g]
        return sorted(res, key=lambda i: -(i.get("rating") or 0))

    def search(self, text: Optional[str] = None, group: Optional[str] = None) -> List[dict]:
        items = self.in_group(group) if group else sorted(self.all, key=lambda i: -(i.get("rating") or 0))
        if text:
            t = text.lower()
            nt = _norm(text)
            items = [i for i in items if t in (i.get("description") or "").lower()
                     or nt in _norm(i["code"]) or t in (i.get("name") or "").lower()]
        return items

    def group_tree(self) -> List[dict]:
        counts: Dict[int, int] = {}
        for i in self.all:
            counts[i.get("groupId")] = counts.get(i.get("groupId"), 0) + 1
        out = []
        for g in self.groups:
            n = len(self.in_group(g["code"]))
            out.append({**g, "count": n})
        return out

    # ------------------------------------------------------------------ history
    def history_start(self, code: str, tf: str) -> datetime:
        """First available time for ``tf`` (aligned to the API chunk)."""
        hist = {h["period"]: h["from"] for h in self.details(code).get("histories", []) if h.get("from") is not None}

        def ts(p):
            return datetime.fromtimestamp(hist[p] / 1000, UTC) if p in hist else None

        period = TF_HISTORY_PERIOD[tf]
        start = ts(period)
        if tf in ("m1", "tick") and ts("HOUR"):
            # MINUTE/TICK 'from' is sometimes far earlier than the real data (e.g. crypto);
            # hourly history is a reliable lower bound.
            h = floor_dt(ts("HOUR"), "day")
            start = max(start, h) if start else h
        if start is None:
            known = [v for v in (ts(p) for p in ("MINUTE", "HOUR", "DAY", "TICK")) if v]
            if not known:
                return datetime(2003, 5, 1, tzinfo=UTC)
            start = min(known)
        unit = {"tick": "hour", "m1": "day", "h1": "month", "d1": "year"}[tf]
        return floor_dt(start, unit)
