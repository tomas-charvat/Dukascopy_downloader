"""Offline unit tests (no network).  Run:  python -m unittest discover -s tests -v"""
import os
import shutil
import sys
import tempfile
import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path

import numpy as np
import pandas as pd

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from dukascopy_downloader.client import DukascopyClient  # noqa: E402
from dukascopy_downloader.decode import (DataError, decode_candles, decode_ticks,  # noqa: E402
                                         ohlc_violations, resample_candles)
from dukascopy_downloader.downloader import Downloader  # noqa: E402
from dukascopy_downloader.export import load  # noqa: E402
from dukascopy_downloader.instruments import Instruments  # noqa: E402
from dukascopy_downloader.timeframes import parse_date, plan_jobs  # noqa: E402
from fake_api import FakeClient, corrupt, encode_candles, fake_minutes  # noqa: E402

UTC = timezone.utc


class TestDecode(unittest.TestCase):
    # first values of the real EUR-USD BID minute response for 2024-01-02
    REAL = {"timestamp": 1704153600000, "multiplier": 1.0E-5, "open": 1.10366, "high": 1.10376,
            "low": 1.10366, "close": 1.1037400000000002, "shift": 60000,
            "times": [0, 1, 1], "opens": [0, 5, 3], "highs": [0, -1, 0], "lows": [0, 2, 3],
            "closes": [0, 1, -1], "volumes": [102.37, 74.55, 29.42]}

    def test_real_sample(self):
        df = decode_candles(self.REAL)
        self.assertEqual(list(df["open"]), [1.10366, 1.10371, 1.10374])
        self.assertEqual(list(df["close"]), [1.10374, 1.10375, 1.10374])
        self.assertEqual(str(df["time"].iloc[1]), "2024-01-02 00:01:00+00:00")
        self.assertEqual(list(df["volume"]), [102.37, 74.55, 29.42])

    def test_roundtrip(self):
        day = datetime(2024, 1, 2, tzinfo=UTC)
        raw = fake_minutes(day, "EUR-USD", "BID")
        df = decode_candles(encode_candles(raw, int(day.timestamp() * 1000), 60000),
                            day, day + timedelta(days=1))
        self.assertEqual(len(df), 1440)
        np.testing.assert_allclose(df["close"].to_numpy(), raw["c"].to_numpy() * 1e-5, atol=1e-12)
        self.assertEqual(ohlc_violations(df), 0)

    def test_empty(self):
        j = dict(self.REAL, times=[], opens=[], highs=[], lows=[], closes=[], volumes=[])
        self.assertTrue(decode_candles(j).empty)

    def test_corrupt(self):
        with self.assertRaises(DataError):
            decode_candles(corrupt(self.REAL))

    def test_out_of_range(self):
        with self.assertRaises(DataError):
            decode_candles(self.REAL, datetime(2024, 1, 3, tzinfo=UTC), datetime(2024, 1, 4, tzinfo=UTC))

    def test_ticks(self):
        j = {"timestamp": 1704189600000, "multiplier": 1e-5, "ask": 1.1015, "bid": 1.10149,
             "times": [224, 104, 0], "asks": [0, 0, 1], "bids": [0, -2, 0],
             "askVolumes": [990000, 3600000, 1], "bidVolumes": [4500000, 6020000, 2]}
        df = decode_ticks(j)
        self.assertEqual(list(df["bid"]), [1.10149, 1.10147, 1.10147])
        self.assertEqual(list(df["ask"]), [1.1015, 1.1015, 1.10151])
        self.assertEqual(df["time"].iloc[0], pd.Timestamp(1704189600224, unit="ms", tz="UTC"))
        self.assertEqual(df["time"].iloc[2], df["time"].iloc[1])  # same millisecond allowed


class TestTime(unittest.TestCase):
    def test_parse(self):
        self.assertEqual(parse_date("2015"), datetime(2015, 1, 1, tzinfo=UTC))
        self.assertEqual(parse_date("2015", end=True), datetime(2016, 1, 1, tzinfo=UTC))
        self.assertEqual(parse_date("2015-12", end=True), datetime(2016, 1, 1, tzinfo=UTC))
        self.assertEqual(parse_date("2015-03-02", end=True), datetime(2015, 3, 3, tzinfo=UTC))
        self.assertEqual(parse_date("2015-03-02T14"), datetime(2015, 3, 2, 14, tzinfo=UTC))
        with self.assertRaises(ValueError):
            parse_date("02.03.2015")

    def test_plan(self):
        now = datetime(2026, 10, 11, 14, 0, tzinfo=UTC)
        jobs = plan_jobs("m1", datetime(2026, 10, 8, tzinfo=UTC), now, now)
        self.assertEqual([j.chunk.key for j in jobs], ["2026-10-08", "2026-10-09", "2026-10-10"])
        jobs = plan_jobs("h1", datetime(2026, 9, 1, tzinfo=UTC), now, now)
        self.assertEqual(jobs[0].chunk.key, "2026-09")
        self.assertTrue(jobs[0].native)
        self.assertEqual(len(jobs), 1 + 10)  # September native + Oct 1..10 from m1
        self.assertTrue(all(not j.native and j.rule == "1h" for j in jobs[1:]))
        jobs = plan_jobs("d1", datetime(2025, 1, 1, tzinfo=UTC), now, now)
        keys = [j.chunk.key for j in jobs]
        self.assertEqual(keys[0], "2025")
        self.assertEqual(keys[1:10], [f"2026-{m:02d}" for m in range(1, 10)])
        self.assertEqual(keys[10:], [f"2026-10-{d:02d}" for d in range(1, 11)])
        jobs = plan_jobs("tick", datetime(2026, 10, 11, 12, tzinfo=UTC), now, now)
        self.assertEqual([j.chunk.url_path("BTC-USD", None) for j in jobs],
                         ["/v1/ticks/BTC-USD/2026/10/11/12", "/v1/ticks/BTC-USD/2026/10/11/13"])


class TestDownloader(unittest.TestCase):
    NOW = datetime(2024, 3, 6, 15, 30, tzinfo=UTC)

    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.fake = FakeClient(self.NOW, codes=("EUR-USD", "XAU-USD"))
        self.dl = Downloader(self.tmp.name, workers=4, client=self.fake, progress=False)
        self.dl.now = lambda: self.NOW

    def tearDown(self):
        self.tmp.cleanup()

    def test_resume_and_cache(self):
        r = self.dl.download_series("EUR-USD", "m1", "2024-02-26", "2024-03-02", "BID")
        self.assertEqual(r.fetched, 6)
        self.assertFalse(r.errors)
        df = self.dl.store.read("m1", "EUR-USD", "BID")
        self.assertEqual(len(df), 5 * 1440)  # Saturday 2024-03-02 is empty
        self.assertTrue(df["time"].is_monotonic_increasing)
        n = self.fake.request_count
        r2 = self.dl.download_series("EUR-USD", "m1", "2024-02-26", "2024-03-02", "BID")
        self.assertEqual((r2.fetched, r2.cached), (0, 6))
        self.assertEqual(self.fake.request_count, n)
        r3 = self.dl.download_series("EUR-USD", "m1", "2024-02-26", "2024-03-02", "BID", force=True)
        self.assertEqual(r3.fetched, 6)
        self.assertEqual(len(self.dl.store.read("m1", "EUR-USD", "BID")), 5 * 1440)

    def test_failed_chunk_is_retried_next_run(self):
        bad = "/v1/candles/minute/EUR-USD/BID/2024/2/27"
        self.fake.fail_urls.add(bad)
        r = self.dl.download_series("EUR-USD", "m1", "2024-02-26", "2024-02-28", "BID")
        self.assertEqual(len(r.errors), 1)
        self.assertEqual(len(self.dl.store.read("m1", "EUR-USD", "BID")), 2 * 1440)
        self.fake.fail_urls.clear()
        self.fake.paths.clear()
        r = self.dl.download_series("EUR-USD", "m1", "2024-02-26", "2024-02-28", "BID")
        self.assertEqual(self.fake.paths, [bad])
        self.assertEqual(len(self.dl.store.read("m1", "EUR-USD", "BID")), 3 * 1440)

    def test_today_not_published(self):
        r = self.dl.download_series("EUR-USD", "m1", "2024-03-05", None, "BID")
        self.assertEqual(r.fetched, 1)  # only 03-05 (03-06 is still running)
        self.assertEqual(self.dl.store.read("m1", "EUR-USD", "BID")["time"].max(),
                         pd.Timestamp("2024-03-05 23:59", tz="UTC"))

    def test_h1_and_d1_derived_equal_aggregates(self):
        self.dl.download_series("EUR-USD", "m1", "2024-02-01", None, "BID")
        self.dl.download_series("EUR-USD", "h1", "2024-02-01", None, "BID")
        self.dl.download_series("EUR-USD", "d1", "2023-01-01", None, "BID")
        m1 = self.dl.store.read("m1", "EUR-USD", "BID")
        h1 = self.dl.store.read("h1", "EUR-USD", "BID", "2024-02-01")
        d1 = self.dl.store.read("d1", "EUR-USD", "BID", "2024-02-01")
        for got, rule in ((h1, "1h"), (d1, "1D")):
            exp = resample_candles(m1, rule)
            self.assertEqual(list(got["time"]), list(exp["time"]))
            np.testing.assert_allclose(got[["open", "high", "low", "close"]].to_numpy(),
                                       exp[["open", "high", "low", "close"]].to_numpy(), atol=1e-9)
            np.testing.assert_allclose(got["volume"].to_numpy(), exp["volume"].to_numpy(), rtol=1e-6)
        self.assertEqual(h1["time"].max(), pd.Timestamp("2024-03-05 23:00", tz="UTC"))
        self.assertEqual(d1["time"].max(), pd.Timestamp("2024-03-05", tz="UTC"))
        self.assertGreater(len(self.dl.store.read("d1", "EUR-USD", "BID", "2023-01-01", "2024-01-01")), 300)

    def test_update(self):
        self.dl.download_series("EUR-USD", "m1", "2024-03-04", None, "BID")
        self.NOW = datetime(2024, 3, 8, 2, 0, tzinfo=UTC)
        self.fake.now = self.NOW
        self.fake.paths.clear()
        res = self.dl.update()
        self.assertEqual(len(res), 1)
        self.assertEqual(sorted(self.fake.paths), ["/v1/candles/minute/EUR-USD/BID/2024/3/6",
                                                   "/v1/candles/minute/EUR-USD/BID/2024/3/7"])

    def test_export_mid_resample_tz(self):
        self.dl.download("EUR-USD", "m1", "2024-03-04", "2024-03-04", sides="both")
        mid = load(self.tmp.name, "EUR-USD", "m1", side="MID", resample="15min", tz="Europe/Prague")
        self.assertEqual(len(mid), 96)
        self.assertEqual(str(mid["time"].dt.tz), "Europe/Prague")
        self.assertTrue((mid["spread"] > 0).all())
        both = load(self.tmp.name, "EUR-USD", "m1", side="BOTH")
        self.assertIn("close_ask", both.columns)

    def test_category_folders(self):
        self.dl.download_series("EUR-USD", "m1", "2024-03-04", "2024-03-05", "BID")
        self.dl.download_series("XAU-USD", "h1", "2024-03-04", "2024-03-05", "BID")
        root = Path(self.tmp.name)
        self.assertTrue((root / "m1/Forex/Majors/EUR-USD/BID/2024-03.parquet").exists())
        self.assertTrue((root / "h1/Forex/Metals/XAU-USD/BID/2024.parquet").exists())
        self.assertEqual([(s["tf"], s["category"], s["code"]) for s in self.dl.store.list_series()],
                         [("m1", "Forex/Majors", "EUR-USD"), ("h1", "Forex/Metals", "XAU-USD")])
        # readers without category info find the series by scanning
        self.assertEqual(len(load(self.tmp.name, "EUR-USD", "m1")), 2 * 1440)

    def test_old_layouts_are_moved(self):
        root = Path(self.tmp.name)
        self.dl.download_series("EUR-USD", "m1", "2024-03-04", "2024-03-05", "BID")
        for old in ("m1/EUR-USD", "m1/FX/FX_MAJORS/EUR-USD"):  # flat layout, group-code folders
            with self.subTest(old=old):
                (root / old).parent.mkdir(parents=True, exist_ok=True)
                shutil.move(str(root / "m1/Forex/Majors/EUR-USD"), str(root / old))
                shutil.rmtree(root / "m1/Forex")
                dl = Downloader(self.tmp.name, workers=4, client=self.fake, progress=False)
                dl.now = lambda: self.NOW
                self.assertEqual(len(dl.store.read("m1", "EUR-USD", "BID")), 2 * 1440)
                self.assertEqual(sorted(p.name for p in (root / "m1").iterdir()), ["Forex"])
                self.assertTrue((root / "m1/Forex/Majors/EUR-USD/BID/_manifest.json").exists())
                self.fake.paths.clear()
                dl.download_series("EUR-USD", "m1", "2024-03-04", "2024-03-05", "BID")
                self.assertEqual(self.fake.paths, [])  # resume state moved along with the data

    def test_symbols_and_groups(self):
        inst = self.dl.instruments
        self.assertEqual(inst.resolve("eurusd"), "EUR-USD")
        self.assertEqual(inst.resolve("EUR/USD"), "EUR-USD")
        self.assertEqual(inst.resolve("usa500"), "USA500.IDX-USD")
        with self.assertRaises(KeyError):
            inst.resolve("BTC")  # ambiguous
        with self.assertRaises(KeyError):
            inst.resolve("NOPE")
        self.assertEqual([i["code"] for i in inst.in_group("FX")], ["EUR-USD", "XAU-USD"])
        self.assertEqual(self.dl.resolve_symbols(["eurusd,xauusd"]), ["EUR-USD", "XAU-USD"])

    def test_all_and_exclude(self):
        rs = self.dl.resolve_symbols
        self.assertEqual(sorted(rs(all_instruments=True)),
                         ["BTC-EUR", "BTC-USD", "EUR-USD", "USA500.IDX-USD", "XAU-USD"])
        self.assertEqual(sorted(rs(all_instruments=True, exclude=["VCCY"])),
                         ["EUR-USD", "USA500.IDX-USD", "XAU-USD"])
        self.assertEqual(sorted(rs(all_instruments=True, exclude=["FX,usa500"])), ["BTC-EUR", "BTC-USD"])
        self.assertEqual(rs(groups=["FX"], exclude=["FX_METALS"]), ["EUR-USD"])

    def test_universe_download_survives_bad_instruments(self):
        res = self.dl.download(tf="d1", start="2023", all_instruments=True)
        self.assertEqual(len(res), 5)
        ok = sorted(r.code for r in res if r.ok)
        self.assertEqual(ok, ["EUR-USD", "XAU-USD"])  # the fake API knows only these two
        self.assertGreater(len(self.dl.store.read("d1", "XAU-USD", "BID")), 300)


class TestOthersGroups(unittest.TestCase):
    def test_ungrouped_instruments_go_to_others(self):
        from dukascopy_downloader.instruments import _add_others
        cat = _add_others({"groups": [{"id": 9, "code": "VCCY", "parentId": None, "description": "Crypto CFD"}],
                           "instruments": [
            {"code": "BTC-USD", "groupId": 9},
            {"code": "IE00B40G6S53.FN-GBP", "groupId": None, "platformGroupId": "MUTUAL_FUNDS"},
            {"code": "SEMI.GB-USD", "groupId": None, "platformGroupId": "STK_CASH",
             "description": "iShares MSCI Global Semiconductors UCITS ETF"},
            {"code": "USC-USD", "groupId": None, "platformGroupId": "CRYPTO_CURR"},
            {"code": "XYZ-USD", "groupId": 12345, "platformGroupId": "FUTURES"},
        ]})
        inst = Instruments(None, tempfile.gettempdir())
        inst._list = cat
        self.assertEqual(inst.category_path("BTC-USD"), "Crypto")
        self.assertEqual(inst.category_path("IE00B40G6S53.FN-GBP"), "Others/Funds")
        self.assertEqual(inst.category_path("SEMI.GB-USD"), "Others/ETF")
        self.assertEqual(inst.category_path("USC-USD"), "Others/Crypto")
        self.assertEqual(inst.category_path("XYZ-USD"), "Others/Misc")
        self.assertEqual(len(inst.in_group("OTHERS")), 4)
        self.assertEqual(_add_others(cat), cat)  # idempotent
        self.assertEqual(sum(g["code"] == "OTHERS" for g in cat["groups"]), 1)


class TestClientRetries(unittest.TestCase):
    def test_retry_then_success(self):
        import requests

        class Resp:
            def __init__(self, status, payload=None, text=""):
                self.status_code, self._p, self.text, self.headers, self.reason = status, payload, text, {}, ""

            def json(self):
                if self._p is None:
                    raise ValueError("no json")
                return self._p

        seq = [requests.ConnectionError("x"), Resp(503, None, "busy"), Resp(200, None, "<html>"),
               Resp(200, {"a": 1})]

        class Sess(requests.Session):
            def get(self, url, timeout=None):
                r = seq.pop(0)
                if isinstance(r, Exception):
                    raise r
                return r

        c = DukascopyClient(session=Sess(), rate_limit=1000)
        c._backoff = staticmethod(lambda attempt, ra=None: 0.0)
        self.assertEqual(c.get_json("/x"), {"a": 1})
        self.assertEqual(c.request_count, 4)

    def test_too_late(self):
        import requests
        from dukascopy_downloader.client import TooLateError

        class Resp:
            status_code, text, headers, reason = 400, "", {}, ""

            def json(self):
                return {"error": "From time is too late", "statusCode": 400}

        class Sess(requests.Session):
            def get(self, url, timeout=None):
                return Resp()

        with self.assertRaises(TooLateError):
            DukascopyClient(session=Sess()).get_json("/x")


if __name__ == "__main__":
    unittest.main()
