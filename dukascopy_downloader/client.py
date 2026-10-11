"""HTTP client for https://jetta.dukascopy.com with retries and rate limiting."""
from __future__ import annotations

import logging
import random
import threading
import time
from typing import Any, Callable, Optional

import requests
from requests.adapters import HTTPAdapter

from .decode import DataError

log = logging.getLogger("dukascopy_downloader")

BASE_URL = "https://jetta.dukascopy.com"
USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/141.0.0.0 Safari/537.36"
)
# The API sits behind AWS WAF: requests without a dukascopy.com Referer/Origin get
# "202 + x-amzn-waf-action: challenge" (empty body) instead of data.
DEFAULT_HEADERS = {
    "User-Agent": USER_AGENT,
    "Accept": "application/json, text/plain, */*",
    "Accept-Language": "en-US,en;q=0.9",
    "Accept-Encoding": "gzip, deflate",
    "Referer": "https://www.dukascopy.com/",
    "Origin": "https://www.dukascopy.com",
}


class ApiError(RuntimeError):
    def __init__(self, status: Optional[int], message: str, url: str = ""):
        super().__init__(f"HTTP {status}: {message} [{url}]" if status else f"{message} [{url}]")
        self.status = status
        self.message = message
        self.url = url


class TooLateError(ApiError):
    """Requested period has not finished yet - data not published."""


class UnknownInstrumentError(ApiError):
    pass


class RateLimiter:
    """Thread-safe token bucket with adaptive slow-down on 429/403."""

    def __init__(self, rate: float):
        self.max_rate = max(0.1, float(rate))
        self.rate = self.max_rate
        self._lock = threading.Lock()
        self._next = time.monotonic()
        self._ok = 0

    def acquire(self) -> None:
        with self._lock:
            now = time.monotonic()
            wait = self._next - now
            self._next = max(now, self._next) + 1.0 / self.rate
        if wait > 0:
            time.sleep(wait)

    def slow_down(self) -> None:
        with self._lock:
            self.rate = max(0.5, self.rate / 2)
            self._ok = 0
            log.warning("Server is throttling - lowering request rate to %.1f req/s", self.rate)

    def success(self) -> None:
        with self._lock:
            if self.rate < self.max_rate:
                self._ok += 1
                if self._ok >= 50:
                    self.rate = min(self.max_rate, self.rate * 1.25)
                    self._ok = 0


class DukascopyClient:
    def __init__(
        self,
        base_url: str = BASE_URL,
        rate_limit: float = 8.0,
        max_retries: int = 8,
        timeout: tuple = (10, 60),
        pool_size: int = 32,
        session: Optional[requests.Session] = None,
    ):
        self.base_url = base_url.rstrip("/")
        self.max_retries = max(1, int(max_retries))
        self.timeout = timeout
        self.limiter = RateLimiter(rate_limit)
        self.session = session or requests.Session()
        adapter = HTTPAdapter(pool_connections=pool_size, pool_maxsize=pool_size, max_retries=0)
        self.session.mount("https://", adapter)
        self.session.mount("http://", adapter)
        self.session.headers.update(DEFAULT_HEADERS)
        self._count_lock = threading.Lock()
        self.request_count = 0

    # ------------------------------------------------------------------ helpers
    @staticmethod
    def _backoff(attempt: int, retry_after: Optional[str] = None) -> float:
        if retry_after:
            try:
                return min(120.0, float(retry_after))
            except ValueError:
                pass
        return min(60.0, 0.75 * (2 ** (attempt - 1))) * (0.7 + 0.6 * random.random())

    @staticmethod
    def _error_text(r: requests.Response) -> str:
        try:
            j = r.json()
            return str(j.get("error") or j.get("message") or j)
        except ValueError:
            return (r.text or r.reason or "").strip()[:200]

    # ------------------------------------------------------------------ public
    def get_json(self, path: str, parse: Optional[Callable[[Any], Any]] = None) -> Any:
        """GET ``path`` and return parsed JSON (optionally passed through ``parse``).

        Retries on network errors, timeouts, 403/408/425/429/5xx, invalid JSON
        and ``DataError`` raised by ``parse``.  Raises ``TooLateError`` when the
        period has not been published yet, ``UnknownInstrumentError`` and
        ``ApiError`` for permanent errors.
        """
        url = self.base_url + path
        last_err: str = ""
        for attempt in range(1, self.max_retries + 1):
            self.limiter.acquire()
            with self._count_lock:
                self.request_count += 1
            try:
                r = self.session.get(url, timeout=self.timeout)
            except (requests.ConnectionError, requests.Timeout,
                    requests.exceptions.ChunkedEncodingError) as e:
                last_err = f"{type(e).__name__}: {e}"
                delay = self._backoff(attempt)
                log.debug("network error (%s), retry %d/%d in %.1fs: %s",
                          last_err, attempt, self.max_retries, delay, url)
                time.sleep(delay)
                continue

            status = r.status_code
            if status == 202 or r.headers.get("x-amzn-waf-action"):
                # AWS WAF challenge (bot protection) - back off and retry
                self.limiter.slow_down()
                last_err = f"HTTP {status}: WAF {r.headers.get('x-amzn-waf-action', 'challenge')}"
                delay = max(5.0, self._backoff(attempt, r.headers.get("Retry-After")))
                log.warning("%s - retry %d/%d in %.1fs: %s", last_err, attempt, self.max_retries, delay, url)
                time.sleep(delay)
                continue
            if status == 200:
                try:
                    data = r.json()
                    result = parse(data) if parse else data
                    self.limiter.success()
                    return result
                except (ValueError, DataError) as e:  # bad JSON or inconsistent payload
                    last_err = f"invalid payload: {e}"
                    delay = self._backoff(attempt)
                    log.warning("%s - retry %d/%d in %.1fs: %s", last_err, attempt, self.max_retries, delay, url)
                    time.sleep(delay)
                    continue

            msg = self._error_text(r)
            if status == 400:
                low = msg.lower()
                if "too late" in low:
                    raise TooLateError(status, msg, url)
                if "unknown instrument" in low:
                    raise UnknownInstrumentError(status, msg, url)
                raise ApiError(status, msg, url)
            if status == 404:
                raise ApiError(status, "not found", url)
            if status in (403, 408, 425, 429) or status >= 500:
                if status in (403, 429):
                    self.limiter.slow_down()
                last_err = f"HTTP {status}: {msg}"
                delay = self._backoff(attempt, r.headers.get("Retry-After"))
                if status in (403, 429):
                    delay = max(delay, 5.0)
                log.warning("%s - retry %d/%d in %.1fs: %s", last_err, attempt, self.max_retries, delay, url)
                time.sleep(delay)
                continue
            raise ApiError(status, msg, url)
        raise ApiError(None, f"giving up after {self.max_retries} attempts ({last_err})", url)

    def close(self) -> None:
        self.session.close()
