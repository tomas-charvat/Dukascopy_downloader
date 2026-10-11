import requests, time
UA = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/141.0.0.0 Safari/537.36"
combos = {
 "default": {},
 "ours": {"User-Agent": UA + " dukascopy-downloader/1.0", "Accept": "application/json"},
 "chrome": {"User-Agent": UA, "Accept": "application/json, text/plain, */*", "Accept-Language": "en-US,en;q=0.9"},
 "chrome+ref": {"User-Agent": UA, "Accept": "application/json, text/plain, */*", "Accept-Language": "en-US,en;q=0.9",
                "Referer": "https://www.dukascopy.com/", "Origin": "https://www.dukascopy.com"},
}
for path in ["/v1/instruments/EUR-USD", "/v1/candles/minute/EUR-USD/BID/2024/1/2", "/v1/instruments"]:
    for name, h in combos.items():
        try:
            r = requests.get("https://jetta.dukascopy.com" + path, headers=h, timeout=30)
            hh = {k: v for k, v in r.headers.items() if k.lower() in ("x-amzn-waf-action", "content-type", "x-cache", "server", "content-length", "set-cookie")}
            print(path, name, r.status_code, len(r.content), hh, repr(r.text[:150]))
        except Exception as e:
            print(path, name, "ERR", e)
        time.sleep(0.5)
