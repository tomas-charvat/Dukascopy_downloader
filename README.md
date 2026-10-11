# Dukascopy Downloader

Download free historical market data from [Dukascopy](https://www.dukascopy.com/) — forex,
metals, commodities, indices, bonds, stocks, ETFs and crypto (~1,500 instruments) — and save
it as clean Parquet files ready for backtesting.

- **Timeframes:** ticks, 1-minute, 1-hour and daily candles (BID and/or ASK)
- **Resumable:** stop any time with Ctrl+C; the next run continues where it left off
- **Safe for the server:** rate limited, slows down automatically when throttled, retries failed requests
- **Organised:** files are sorted into folders by timeframe and category (`m1/Forex/Majors/EUR-USD/...`)
- **Easy to use:** one command to download, one to update, one to export to CSV/Parquet

## Installation

You need **Python 3.9 or newer**. Install straight from GitHub:

```bash
pip install git+https://github.com/tomas-charvat/Dukascopy_downloader.git
```

This gives you the `duka` command. Check that it works:

```bash
duka --version
```

> **Tip:** if you only want the command-line tool, [pipx](https://pipx.pypa.io/) or
> [uv](https://docs.astral.sh/uv/) installs it into its own isolated environment:
> `pipx install git+https://github.com/tomas-charvat/Dukascopy_downloader.git`
> or `uv tool install git+https://github.com/tomas-charvat/Dukascopy_downloader.git`

To update to the latest version later:

```bash
pip install --upgrade git+https://github.com/tomas-charvat/Dukascopy_downloader.git
```

## Quick start

```bash
# 1. Find the instrument you want
duka instruments --search gold

# 2. Download 1-minute candles since 2020 (saved into ./data)
duka download EURUSD XAUUSD BTCUSD --from 2020

# 3. Later: fetch only what's new
duka update

# 4. Export to a single CSV file (here resampled to 15-minute candles)
duka export EURUSD --from 2020 --resample 15min -o eurusd_15m.csv
```

Symbols can be written as `EURUSD`, `eurusd`, `EUR/USD` or `EUR-USD`.

## Commands

| Command | What it does |
|---|---|
| `duka groups` | List instrument categories (`FX_MAJORS`, `CMD_ENERGY`, `VCCY`, ...) |
| `duka instruments` | List or search instruments (`--search gold`, `--group FX_MAJORS`) |
| `duka info EURUSD` | Instrument details and how far back its history goes |
| `duka download ...` | Download data (see options below) |
| `duka update` | Bring everything you've already downloaded up to date |
| `duka status` | Show what's downloaded: rows, date range, size |
| `duka export ...` | Write one symbol to a single CSV / CSV.GZ / Parquet file |
| `duka selftest` | Live check: downloads a little data for 2 symbols from every category |

Run `duka <command> --help` for all options.

### Downloading

```bash
duka download EURUSD                                # full 1-minute history (BID)
duka download EURUSD GBPUSD --from 2015 --side both # BID and ASK since 2015
duka download --group FX_MAJORS --from 2018         # a whole category
duka download EURUSD --tf h1                        # hourly candles
duka download EURUSD --tf d1                        # daily candles
duka download BTCUSD --tf tick --from 2024-03-01 --to 2024-03-31
```

| Option | Meaning | Default |
|---|---|---|
| `--tf` | `tick`, `m1`, `h1` or `d1` | `m1` |
| `--from` / `--to` | `2020`, `2020-05` or `2020-05-01` (`--to` is inclusive) | whole history / now |
| `--side` | `BID`, `ASK` or `BOTH` | `BID` |
| `--group` | download every instrument in a category (repeatable) | |
| `--data-dir` | where to store the data (or set `DUKA_DATA_DIR`) | `./data` |
| `--rate` | max requests per second | `8` |
| `--force` | download again even if already stored | |

> **How long does it take?** One request = one day of 1-minute data, so ~5 years of
> `m1` for one symbol is ~1,800 requests ≈ 4–8 minutes. Tick data is one request per
> hour and is much slower — download it for short periods only.

### Exporting

```bash
duka export EURUSD --from 2020 --to 2024 -o eurusd_m1.csv
duka export EURUSD --resample 1h --tz Europe/London -o eurusd_h1.parquet
duka export XAUUSD --side mid --resample 4h -o xauusd_h4.csv
```

`--resample` accepts any pandas rule (`5min`, `15min`, `1h`, `4h`, `1D`, ...),
`--side` can also be `MID` (average of bid and ask) or `BOTH`, and `--drop-flat` removes
zero-volume candles.

## Where the data goes

Everything is stored under `./data` (change with `--data-dir`), sorted by timeframe,
then category, then symbol:

```
data/
├── m1/
│   ├── Forex/Majors/EUR-USD/BID/2024-01.parquet    one file per month
│   ├── Commodities/Energy/LIGHT.CMD-USD/BID/...
│   └── Crypto/BTC-USD/BID/...
├── h1/Indices/America/USA500.IDX-USD/BID/2024.parquet    one file per year
├── d1/Stocks/US/NVDA.US-USD/BID/all.parquet              one file
├── tick/Forex/Metals/XAU-USD/2024-01-02.parquet          one file per day
├── _meta/    instrument list cache
└── _logs/    duka.log
```

Categories: `Forex` (`Majors`, `Crosses`, `Metals`), `Commodities` (`Energy`, `Metals`,
`Agricultural`), `Indices` (`America`, `Europe`, `Asia_Pacific`, `Africa`), `Bonds`,
`Stocks/<country>`, `ETF/<country>`, `Crypto`.

All times are **UTC**. The `_manifest.json` files record what's been downloaded — don't
delete them unless you want to download everything again.

## Using it from Python

```python
from dukascopy_downloader import Downloader, load

# download
dl = Downloader("data")
dl.download(["EURUSD", "XAUUSD"], tf="m1", start="2020-01-01", sides="both")

# load stored data as a pandas DataFrame (works offline)
df = load("data", "EUR-USD", "m1", start="2020", end="2024")
h1 = load("data", "EUR-USD", "m1", side="MID", resample="1h", tz="Europe/London")
```

`load()` takes the Dukascopy code with a dash (`EUR-USD`, `XAU-USD`, `BTC-USD`); use
`dl.load("EURUSD", ...)` if you want symbol names resolved for you.

You can also read the files with plain pandas: `pd.read_parquet("data/m1/Forex/Majors/EUR-USD/BID")`.

**Columns**

- candles: `time, open, high, low, close, volume`
- ticks: `time, bid, ask, bid_volume, ask_volume`

`time` is a timezone-aware UTC timestamp. For older periods Dukascopy fills inactive
minutes with flat zero-volume candles; drop them with `drop_flat=True` / `--drop-flat`.

## Good to know

- **Latest data:** Dukascopy publishes only finished periods. The current month (`h1`)
  and year (`d1`) are built from smaller timeframes and replaced with official data once
  the period closes. Run `duka update` whenever you want fresh data.
- **Rate limits:** the server starts refusing requests at around 10 per second. The default
  of 8/s is safe; if you see `Server is throttling` messages, lower it with `--rate 5`.
- **Logs:** details of every run are written to `data/_logs/duka.log`.

## Development

```bash
git clone https://github.com/tomas-charvat/Dukascopy_downloader.git
cd Dukascopy_downloader
pip install -e .
python -m unittest discover -s tests -v   # offline tests, no network needed
duka selftest                             # live test against the Dukascopy API
```

<details>
<summary>API notes (for maintainers)</summary>

Data comes from `https://jetta.dukascopy.com`:

| timeframe | endpoint | one request = |
|---|---|---|
| tick | `/v1/ticks/{CODE}/{Y}/{M}/{D}/{H}` | 1 hour |
| m1 | `/v1/candles/minute/{CODE}/{BID\|ASK}/{Y}/{M}/{D}` | 1 day |
| h1 | `/v1/candles/hour/{CODE}/{BID\|ASK}/{Y}/{M}` | 1 month |
| d1 | `/v1/candles/day/{CODE}/{BID\|ASK}/{Y}` | 1 year |

Instrument list: `/v1/instruments`, details: `/v1/instruments/{CODE}`. Values are
delta-encoded: `time = timestamp + cumsum(times) * shift`,
`open = (round(open / multiplier) + cumsum(opens)) * multiplier` (same for high/low/close
and tick bid/ask); volumes are plain. A period that hasn't closed yet returns
`400 From time is too late`.
</details>

## License

[MIT](LICENSE) — free to use, modify and share.

This project is not affiliated with Dukascopy Bank SA. Data is provided by Dukascopy
under their terms of use; please use it responsibly.
