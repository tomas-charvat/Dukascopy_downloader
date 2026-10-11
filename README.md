# Dukascopy downloader

Robustní stahovač historických dat z Dukascopy (API `https://jetta.dukascopy.com`) –
ticky, 1min, 1h a denní svíčky (BID/ASK) pro všech ~1500 instrumentů (forex, kovy,
komodity, indexy, dluhopisy, akcie, ETF, krypto).

* **Navazuje, kde skončil** – každý stažený kus se zapíše do `_manifest.json`, opakované
  spuštění stáhne jen to, co chybí (přerušení přes Ctrl+C nevadí).
* **Robustní síť** – retry s exponenciálním backoffem, respektuje `Retry-After`,
  při 429/403 automaticky zpomalí, validuje každou odpověď (délky polí, časy v rozsahu),
  atomický zápis souborů.
* **Paralelní** – 6 vláken, limit 8 req/s (server nad ~10 req/s vrací 429; při 429 se rychlost sama sníží).
* **Úložiště** – Parquet (UTC), 1 soubor na měsíc (m1) / rok (h1) / den (tick).
* **Aktuální data** – API vydává jen uzavřená období (minuty do včerejška, hodiny do
  konce minulého měsíce…). Rozběhnutý měsíc u `h1` a rok u `d1` se proto dopočítá
  z menších timeframů a po uzavření nahradí oficiálními daty.

## Instalace

```bash
conda activate dukascopy-dowloader      # nebo jiný env / venv
pip install -r requirements.txt
pip install -e .                        # volitelné: přidá příkaz `duka`
```

Bez instalace jde všechno spouštět i jako `python duka.py ...`.

## Příkazy

```bash
duka groups                                   # kategorie a počty instrumentů
duka instruments --group FX_MAJORS            # instrumenty ve skupině
duka instruments --search gold                # hledání
duka info EURUSD XAUUSD                       # detail + odkdy je historie

# stahování (symboly lze psát EURUSD / EUR/USD / EUR-USD / eurusd)
duka download EURUSD                          # celá 1min historie BID
duka download EURUSD GBPUSD XAUUSD --from 2014 --side both
duka download --group FX_MAJORS --from 2014   # celá skupina
duka download --group FX_CROSSES --group FX_METALS --from 2014
duka download USA500.IDX DEU.IDX LIGHT.CMD BTCUSD --from 2015
duka download EURUSD --tf h1                  # hodinové od roku 2003
duka download EURUSD --tf d1                  # denní (od 1973)
duka download EURUSD --tf tick --from 2024-03-01 --to 2024-03-31

duka update                                   # dotáhne všechno stažené do dneška
duka update EURUSD --tf m1
duka status                                   # co je staženo (řádky, rozsah, MB)

# export (CSV / CSV.GZ / Parquet), přepočet timeframu, časová zóna, MID cena
duka export EURUSD --from 2020 --to 2024 -o eurusd_m1.csv
duka export EURUSD --resample 15min --tz Europe/Prague -o eurusd_m15.csv
duka export XAUUSD --side mid --resample 4h -o xau_h4.parquet
duka export EURUSD --tf tick --from 2024-03-01 --to 2024-03-01 --resample 10s -o eurusd_10s.csv

duka selftest                                 # živý test: 2 symboly z každé kategorie
duka selftest EURUSD BTCUSD                   # test jen vybraných symbolů
```

Užitečné volby: `--data-dir D:\duka_data` (nebo proměnná `DUKA_DATA_DIR`),
`-w/--workers`, `--rate`, `--retries`, `--force` (stáhnout znovu), `-v` (víc logů).
Log se ukládá do `data/_logs/duka.log`.

Hlavní skupiny: `FX` (`FX_MAJORS`, `FX_CROSSES`, `FX_METALS`), `CMD` (`CMD_ENERGY`,
`CMD_METALS`, `CMD_AGRICULTURAL`), `IDX` (`IDX_AMERICA`, `IDX_EUROPE`, `IDX_ASIA`),
`BND_CFD`, `STCK_CFD`, `ETF_CFD`, `VCCY` (krypto).

## Struktura složky s daty

Nahoře timeframe, pod ním kategorie a podkategorie, pak symbol:

```
data/
  m1/Forex/Majors/EUR-USD/BID/2024-01.parquet             (1 soubor na měsíc)
  m1/Commodities/Energy/LIGHT.CMD-USD/BID/...
  h1/Indices/America/USA500.IDX-USD/BID/2024.parquet      (1 soubor na rok)
  d1/Stocks/US/NVDA.US-USD/BID/all.parquet                (1 soubor)
  d1/Crypto/BTC-USD/BID/all.parquet
  tick/Forex/Metals/XAU-USD/2024-01-02.parquet            (1 soubor na den, bid+ask)
  _meta/, _logs/
```

Kategorie: `Forex` (`Majors`, `Crosses`, `Metals`), `Commodities` (`Energy`, `Metals`,
`Agricultural`), `Indices` (`America`, `Europe`, `Asia_Pacific`, `Africa`), `Bonds`,
`Stocks/<země>`, `ETF/<země>`, `Crypto`.

Data uložená ve starším rozložení (`m1/EUR-USD/...` nebo `m1/FX/FX_MAJORS/...`) se přesunou samy při
dalším `download` nebo `update`. `load()`, `export` a `status` najdou data v obou
rozloženích.

## Použití v Pythonu (backtesty)

```python
from dukascopy_downloader import Downloader, load

dl = Downloader("data")
dl.download(["EURUSD", "XAUUSD"], tf="m1", start="2015-01-01", sides="both")

df = load("data", "EUR-USD", "m1", start="2020", end="2024", side="BID")
m15 = load("data", "EUR-USD", "m1", side="MID", resample="15min", tz="Europe/Prague")
```

Sloupce: `time` (UTC, tz-aware), `open, high, low, close, volume`; ticky:
`time, bid, ask, bid_volume, ask_volume`. Dukascopy u starších dat vyplňuje neaktivní
minuty plochými svíčkami s nulovým objemem – lze je zahodit `drop_flat=True` /
`--drop-flat`.

## Formát API (pro údržbu)

| timeframe | endpoint | 1 request = |
|---|---|---|
| tick | `/v1/ticks/{CODE}/{Y}/{M}/{D}/{H}` | 1 hodina |
| m1 | `/v1/candles/minute/{CODE}/{BID\|ASK}/{Y}/{M}/{D}` | 1 den |
| h1 | `/v1/candles/hour/{CODE}/{BID\|ASK}/{Y}/{M}` | 1 měsíc |
| d1 | `/v1/candles/day/{CODE}/{BID\|ASK}/{Y}` | 1 rok |

Seznam instrumentů `/v1/instruments`, detail `/v1/instruments/{CODE}`. Hodnoty jsou
delta‑kódované: `time = timestamp + cumsum(times) * shift`,
`open = (round(open/multiplier) + cumsum(opens)) * multiplier` (stejně high/low/close,
u ticků bid/ask), objemy jsou přímo. Neuzavřené období vrací `400 From time is too late`.

## Testy

```bash
python -m unittest discover -s tests -v      # offline testy (bez sítě)
duka selftest                                # živý test proti API
```
