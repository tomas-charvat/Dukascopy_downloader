#!/usr/bin/env python
"""Run the CLI without installing:  python duka.py download EURUSD --from 2020"""
import sys

from dukascopy_downloader.cli import main

if __name__ == "__main__":
    sys.exit(main())
