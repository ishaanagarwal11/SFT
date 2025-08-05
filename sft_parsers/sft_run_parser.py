#!/usr/bin/env python3
"""
Unified runner for all SEC form parsers in ./parsers/
"""

import os
import subprocess
from pathlib import Path

# COMMON CONFIGURATION 
#process all tickers
# Tickers to process — empty list means all
TICKERS_TO_PROCESS = [
    "WMT", "AMZN", "UNH", "AAPL", "CVS", "BRK.B", "GOOGL", "XOM",
    "MCK", "COR", "JPM", "COST", "CI", "MSFT", "CAH"
]

# Shared directories
FILINGS_DIR = "filings/filings"
LINKS_DIR = "links"
OUT_DIR = "chunks"

# Parser script names (must exist in ./parsers)
PARSER_SCRIPTS = [
    "parser_3.py",
    "parser_4.py",
    "parser_5.py",
    "parser_8-k.py",
    "parser_10-k.py",
    "parser_10-Q.py",
    "parser_DEF 14A.py",
]

# ENV SETUP

env = os.environ.copy()
env["TICKERS_TO_PROCESS"] = ",".join(TICKERS_TO_PROCESS)
env["FILINGS_DIR"] = FILINGS_DIR
env["LINKS_DIR"] = LINKS_DIR
env["OUT_DIR"] = OUT_DIR

parser_dir = Path(__file__).parent / "parsers"

# RUN ALL

print("\nLaunching all SEC parsers from ./parsers/\n")

for script in PARSER_SCRIPTS:
    script_path = parser_dir / script
    print(f"\n\nRunning: {script} …")
    result = subprocess.run(["python3", str(script_path)], env=env)

    if result.returncode != 0:
        print(f"xxxxxxxx{script} failed with exit code {result.returncode}\n")
    else:
        print(f"========{script} completed successfully\n")

print("All parser scripts finished.")
