"""Client-side configuration: everything the CLIENT code reads.

Data root, strategy pool, backtest window, capital, tokens.  Values consumed
by release-provided components (excel report output, engine tuning) live in
``config/release_settings.py`` instead.
"""

import os

BACKTEST_START = "20240102"
BACKTEST_END = "20240103"
INITIAL_CAPITAL = 1_000_000.0
STOCK_POOL = ["000001.SZ", "600000.SH"]
QUOTE_TOKEN = ""

# Root of the consolidated workspace volume (duckdb tables + per-day parquet)
# used by the adapters.workspace_data package (the USB data drive).
DATA_ROOT = os.environ.get("QUANT_DATA_ROOT", "E:/ProgramData")
