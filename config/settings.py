"""Tracked, credential-free defaults for the example project."""

import os

BACKTEST_START = "20240102"
BACKTEST_END = "20240103"
INITIAL_CAPITAL = 1_000_000.0
STOCK_POOL = ["000001.SZ", "600000.SH"]
QUOTE_TOKEN = ""

# Root of the legacy Tushare-layout parquet volume used by the
# adapters.workspace_data package (the USB data drive).
DATA_ROOT = os.environ.get("QUANT_DATA_ROOT", "E:/ProgramData")
