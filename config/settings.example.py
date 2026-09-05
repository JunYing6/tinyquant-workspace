"""Example settings.  Copy to ``settings.py`` and fill in your values."""

# Data root for your local parquet/duckdb files (unused by the in-memory demo).
QUANT_DATA_PATH = ""

# Backtest defaults
BACKTEST_START = "20240101"
BACKTEST_END = "20241231"
INITIAL_CAPITAL = 1_000_000.0

# Realtime quote/trade config (depends on your adapter)
QUOTE_TOKEN = ""
