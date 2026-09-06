"""Example settings.  Copy to ``settings.py`` and fill in your values."""

# Data root of the consolidated workspace volume (duckdb tables + per-day
# parquet).  settings.py resolves it from the QUANT_DATA_ROOT environment
# variable with E:/ProgramData as the default.
DATA_ROOT = "E:/ProgramData"

# Backtest defaults
BACKTEST_START = "20240101"
BACKTEST_END = "20241231"
INITIAL_CAPITAL = 1_000_000.0

# Realtime quote/trade config (depends on your adapter)
QUOTE_TOKEN = ""

# Excel backtest report output directory (absolute or relative to CWD).
EXCEL_OUTPUT_DIR = "excel_reports"
