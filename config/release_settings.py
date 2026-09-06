"""Configuration for release-provided components (tinyquant framework knobs).

This file holds the values that the RELEASE-side components consume — Excel
report output, and later engine tuning (slippage, order costs) or other
``tools.*`` options.  The release itself never reads configuration: every
parameter is injected by the client, and this file is where the client keeps
those injections.

Ownership rule:

- keys here follow the release's interfaces — re-verify this file when
  upgrading the ``tinyquant`` dependency;
- everything the CLIENT code reads (data root, pools, backtest windows,
  tokens) lives in ``settings.py`` instead.
"""

import os

# Excel backtest report output folder, anchored at the project root.
EXCEL_OUTPUT_DIR = os.path.join(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "excel_reports"
)

# FastBacktestEngine tuning (passed straight through to the release engine):
# SLIPPAGE = None          # e.g. {"spread": 0.01} or a SlippageModel
# ORDER_COST = None        # e.g. {"commission": 0.0003, "stamp_tax": 0.0005}

