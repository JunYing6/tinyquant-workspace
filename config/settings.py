"""客户端配置：CLIENT 代码读取的全部内容。

数据根目录、策略池、回测窗口、资金。敏感凭据（API 密钥与令牌）存放在
被 git 忽略的 ``config/secrets.py`` 中，此处读取时为空值兜底。由 release
提供的组件（excel 报告输出、引擎调优）消费的值则位于
``config/release_settings.py``。
"""

import os

try:
    from config import secrets
except ImportError:
    secrets = None

BACKTEST_START = "20240102"
BACKTEST_END = "20240103"
INITIAL_CAPITAL = 1_000_000.0
STOCK_POOL = ["000001.SZ", "600000.SH"]
QUOTE_TOKEN = getattr(secrets, "QUOTE_TOKEN", "") if secrets else ""

# 整合工作区卷的根目录（duckdb 表 + 每日 parquet），
# 供 adapters.workspace_data 包使用（USB 数据盘）。
DATA_ROOT = os.environ.get("QUANT_DATA_ROOT", "E:/ProgramData")

# 用于组装 DataGateway 的 module:function 工厂。可将其指向
# 另一个适配器构建器而无需修改代码。
DATA_GATEWAY_FACTORY = "data.adapters.workspace_data:build_workspace_gateway"

# 实盘交易：实时行情客户端工厂、执行器路由、资金、市场。
REALTIME_QUOTE_CLIENT = "data.realtime.jvquant:JvQuantQuoteClient"
LIVE_EXECUTOR_ROUTE = "paper"
LIVE_INITIAL_CAPITAL = 1_000_000.0
LIVE_MARKET = "CN"
