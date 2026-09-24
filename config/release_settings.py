"""release 提供组件的配置（tinyquant 框架旋钮）。

本文件保存 RELEASE 侧组件消费的值——Excel 报告输出，以及后续的引擎调优
（滑点、下单成本）或其他 ``tools.*`` 选项。release 本身从不读取配置：
每个参数均由客户端注入，而本文件就是客户端保存这些注入值的地方。

归属规则：

- 此处的键遵循 release 的接口——升级 ``tinyquant`` 依赖时请重新核对本文件；
- CLIENT 代码读取的全部内容（数据根目录、池、回测窗口、令牌）则放在
  ``settings.py`` 中。
"""

import os

# Excel 回测报告输出文件夹，锚定在项目根目录。
EXCEL_OUTPUT_DIR = os.path.join(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "excel_reports"
)

# FastBacktestEngine 调优（直接透传给 release 引擎）：
# SLIPPAGE = None          # 例如 {"spread": 0.01} 或一个 SlippageModel
# ORDER_COST = None        # 例如 {"commission": 0.0003, "stamp_tax": 0.0005}

