"""面向用户侧项目的数据适配器实现。

数据适配器实现了发布版 ``tools.data`` 的某个 Port，将外部数据源（本地文件、数据库或 API）
转换为规范的 ``tools.data`` 记录。这些适配器刻意保持为薄示例；请用你自己的数据源逻辑替换它们。
"""

from __future__ import annotations

import importlib

from config import settings

__all__ = ["build_gateway"]


def build_gateway():
    """根据配置的工厂组装一个 DataGateway。

    读取 ``settings.DATA_GATEWAY_FACTORY``（``模块:函数``），并用 ``settings.DATA_ROOT``
    构建网关。修改 config 中的工厂即可切换数据源，无需改动应用代码。
    """
    module_name, separator, func_name = settings.DATA_GATEWAY_FACTORY.partition(":")
    if not separator or not module_name or not func_name:
        raise ValueError(f"DATA_GATEWAY_FACTORY must be 'module:function', got {settings.DATA_GATEWAY_FACTORY!r}")
    module = importlib.import_module(module_name)
    factory = getattr(module, func_name)
    return factory(settings.DATA_ROOT)