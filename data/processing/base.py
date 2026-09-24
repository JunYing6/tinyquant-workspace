"""衍生适配器辅助函数（基于标准数据的第二阶段处理）。"""

from __future__ import annotations

import hashlib
from typing import Any


def parameter_hash(*parts: Any) -> str:
    """指标名及其参数的稳定哈希。"""
    digest = hashlib.md5("|".join(str(p) for p in parts).encode())
    return digest.hexdigest()[:16]


def calculation_version(module: str, version: str) -> str:
    """将计算实现与版本绑定在一起的溯源字符串。"""
    return f"{module}@{version}"


__all__ = ["calculation_version", "parameter_hash"]
