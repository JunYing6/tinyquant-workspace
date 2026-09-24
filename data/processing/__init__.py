"""基于标准数据集的第二阶段数据处理。

指标及其他衍生数据集从*标准*输入（绝不用原始厂商表）计算得出。结果以普通网关数据集形式暴露，并带有输入指纹溯源。
"""

from data.processing.base import calculation_version, parameter_hash
from data.processing.indicators import DerivedIndicatorAdapter

__all__ = ["DerivedIndicatorAdapter", "calculation_version", "parameter_hash"]
