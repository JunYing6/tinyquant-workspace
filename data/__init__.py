"""用户项目的数据域。

三个职责，同一流水线方向：

- ``data.downloader``  厂商 API -> 原始 parquet 数据卷（E:/ProgramData 布局）
- ``data.adapters``    原始数据卷 -> 标准 ``tools.data`` 网关数据集
- ``data.processing``  标准数据集 -> 衍生数据集（指标等）
"""

