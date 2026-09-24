"""面向旧版 parquet 布局的按作用域 Tushare 获取器。

每个模块移植研究工作区的一个旧下载器。所有获取器具有统一签名::

    fetch(client, start, end, *, sleep=0.0, calendar=None, data_root=None, **options)
        -> pd.DataFrame | dict[str, pd.DataFrame]

``client`` 是注入的厂商 SDK 对象（获取器从不读取 token 或凭据），``calendar(start, end)`` 解析
交易日（注入或从厂商 ``trade_cal`` 接口获取），``data_root`` 是唯一一个从已下载文件做聚合的
作用域的原始卷根目录。获取器返回厂商原生表——不做重命名，不做单位换算；语义规则位于
``data.adapters.workspace_data.mappings``。
"""
