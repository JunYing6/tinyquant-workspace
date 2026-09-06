"""Per-scope Tushare fetchers for the legacy parquet layout.

Each module ports one legacy downloader from the research workspace.  Every
fetcher has the uniform signature::

    fetch(client, start, end, *, sleep=0.0, calendar=None, data_root=None, **options)
        -> pd.DataFrame | dict[str, pd.DataFrame]

``client`` is an injected vendor SDK object (the fetchers never read tokens or
credentials), ``calendar(start, end)`` resolves trading dates (injected or
fetched from the vendor ``trade_cal`` interface), and ``data_root`` is the raw
volume root for the one scope that aggregates from already-downloaded files.
Fetchers return vendor-native frames — no renaming, no unit conversion; the
semantic rules live in ``data.adapters.workspace_data.mappings``.
"""
