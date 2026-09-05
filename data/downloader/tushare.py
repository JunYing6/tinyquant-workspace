"""Tushare downloader skeleton.

The downloader only fetches *raw* vendor frames into the legacy parquet layout;
field renaming, unit conversion and PIT semantics happen in
``data.adapters.workspace_data``.  A real run needs a Tushare pro-api client:

    import tushare as ts
    client = ts.pro_api(token)
    downloader = TushareDownloader(client=client, root="E:/ProgramData")
    downloader.download("trade_data/daily", "20240102", "20240131")

Only the daily-bar scope is wired as a working example; the remaining scopes
follow the same pattern (extend ``_SCOPE_FETCHERS``).
"""

from __future__ import annotations

from typing import Any, Callable

import pandas as pd

from data.downloader.base import RawVolumeWriter

# legacy scope -> (client method name, param renames)
_SCOPE_CLIENT_METHODS: dict[str, str] = {
    "trade_data/daily": "daily",
    "trade_data/moneyflow": "moneyflow",
    "trade_data/margin": "margin_detail",
    "fina/indicator": "fina_indicator",
}


class TushareDownloader:
    """Fetch raw Tushare frames and land them in the legacy parquet volume."""

    def __init__(
        self,
        client: Any = None,
        *,
        root: str | Path = "E:/ProgramData",
        rate_limit_sleep: float = 0.35,
    ) -> None:
        if client is None:
            raise ValueError(
                "a tushare pro-api client is required (inject it; the downloader never reads credentials)"
            )
        self._client = client
        self._writer = RawVolumeWriter(root)
        self._rate_limit_sleep = rate_limit_sleep

    def fetch(self, scope: str, start: str, end: str) -> pd.DataFrame:
        fetcher = _SCOPE_FETCHERS.get(scope)
        if fetcher is None:
            raise NotImplementedError(
                f"scope {scope!r} has no fetcher yet; extend _SCOPE_FETCHERS "
                f"following the trade_data/daily example"
            )
        return fetcher(self._client, start, end, self._rate_limit_sleep)

    def download(self, scope: str, start: str, end: str, **write_kwargs: Any) -> list:
        frame = self.fetch(scope, start, end)
        return self._writer.write_scope(scope, frame, **write_kwargs)


def _fetch_daily(
    client: Any,
    start: str,
    end: str,
    sleep: float,
) -> pd.DataFrame:
    """Working example: daily bars for the whole market, kline layout in/out."""
    frames: list[pd.DataFrame] = []
    current = _to_date(start)
    final = _to_date(end)
    while current <= final:
        trade_date = current.strftime("%Y%m%d")
        raw = _call(client, "daily", trade_date=trade_date)
        if raw is not None and len(raw):
            raw = raw.copy()
            raw["timeframe"] = "1d"
            raw["data_type"] = "stock"  # indexes/funds need their own calls
            frames.append(raw)
        current += pd.Timedelta(days=1)
        import time as _time

        _time.sleep(sleep)
    return pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()


def _call(client: Any, method: str, **params: Any) -> pd.DataFrame | None:
    fn: Callable[..., pd.DataFrame] = getattr(client, method)
    return fn(**params)


def _to_date(value: str):
    from datetime import datetime

    return datetime.strptime(value, "%Y%m%d").date()


_SCOPE_FETCHERS: dict[str, Callable[[Any, str, str, float], pd.DataFrame]] = {
    "trade_data/daily": _fetch_daily,
}

__all__ = ["TushareDownloader"]
