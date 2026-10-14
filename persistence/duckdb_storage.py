"""共享上下文的 DuckDB 持久化后端（收盘落盘 / 盘前恢复）。

core 只定义抽象 :class:`StateStorage` 协议；本实现放工作区（依赖 duckdb）。
"""

from __future__ import annotations

import json
from typing import Any, Dict

import duckdb


class DuckDBStorage:
    """把共享上下文按节点存入本地 duckdb 文件。

    兼容 ``save`` 的两种输入：
    - 全量 ``{node: payload}``（:meth:`TradingContext.save` 用）；
    - 增量 ``{"nodes": {node: payload}}``（:meth:`TradingContext.save_dirty` 用，合并进库）。
    ``load`` 恒返回全量 ``{node: payload}``。
    """

    def __init__(self, db_path: str) -> None:
        self.db_path = str(db_path)
        self._init_table()

    def _connect(self) -> duckdb.DuckDBPyConnection:
        return duckdb.connect(self.db_path)

    def _init_table(self) -> None:
        con = self._connect()
        try:
            con.execute(
                "CREATE TABLE IF NOT EXISTS context_state "
                "(node VARCHAR PRIMARY KEY, payload VARCHAR)"
            )
        finally:
            con.close()

    def save(self, state: Dict[str, Any]) -> None:
        nodes = state.get("nodes") if isinstance(state, dict) and "nodes" in state else state
        if not isinstance(nodes, dict) or not nodes:
            return
        con = self._connect()
        try:
            for name, payload in nodes.items():
                con.execute(
                    "INSERT OR REPLACE INTO context_state (node, payload) VALUES (?, ?)",
                    [str(name), json.dumps(payload, ensure_ascii=False)],
                )
        finally:
            con.close()

    def load(self) -> Dict[str, Any]:
        con = self._connect()
        try:
            rows = con.execute("SELECT node, payload FROM context_state").fetchall()
        finally:
            con.close()
        out: Dict[str, Any] = {}
        for name, payload in rows:
            out[name] = json.loads(payload) if isinstance(payload, str) else payload
        return out


__all__ = ["DuckDBStorage"]