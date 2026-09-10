"""Data adapter implementations for the user-side project.

A data adapter implements one of the release ``tools.data`` Ports and converts
an external source (local file, database, or API) into canonical
``tools.data`` records.  These adapters are deliberately thin examples; replace
them with your own source logic.
"""

from __future__ import annotations

import importlib

from config import settings

__all__ = ["build_gateway"]


def build_gateway():
    """Assemble a DataGateway from the configured factory.

    Reads ``settings.DATA_GATEWAY_FACTORY`` (``module:function``) and
    constructs the gateway with ``settings.DATA_ROOT``.  Change the factory in
    config to switch data source without touching application code.
    """
    module_name, separator, func_name = settings.DATA_GATEWAY_FACTORY.partition(":")
    if not separator or not module_name or not func_name:
        raise ValueError(f"DATA_GATEWAY_FACTORY must be 'module:function', got {settings.DATA_GATEWAY_FACTORY!r}")
    module = importlib.import_module(module_name)
    factory = getattr(module, func_name)
    return factory(settings.DATA_ROOT)