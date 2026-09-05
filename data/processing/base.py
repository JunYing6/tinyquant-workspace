"""Derived-adapter helpers (second-stage processing over canonical data)."""

from __future__ import annotations

import hashlib
from typing import Any


def parameter_hash(*parts: Any) -> str:
    """Stable hash over the indicator name and its parameters."""
    digest = hashlib.md5("|".join(str(p) for p in parts).encode())
    return digest.hexdigest()[:16]


def calculation_version(module: str, version: str) -> str:
    """Provenance string binding a calculation implementation to a version."""
    return f"{module}@{version}"


__all__ = ["calculation_version", "parameter_hash"]
