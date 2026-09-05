"""Second-stage data processing over canonical datasets.

Indicators and other derived datasets computed from *standard* inputs (never
raw vendor tables).  Results are exposed as regular gateway datasets with
input-fingerprint provenance.
"""

from data.processing.base import calculation_version, parameter_hash
from data.processing.indicators import DerivedIndicatorAdapter

__all__ = ["DerivedIndicatorAdapter", "calculation_version", "parameter_hash"]
