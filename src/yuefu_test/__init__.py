"""Client tooling for exercising the Yuefu OMR service."""
from __future__ import annotations

from importlib import metadata

try:
    __version__ = metadata.version("yuefu-test")
except metadata.PackageNotFoundError:  # pragma: no cover
    __version__ = "0.0.0"

__all__ = ["__version__"]

