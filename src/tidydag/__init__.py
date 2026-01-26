from importlib.metadata import version as _metadata_version

from .node import Node

__all__ = ("__version__", "Node", "Orchestrator")
__version__ = _metadata_version("tidydag")
