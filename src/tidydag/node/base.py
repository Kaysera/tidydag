from __future__ import annotations

from abc import ABC, abstractmethod
from collections.abc import Iterable
from dataclasses import dataclass
from typing import TypeVar

StateT = TypeVar("StateT", default=None)
"""Type variable for the state in a graph."""


@dataclass(kw_only=True)
class OrchestratorContext:
    """Context for a graph."""

    state: StateT
    """The state of the graph."""


@dataclass
class NodeState:
    success: bool
    reason: str | None = None


@dataclass
class SuccessState(NodeState):
    success: bool = True
    reason: None = None


class ErrorState(NodeState):
    def __init__(self, reason: str):
        self.success = False
        self.reason = reason


class Node(ABC):
    def __init__(self, name: str | None = None, parents: Node | Iterable[Node] | None = None):
        self.parents = self._verify_parents(parents)
        self.name = name

    def _verify_parents(self, parents: Node | Iterable[Node] | None):
        if parents is None:
            return tuple([])
        elif isinstance(parents, Node):
            return tuple([parents])
        elif isinstance(parents, Iterable):
            return tuple(parents)
        else:
            raise ValueError("Parents must be a Node or Iterable")

    @abstractmethod
    async def execute(self, ctx: OrchestratorContext) -> NodeState:
        raise NotImplementedError
