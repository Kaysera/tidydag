from __future__ import annotations

from abc import ABC, abstractmethod
from collections.abc import Iterable
from dataclasses import dataclass, field
from typing import Generic, TypeVar

StateT = TypeVar("StateT", default=None)
"""Type variable for the state in a graph."""

DepsT = TypeVar("DepsT", default=None)
"""Type variable for the dependencies in a graph."""

ProfileT = TypeVar("ProfileT", default=None)
"""Type variable for the dependencies in a graph."""


@dataclass
class OrchestratorMetadata[ProfileT]:
    executed: set[int] = field(default_factory=set)
    profile: dict[int, ProfileT] = field(default_factory=dict)


@dataclass(kw_only=True)
class OrchestratorContext(Generic[StateT, DepsT, ProfileT]):
    """Context for a graph."""

    state: StateT
    """The state of the graph."""

    deps: DepsT
    """The dependencies of the graph."""

    metadata: OrchestratorMetadata[ProfileT] = field(default_factory=OrchestratorMetadata[ProfileT])
    """The metadata of the execution of the graph"""


@dataclass
class NodeState:
    """The state of a node after execution."""

    success: bool
    """Whether the node execution was successful."""
    reason: str | None = None
    """The reason for failure, if any."""


@dataclass
class SuccessState(NodeState):
    """A successful state for a node."""

    success: bool = True


class ErrorState(NodeState):
    """An error state for a node."""

    def __init__(self, reason: str):
        """Initialize the error state.

        Args:
            reason: The reason for the failure.
        """
        self.success = False
        self.reason = reason


class Node(ABC, Generic[StateT, DepsT, ProfileT]):
    """Base class for a Node"""

    def __init__(
        self,
        name: str | None = None,
        parents: Node[StateT, DepsT, ProfileT] | Iterable[Node[StateT, DepsT, ProfileT]] | None = None,
    ):
        """Initialize the node.

        Args:
            name: The name of the node.
            parents: The parents of the node. Set to None if the node has no parents
        """
        self.parents = self._verify_parents(parents)
        self.name = name
        self.id: int = None

    def _verify_parents(
        self, parents: Node[StateT, DepsT, ProfileT] | Iterable[Node[StateT, DepsT, ProfileT]] | None = None
    ):
        if parents is None:
            return tuple([])
        elif isinstance(parents, Node):
            return tuple([parents])
        elif isinstance(parents, Iterable) and not isinstance(parents, (str, bytes)):
            return tuple(parents)
        else:
            raise ValueError("Parents must be a Node or Iterable")

    @abstractmethod
    async def execute(self, ctx: OrchestratorContext[StateT, DepsT, ProfileT]) -> NodeState:
        """Execute the node.

        Args:
            ctx: The context for the graph.

        Returns:
            The state of the node after execution.
        """
        raise NotImplementedError

    def profile(
        self, ctx: OrchestratorContext[StateT, DepsT, ProfileT], state: NodeState, start_ns: float
    ) -> ProfileT:
        """Compute the necessary elements for the node profile

        Args:
            ctx: The context for the graph.
            state: The node state upon execution
            start_ns: The time of the node execution start in milliseconds

        Returns:
            The node profile
        """
        pass
