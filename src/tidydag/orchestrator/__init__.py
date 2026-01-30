import asyncio
from collections.abc import AsyncIterator
from dataclasses import dataclass
from graphlib import TopologicalSorter

from .._utils import get_event_loop
from ..node.base import Node, OrchestratorContext

__all__ = "Orchestrator"


@dataclass
class OrchestratorExecution[StateT, DepsT]:
    ctx: OrchestratorContext[StateT, DepsT]
    success: bool
    reason: str | None = None
    last_node: Node[StateT, DepsT] | None = None


class Orchestrator[StateT, DepsT]:
    """An orchestrator for a graph."""

    def __init__(
        self,
        ctx: OrchestratorContext[StateT, DepsT] | None = None,
        step: float = 0.1,
    ):
        """Initialize the orchestrator.

        Args:
            step: Time to wait between checks for the next ready node.
        """
        self.sorter = TopologicalSorter()
        self.step = step
        self.id_counter = 0
        self.loop = get_event_loop()
        self.stop = False
        self.ctx: OrchestratorContext[StateT, DepsT] = ctx
        self.execution: OrchestratorExecution = None

    def add_node(self, node: Node[StateT, DepsT]):
        """Add a node to the orchestrator.

        Args:
            node: The node to add.
        """
        self.sorter.add(node, *node.parents)

    def run_sync(self, state: StateT = None, deps: DepsT = None) -> OrchestratorExecution:
        """Run the orchestrator synchronously.

        Args:
            state: The state of the graph.
        """
        return self.loop.run_until_complete(self.run(state, deps))

    async def iterate(self) -> AsyncIterator[list[tuple[int, Node[StateT, DepsT]]]]:
        """Iterate over the nodes in the graph as they become ready.

        Yields:
            A tuple containing a unique ID and a single Node instance.
        """
        self.sorter.prepare()

        # Use is_active() if using standard graphlib.TopologicalSorter
        while self.sorter and not self.stop:
            node_group = self.sorter.get_ready()

            if not node_group:
                # If no nodes are ready, wait and check again
                await asyncio.sleep(self.step)
                continue

            # Create a batch of (id, node) pairs for this dependency level
            batch = []
            for node in node_group:
                self.id_counter += 1
                batch.append((self.id_counter, node))

            yield batch  # Yield the entire ready-to-run group

    async def run(self, state: StateT = None, deps: DepsT = None) -> OrchestratorExecution:
        """Run the orchestrator.

        Args:
            state: The state of the graph.
        """
        if self.ctx is None:
            self.ctx = OrchestratorContext(state=state, deps=deps)
        self.execution = OrchestratorExecution(self.ctx, True)
        try:
            async with asyncio.TaskGroup() as tg:
                # The loop spawns tasks into the group
                async for node_group in self.iterate():
                    for id, node in node_group:
                        node.id = id
                        tg.create_task(self._visit(node))

        except* Exception as e:
            # 'except*' handles ExceptionGroups (multiple errors at once)
            print(f"One or more nodes failed: {e}")

        return self.execution

    async def _visit(
        self,
        node: Node[StateT, DepsT],
    ):
        if node.id in self.ctx.metadata.executed:
            self.sorter.done(node)
            return True

        node_state = await node.execute(self.ctx)

        if node_state.success:
            self._checkpoint(node, self.ctx)
        else:
            print(f"Error in node {node.name} , reason: {node_state.reason}")
            self.stop = True
            self.execution.success = False
            self.execution.reason = node_state.reason

        self.execution.last_node = node
        self.sorter.done(node)
        return True

    def _checkpoint(
        self,
        node: Node[StateT, DepsT],
        ctx: OrchestratorContext[StateT, DepsT],
    ):
        ctx.metadata.executed.add(node.id)
