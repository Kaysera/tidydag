import asyncio
from collections.abc import AsyncIterator
from graphlib import TopologicalSorter

from .._utils import get_event_loop
from ..node.base import Node, OrchestratorContext

__all__ = "Orchestrator"


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

    def add_node(self, node: Node[StateT, DepsT]):
        """Add a node to the orchestrator.

        Args:
            node: The node to add.
        """
        self.sorter.add(node, *node.parents)

    def run_sync(self, state: StateT = None, deps: DepsT = None):
        """Run the orchestrator synchronously.

        Args:
            state: The state of the graph.
        """
        self.loop.run_until_complete(self.run(state, deps))

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

    async def run(self, state: StateT = None, deps: DepsT = None):
        """Run the orchestrator.

        Args:
            state: The state of the graph.
        """
        if self.ctx is None:
            self.ctx = OrchestratorContext(state=state, deps=deps)

        async for node_group in self.iterate():
            for id, node in node_group:
                node.id = id
                self.loop.create_task(self._visit(node, self.ctx, self.sorter))

    async def _visit(
        self,
        node: Node[StateT, DepsT],
        ctx: OrchestratorContext[StateT, DepsT],
        sorter: TopologicalSorter,
    ):
        if node.id in ctx.metadata.executed:
            sorter.done(node)
            return True

        node_state = await node.execute(ctx)

        if node_state.success:
            ctx.metadata.executed.add(node.id)
        else:
            print(f"Error in node {node.name} , reason: {node_state.reason}")
            self.stop = True

        sorter.done(node)
        return True
