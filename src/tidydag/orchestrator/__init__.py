import asyncio
from collections.abc import AsyncIterator
from graphlib import TopologicalSorter

from tidydag._utils import get_event_loop
from tidydag.node.base import DepsT, Node, OrchestratorContext, StateT


class Orchestrator:
    """An orchestrator for a graph."""

    def __init__(self, step: float = 0.1):
        """Initialize the orchestrator.

        Args:
            step: Time to wait between checks for the next ready node.
        """
        self.sorter = TopologicalSorter()
        self.step = step
        self.loop = get_event_loop()
        self.stop = False

    def add_node(self, node: Node):
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

    async def iterate(self) -> AsyncIterator[tuple[Node, ...]]:
        """Iterate over the nodes in the graph as they become ready.

        Yields:
            A list of nodes that are ready to be executed.
        """
        self.sorter.prepare()
        while self.sorter and not self.stop:
            node_group = self.sorter.get_ready()

            if not node_group:
                await asyncio.sleep(self.step)
            else:
                yield node_group

    async def run(self, state: StateT = None, deps: DepsT = None):
        """Run the orchestrator.

        Args:
            state: The state of the graph.
        """
        ctx = OrchestratorContext(state=state, deps=deps)
        async for node_group in self.iterate():
            for node in node_group:
                self.loop.create_task(self._visit(node, ctx, self.sorter))

    async def _visit(self, node: Node, ctx: OrchestratorContext, sorter: TopologicalSorter):
        node_state = await node.execute(ctx)
        if not node_state.success:
            print(f"Error in node {node.name} , reason: {node_state.reason}")
            self.stop = True
        sorter.done(node)
