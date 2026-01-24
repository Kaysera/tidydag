import asyncio
from graphlib import TopologicalSorter

from tidydag._utils import get_event_loop
from tidydag.node.base import Node, OrchestratorContext, StateT


class Orchestrator:
    def __init__(self, step=0.1):
        self.sorter = TopologicalSorter()
        self.step = step
        self.loop = get_event_loop()
        self.stop = False

    def add_node(self, node: Node):
        self.sorter.add(node, *node.parents)

    def run_sync(self, state: StateT = None):
        self.loop.run_until_complete(self.run(state))

    async def run(self, state: StateT = None):
        ctx = OrchestratorContext(state=state)
        self.sorter.prepare()
        while self.sorter and not self.stop:
            node_group = self.sorter.get_ready()

            if not node_group:
                await asyncio.sleep(self.step)
            else:
                for node in node_group:
                    self.loop.create_task(self._visit(node, ctx, self.sorter))

    async def _visit(self, node: Node, ctx: OrchestratorContext, sorter: TopologicalSorter):
        node_state = await node.execute(ctx)
        if not node_state.success:
            print(f"Error in node {node.name} , reason: {node_state.reason}")
            self.stop = True
        sorter.done(node)
