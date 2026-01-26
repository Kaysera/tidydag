import asyncio
import random

import pytest

from tidydag.node.base import Node, NodeState, OrchestratorContext, SuccessState
from tidydag.orchestrator import Orchestrator


@pytest.mark.asyncio
async def test_orchestrator_order():
    flow = []

    class MockNode(Node):
        def __init__(self, name, parents=None, wait=5):
            self.wait = wait
            super().__init__(name, parents)

        async def execute(self, ctx: OrchestratorContext) -> NodeState:
            print(f"Node {self.name}, waiting for {self.wait}")
            await asyncio.sleep(self.wait)
            print(f"Node {self.name} finished waiting")
            flow.append(self.name)
            return SuccessState()

    node_a = MockNode(name="a", wait=0.1)
    node_b = MockNode(name="b", parents=node_a, wait=0.2)
    node_c = MockNode(name="c", parents=node_a, wait=0.3)
    node_d = MockNode(name="d", parents=[node_b, node_c], wait=0.1)

    node_collection = [node_a, node_b, node_c, node_d]
    random.shuffle(node_collection)

    orchestrator = Orchestrator()
    for node in node_collection:
        orchestrator.add_node(node)

    await orchestrator.run()

    flow_dict = {name: i for i, name in enumerate(flow)}

    assert flow_dict["a"] < flow_dict["b"]
    assert flow_dict["a"] < flow_dict["c"]
    assert flow_dict["a"] < flow_dict["d"]
    assert flow_dict["b"] < flow_dict["c"]
    assert flow_dict["b"] < flow_dict["d"]
    assert flow_dict["c"] < flow_dict["d"]
