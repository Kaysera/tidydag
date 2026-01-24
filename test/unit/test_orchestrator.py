import random
from dataclasses import dataclass

from tidydag.node.base import ErrorState, Node, NodeState, OrchestratorContext, SuccessState
from tidydag.orchestrator import Orchestrator


def test_orchestrator_order():
    flow = []

    class MockNode(Node):
        async def execute(self, ctx):
            flow.append(self.name)
            return SuccessState()

    node_a = MockNode(name="a")
    node_b = MockNode(name="b", parents=node_a)
    node_c = MockNode(name="c", parents=node_a)
    node_d = MockNode(name="d", parents=[node_b, node_c])

    node_collection = [node_a, node_b, node_c, node_d]
    random.shuffle(node_collection)

    orchestrator = Orchestrator()
    for node in node_collection:
        orchestrator.add_node(node)

    orchestrator.run_sync()

    flow_dict = {name: i for i, name in enumerate(flow)}

    assert flow_dict["a"] < flow_dict["b"]
    assert flow_dict["a"] < flow_dict["c"]
    assert flow_dict["a"] < flow_dict["d"]
    assert flow_dict["b"] < flow_dict["d"]
    assert flow_dict["c"] < flow_dict["d"]


def test_orchestrator_state_mutation():
    @dataclass
    class MockState:
        counter: int = 0

    class MockNode(Node):
        async def execute(self, ctx: OrchestratorContext) -> NodeState:
            ctx.state.counter += 1
            return SuccessState()

    node_a = MockNode(name="a")
    node_b = MockNode(name="b", parents=node_a)
    node_c = MockNode(name="c", parents=node_a)
    node_d = MockNode(name="d", parents=[node_b, node_c])

    node_collection = [node_a, node_b, node_c, node_d]
    random.shuffle(node_collection)

    orchestrator = Orchestrator()
    for node in node_collection:
        orchestrator.add_node(node)

    state = MockState()
    orchestrator.run_sync(state=state)

    assert state.counter == 4


def test_orchestrator_state_ordered_mutation():
    @dataclass
    class MockState:
        flow: tuple = tuple([])

    class MockNode(Node):
        async def execute(self, ctx: OrchestratorContext) -> NodeState:
            ctx.state.flow = tuple([*ctx.state.flow, self.name])
            return SuccessState()

    node_a = MockNode(name="a")
    node_b = MockNode(name="b", parents=node_a)
    node_c = MockNode(name="c", parents=node_a)
    node_d = MockNode(name="d", parents=[node_b, node_c])

    node_collection = [node_a, node_b, node_c, node_d]
    random.shuffle(node_collection)

    orchestrator = Orchestrator()
    for node in node_collection:
        orchestrator.add_node(node)

    state = MockState()
    orchestrator.run_sync(state=state)
    flow_dict = {name: i for i, name in enumerate(state.flow)}

    assert flow_dict["a"] < flow_dict["b"]
    assert flow_dict["a"] < flow_dict["c"]
    assert flow_dict["a"] < flow_dict["d"]
    assert flow_dict["b"] < flow_dict["d"]
    assert flow_dict["c"] < flow_dict["d"]


def test_orchestrator_fail():
    @dataclass
    class MockState:
        flow: tuple = tuple([])

    class MockNode(Node):
        def __init__(self, name, parents=None, fail=False):
            self.fail = fail
            super().__init__(name, parents)

        async def execute(self, ctx: OrchestratorContext) -> NodeState:
            ctx.state.flow = tuple([*ctx.state.flow, self.name])
            if self.fail:
                return ErrorState("Triggered Error")
            return SuccessState()

    node_a = MockNode(name="a")
    node_b = MockNode(name="b", parents=node_a)
    node_c = MockNode(name="c", parents=node_a, fail=True)
    node_d = MockNode(name="d", parents=[node_b, node_c])

    node_collection = [node_a, node_b, node_c, node_d]
    random.shuffle(node_collection)

    orchestrator = Orchestrator()
    for node in node_collection:
        orchestrator.add_node(node)

    state = MockState()
    orchestrator.run_sync(state=state)
    flow_dict = {name: i for i, name in enumerate(state.flow)}

    assert flow_dict["a"] < flow_dict["b"]
    assert flow_dict["a"] < flow_dict["c"]

    assert "d" not in flow_dict
