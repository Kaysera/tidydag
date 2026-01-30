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

    result = orchestrator.run_sync()
    assert result.success
    assert result.last_node.name == "d"
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

    class MockNode(Node[MockState]):
        async def execute(self, ctx: OrchestratorContext[MockState]) -> NodeState:
            ctx.state.counter += 1
            return SuccessState()

    node_a = MockNode(name="a")
    node_b = MockNode(name="b", parents=node_a)
    node_c = MockNode(name="c", parents=node_a)
    node_d = MockNode(name="d", parents=[node_b, node_c])

    node_collection: list[MockNode] = [node_a, node_b, node_c, node_d]
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

    class MockNode(Node[MockState]):
        async def execute(self, ctx: OrchestratorContext[MockState]) -> NodeState:
            ctx.state.flow = tuple([*ctx.state.flow, self.name])
            return SuccessState()

    node_a = MockNode(name="a")
    node_b = MockNode(name="b", parents=node_a)
    node_c = MockNode(name="c", parents=node_a)
    node_d = MockNode(name="d", parents=[node_b, node_c])

    node_collection: list[MockNode] = [node_a, node_b, node_c, node_d]
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

    class MockNode(Node[MockState]):
        def __init__(self, name, parents=None, fail=False):
            self.fail = fail
            super().__init__(name, parents)

        async def execute(self, ctx: OrchestratorContext[MockState]) -> NodeState:
            ctx.state.flow = tuple([*ctx.state.flow, self.name])
            if self.fail:
                return ErrorState("Triggered Error")
            return SuccessState()

    node_a = MockNode(name="a")
    node_b = MockNode(name="b", parents=node_a)
    node_c = MockNode(name="c", parents=node_a, fail=True)
    node_d = MockNode(name="d", parents=[node_b, node_c])

    node_collection: list[MockNode] = [node_a, node_b, node_c, node_d]
    random.shuffle(node_collection)

    orchestrator = Orchestrator()
    for node in node_collection:
        orchestrator.add_node(node)

    state = MockState()
    result = orchestrator.run_sync(state=state)
    assert not result.success
    assert result.last_node.name == "c"

    flow_dict = {name: i for i, name in enumerate(state.flow)}

    assert flow_dict["a"] < flow_dict["b"]
    assert flow_dict["a"] < flow_dict["c"]

    assert "d" not in flow_dict


def test_orchestrator_checkpoint():
    @dataclass
    class MockState:
        flow: tuple = tuple([])

    class MockNode(Node[MockState]):
        async def execute(self, ctx: OrchestratorContext[MockState]) -> NodeState:
            ctx.state.flow = tuple([*ctx.state.flow, self.name])
            return SuccessState()

    node_a = MockNode(name="a")
    node_b = MockNode(name="b", parents=node_a)
    node_c = MockNode(name="c", parents=node_a)
    node_d = MockNode(name="d", parents=[node_b, node_c])

    node_collection: list[MockNode] = [node_a, node_b, node_c, node_d]
    random.shuffle(node_collection)

    orchestrator = Orchestrator()
    for node in node_collection:
        orchestrator.add_node(node)

    state = MockState()
    orchestrator.run_sync(state=state)

    for node in node_collection:
        assert node.id in orchestrator.ctx.metadata.executed


def test_orchestrator_fail_checkpoint():
    @dataclass
    class MockState:
        flow: tuple = tuple([])

    class MockNode(Node[MockState]):
        def __init__(self, name, parents=None, fail=False):
            self.fail = fail
            super().__init__(name, parents)

        async def execute(self, ctx: OrchestratorContext[MockState]) -> NodeState:
            ctx.state.flow = tuple([*ctx.state.flow, self.name])
            if self.fail:
                return ErrorState("Triggered Error")
            return SuccessState()

    node_a = MockNode(name="a")
    node_b = MockNode(name="b", parents=node_a)
    node_c = MockNode(name="c", parents=node_a, fail=True)
    node_d = MockNode(name="d", parents=[node_b, node_c])

    node_collection: list[MockNode] = [node_a, node_b, node_c, node_d]
    random.shuffle(node_collection)

    orchestrator = Orchestrator()
    for node in node_collection:
        orchestrator.add_node(node)

    state = MockState()
    orchestrator.run_sync(state=state)

    assert node_a.id in orchestrator.ctx.metadata.executed
    assert node_c.id not in orchestrator.ctx.metadata.executed
    assert node_d.id not in orchestrator.ctx.metadata.executed


def test_orchestrator_resume_checkpoint():
    executions = []

    @dataclass
    class MockState:
        flow: tuple = tuple([])

    class MockNode(Node[MockState]):
        def __init__(self, name, parents=None, fail=False):
            self.fail = fail
            super().__init__(name, parents)

        async def execute(self, ctx: OrchestratorContext[MockState]) -> NodeState:
            if self.fail:
                return ErrorState("Triggered Error")
            ctx.state.flow = tuple([*ctx.state.flow, self.name])
            executions.append(self.id)
            return SuccessState()

    # Build an orchestrator that fails

    node_a = MockNode(name="a")
    node_b = MockNode(name="b", parents=node_a)
    node_c = MockNode(name="c", parents=node_a, fail=True)
    node_d = MockNode(name="d", parents=[node_b, node_c])

    node_collection: list[MockNode] = [node_a, node_b, node_c, node_d]
    random.shuffle(node_collection)

    orchestrator = Orchestrator()
    for node in node_collection:
        orchestrator.add_node(node)

    state = MockState()
    orchestrator.run_sync(state=state)

    # Assert that the process has failed
    assert node_a.id in orchestrator.ctx.metadata.executed
    assert node_c.id not in orchestrator.ctx.metadata.executed
    assert node_d.id not in orchestrator.ctx.metadata.executed
    # Assert that node_a has been executed and node_d has not
    assert 1 < len(executions) < 3

    ctx = orchestrator.ctx

    # Fix Node C and run a new orchestrator
    node_c.fail = False

    resume_orchestrator = Orchestrator(ctx)
    for node in node_collection:
        resume_orchestrator.add_node(node)

    state = MockState()
    resume_orchestrator.run_sync(state=state)

    # Assert that all nodes finish
    for node in node_collection:
        assert node.id in resume_orchestrator.ctx.metadata.executed

    # Check that all executions are there and none duplicated
    assert len(executions) == 4
