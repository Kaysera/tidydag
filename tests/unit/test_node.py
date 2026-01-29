from tidydag import Node


def test_node_has_parents():
    class MockNode(Node):
        async def execute(self, ctx):
            pass

    node_a = MockNode(name="node1")
    node_b = MockNode(name="node2", parents=node_a)

    assert node_a.parents is tuple([])
    assert node_a in node_b.parents
    assert node_a.id == node_b.parents[0].id
