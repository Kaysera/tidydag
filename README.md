# TidyDAG

TidyDAG is a lightweight, asynchronous Directed Acyclic Graph (DAG) orchestrator for Python. It allows you to define complex task dependencies and execute them concurrently while maintaining a shared state.

## Features

- **Asynchronous Execution:** Built on `asyncio` for non-blocking task orchestration.
- **Topological Sorting:** Automatically resolves execution order based on dependencies.
- **Shared Context:** Pass and mutate state across your graph nodes seamlessly.
- **Type Safety:** Built with modern Python 3.13+ features and comprehensive type hints.
- **Simple API:** Define nodes and dependencies with minimal boilerplate.

## Installation

This project uses `uv` for dependency management.

```bash
# Install dependencies
uv sync
```

## Core Concepts

### 1. Node
The basic building block of a DAG. Every task must inherit from the `Node` class and implement the `execute` method.

### 2. Orchestrator
The engine that manages the execution of the graph. It ensures nodes are executed only after their parents have successfully completed.

### 3. OrchestratorContext
A container for shared state that is passed to every node during execution.

## Quick Start

### Defining Custom Nodes

```python
from dataclasses import dataclass
from tidydag.node.base import Node, NodeState, SuccessState, OrchestratorContext

class HelloNode(Node):
    async def execute(self, ctx: OrchestratorContext) -> NodeState:
        print(f"Hello from {self.name}!")
        return SuccessState()

class ProcessNode(Node):
    async def execute(self, ctx: OrchestratorContext) -> NodeState:
        ctx.state.count += 1
        return SuccessState()
```

### Running the DAG

```python
from tidydag.orchestrator import Orchestrator, OrchestratorContext

@dataclass
class HelloContext(OrchestratorContext):
    count: int = 0

# Initialize the orchestrator
orchestrator = Orchestrator()

# Define nodes and dependencies
node_a = HelloNode(name="A")
node_b = ProcessNode(name="B", parents=node_a)
node_c = HelloNode(name="C", parents=node_a)
node_d = HelloNode(name="D", parents=[node_b, node_c])

# Add nodes to the orchestrator
for node in [node_a, node_b, node_c, node_d]:
    orchestrator.add_node(node)

initial_state = HelloContext()
# Run the graph with an initial state
orchestrator.run_sync(state=initial_state)

print(f"Final state: {initial_state}")
```

## Project Structure

```text
.
├── AGENTS.md           # Development guide for AI agents
├── pyproject.toml      # Project configuration and dependencies
├── README.md           # Project documentation
├── src/
│   └── tidydag/        # Core library source code
│       ├── context/    # Shared context management
│       ├── node/       # Base Node and state definitions
│       └── orchestrator/ # DAG execution engine
├── Taskfile.yml        # Automation tasks
└── test/               # Test suite
    └── unit/           # Unit tests
```

## Development

### Running Tests
The project uses `pytest` and `pytest-asyncio` for testing.

```bash
# Run all tests
uv run pytest

# Run with verbose output
task test:run
```

### Linting and Formatting
We use `ruff` to maintain high code quality.

```bash
# Check for linting issues
uv run ruff check .

# Format the code
uv run ruff format .
```

## License

[Add License Information Here]
