<p align="center">
    <img src="https://avatars.githubusercontent.com/u/35530357?s=400&u=1e6ac5572e3786df64910b23007330e71a796d35&v=4" align="center" width="30%">
</p>
<p align="center"><h1 align="center">TidyDAG</h1></p>
<p align="center">
    <em>Lightweight, asynchronous Directed Acyclic Graph (DAG) orchestrator for Python.</em>
</p>

<p align="center">
    <img src="https://img.shields.io/badge/Python-3776AB.svg?style=default&logo=Python&logoColor=white" alt="Python">
    <img src="https://img.shields.io/badge/Pytest-0A9EDC.svg?style=default&logo=Pytest&logoColor=white" alt="Pytest">
    <img src="https://img.shields.io/badge/Ruff-D37D44?style=default&logo=ruff&logoColor=white" alt="Ruff">
    <img src="https://img.shields.io/badge/Taskfile-4d2a85?style=default" alt="Taskfile">
    <img src="https://img.shields.io/badge/AWS%20CodeBuild-34495E?style=default&logo=aws-codebuild&logoColor=white" alt="AWS CodeBuild">
    <img src="./docs/coverage.svg" alt="coverage">
</p>
<br>

TidyDAG allows you to define complex task dependencies and execute them concurrently while maintaining a shared state. Built on `asyncio`, it provides a simple API to resolve execution order automatically via topological sorting.

---

## 📦 Features

*   ⚡ **Asynchronous Execution:** Built on `asyncio` for non-blocking task orchestration.
*   🔗 **Topological Sorting:** Automatically resolves execution order based on dependencies.
*   🧠 **Shared Context:** Pass and mutate state across your graph nodes seamlessly.
*   🛡️ **Type Safety:** Built with modern Python 3.13+ features and comprehensive type hints.
*   🧩 **Simple API:** Define nodes and dependencies with minimal boilerplate.
*   🤖 **Task Automation:** Managed with `uv`, `ruff`, and `Taskfile.yml`.

---

## 📁 Project Layout

```text
.
├── src/                          # Core library source code
│   └── tidydag/
│       ├── node/                 # Base Node and state definitions
│       └── orchestrator/         # DAG execution engine
├── test/                         # Pytest-based test suite
│   └── unit/
├── Taskfile.yml                  # Task automation
├── pyproject.toml                # Python project config
└── README.md
```

---

## 🔧 Setup

1.  **Install dependencies** (requires Python ≥ 3.13 and `uv`):

    ```bash
    task deps:install
    ```

---

## 💡 Core Concepts

### 1. Node
The basic building block of a DAG. Every task must inherit from the `Node` class and implement the `execute` method.

### 2. Orchestrator
The engine that manages the execution of the graph. It ensures nodes are executed only after their parents have successfully completed.

### 3. OrchestratorContext
A container for shared state that is passed to every node during execution.

---

## 🚀 Quick Start

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

---

## 🧪 Testing

Run all tests with coverage:

```bash
task test:run
```

To generate a coverage badge (`docs/coverage.svg`), run:
```bash
task test:run BADGE=true
```

---

## ✨ Automation (Taskfile)

Common commands available via `task`:

```bash
task                    # Show available tasks
task deps:install       # Create venv and sync Python dependencies
task test:run           # Run all tests with pytest
task lint:check         # Run linters and formatters in check mode
task lint:format        # Format code with ruff
```

---

## 📄 License

Proprietary — ©Taidy. All rights reserved.

This project is intended for internal use only and may not be copied, distributed, or modified outside of authorized teams without written permission from Taidy.
