# Operations, Testing, and Verification Guide

## 1. Environment Setup and Installation

### Prerequisites
- Python 3.10, 3.11, 3.12, or 3.13.
- Operating System: Windows, Linux, or macOS.

### Installation Steps
```bash
# 1. Clone repository and navigate to root directory
cd Fault-Tolerant-GRPC--main

# 2. Create isolated virtual environment
python -m venv venv

# 3. Activate virtual environment
# On Windows (PowerShell):
.\venv\Scripts\Activate.ps1
# On Windows (Command Prompt):
.\venv\Scripts\activate.bat
# On Linux / macOS:
source venv/bin/activate

# 4. Install dependencies
pip install -r requirements.txt

# 5. Compile Protocol Buffer schemas into Python stubs
python generate_protos.py
```

---

## 2. Cluster Execution Runbook

### Option A: Unified Orchestrator (Recommended)
Launches all three backend replicas and the web dashboard in a single console session:
```bash
python run.py
```
- Web Dashboard accessible at: `http://127.0.0.1:5000`
- Press `Ctrl+C` to cleanly terminate the dashboard and all child server processes.

#### Execution Flags:
- `python run.py --separate-windows`: (Windows only) Launches each backend server in its own independent console window. Ideal for live demonstrations allowing individual process termination via `Ctrl+C`.
- `python run.py --servers-only`: Starts only the three backend server replicas without the dashboard.

---

### Option B: Multi-Terminal Manual Execution
For fine-grained debugging, each component can be launched in a dedicated terminal:

```bash
# Terminal 1: Backend Replica 1
python servers/server.py --id server-1

# Terminal 2: Backend Replica 2
python servers/server.py --id server-2

# Terminal 3: Backend Replica 3
python servers/server.py --id server-3

# Terminal 4: Web Dashboard
python dashboard/app.py

# Terminal 5: Standalone Command Line Client
python client/rpc_client.py --operation sum --payload 5,10,15 --count 6
```

---

### Option C: Convenience Automation Scripts

Cross-platform helper scripts are provided in the `scripts/` directory:

| Task | Windows Batch | PowerShell |
|:---|:---|:---|
| **Compile Protos** | `scripts\compile_protos.bat` | `.\scripts\compile_protos.ps1` |
| **Run Full Cluster** | `scripts\run_cluster.bat` | `.\scripts\run_cluster.ps1` |
| **Run Demo Client** | `scripts\demo_client.bat` | `.\scripts\demo_client.ps1` |
| **Run Unit Tests** | `scripts\run_tests.bat` | `.\scripts\run_tests.ps1` |

---

## 3. Web Dashboard and Fault Injection Manual

The observability interface at `http://127.0.0.1:5000` provides interactive operational controls:

```
+-----------------------------------------------------------------------------+
| FAULT-TOLERANT RPC CLUSTER DASHBOARD                (3/3 online) uptime 1m  |
+-----------------------------------------------------------------------------+
| TOTAL REQUESTS | SUCCESSES | FAILURES | RETRIES | FAILOVERS | ACTIVE LEADER |
|      42        |    42     |    0     |    4    |     2     |   server-1    |
+-----------------------------------------------------------------------------+
| SERVERS                                                                     |
| Node ID   Address         Status   Reqs  OK  Fail  Latency  Actions         |
| server-1  127.0.0.1:50051 ONLINE   14    14   0    1.1 ms   [Kill] [Revive] |
| server-2  127.0.0.1:50052 ONLINE   14    14   0    0.9 ms   [Kill] [Revive] |
| server-3  127.0.0.1:50053 ONLINE   14    14   0    1.2 ms   [Kill] [Revive] |
+-----------------------------------------------------------------------------+
```

### Demonstration Scenarios:
1. **Live Failover Demonstration**:
   - Issue a batch of 10 requests via the web client or CLI.
   - Click `Kill` on `server-1`.
   - Observe `server-1` immediately transition to `OFFLINE`.
   - Send another batch of requests: Notice requests route strictly to `server-2` and `server-3` with zero failed transactions.
2. **Probationary Recovery Demonstration**:
   - Click `Revive` on `server-1`.
   - Observe `server-1` transition to `RECOVERING` (yellow badge).
   - After two successful health probe sweeps, observe `server-1` transition to `ONLINE` (green badge) and re-enter the load distribution pool.
3. **Leader Re-Election Demonstration**:
   - Note the active leader (e.g. `server-1`).
   - Click `Kill Leader`.
   - Observe leadership immediately transition to `server-2`.
   - Revive `server-1` and click `Force Election` to demonstrate deterministic lowest-order re-election.

---

## 4. Standalone CLI Client Usage

The client script (`client/rpc_client.py`) can be executed directly:

```bash
# Basic echo test
python client/rpc_client.py --operation echo --payload "hello world" --count 5

# Arithmetic sum test
python client/rpc_client.py --operation sum --payload "10,20,30,40" --count 3

# Arithmetic square test
python client/rpc_client.py --operation square --payload "25" --count 1

# Transient failure test (flaky operation)
python client/rpc_client.py --operation flaky --payload "2" --count 1

# Timeout test (slow operation exceeding 3.0s deadline)
python client/rpc_client.py --operation slow --payload "4.0" --count 1

# Permanent failure test
python client/rpc_client.py --operation boom --payload "" --count 1
```

---

## 5. Automated Unit Testing Architecture

The test suite in `tests/test_fault_tolerance.py` verifies all fault tolerance, load balancing, health monitoring, and election logic in memory without starting backend processes.

### Test Doubles Design
- **`FakeStub`**: Intercepts `Execute` and `HealthCheck` calls, invoking a test-defined behavior callable.
- **`make_factory`**: Returns mock stubs keyed by server address, enabling deterministic simulation of network partitions, latency, and status codes.

### Running Tests
```bash
# Direct unittest runner
python -m unittest discover -s tests -v

# Or via convenience scripts
scripts\run_tests.bat
```

### Coverage Scope:
- **`TestLoadBalancing`**: Proves round-robin fairness, skipping of offline nodes, quarantine of recovering nodes, and even request distribution.
- **`TestRetry`**: Proves successful linear backoff recovery, enforcement of retry budgets, non-retry of permanent errors, and status preservation on transient errors.
- **`TestFailover`**: Proves automatic redirection on crash, permanent exclusion from pool, and cluster failure behavior when all nodes crash.
- **`TestHealthMonitor`**: Proves detection of offline servers and step-by-step two-stage recovery.
- **`TestLeaderElection`**: Proves deterministic lowest-order promotion, leader crash handover, and total cluster failure handling.

---

## 6. Continuous Integration (CI) Automation

The repository includes GitHub Actions workflows under `.github/workflows/`:
1. **`ci.yml`**:
   - Test Matrix: Python 3.10, 3.11, 3.12, 3.13.
   - Operating Systems: Ubuntu and Windows.
   - Workflow: Installs dependencies, compiles protobufs, and runs the entire unit test suite.
2. **`code-quality.yml`**:
   - Executes flake8 linting to enforce PEP 8 standards.

---

## 7. Troubleshooting Runbook

| Symptom | Root Cause | Remediation |
|:---|:---|:---|
| `ImportError: gRPC stubs not found` | Protobufs not compiled | Run `python generate_protos.py`. |
| `cannot bind 127.0.0.1:50051: Address already in use` | Orphaned server process holding port | Terminate lingering Python processes or change port in `server_config.py`. |
| `Health monitor sweep error: UNAVAILABLE` | Backend servers not started | Ensure backend servers are launched before client or use `python run.py`. |
| Browser shows `Failed to fetch` on dashboard | Dashboard backend not running | Verify `python dashboard/app.py` is running and bound to `127.0.0.1:5000`. |
