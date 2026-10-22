# System Architecture Specification

## 1. Architectural Philosophy and Design Goals

The Fault-Tolerant Distributed RPC Framework is built around the core distributed systems principle of active replication with client-coordinated fault tolerance.

### Key Goals
1. **Zero Downtime on Node Crash**: The failure of any single replica process must not cause client requests to drop; requests must seamlessly reroute to surviving replicas.
2. **Deterministic Load Distribution**: Request traffic must be partitioned equitably across healthy nodes without starving any node.
3. **Flap and Oscillation Prevention**: Nodes returning from failure must not receive immediate production traffic until their recovery is sustained and validated.
4. **Transparent Observability**: The internal state of every replica, channel latency, metric counters, and routing decisions must be visible in real time.

---

## 2. Three-Tier System Topology

The architecture separates responsibilities into three distinct tiers:

```
+-------------------------------------------------------------------------+
|                    TIER 1: PRESENTATION & CONTROL                       |
|                                                                         |
|  [Browser Dashboard Client] <---> [Flask Web Application (:5000)]       |
|    - DOM status rendering           - REST API Controller               |
|    - 1500ms auto-poll loop          - In-process coordinator host       |
+------------------------------------+------------------------------------+
                                     | Direct memory reference
+------------------------------------v------------------------------------+
|                    TIER 2: COORDINATION & CLIENT                        |
|                                                                         |
|  [ServerRegistry]              [RoundRobinLoadBalancer]                 |
|    - Synchronized via RLock       - Circular list traversal             |
|    - Status & metrics storage     - Exclusion set filtering             |
|                                                                         |
|  [HealthMonitor]               [LeaderElector]                          |
|    - 2.0s daemon sweep thread     - Deterministic first-online rule     |
|    - Two-stage state machine      - Out-of-band Admin RPC dispatch      |
|                                                                         |
|  [FailoverManager]             [RpcClient]                              |
|    - Node eviction dispatch       - Linear backoff retry engine         |
|    - Failover telemetry           - Error classification engine         |
+------------------------------------+------------------------------------+
                                     | Network I/O (gRPC over HTTP/2)
+------------------------------------v------------------------------------+
|                    TIER 3: BACKEND REPLICA CLUSTER                      |
|                                                                         |
|  +--------------------+  +--------------------+  +--------------------+ |
|  |  server-1 (:50051) |  |  server-2 (:50052) |  |  server-3 (:50053) | |
|  |  ThreadPool(8)     |  |  ThreadPool(8)     |  |  ThreadPool(8)     | |
|  |  ComputeServicer   |  |  ComputeServicer   |  |  ComputeServicer   | |
|  +--------------------+  +--------------------+  +--------------------+ |
+-------------------------------------------------------------------------+
```

---

## 3. Component Deep Dive

### 3.1 Protocol Buffers & Stub Resolution Layer
- **`proto/service.proto`**: Declares the `ComputeService` interface with three RPC definitions:
  - `Execute(ComputeRequest)`: Primary application payload processing.
  - `HealthCheck(HealthRequest)`: Periodic liveness verification by the monitoring daemon.
  - `Admin(AdminRequest)`: Control-plane channel for chaos injection and leadership signaling.
- **`proto_stubs.py`**: Solves Python import namespace conflicts. When `protoc` compiles proto files, generated modules use flat imports (`import service_pb2`). `proto_stubs.py` automatically injects the repository root and `generated/` directory into `sys.path` and exposes clean exports.

### 3.2 Backend Server Replicas (`servers/server.py`)
Each backend server runs as an independent OS process on an assigned port (`50051`, `50052`, `50053`).
- **Concurrency**: Backed by a `concurrent.futures.ThreadPoolExecutor(max_workers=8)`.
- **`ComputeServicer`**:
  - `self.alive`: Boolean flag toggled by `Admin(KILL)` or `Admin(REVIVE)`. When `False`, the server rejects `Execute` and `HealthCheck` with `grpc.StatusCode.UNAVAILABLE`, emulating a process crash without terminating the operating system process.
  - `self.fail_rate`: Floating point probability (0.0 to 1.0) of injecting transient `grpc.StatusCode.ABORTED` responses.
  - `self._flaky_counters`: Dictionary tracking attempts per `request_id` to simulate intermittent failures that recover after a configured threshold.
  - `Admin` RPC bypass: The `Admin` RPC deliberately checks no `alive` flag, allowing out-of-band revival commands even when the server simulates a crash state.

### 3.3 Cluster State & Server Registry (`coordinator/cluster.py`)
Acts as the centralized, thread-safe memory model for the cluster.
- **`ServerState`**: Encapsulates state for one node:
  - Node identity and address (`host:port`).
  - Status lifecycle (`ONLINE`, `OFFLINE`, `RECOVERING`, `UNKNOWN`).
  - Request counters (total attempts, successes, failures).
  - Last observed probe latency in milliseconds.
  - Leadership flag (`is_leader`).
  - Consecutive probe counters (`consecutive_ok`, `consecutive_fail`).
- **`ServerRegistry`**:
  - Internal storage uses an `OrderedDict` to preserve determinism.
  - Concurrency is protected by a re-entrant lock (`threading.RLock`).
  - Maintains a circular event log using `collections.deque(maxlen=120)`.

### 3.4 Round-Robin Load Balancer (`coordinator/load_balancer.py`)
- Employs a circular pointer walk over configured server keys.
- **Algorithm**:
  1. Identifies total server count $N$.
  2. Iterates offset $i \in [0, N-1]$, checking index $(\text{pointer} + i) \pmod N$.
  3. Evaluates if the candidate is `ONLINE` and not present in the caller-supplied `exclude` set.
  4. Upon finding a valid server, updates $\text{pointer} \leftarrow (\text{index} + 1) \pmod N$ and returns the node.
  5. Returns `None` if all nodes are either offline or excluded.
- This design allows skipping offline or failing nodes while maintaining proportional scheduling for survivors.

### 3.5 Failover Manager (`coordinator/failover_manager.py`)
- Coordinates the interface between the client and load balancer during failures.
- When `RpcClient` detects a connection failure (`UNAVAILABLE` or `DEADLINE_EXCEEDED`):
  - Calls `mark_failed(server_id, error)`.
  - Sets the server status immediately to `OFFLINE`.
  - Increments the cluster failover counter.
  - Appends an error event to the audit log.

### 3.6 Health Monitor Daemon (`coordinator/health_monitor.py`)
- Runs as a background Python daemon thread (`threading.Thread(daemon=True)`).
- Executes `check_all()` every `HEALTH_INTERVAL` (2.0s).
- **Channel Optimization**: Uses a stub factory that caches one `grpc.Channel` per address, eliminating repeated socket teardowns.
- **Two-Stage Recovery Machine**:
  - `OFFLINE` -> 1 good probe -> `RECOVERING` (excluded from client routing).
  - `RECOVERING` -> 1 additional good probe -> `ONLINE` (restored to active routing).
  - `ONLINE` -> 1 probe failure -> `OFFLINE` (immediately evicted).
- After each sweep, triggers `LeaderElector.ensure_leader()`.

### 3.7 Coordinator-Managed Leader Election (`coordinator/leader_election.py`)
- Implements a deterministic coordinator rule: **The active leader is always the first `ONLINE` server in configured order**.
- When the leader node transitions to `OFFLINE`:
  - `ensure_leader()` detects the status departure.
  - Clears leadership on the old node (best-effort `Admin(CLEAR_LEADER)`).
  - Promotes the first candidate in `registry.available()`.
  - Notifies the new leader via `Admin(SET_LEADER)` RPC.
- **Distinction from Consensus Protocols**: This is a centralized demonstration coordinator, not Raft or Paxos. It has no quorum requirement or split-brain protection.

### 3.8 Fault-Tolerant RPC Client (`client/rpc_client.py`)
- Encapsulates execution, error handling, linear backoff, and attempt logging.
- Manages an internal channel pool (`_channels`).
- Returns a rich `RpcResult` object containing overall outcome, server ID, payload, latency, and individual `Attempt` details.

### 3.9 Web Observability Dashboard (`dashboard/app.py`)
- Built on Flask, hosting the coordinator kernel in-process.
- Exposes REST endpoints (`/api/status`, `/api/request`, `/api/servers/<id>/<action>`, `/api/reset`).
- Static web client polls `/api/status` every 1500ms to update metrics cards, distribution charts, server status badges, and event logs.

---

## 4. Concurrency, Locking, and Synchronization Architecture

```
Thread Model:
+-------------------------------------------------------------+
| Main Web Process (Flask Worker)                             |
|  - Request handling threads (ThreadPool)                    |
|  - REST queries to ServerRegistry                           |
+-------------------------------------------------------------+
                              |
                     Contends for Lock
                              v
+-------------------------------------------------------------+
| Shared Memory: ServerRegistry._lock (threading.RLock)       |
|  Guards:                                                    |
|  - _servers dictionary                                      |
|  - totals counters (requests, success, failed, retries...)  |
|  - circular events deque                                    |
|  - leader_id pointer                                        |
+-------------------------------------------------------------+
                              ^
                     Contends for Lock
                              |
+-------------------------------------------------------------+
| HealthMonitor Thread (daemon=True)                          |
|  - Periodic sweep every 2.0s                                |
|  - Writes state updates and logs probe events               |
+-------------------------------------------------------------+
```

1. **Re-entrant Locking**: `ServerRegistry` uses `threading.RLock` to allow nested calls within the same execution thread (for example, logging an event from inside a status-update method).
2. **Channel Re-use**: gRPC channels are thread-safe and maintain internal connection pooling over HTTP/2 multiplexing. Both the client and health monitor cache channels rather than re-establishing TCP connections per RPC.
3. **Thread Safety in Backend Servicer**: `ComputeServicer` wraps mutable variables (`alive`, `fail_rate`, `handled`, `_flaky_counters`) in a dedicated `threading.Lock`.
