# System-Wide Architecture & Member 3 Conceptual Deep Dive

---

## 1. How the Entire Application Works (High-Level Overview)

The project is an active-active, fault-tolerant distributed RPC platform. It consists of two primary tiers running locally:

```
                      ┌────────────────────────────────────────────────────────┐
                      │              FLASK DASHBOARD (Port 5000)               │
                      │  - Auto-polls /api/status every 1.5s                   │
                      │  - Dispatches RPC requests & chaos actions (kill/revive)│
                      └───────────────────────────┬────────────────────────────┘
                                                  │
                                                  ▼
                      ┌────────────────────────────────────────────────────────┐
                      │          IN-PROCESS COORDINATOR & CLIENT LAYER         │
                      │                                                        │
                      │  [Member 2] ServerRegistry: Shared cluster state       │
                      │  [Member 2] HealthMonitor: Proactive daemon (2.0s)     │
                      │  [Member 2] LeaderElector: Promotes healthy leader     │
                      │                                                        │
                      │  ────────────────────────────────────────────────────  │
                      │  [MEMBER 3] RpcClient: Drives retry loop & backoff     │
                      │  [MEMBER 3] LoadBalancer: Round-Robin ring pointer     │
                      │  [MEMBER 3] FailoverManager: Reactive instant eviction │
                      └───────────────────────────┬────────────────────────────┘
                                                  │
                                                  ▼ (gRPC Protobuf over HTTP/2)
            ┌─────────────────────────────────────┼─────────────────────────────────────┐
            │                                     │                                     │
            ▼                                     ▼                                     ▼
┌───────────────────────┐             ┌───────────────────────┐             ┌───────────────────────┐
│       SERVER-1        │             │       SERVER-2        │             │       SERVER-3        │
│   Port: 50051 (Leader)│             │      Port: 50052      │             │      Port: 50053      │
│  [Member 1 Backend]   │             │  [Member 1 Backend]   │             │  [Member 1 Backend]   │
└───────────────────────┘             └───────────────────────┘             └───────────────────────┘
```

### The System Lifecycle in 4 Steps
1. **Cluster Startup (`run.py`)**: Spawns 3 isolated backend server processes (`50051`, `50052`, `50053`), each with an 8-thread worker pool.
2. **State & Health Tracking (Member 2)**: The coordinator maintains an in-memory `ServerRegistry`. A background daemon thread sends a `HealthCheck` RPC every 2.0s to measure latency and maintain a 3-stage state machine (`ONLINE` -> `OFFLINE` -> `RECOVERING` -> `ONLINE`).
3. **Execution & Resilience (Member 3)**: When a request arrives, Member 3 selects the next available node, executes the call, handles retries/failover if glitches occur, and records an attempt audit trail.
4. **Live Visualization (Member 4)**: The browser dashboard reflects live metrics, latency, distribution charts, and per-attempt traces without page reloads.

---

## 2. Concepts Used Exclusively by Member 3

Member 3 owns the logic between when a request is submitted and when the final result is returned. Here are the 7 core concepts implemented by Member 3:

---

### Concept 1: Round-Robin Fair Scheduling with a Ring Pointer
* **Files:** `coordinator/load_balancer.py`
* **The Problem:** If all requests hit `server-1`, it gets overloaded while `server-2` and `server-3` sit idle (node starvation).
* **The Concept:** Standard CPU Round-Robin scheduling adapted for distributed networks.
* **How It Works:**
  - Maintains an internal index `self._pointer` pointing to the last chosen slot in the server ring.
  - Formula: `index = (self._pointer + offset) % count`.
  - Walks the list starting from the pointer. As soon as it encounters a server that is `ONLINE` and not excluded, it returns that server and advances the pointer to `(index + 1) % count`.
  - Walking with an offset instead of just picking `pointer % count` allows the load balancer to cleanly skip crashed or recovering servers without breaking the rotation.
* **Thread Safety:** Protected by a `threading.Lock()` mutex so concurrent requests from multiple threads never corrupt the pointer.

---

### Concept 2: Dynamic Exclusion Routing (`exclude=tried`)
* **Files:** `coordinator/load_balancer.py`, `client/rpc_client.py`
* **The Problem:** If `server-1` fails during Attempt 1, a naive load balancer might pick `server-1` again on Attempt 2 if it merely rotates or sees it as next in line.
* **The Concept:** State-aware retry filtering.
* **How It Works:**
  - `rpc_client.py` initializes an empty set: `tried = set()`.
  - Before each attempt, the selected server ID is added to `tried`.
  - When requesting a retry server, the client calls `failover.pick(exclude=tried)`.
  - The load balancer skips any node present in the exclusion set, guaranteeing that Attempt 2 and Attempt 3 always target different physical nodes.

---

### Concept 3: Fast-Path Reactive Failover vs Proactive Health Probing
* **Files:** `coordinator/failover_manager.py`
* **The Problem:** The background `HealthMonitor` only polls every 2.0 seconds. If `server-1` crashes at $t = 0.1\text{s}$, a 1.9-second "blind window" exists where other requests might still try to contact `server-1`.
* **The Concept:** Two-tier failure detection (Proactive vs Reactive).
* **How It Works:**
  - **Proactive (Member 2):** Periodic heartbeats detect quiet recoveries and steady-state health every 2 seconds.
  - **Reactive (Member 3):** The moment `RpcClient` encounters a socket connection failure (`UNAVAILABLE`), it immediately calls `failover.mark_failed(server_id)`.
  - In under 1 millisecond, the registry marks the server `OFFLINE` and records a failover event, protecting all other in-flight requests from hitting the dead node.

---

### Concept 4: Three-Way Error Classification Engine
* **Files:** `client/rpc_client.py` (`CONNECTION_ERRORS`, `TRANSIENT_ERRORS`)
* **The Problem:** Not all errors are the same. Blindly retrying every error causes server crashes; giving up on every error makes the system fragile.
* **The Concept:** Semantic error differentiation based on gRPC status codes:
  1. **Connection Errors (`UNAVAILABLE`, `DEADLINE_EXCEEDED`)**:
     - *Meaning:* Physical crash, network drop, or server process terminated.
     - *Action:* Evict node to `OFFLINE` immediately; retry request on another node.
  2. **Transient Errors (`ABORTED`, `RESOURCE_EXHAUSTED`, `INTERNAL`)**:
     - *Meaning:* Temporary glitch or lock contention (e.g. `flaky`). The server hardware is fine.
     - *Action:* Keep server `ONLINE`; pause with backoff and retry.
  3. **Permanent Errors (`INVALID_ARGUMENT`, `NOT_FOUND`)**:
     - *Meaning:* Invalid user input or illegal operation (e.g. `boom` or letters passed to sum).
     - *Action:* Abort immediately on Attempt 1. Retrying will never succeed.

---

### Concept 5: Linear Backoff with Congestion Avoidance
* **Files:** `client/rpc_client.py`
* **The Problem:** When an overloaded server returns a transient error, retrying instantly creates a "Thundering Herd" that overwhelms the backend.
* **The Concept:** Grace period backoff delay before retrying.
* **How It Works:**
  - Formula: `delay = RETRY_DELAY * (attempt - 1)` with `RETRY_DELAY = 0.4s`.
  - Attempt 1: $0.4 \times (1 - 1) = 0.0\text{s}$ (instant).
  - Attempt 2: $0.4 \times (2 - 1) = 0.4\text{s}$ sleep.
  - Attempt 3: $0.4 \times (3 - 1) = 0.8\text{s}$ sleep.
  - Total maximum retry budget: 3 attempts.

---

### Concept 6: Bounded Waiting & Deadlock Avoidance
* **Files:** `client/rpc_client.py`, `servers/server_config.py`
* **The Problem:** If a backend server freezes or hangs in an infinite loop, the client thread could block forever.
* **The Concept:** Hard client-side execution deadlines.
* **How It Works:** Every gRPC call specifies `timeout=RPC_TIMEOUT` (`3.0s`). If the server does not reply within 3 seconds, gRPC throws `DEADLINE_EXCEEDED`, which Member 3's classifier catches, marks the node failed, and reroutes.

---

### Concept 7: Request Idempotency & Per-Attempt Audit Telemetry
* **Files:** `client/rpc_client.py` (`Attempt`, `RpcResult`)
* **The Problem:** In distributed systems, if a client retries, the server might execute the request twice. Also, operators need to see exactly why an RPC took multiple tries.
* **The Concept:**
  - **Idempotency Token:** Generates an immutable 8-character hex `request_id` passed in `ComputeRequest` protobuf across all retries.
  - **Attempt Audit Trail:** Every try records an `Attempt` object (server target, duration ms, outcome `SUCCESS|RETRY|FAILED`, gRPC status code, message). These are packaged into an `RpcResult` for live dashboard rendering.
