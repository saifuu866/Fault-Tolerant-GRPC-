# Team Contributions & Role Distribution

**Project Title:** Fault-Tolerant Distributed RPC Framework  
**Course:** Operating Systems  
**Team Size:** 4 Members  

---

## 1. Executive Summary Matrix

| Member | Name & Roll No. | Primary Role | Core Files Owned | Workload Share |
| :--- | :--- | :--- | :--- | :---: |
| **Member 1** | `[Member 1 Name]`<br>`[Roll No.]` | **gRPC Protocol & Backend Server Architecture** | `proto/service.proto`<br>`generate_protos.py`, `proto_stubs.py`<br>`servers/server_config.py`<br>`servers/server.py`, `run.py` | **25%** |
| **Member 2** | `[Member 2 Name]`<br>`[Roll No.]` | **Cluster Registry, Health Monitor & Leader Election** | `coordinator/cluster.py`<br>`coordinator/health_monitor.py`<br>`coordinator/leader_election.py`<br>`coordinator/admin_client.py` | **25%** |
| **Member 3** | `[Member 3 Name]`<br>`[Roll No.]` | **Load Balancer, Retry Engine & Failover Logic** | `coordinator/load_balancer.py`<br>`coordinator/failover_manager.py`<br>`client/rpc_client.py` | **25%** |
| **Member 4** | `[Member 4 Name]`<br>`[Roll No.]` | **Web Monitoring Dashboard, Chaos Controls & Unit Testing** | `dashboard/app.py`<br>`dashboard/templates/index.html`<br>`dashboard/static/style.css & script.js`<br>`tests/test_fault_tolerance.py`, `README.md` | **25%** |

---

## 2. Detailed Member Breakdown

---

### Member 1: `[Member 1 Name]` (`[Roll No.]`)
**Role:** *gRPC Protocol Designer & Multi-Server Backend Developer*

#### Files Owned
* `proto/service.proto`
* `generate_protos.py` & `proto_stubs.py`
* `servers/server_config.py`
* `servers/server.py`
* `run.py`

#### Key Technical Contributions
1. **Protocol Buffers Contract Design:** Designed `service.proto` defining the three core RPC methods (`Execute`, `HealthCheck`, and `Admin`) and strongly typed request/response schemas.
2. **Automated Stub Generation:** Built `generate_protos.py` and `proto_stubs.py` to compile `.proto` definitions into Python gRPC classes cleanly across Windows/Linux environments.
3. **Multithreaded Backend Server Implementation:** Developed `ComputeServicer` inside `servers/server.py` using `concurrent.futures.ThreadPoolExecutor` (8 worker threads per server) running on isolated local ports (`50051`, `50052`, `50053`).
4. **Workload & Fault Injection Operations:** Implemented server business operations (`echo`, `sum`, `square`, `slow`) alongside test fault modes (`flaky` for transient `ABORTED` glitches and `boom` for permanent `INVALID_ARGUMENT` errors).
5. **Process Orchestration & Crash Simulation:** Implemented the out-of-band `Admin` RPC (`KILL`, `REVIVE`, `SET_LEADER`) so crashes can be simulated without terminating the OS process, plus `run.py` to spawn and gracefully terminate all server subprocesses with one command.

#### OS Concepts Handled
* **Inter-Process Communication (IPC)** via network sockets & RPC marshalling.
* **Process Isolation & Multithreading** using bounded worker thread pools and thread locks (`threading.Lock`).

#### Viva Questions Prepared for Member 1
* *How can a "crashed" server be revived from the dashboard if it is down?*  
  **Answer:** When `Admin(KILL)` is called, the server sets `self.alive = False` and rejects `Execute` and `HealthCheck` with `grpc.StatusCode.UNAVAILABLE` (behaving identically to a dead node), while still keeping the `Admin` control RPC open for `REVIVE`. You can also do a real crash via `Ctrl+C` in its terminal!
* *What does `ThreadPoolExecutor(max_workers=8)` do in `server.py`?*  
  **Answer:** It allows a single backend server process to handle up to 8 concurrent client RPC requests simultaneously without blocking incoming health probes.

---

### Member 2: `[Member 2 Name]` (`[Roll No.]`)
**Role:** *Cluster State Management, Health Monitoring & Leader Election Developer*

#### Files Owned
* `coordinator/cluster.py`
* `coordinator/health_monitor.py`
* `coordinator/leader_election.py`
* `coordinator/admin_client.py`

#### Key Technical Contributions
1. **Thread-Safe Shared Cluster Registry:** Built `ServerState` and `ServerRegistry` (`cluster.py`) protected by a reentrant mutex (`threading.RLock`) to safely store server health states, per-server counters, global retry/failover totals, and a rolling 120-entry system event log.
2. **Background Heartbeat Daemon:** Implemented `HealthMonitor` (`health_monitor.py`) as a background daemon thread (`daemon=True`) that probes every server every `2.0` seconds via `HealthCheck` RPCs and measures round-trip latency in milliseconds.
3. **Probationary Recovery State Machine:** Designed the 3-stage health transition (`ONLINE` → `OFFLINE` on 1 failed check; `OFFLINE` → `RECOVERING` on 1st good check; `RECOVERING` → `ONLINE` after 2 consecutive good checks) so flapping servers don't prematurely receive live traffic.
4. **Coordinator-Managed Leader Election:** Implemented `LeaderElector` (`leader_election.py`) and `admin_client.py` to automatically elect the first healthy server as leader, detect leader crashes immediately after health sweeps, clear old leadership, and promote the next `ONLINE` replica.

#### OS Concepts Handled
* **Mutual Exclusion & Critical Sections** (`RLock` preventing race conditions across client, Flask, and monitor threads).
* **Background Daemon Threads & Heartbeat Failure Detection**.
* **Distributed Coordination** via automated leader failover.

#### Viva Questions Prepared for Member 2
* *Why did you use `threading.RLock()` instead of a normal `Lock()` in `ServerRegistry`?*  
  **Answer:** `RLock` (Reentrant Lock) allows the same thread to acquire the lock multiple times safely—for example, when `set_status()` acquires the lock and internally calls `log_event()`, which also acquires the same lock. A normal `Lock` would deadlock there!
* *Why does a revived server enter `RECOVERING` before `ONLINE`?*  
  **Answer:** Just like real load balancers (e.g., NGINX/AWS ELB), requiring `RECOVERY_THRESHOLD = 2` consecutive healthy heartbeats acts as a probation period to confirm stability before routing real client traffic back to it.

---

### Member 3: `[Member 3 Name]` (`[Roll No.]`)
**Role:** *Load Balancing, Automatic Retry & Failover Engine Developer*

#### Files Owned
* `coordinator/load_balancer.py`
* `coordinator/failover_manager.py`
* `client/rpc_client.py`

#### Key Technical Contributions
1. **Health-Aware Round-Robin Algorithm:** Developed `RoundRobinLoadBalancer` (`load_balancer.py`) using a thread-safe rotating pointer that walks the server ring, skipping `OFFLINE`/`RECOVERING` servers and servers already excluded in the current request attempt.
2. **Smart Error Classification Engine:** Built the fault classification logic in `rpc_client.py` that inspects gRPC status codes:
   * **Connection/Crash Errors (`UNAVAILABLE`, `DEADLINE_EXCEEDED`):** Triggers immediate failover and retries on another healthy server.
   * **Transient Errors (`ABORTED`, `RESOURCE_EXHAUSTED`, `INTERNAL`):** Retries with backoff while keeping the server `ONLINE`.
   * **Permanent Application Errors (`INVALID_ARGUMENT`, etc.):** Aborts immediately on Attempt 1 without wasting retries.
3. **Linear Backoff & Attempt Tracing:** Implemented the retry loop (`MAX_ATTEMPTS = 3`, backoff `0.4s × (attempt - 1)`) and recorded granular `Attempt` and `RpcResult` trace objects with per-attempt latency.
4. **Active Failover Integration & CLI Client:** Built `FailoverManager` (`failover_manager.py`) to immediately evict crashed servers mid-request without waiting for the next background health sweep, plus a standalone terminal client (`rpc_client.py` CLI).

#### OS Concepts Handled
* **Fair Scheduling Algorithms** (Round-Robin scheduling preventing server starvation).
* **Bounded Waiting & Deadlock Prevention** via per-RPC timeouts (`RPC_TIMEOUT = 3.0s`).
* **Fault Masking & Graceful Degradation**.

#### Viva Questions Prepared for Member 3
* *How does your retry logic avoid wasting time on bad user inputs?*  
  **Answer:** By classifying gRPC status codes. If a user passes letters to `sum` or calls `boom`, the server returns `INVALID_ARGUMENT`. Our client recognizes this as a `PERMANENT` error and breaks out of the retry loop after Attempt 1.
* *What happens if Server 1 crashes right after the Load Balancer selects it, before the 2-second Health Monitor notices?*  
  **Answer:** `RpcClient` catches `UNAVAILABLE` immediately, calls `failover.mark_failed("server-1")` to mark it `OFFLINE` right away, adds `"server-1"` to the `tried` exclusion set, and transparently completes the exact same user request on `"server-2"`.

---

### Member 4: `[Member 4 Name]` (`[Roll No.]`)
**Role:** *Full-Stack Monitoring Dashboard, Chaos Controls & Automated Testing Lead*

#### Files Owned
* `dashboard/app.py`
* `dashboard/templates/index.html`
* `dashboard/static/style.css`
* `dashboard/static/script.js`
* `tests/test_fault_tolerance.py`
* `README.md`

#### Key Technical Contributions
1. **Flask REST API & Coordinator Integration:** Built `dashboard/app.py` hosting live API endpoints (`/api/status`, `/api/request`, `/api/servers/<id>/<action>`, `/api/leader/kill`, `/api/reset`) wired directly to the live `ServerRegistry` and `RpcClient`.
2. **Real-Time Interactive UI:** Designed a clean, dark-themed dashboard (`index.html`, `style.css`, `script.js`) polling live cluster snapshots every `1.5s` to display KPI cards, server status badges (`ONLINE`, `OFFLINE`, `RECOVERING`), dynamic load-distribution bars, per-attempt retry traces, and color-coded system event logs.
3. **Interactive Chaos Engineering Controls:** Built one-click **Kill**, **Revive**, **Simulate Leader Failure**, **Force Re-election**, and **Batch Request Testing** buttons so all 6 fault-tolerance scenarios can be demonstrated live in class.
4. **Comprehensive Unit Test Suite:** Authored **17 automated unit tests** in `tests/test_fault_tolerance.py` using custom mock gRPC stubs (`FakeStub` & `FakeRpcError`) to verify round-robin rotation, retry limits, permanent error handling, failover routing, recovery transitions, and leader election without requiring live network sockets.

#### OS Concepts Handled
* **System Observability & Telemetry** (real-time inspection of process health, latency, and throughput).
* **Deterministic Fault Injection & Verification** via isolated mock testing.

#### Viva Questions Prepared for Member 4
* *Is your dashboard showing static/mock numbers or real system state?*  
  **Answer:** 100% live system state. Every 1.5 seconds `script.js` fetches `/api/status`, which reads directly from the thread-safe `ServerRegistry` updated in real time by gRPC calls and the background `HealthMonitor` thread.
* *How do your 17 unit tests run without starting the 3 backend servers?*  
  **Answer:** We used dependency injection (`stub_factory`) in `RpcClient` and `HealthMonitor`. During unit tests, we pass a `FakeStub` that simulates exact gRPC responses and `RpcError` codes deterministically in under 0.2 seconds.

---

## 3. How All 4 Modules Connect Together

```
 [Member 4: Flask UI & Chaos Controls]
                 │
                 ▼  (HTTP JSON API)
 [Member 3: RpcClient + LoadBalancer + FailoverManager]
                 │
                 ├──► reads/updates ──► [Member 2: ServerRegistry + HealthMonitor + LeaderElector]
                 │
                 ▼  (gRPC / HTTP2 Binary Protobuf)
 [Member 1: Backend Server 1 (:50051) | Server 2 (:50052) | Server 3 (:50053)]
```

### Collaborative Workflow Used by the Team
1. **Phase 1 — Contract First:** All 4 members agreed on `proto/service.proto` and `ServerRegistry.snapshot()` dictionary keys on Day 1 so everyone could code in parallel without blocking each other.
2. **Phase 2 — Parallel Development:**
   * *Member 1* built and tested standalone gRPC servers.
   * *Member 2* built the thread-safe registry, health state machine, and leader election.
   * *Member 3* built round-robin routing, retry backoff, and failover logic.
   * *Member 4* built the Flask dashboard UI and unit test suite against mock stubs.
3. **Phase 3 — Integration & Scenario Verification:** Combined all components via `run.py` and verified all 6 required fault-tolerance scenarios and all 17 unit tests.