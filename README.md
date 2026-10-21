
# Fault-Tolerant Distributed RPC Framework

An Operating-Systems course project that demonstrates how distributed services
communicate (gRPC + Protocol Buffers), how requests are distributed
(round-robin load balancing), how failures are survived (retry + failover +
health monitoring) and how a simple coordinator-managed **leader election**
works — all on a single laptop, no Docker, no cloud.



### 📚 Documentation & Reference Guides

* 🏛️ **[Architecture Specification (`ARCHITECTURE.md`)](ARCHITECTURE.md)** — Comprehensive architectural blueprint, OS concepts mapping, component design, state machines, and concurrency model.
* 🔄 **[System Workflows Guide (`WORKFLOWS.md`)](WORKFLOWS.md)** — Step-by-step sequence diagrams, operational message flows, failure handling, and recovery traces.
* ⚙️ **[CI/CD Pipelines (`.github/workflows/`)](.github/workflows/)** — Automated test matrix across Python 3.10–3.13 on Ubuntu and Windows.
* 🚀 **[Quick-Start Scripts (`scripts/`)](scripts/)** — Batch and PowerShell runners for cluster launch, testing, and demo execution.

---

## 1. Problem Statement

A single server is a single point of failure: if it crashes, every client
request fails. Real systems therefore replicate a service across several
machines and put a layer in front of them that

* chooses which replica serves each request (load balancing),
* retries requests that fail for temporary reasons,
* removes crashed replicas from the pool (failover),
* puts them back when they recover (health monitoring),
* and keeps exactly one replica designated as leader for coordination tasks.

This project implements that layer in ~1200 lines of readable Python.

---

## 2. Objectives

1. Implement RPC between a client and 3 replicated backend servers.
2. Implement automatic retries with backoff and error classification.
3. Implement round-robin load balancing with per-server statistics.
4. Implement failover with health-based recovery.
5. Implement (optional) coordinator-managed leader election.
6. Provide a live web dashboard to observe and to inject failures.

---

## 3. System Architecture

```
                    +--------------------------------------------+
                    |            Flask Dashboard (:5000)         |
                    |  metrics | server table | kill/revive | log |
                    +----------------------+---------------------+
                                           | (in-process)
   +---------------------------------------v---------------------------------+
   |                            COORDINATOR                                   |
   |                                                                          |
   |  ServerRegistry  <-- shared state: status, counters, events, leader      |
   |        ^   ^   ^                                                         |
   |        |   |   +-------------------- HealthMonitor (thread, every 2 s)   |
   |        |   |                                 |                           |
   |        |   +---- FailoverManager             +--> LeaderElector          |
   |        |              ^                                                   |
   |        +---- RoundRobinLoadBalancer                                      |
   +-------------------------------|------------------------------------------+
                                   |
                            +------v------+
                            |  RpcClient  |  retry loop + attempt log
                            +------+------+
                                   | gRPC (HTTP/2 + Protobuf)
        +--------------------------+--------------------------+
        |                          |                          |
+-------v-------+        +---------v------+        +----------v-----+
| Backend  :50051|       | Backend  :50052 |       | Backend  :50053 |
|  server-1      |       |  server-2       |       |  server-3       |
| Execute/Health |       | Execute/Health  |       | Execute/Health  |
| Admin (KILL..) |       | Admin (KILL..)  |       | Admin (KILL..)  |
+----------------+       +-----------------+       +-----------------+
```

**Components (as required in §6 of the spec)**

| # | Component | File |
|---|-----------|------|
| 1 | RPC Client (retry logic) | `client/rpc_client.py` |
| 2-4 | Backend Servers 1-3 | `servers/server.py` (3 processes) |
| 5 | Load Balancer | `coordinator/load_balancer.py` |
| 6 | Health Monitor | `coordinator/health_monitor.py` |
| 7 | Failover Manager | `coordinator/failover_manager.py` |
| 8 | Flask Dashboard | `dashboard/app.py` |
| + | Shared state / metrics | `coordinator/cluster.py` |
| + | Leader election | `coordinator/leader_election.py` |

---

## 4. RPC and Protocol Buffers (short explanation)

**RPC (Remote Procedure Call)** lets a client call a function that actually runs
in another process/machine: the client calls `stub.Execute(request)` exactly like
a local function; the framework marshals the arguments, sends them over the
network, executes the function remotely and returns the result. This is the
distributed-systems counterpart of an OS **system call** — the caller is blocked
while someone else does the work in another address space.

**Protocol Buffers** is the Interface Definition Language used by gRPC. The
`.proto` file (`proto/service.proto`) declares the messages and the service
methods; `protoc` generates Python classes (`service_pb2.py`) and client/server
glue (`service_pb2_grpc.py`). Benefits: compact binary encoding, strong typing,
language independence, and a contract both sides agree on.

gRPC runs over HTTP/2 and gives us a *status code* for every call — that code is
what our client uses to decide between "retry" and "give up".

---

## 5. How each fault-tolerance feature works

### 5.1 Round-Robin Load Balancing (`load_balancer.py`)
Servers are kept in a fixed order with a rotating pointer. `select()` walks the
list from the pointer and returns the first server that is **ONLINE** and not
already tried for the current request; the pointer moves one slot forward.
Walking (instead of `pointer % n`) is what lets us *skip* unavailable servers
while keeping a fair rotation. This is the same idea as round-robin CPU
scheduling in an OS, applied to servers instead of processes.

### 5.2 Retry (`rpc_client.py`)
Up to `MAX_ATTEMPTS = 3` attempts per request, with a linear backoff
(`0.4 s`, `0.8 s`). The gRPC status code is classified:

| Status code | Meaning | Client action |
|---|---|---|
| `UNAVAILABLE`, `DEADLINE_EXCEEDED` | connection/crash | mark server OFFLINE → **failover** + retry elsewhere |
| `ABORTED`, `RESOURCE_EXHAUSTED`, `INTERNAL`, `UNKNOWN` | temporary app error | **retry** (server stays ONLINE) |
| `INVALID_ARGUMENT`, `NOT_FOUND`, `UNIMPLEMENTED`, ... | permanent error | **stop immediately** (no retry) |

Every attempt is recorded (`server`, `outcome`, `code`, `latency`) and shown in
the terminal *and* on the dashboard.

### 5.3 Failover (`failover_manager.py`)
When a connection-level error occurs, the server is immediately set to
`OFFLINE`, the failover counter increases, and the next attempt asks the load
balancer for a *different* healthy server. The request succeeds as long as at
least one replica is alive.

### 5.4 Health Monitoring & Recovery (`health_monitor.py`)
A daemon thread calls `HealthCheck` on every server every 2 seconds:

```
ONLINE  --1 failed probe-->  OFFLINE
OFFLINE --1 good probe  -->  RECOVERING (probation, receives no traffic)
RECOVERING --1 more good probe--> ONLINE (back in the load-balancing pool)
```

### 5.5 Leader Election (`leader_election.py`) — demonstration only
The coordinator stores the leader id and applies one deterministic rule:
*the leader is the first ONLINE server in the configured order*. After every
health sweep, if the leader is no longer ONLINE a new one is elected and
notified with an `Admin(SET_LEADER)` RPC.

> **Note:** this is a *centralised, coordinator-managed* election used to
> illustrate the concept. It is **not** a fault-tolerant consensus protocol —
> there is no quorum, no term numbers, no split-brain protection, and the
> coordinator itself is a single point of failure. Raft/Paxos would be required
> for a production system.

---

## 6. Installation (Windows)

```bat
:: 1. go to the project folder
cd fault-tolerant-rpc

:: 2. create and activate a virtual environment
python -m venv venv
venv\Scripts\activate

:: 3. install dependencies
pip install -r requirements.txt

:: 4. generate the gRPC code from the .proto file
python generate_protos.py
```

(Linux/macOS: use `source venv/bin/activate`, everything else is identical.)

---

## 7. Execution

### Option A — everything with one command (recommended)

```bat
python run.py
```

Then open **http://127.0.0.1:5000**.
`run.py` starts `server-1`, `server-2`, `server-3` and the dashboard, and stops
everything on `Ctrl+C`.
Use `python run.py --separate-windows` to get one console window per server
(nice for a live demo, because you can `Ctrl+C` a single server).

### Option B — manual, one terminal per component (best for the demo)

```bat
:: Terminal 1
venv\Scripts\activate
python servers\server.py --id server-1

:: Terminal 2
venv\Scripts\activate
python servers\server.py --id server-2

:: Terminal 3
venv\Scripts\activate
python servers\server.py --id server-3

:: Terminal 4 - dashboard
venv\Scripts\activate
python dashboard\app.py

:: Terminal 5 - command line client (optional)
venv\Scripts\activate
python client\rpc_client.py --operation echo --payload hello --count 6
```

### Option C — using helper scripts (Windows CMD / PowerShell)

```bat
:: Run the full cluster
scripts\run_cluster.bat
:: or in PowerShell:
.\scripts\run_cluster.ps1

:: In a separate terminal, run sample requests
scripts\demo_client.bat
:: or in PowerShell:
.\scripts\demo_client.ps1
```

### Running the tests

```bat
:: Direct unittest invocation
python -m unittest discover -s tests -v

:: Or using convenience scripts
scripts\run_tests.bat
:: or in PowerShell:
.\scripts\run_tests.ps1
```

---

## 8. Available operations

| Operation | Payload | Behaviour |
|---|---|---|
| `echo`   | any text | returns `echo:<text>` |
| `sum`    | `1,2,3`  | returns the sum (bad input = permanent error) |
| `square` | `7`      | returns 49 |
| `slow`   | `2`      | sleeps 2 s (use with a short timeout to trigger retries) |
| `flaky`  | `1` / `2`| fails the first N attempts of a request, then succeeds → **retry demo** |
| `boom`   | –        | always `INVALID_ARGUMENT` → **permanent error, no retry** |

---

## 9. Demonstration Scenarios

### Scenario 1 — Normal operation (round-robin)
1. `python run.py`, open the dashboard.
2. Operation `echo`, Count `6`, **Send Request(s)**.
3. **Expected:** all 6 succeed; the distribution bars show 2 requests per
   server; attempts show `attempt 1 → server-N [SUCCESS]`.

### Scenario 2 — Server failure
1. Click **Kill** on `server-1` (or press `Ctrl+C` in its console).
2. Send 4 requests.
3. **Expected:** `server-1` badge becomes `OFFLINE`; all requests are served by
   `server-2` / `server-3`; event log shows "server-1 → OFFLINE".

### Scenario 3 — Retry mechanism
1. Operation `flaky`, Payload `1`, Count `1`.
2. **Expected:** `attempt 1 → server-X [RETRY] ABORTED temporary glitch`,
   then `attempt 2 → server-Y [SUCCESS]`. The *Retry Attempts* card increases.
3. Now Payload `9` (always fails): 3 attempts, then
   `FAILED (RETRY_LIMIT)` — a clear failure after the retry limit.
4. Operation `boom`: exactly **one** attempt, `FAILED (PERMANENT)` — proving
   that permanent errors are not retried.

### Scenario 4 — Failover
1. Kill `server-2` while leaving 1 and 3 up.
2. Send requests repeatedly.
3. **Expected:** the first request that happens to select `server-2` logs
   `UNAVAILABLE`, the *Failovers* counter increases, and the very same request
   succeeds on another server (the client still gets a successful response).

### Scenario 5 — Server recovery
1. Click **Revive** on the killed server (or restart its process).
2. Watch for ~4 seconds.
3. **Expected:** status goes `OFFLINE → RECOVERING → ONLINE`, event log shows
   "recovered and re-added to the pool", and new requests are again distributed
   over all three servers.

### Scenario 6 — Leader election (optional feature)
1. The *Current Leader* card shows `server-1` at startup.
2. Click **Simulate Leader Failure**.
3. **Expected:** `server-1` → `OFFLINE`, and the leader card immediately shows
   `server-2`; the event log contains
   "ELECTION: server-1 lost leadership → server-2 elected".
4. Revive `server-1`: it rejoins the pool but **stays a follower** (no
   pre-emption), which is also what real elections normally do.

---

## 10. Screenshots

Place your captures in a `docs/` folder and reference them here:

| Screenshot | Placeholder |
|---|---|
| Dashboard, normal operation | `docs/01-normal.png` |
| Round-robin distribution bars | `docs/02-distribution.png` |
| Server killed / OFFLINE | `docs/03-failure.png` |
| Retry trace of a flaky request | `docs/04-retry.png` |
| Recovery (RECOVERING → ONLINE) | `docs/05-recovery.png` |
| Leader election after leader failure | `docs/06-leader.png` |
| Server terminal logs | `docs/07-servers.png` |
| Unit tests passing | `docs/08-tests.png` |

---

## 11. Test Cases and Expected Results

| # | Test (in `tests/test_fault_tolerance.py`) | Expected |
|---|---|---|
| 1 | `test_round_robin_cycles_through_all_servers` | order 1,2,3,1,2,3 |
| 2 | `test_offline_servers_are_skipped` | OFFLINE server never selected |
| 3 | `test_recovering_server_is_not_in_the_pool` | RECOVERING gets no traffic |
| 4 | `test_select_returns_none_when_everything_is_down` | `None` |
| 5 | `test_requests_are_distributed_evenly` | 2 requests per server out of 6 |
| 6 | `test_retries_until_success` | 3 attempts, success, 2 retries counted |
| 7 | `test_stops_after_retry_limit` | exactly 3 attempts, `RETRY_LIMIT` |
| 8 | `test_permanent_error_is_not_retried` | exactly 1 attempt, `PERMANENT` |
| 9 | `test_transient_failure_does_not_take_server_offline` | server stays ONLINE |
| 10 | `test_request_is_redirected_to_a_healthy_server` | answer from server-2, server-1 OFFLINE |
| 11 | `test_failed_server_leaves_the_pool_for_later_requests` | no further traffic to server-1 |
| 12 | `test_failure_when_no_server_is_available` | request fails, all OFFLINE |
| 13 | `test_detects_a_dead_server` | health monitor marks it OFFLINE |
| 14 | `test_recovery_goes_through_recovering_then_online` | state machine verified |
| 15 | `test_elects_first_online_server` | leader = server-1 |
| 16 | `test_new_leader_elected_when_leader_dies` | leader = server-2 |
| 17 | `test_no_leader_when_all_servers_are_down` | leader = None |

Run: `python -m unittest discover -s tests -v` → **17 tests, OK**.

---

## 12. Relation to Operating-Systems concepts

| Project feature | OS concept |
|---|---|
| gRPC `Execute` call | **IPC / RPC** — the classic extension of system calls across machines; marshalling ≈ copying arguments across address spaces |
| Protocol Buffers | data **marshalling / serialisation**, like packing arguments for a syscall |
| 3 backend processes on 3 ports | **process isolation**; a port is a kernel-managed communication endpoint |
| Thread pool in each server (`ThreadPoolExecutor`) | **concurrency**, multithreaded server design, bounded worker pools |
| `ServerRegistry` with `RLock` | **mutual exclusion / critical sections**; shared state guarded by locks |
| Round-robin selection | **round-robin scheduling**, fairness, starvation avoidance |
| Health monitor thread + sleep interval | **periodic daemon / background kernel thread**, heartbeat & failure detection |
| Timeouts (`RPC_TIMEOUT`) | **deadlock/hang avoidance**, bounded waiting |
| Retry with backoff | **transient fault handling**, congestion avoidance (same idea as exponential backoff in CSMA/CD) |
| Failover + recovery | **fault tolerance, redundancy, graceful degradation, high availability** |
| Leader election | **coordination / mutual exclusion in distributed systems** (one coordinator at a time) |

---

## 13. Limitations

* The coordinator (load balancer + health monitor + dashboard) is itself a
  **single point of failure**; it is not replicated.
* Leader election is coordinator-managed, **not consensus-based** (no Raft).
* No persistence: all counters and the leader identity are in memory.
* No authentication/TLS — insecure channels on localhost only.
* Requests are not idempotent-safe: a retry after a *partially executed*
  request could duplicate side effects (no exactly-once semantics).
* Load balancing is purely round-robin: it ignores load, latency and capacity.
* Health detection granularity is the probe interval (≈2 s).

## 14. Possible Future Improvements

* Replace the single coordinator with a client-side library on every client.
* Implement a real Raft leader election (terms, votes, heartbeats).
* Add weighted / least-connections load balancing and a **circuit breaker**.
* Add exponential backoff with jitter, and request hedging.
* Add idempotency keys so retries are safe (exactly-once semantics).
* Persist metrics (SQLite) and stream the dashboard with WebSockets instead of polling.
* Add TLS + token authentication; add Prometheus-style metrics export.

---

## 15. Short Project Report

**Title:** Fault-Tolerant Distributed RPC Framework
**Course:** Operating Systems

**Abstract.** We designed and implemented a small distributed system in which a
client invokes a remote procedure on one of three replicated gRPC servers. A
coordinator layer provides round-robin load balancing, health monitoring,
automatic retry with error classification, failover to healthy replicas, and a
demonstration leader election. A Flask dashboard exposes the live state of the
cluster and lets the user inject crashes and recoveries.

**Method.** The service contract is defined in Protocol Buffers and compiled to
Python stubs. Each backend runs in its own OS process with a thread pool and
supports an `Admin` RPC used to simulate crashes without killing the process. A
background monitor thread probes all replicas every two seconds and maintains an
ONLINE/RECOVERING/OFFLINE state machine in a mutex-protected shared registry.
The client wraps every call in a retry loop that, for each attempt, asks the
load balancer for a healthy replica not yet tried and classifies gRPC status
codes into *connection failure*, *transient* and *permanent*.

**Results.** All six required demonstration scenarios work: round-robin
distribution is exactly uniform (2/2/2 for six requests), a killed server is
detected within one health interval and removed from the pool, in-flight
requests targeting a dead server are transparently re-routed and still succeed,
a flaky operation succeeds on retry while a permanent error terminates after a
single attempt, a revived server is re-admitted after two successful probes, and
leader failure triggers immediate re-election. Seventeen unit tests cover load
balancing, retry, failover, health monitoring and election.

**Conclusion.** The project shows that a small amount of client-side logic
(retries, timeouts, health-aware load balancing) converts a set of fragile
single servers into a highly available service, and it concretely illustrates
OS concepts such as IPC, process isolation, concurrency control, scheduling and
fault tolerance in a distributed setting.
