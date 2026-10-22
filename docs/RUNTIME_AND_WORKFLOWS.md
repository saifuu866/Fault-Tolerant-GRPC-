# Runtime Lifecycles and System Workflows

## 1. Workflow Architecture Overview

The framework operates across coordinated workflows categorized into runtime operations, resilience/recovery, and operator controls:

```
Runtime Operations:
  [Bootstrap / Startup] ---> [Normal Request Dispatch]
                                    |
                    +---------------+---------------+
                    |                               |
        (Transient Error)                   (Node Crash)
                    |                               |
                    v                               v
         [Linear Backoff Retry]             [Dynamic Failover]
                                                    |
                                                    v
Resilience & Recovery:                      [Health Sweep Detection]
                                                    |
                                                    v
                                            [Two-Stage Recovery]
                                                    |
                                                    v
                                            [Leader Re-Election]
```

---

## 2. Workflow 1: System Bootstrapping and Initialization

### Objective
Start the backend replica processes, ensure compiled protobuf stubs are available, initialize coordinator state, start the health monitor daemon, and bind the Flask dashboard.

### Sequence Flow
```
Operator/User          run.py               Backend Servers         Flask Dashboard      HealthMonitor
     |                    |                        |                       |                   |
     |--- python run.py ->|                        |                       |                   |
     |                    |-- ensure_stubs() ----->|                       |                   |
     |                    |-- Popen(server-1) ---->| (listen :50051)       |                   |
     |                    |-- Popen(server-2) ---->| (listen :50052)       |                   |
     |                    |-- Popen(server-3) ---->| (listen :50053)       |                   |
     |                    |-- sleep(2.0s) -------->| (socket bind grace)   |                   |
     |                    |-- start dashboard ---->|                       |                   |
     |                    |                        |                       |-- init state ---->|
     |                    |                        |                       |-- check_all() --->| (sync sweep)
     |                    |                        |<-- HealthCheck() -----|-------------------|
     |                    |                        |--- healthy=True ----->|-------------------|
     |                    |                        |                       |-- start() ------->| (daemon thread)
     |                    |<-- listening :5000 ----|                       |                   |
```

### Steps:
1. `run.py` tests stub availability by importing `proto_stubs`. If stubs are missing, it executes `generate_protos.py`.
2. `start_servers()` spawns three independent background processes for `server-1`, `server-2`, and `server-3`.
3. The launcher pauses for 2.0 seconds to allow gRPC servers to initialize thread pools and bind their listening sockets.
4. `dashboard/app.py` instantiates `ServerRegistry`, `RoundRobinLoadBalancer`, `FailoverManager`, `LeaderElector`, and `HealthMonitor`.
5. `monitor.check_all()` performs a synchronous health sweep, establishing initial connectivity before the web server begins accepting HTTP traffic.
6. `monitor.start()` launches the background polling thread, and Flask binds to `127.0.0.1:5000`.

---

## 3. Workflow 2: Normal Client Request Execution (Happy Path)

### Objective
A client issues an RPC request that executes successfully on the first attempt via round-robin dispatch.

### Sequence Flow
```
Client App             RpcClient             LoadBalancer             Selected Replica (server-1)
    |                      |                      |                               |
    |-- call(op, payload)->|                      |                               |
    |                      |-- generate UUID ---->|                               |
    |                      |-- record_request() ->|                               |
    |                      |-- select(exclude={})-|                               |
    |                      |<-- return server-1 --| (pointer advances 0 -> 1)     |
    |                      |                                                      |
    |                      |-- gRPC Execute(ComputeRequest) --------------------->|
    |                      |                                                      |-- verify alive=True
    |                      |                                                      |-- execute operation
    |                      |                                                      |-- handled count++
    |                      |<-- ComputeResponse(status="OK", result) -------------|
    |                      |                                                      |
    |                      |-- record_success(server-1)                           |
    |                      |-- log_event("SUCCESS", ...)                          |
    |<-- return RpcResult -|                                                      |
```

### Processing Steps:
1. `RpcClient.call()` generates a unique 8-character hex `request_id`.
2. The client records the global request counter in `ServerRegistry`.
3. `RoundRobinLoadBalancer.select()` returns the current node at the pointer (`server-1`) and shifts the internal pointer to index 1.
4. The client dispatches `ComputeRequest` over gRPC.
5. `server-1` processes the payload, logs the transaction, updates its local `handled` counter, and returns `ComputeResponse`.
6. `RpcClient` records success in the registry and returns an `RpcResult` object with `success=True`.

---

## 4. Workflow 3: Transient Error and Linear Backoff Retry

### Objective
An application encounters an intermittent server glitch (e.g. `StatusCode.ABORTED` or `StatusCode.RESOURCE_EXHAUSTED`). The client retries with linear backoff without marking the server offline.

### Sequence Flow
```
RpcClient                     server-1 (glitched)                  server-2 (healthy)
    |                                 |                                    |
    |-- Attempt 1: Execute() -------->|                                    |
    |<-- RpcError(ABORTED) -----------|                                    |
    |-- classify: TRANSIENT_ERROR     |                                    |
    |-- record_attempt_failure()      |                                    |
    |   (server-1 remains ONLINE)     |                                    |
    |                                 |                                    |
    |-- Backoff Sleep (0.4s)          |                                    |
    |-- record_retry()                |                                    |
    |                                 |                                    |
    |-- Attempt 2: select() -> server-2                                    |
    |-- Attempt 2: Execute() --------------------------------------------->|
    |<-- ComputeResponse(OK) ----------------------------------------------|
    |-- record_success(server-2)                                           |
    |-- log_event("SUCCESS", "OK after 2 attempts")                        |
```

### Key Rules:
- The error code `ABORTED` indicates the server process is healthy but could not process the current request (e.g., contention or transient workload).
- `FailoverManager.mark_failed()` is **not** called. `server-1` retains its `ONLINE` status.
- Backoff duration follows the formula:
  $$\text{Delay} = \text{RETRY\_DELAY} \times (\text{attempt} - 1) = 0.4 \times (2 - 1) = 0.4\text{ seconds}$$
- The request succeeds on attempt 2, incrementing the cluster retry metric.

---

## 5. Workflow 4: Node Crash, Instant Eviction, and Failover

### Objective
A replica process crashes or becomes unresponsive (`UNAVAILABLE` or `DEADLINE_EXCEEDED`). The client detects the failure, evicts the node immediately, and routes to a backup replica.

### Sequence Flow
```
RpcClient               LoadBalancer            server-1 (Crashed)        server-2 (Healthy)
    |                        |                          |                         |
    |-- select() ----------->|                          |                         |
    |<-- returns server-1 ---|                          |                         |
    |                        |                          |                         |
    |-- Execute() ------------------------------------->X (Connection Refused)    |
    |<-- RpcError(UNAVAILABLE) -------------------------|                         |
    |                                                                             |
    |-- classify: CONNECTION_ERROR                                                |
    |-- FailoverManager.mark_failed("server-1")                                   |
    |   * server-1 status -> OFFLINE                                              |
    |   * failover count++                                                        |
    |   * log_event("FAILOVER: server-1 removed from pool")                       |
    |                                                                             |
    |-- Linear Backoff Sleep (0.4s)                                               |
    |-- select(exclude={"server-1"})                                              |
    |   * LB skips server-1 (marked OFFLINE and in exclusion set)                 |
    |<-- returns server-2 --------------------------------------------------------|
    |                                                                             |
    |-- Execute() --------------------------------------------------------------->|
    |<-- ComputeResponse(OK) -----------------------------------------------------|
    |-- record_success("server-2")                                                |
```

### Key Resilience Guarantees:
1. **Immediate Protection**: The client does not wait for the next 2.0s background health probe; eviction is instantaneous upon receipt of `UNAVAILABLE`.
2. **Double Shielding**: Even if status propagation had latency, `tried.add(server.server_id)` ensures the failed node is added to the exclusion set for subsequent attempts of this request.
3. **Transparency**: The end caller receives a successful response with `total_ms` reflecting the failover latency.

---

## 6. Workflow 5: Background Health Polling and Two-Stage Recovery

### Objective
A previously crashed server restarts. The health monitor detects its return but subjects it to probationary verification before restoring it to the active routing pool.

### Sequence Flow
```
HealthMonitor Daemon         ServerRegistry            server-1 (Recovering)
        |                           |                           |
        | (Sweep interval: 2.0s)    |                           |
        |-- HealthCheck() ------------------------------------->| (Probes server-1)
        |<-- HealthResponse(healthy=True) ----------------------|
        |                           |                           |
        |-- _on_success()           |                           |
        |   consecutive_ok = 1      |                           |
        |   threshold = 2           |                           |
        |-- set_status() ---------->|                           |
        |   status -> RECOVERING    | (Node remains OUT of load balancer pool)
        |                           |                           |
        | (Next sweep: 2.0s later)  |                           |
        |-- HealthCheck() ------------------------------------->|
        |<-- HealthResponse(healthy=True) ----------------------|
        |                           |                           |
        |-- _on_success()           |                           |
        |   consecutive_ok = 2      |                           |
        |   threshold reached (>= 2)|                           |
        |-- set_status() ---------->|                           |
        |   status -> ONLINE        | (Node restored to load balancer pool)
        |-- log_event("SUCCESS", "server-1 recovered and re-added to the pool")
```

---

## 7. Workflow 6: Leader Demotion and Handover Election

### Objective
The active cluster leader crashes. The coordinator detects the loss and promotes the next eligible online replica.

### Sequence Flow
```
HealthMonitor            ServerRegistry            LeaderElector         server-2 (New Leader)
      |                         |                        |                         |
      |-- check_all() completed |                        |                         |
      |-- ensure_leader() ------------------------------>|                         |
      |                         |-- get(leader_id) ----->|                         |
      |                         |   status is OFFLINE    |                         |
      |                         |                        |-- available() --------->|
      |                         |                        |<-- [server-2, server-3]-|
      |                         |                        |                         |
      |                         |                        |-- Admin(SET_LEADER) --->|
      |                         |                        |<-- AdminResponse(ok) ---|
      |                         |-- set_leader(server-2)-|                         |
      |                         |-- log_event("WARN", "ELECTION: server-1 lost -> server-2 elected")
```
