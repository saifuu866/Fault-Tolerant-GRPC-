# Fault Tolerance and Resilience Engineering

## 1. Fault Model and Resilience Principles

The framework assumes a **Fail-Stop / Crash-Recovery** distributed failure model:
- Replicas may crash unexpectedly, terminate via OS signals, or experience simulated crashes.
- Communication links may introduce latency, packet drops, or timeouts.
- Byzantine failures (malicious corruption or arbitrary message alteration) are assumed not to occur.

To guarantee continuous availability under this model, the architecture implements four defensive layers:
1. **Error Classification**: Distinguishing connection breakage from software bugs and transient overload.
2. **Deterministic Retries with Linear Backoff**: Bounded retry attempts with graduated backoff to mitigate thundering herds.
3. **Exclusion-Set Routing**: Dynamic eviction of failing nodes during an in-flight request.
4. **Quarantine Probation**: Preventing route flapping during node recovery.

---

## 2. Error Classification Engine

When a client RPC execution raises `grpc.RpcError`, the client must avoid two failure extremes:
- Retrying an unrecoverable request (e.g. invalid syntax), which wastes CPU and saturates queues.
- Abandoning a recoverable request (e.g. temporary network blip), which degrades availability.

The framework implements a strict tripartite error classification scheme:

```
                          [grpc.RpcError Raised]
                                    |
            +-----------------------+-----------------------+
            |                                               |
  [Status Code Check]                             [Status Code Check]
  UNAVAILABLE / DEADLINE_EXCEEDED                 ABORTED / RESOURCE_EXHAUSTED /
            |                                     INTERNAL / UNKNOWN
            v                                               |
  +-------------------------------+                         v
  |      CONNECTION ERROR         |       +-----------------------------------+
  | - Node crashed or timed out   |       |         TRANSIENT ERROR           |
  | - Action: mark_failed()       |       | - Server process is healthy       |
  | - Node marked OFFLINE         |       | - Node remains ONLINE             |
  | - Trigger FAILOVER            |       | - Backoff sleep & RETRY           |
  | - Exclude node from next try  |       +-----------------------------------+
  +-------------------------------+                         |
                                                            |
            +-----------------------------------------------+
            |
  All other status codes:
  INVALID_ARGUMENT / NOT_FOUND / PERMISSION_DENIED / UNIMPLEMENTED
            |
            v
  +-------------------------------+
  |        PERMANENT ERROR        |
  | - Fatal application bug       |
  | - Action: Abort immediately   |
  | - No further retries          |
  +-------------------------------+
```

### Classification Reference Table

| gRPC Status Code | Classification | Node Health Action | Retry Decision | Example Cause |
|:---|:---|:---|:---|:---|
| `StatusCode.UNAVAILABLE` | Connection | Immediate `OFFLINE` eviction | Retry on different node | Process killed, port closed, network partition |
| `StatusCode.DEADLINE_EXCEEDED` | Connection | Immediate `OFFLINE` eviction | Retry on different node | Server hung, deadlocked, or heavy queue delay |
| `StatusCode.ABORTED` | Transient | Node remains `ONLINE` | Retry with backoff | Injected chaos glitch, concurrency collision |
| `StatusCode.RESOURCE_EXHAUSTED` | Transient | Node remains `ONLINE` | Retry with backoff | Temporary thread pool saturation |
| `StatusCode.INTERNAL` | Transient | Node remains `ONLINE` | Retry with backoff | Unexpected transient runtime exception |
| `StatusCode.UNKNOWN` | Transient | Node remains `ONLINE` | Retry with backoff | Unclassified intermediate network proxy error |
| `StatusCode.INVALID_ARGUMENT` | Permanent | Node remains `ONLINE` | Abort immediately | Malformed payload (e.g. bad parameters to `sum`) |
| `StatusCode.UNIMPLEMENTED` | Permanent | Node remains `ONLINE` | Abort immediately | Unrecognized operation name |

---

## 3. Retry Budget and Backoff Formulation

### Mathematical Specification
For a request $R$ with maximum allowed attempts $M = 3$ and base backoff interval $T_{\text{base}} = 0.4\text{ seconds}$:

$$\text{Backoff}(a) = \begin{cases} 
0, & \text{if } a = 1 \\
T_{\text{base}} \times (a - 1), & \text{if } 1 < a \le M 
\end{cases}$$

- Attempt 1: Immediate execution (0.0s delay).
- Attempt 2: $0.4 \times (2 - 1) = 0.4\text{ seconds}$ delay.
- Attempt 3: $0.4 \times (3 - 1) = 0.8\text{ seconds}$ delay.
- Maximum cumulative backoff wait: $0.4 + 0.8 = 1.2\text{ seconds}$.

### Request Idempotency Guarantee
Every RPC request assigns a stable identifier (`request_id = uuid.uuid4().hex[:8]`). This ID is preserved across all retry attempts:
```protobuf
message ComputeRequest {
  string request_id = 1;   // Stable across retries
  string operation  = 2;
  string payload    = 3;
  int32  attempt    = 4;   // Incremented counter (1, 2, 3)
}
```
Servers utilize `request_id` to track duplicate submissions and maintain internal idempotency state (e.g. tracking flaky counters).

---

## 4. Exclusion-Set Routing Strategy

When failover occurs, the client must avoid immediately re-selecting the node that just failed.

```python
tried = set()
for attempt_no in range(1, self.max_attempts + 1):
    # Select a healthy node excluding already attempted replicas
    server = self.failover.pick(exclude=tried)
    if server is None:
        # Fallback: if all healthy replicas have been tried, reset filter
        server = self.failover.pick(exclude=None)
    
    if server is not None:
        tried.add(server.server_id)
        # Attempt execution...
```

### Fallback Mechanism:
If the cluster consists of 3 nodes and all 3 nodes fail during attempts 1 and 2, `pick(exclude=tried)` returns `None`. The client executes a fallback call `pick(exclude=None)`, checking if a previously failed server has recovered in the interim before declaring the request totally failed.

---

## 5. Two-Stage Quarantine State Machine

To prevent route flapping (where an unstable node repeatedly toggles between online and offline, absorbing and failing requests), newly revived nodes must pass a two-stage probationary check:

```
                  +---------------------------+
                  |          UNKNOWN          |
                  +-------------+-------------+
                                |
                                | Initial Probe Success
                                v
                  +---------------------------+
         +------->|          ONLINE           |<--------+
         |        +-------------+-------------+         |
         |                      |                       |
2 Consecutive                   | 1 Failed              | 2 Consecutive
Good Probes                     | Probe                 | Good Probes
         |                      v                       |
         |        +---------------------------+         |
         +--------|        RECOVERING         |         |
                  |  (Quarantine Probation)   |         |
                  +-------------^-------------+         |
                                |                       |
                                | 1 Good Probe          |
                                |                       |
                  +-------------+-------------+         |
                  |          OFFLINE          |---------+
                  +---------------------------+
```

### State Definitions:
1. **`UNKNOWN`**: Unverified state upon system initialization.
2. **`ONLINE`**: Fully healthy and verified. Eligible for client traffic in `load_balancer.select()`.
3. **`OFFLINE`**: Confirmed down by health monitor or client eviction. Ineligible for traffic.
4. **`RECOVERING`**: The node has answered 1 successful probe after being offline. It is held in quarantine and remains **excluded** from `registry.available()` until a second consecutive successful probe confirms stability.

---

## 6. Chaos Engineering and Failure Simulation

The framework provides hooks for automated and interactive fault injection:

### 1. In-Process Crash Emulation (`Admin KILL` / `REVIVE`)
- An `Admin(command="KILL")` sets `self.alive = False`.
- The process continues running but immediately terminates incoming `Execute` and `HealthCheck` requests with `StatusCode.UNAVAILABLE`.
- `Admin(command="REVIVE")` resets `self.alive = True` and clears internal failure counters.

### 2. Probabilistic Glitch Injection (`Admin SET_FAIL_RATE`)
- Sets `fail_rate` to a probability $P \in [0.0, 1.0]$.
- For each request, if $\text{random}() < P$, the servicer aborts with `StatusCode.ABORTED`.
- Tests client resilience against transient errors.

### 3. Built-In Chaos Operations
- `operation="flaky"`, `payload="2"`: Deliberately fails the first 2 attempts of a specific `request_id` with `StatusCode.ABORTED`, then succeeds on attempt 3.
- `operation="slow"`, `payload="5.0"`: Sleeps for 5.0 seconds, exceeding the 3.0s `RPC_TIMEOUT` to trigger `StatusCode.DEADLINE_EXCEEDED`.
- `operation="boom"`: Always raises `PermanentError` (`StatusCode.INVALID_ARGUMENT`) to test client immediate abort behavior.

---

## 7. Resilience Verification Matrix

| # | Test Scenario | Trigger | Expected System Behavior | Verified in Test Suite |
|:---|:---|:---|:---|:---|
| 1 | Standard Load Distribution | 6 sequential requests | Even distribution: 2 requests per node | `test_requests_are_distributed_evenly` |
| 2 | Offline Node Exclusion | Mark `server-2` OFFLINE | Load balancer distributes strictly across `server-1` and `server-3` | `test_offline_servers_are_skipped` |
| 3 | Probationary Quarantine | Mark `server-1` RECOVERING | Node excluded from routing pool until fully promoted | `test_recovering_server_is_not_in_the_pool` |
| 4 | Transient Retry Success | Operation `flaky` with 2 glitches | Attempts 1 & 2 retry; attempt 3 succeeds; server remains ONLINE | `test_retries_until_success` |
| 5 | Retry Budget Exhaustion | Persistent transient failures (> 3) | Fails after 3 attempts with `RETRY_LIMIT` | `test_stops_after_retry_limit` |
| 6 | Permanent Error Abort | Operation `boom` | Immediate abort after 1 attempt with `PERMANENT` | `test_permanent_error_is_not_retried` |
| 7 | Dynamic Failover Routing | `server-1` throws `UNAVAILABLE` | `server-1` marked OFFLINE; request routed to `server-2` | `test_request_is_redirected_to_a_healthy_server` |
| 8 | Leader Failover Handover | Kill current leader node | Next lowest-order online node elected leader | `test_new_leader_elected_when_leader_dies` |
