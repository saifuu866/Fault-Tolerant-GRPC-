# Member 3 Master Guide: Load Balancing, Retry Engine & Failover Logic

**Role:** Load Balancing, Automatic Retry & Failover Engine Developer  
**Module Share:** 25% of Project  
**Target Audience:** Member 3 Viva, Presentation, and Code Walkthrough Preparation  

---

## 1. Executive Summary: What is Member 3's Job?

In this distributed architecture, **Member 3 is the "Brain" of the client layer**. 

While Member 1 runs the server processes and Member 2 monitors cluster health in the background, **Member 3 decides how every single user request is routed, retried, and protected against crashes**.

```
[User / Dashboard]
         │
         ▼
 ┌──────────────────────────────────────────────────────────────┐
 │ MEMBER 3: Fault-Tolerant Client Layer                        │
 │                                                              │
 │  1. LoadBalancer:   "Who should handle this request next?"   │
 │  2. RpcClient:      "Execute, classify outcome, and retry"   │
 │  3. FailoverManager:"Node crashed? Evict it immediately!"    │
 └──────────────────────────────────────────────────────────────┘
         │
         ├── Attempt 1 (S1 failed)
         └── Attempt 2 (S2 success) ──► [Backend Server Cluster]
```

---

## 2. The 3 Files You Own (Read This Thoroughly)

Member 3 owns exactly three files in the repository:

### File 1: `coordinator/load_balancer.py` (RoundRobinLoadBalancer)
* **What it does:** Selects the next healthy backend server using a rotating pointer across a ring buffer.
* **Key Mechanism (`select(exclude=None)`):**
  1. Retrieves all server IDs from the registry (`server-1`, `server-2`, `server-3`).
  2. Walks starting from `self._pointer`.
  3. Checks two conditions:
     - Is the server `STATUS_ONLINE`?
     - Is the server NOT in the `exclude` set (servers already tried for this specific request)?
  4. Once a valid server is found, advances `self._pointer = (index + 1) % count` and returns the server.
  5. If all servers are down or excluded, returns `None`.
* **Thread Safety:** Wrapped in a `threading.Lock()` mutex so concurrent client requests do not corrupt the pointer.

### File 2: `coordinator/failover_manager.py` (FailoverManager)
* **What it does:** Acts as the bridge between the client retry loop, the load balancer, and the cluster registry.
* **Key Methods:**
  - `pick(exclude=None)`: Asks `load_balancer.select(exclude=exclude)` for the next candidate.
  - `mark_failed(server_id, error)`: When a connection drops (`UNAVAILABLE`), it immediately tells the registry `set_status(server_id, STATUS_OFFLINE)` and increments the global failover counter.
* **Why this matters:** The background health monitor only sweeps every 2.0 seconds. If a node crashes mid-request, `FailoverManager` evicts it in **under 1 millisecond** so subsequent requests do not hit the dead node.

### File 3: `client/rpc_client.py` (RpcClient, Attempt, RpcResult)
* **What it does:** Implements the complete fault-tolerant RPC execution pipeline.
* **Key Classes:**
  - `Attempt`: Stores one single try (number, target server, outcome `SUCCESS|RETRY|FAILED|NO_SERVER`, status code, duration ms).
  - `RpcResult`: Aggregates all attempts for a request, overall success flag, and total round-trip time.
  - `RpcClient`: The core driver containing `call(operation, payload)` and `_try_once(...)`.

---

## 3. The Core Algorithm: What Happens When `call()` is Invoked?

Every RPC follows this step-by-step lifecycle:

```
Start RpcClient.call()
  │
  ├─► Generate unique request_id (UUID 8-char hex)
  ├─► tried = set()  [Tracks servers contacted for this request]
  │
  FOR attempt = 1 TO MAX_ATTEMPTS (3):
  │
  ├──► If attempt > 1:
  │       Calculate linear delay: 0.4s * (attempt - 1)
  │       Sleep(delay)  [Backoff to relieve server congestion]
  │
  ├──► Ask failover.pick(exclude=tried)
  │       If None: Fallback to pick(exclude=None)
  │       If still None: Record NO_SERVER attempt and continue
  │
  ├──► tried.add(server.server_id)
  │
  ├──► Execute RPC over gRPC with RPC_TIMEOUT = 3.0s
  │
  ├──► Check Result:
  │       CASE 1: Success -> Break loop -> Return SUCCESS
  │       CASE 2: Transient Error -> Classify as RETRY -> Loop continues
  │       CASE 3: Connection Error -> Mark OFFLINE -> Classify as RETRY -> Loop continues
  │       CASE 4: Permanent Error -> Classify as FAILED -> Break loop immediately
  │
  END FOR
  │
  └─► If not successful: Mark as RETRY_LIMIT or NO_SERVER -> Return RpcResult
```

---

## 4. The 3-Way Error Classification Table (Crucial for Viva)

This is the most critical concept for Member 3. The client categorizes errors into three buckets:

| Category | gRPC Status Codes | System Meaning | Action Taken by Client | Server Status |
| :--- | :--- | :--- | :--- | :--- |
| **1. Connection Failure** | `StatusCode.UNAVAILABLE`<br>`StatusCode.DEADLINE_EXCEEDED` | Server crashed, port closed, or cable unplugged. | **Failover immediately**: Mark node `OFFLINE`, add to `tried`, retry on another node. | Marked `OFFLINE` |
| **2. Transient Failure** | `StatusCode.ABORTED`<br>`StatusCode.RESOURCE_EXHAUSTED`<br>`StatusCode.INTERNAL` | Temporary glitch, lock contention, or queue overload (e.g. `flaky`). | **Retry with backoff**: Wait `0.4s * attempt`, try next server or retry. | Stays `ONLINE` |
| **3. Permanent Failure** | `StatusCode.INVALID_ARGUMENT`<br>`StatusCode.NOT_FOUND`<br>`StatusCode.UNIMPLEMENTED` | Bad user input, invalid payload, or unhandled syntax (e.g. `boom`). | **Halt immediately**: Do not retry. Retrying will never succeed and wastes CPU. | Stays `ONLINE` |

---

## 5. Key Operating System Concepts Handled by Member 3

When the examiner asks *"What OS concepts are demonstrated in your module?"*, present these three:

1. **Process Scheduling (Round-Robin Fair Share)**:
   - *OS Concept:* Similar to the CPU round-robin scheduler allocating time slices to processes.
   - *Implementation:* `RoundRobinLoadBalancer` maintains a circular queue pointer to distribute client request bursts evenly across backend worker processes, preventing node starvation.
2. **Bounded Waiting & Deadlock Avoidance**:
   - *OS Concept:* Preventing indefinite blocking when an external resource becomes unresponsive.
   - *Implementation:* Enforces strict `RPC_TIMEOUT = 3.0s` on every network socket call so client threads never freeze if a server hangs.
3. **Fault Masking & Graceful Degradation**:
   - *OS Concept:* High-availability distributed systems masking partial failures from end users.
   - *Implementation:* If Server 1 dies mid-execution, the user never receives an error; Member 3's retry engine intercepts the socket exception and transparently completes the call on Server 2.

---

## 6. Top 8 Viva Questions & Confident Answers

### Q1: What is the difference between Load Balancing and Failover?
**Answer:** Load balancing is the normal routing strategy that distributes requests evenly across active, healthy servers (Round-Robin). Failover is the reactive recovery mechanism that detects when a selected server has failed, evicts it from the active pool, and reroutes the in-flight request to an alternative healthy replica.

### Q2: Why did you use Linear Backoff instead of retrying immediately?
**Answer:** If a server returns a transient error (such as `ABORTED` or `RESOURCE_EXHAUSTED`) due to temporary load or lock contention, retrying immediately without delay causes a "thundering herd" problem and worsens congestion. Adding linear delay (`0.4s * (attempt - 1)`) gives the backend time to recover.

### Q3: What is the purpose of the `exclude` set in `load_balancer.select(exclude=tried)`?
**Answer:** During a retry loop, `tried` tracks all servers that have already failed for that specific request. Passing `exclude=tried` prevents the load balancer from picking the same dead or failing server twice in a single request lifecycle, forcing it to route to a different node.

### Q4: Why doesn't the client retry when `boom` is called?
**Answer:** `boom` returns `StatusCode.INVALID_ARGUMENT`, which our error classifier categorizes as a `PERMANENT` application error. Retrying bad user inputs or fatal errors is useless because repeating the exact same input will always produce the exact same error, wasting CPU and network bandwidth.

### Q5: How does your code ensure thread safety in the Load Balancer?
**Answer:** `RoundRobinLoadBalancer` protects its internal pointer (`self._pointer`) with a `threading.Lock()`. When multiple threads send RPCs simultaneously from Flask or CLI threads, the mutex prevents race conditions when updating the rotation index.

### Q6: What happens if all three backend servers are dead?
**Answer:** The load balancer returns `None`. `RpcClient` records an attempt with status `NO_SERVER`, repeats until `MAX_ATTEMPTS = 3`, and returns an `RpcResult` marked `success = False` and `error_type = "NO_SERVER"`. It fails cleanly without crashing the client or throwing unhandled exceptions.

### Q7: What parameters configure your retry engine?
**Answer:** They are centralized in `servers/server_config.py`:
- `MAX_ATTEMPTS = 3` (Maximum tries before failing).
- `RETRY_DELAY = 0.4` seconds (Base linear backoff factor).
- `RPC_TIMEOUT = 3.0` seconds (Maximum wait time per individual attempt).

### Q8: What recent bug did you fix in your module?
**Answer:** In `client/rpc_client.py`, when a transient gRPC error occurred, `Attempt.code` held a raw Python `grpc.StatusCode` enum object. When the dashboard attempted to serialize this to JSON via `jsonify()`, it raised `TypeError: Object of type StatusCode is not JSON serializable`. We fixed this by ensuring `Attempt.code` is always coerced to a string (`code.name if hasattr(code, 'name') else str(code)`).
