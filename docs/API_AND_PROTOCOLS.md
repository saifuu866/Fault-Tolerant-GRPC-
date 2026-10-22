# Communication Protocols and API Specifications

## 1. Protocol Buffers & gRPC Interface

The primary communication layer uses Protocol Buffers (proto3) running over HTTP/2. The service contract is declared in `proto/service.proto`.

### 1.1 Service Interface
```protobuf
syntax = "proto3";

package ftrpc;

service ComputeService {
  rpc Execute     (ComputeRequest) returns (ComputeResponse);
  rpc HealthCheck (HealthRequest)  returns (HealthResponse);
  rpc Admin       (AdminRequest)   returns (AdminResponse);
}
```

---

### 1.2 Message Definitions

#### ComputeRequest
Carries computation commands from client to server.
```protobuf
message ComputeRequest {
  string request_id = 1;   // Stable UUID across retries
  string operation  = 2;   // echo | sum | square | slow | flaky | boom
  string payload    = 3;   // Operation arguments
  int32  attempt    = 4;   // 1-based attempt sequence number
}
```

#### ComputeResponse
Returned upon successful execution of an application RPC.
```protobuf
message ComputeResponse {
  string request_id    = 1; // Echoed request identifier
  string server_id     = 2; // Identity of the processing replica (e.g. server-1)
  string result        = 3; // Computation result string
  string status        = 4; // Status indicator (typically "OK")
  string message       = 5; // Human-readable status details
  double processing_ms = 6; // Internal execution duration in milliseconds
  int32  attempt       = 7; // Sequence attempt number processed
}
```

#### HealthRequest & HealthResponse
Used by the background monitoring daemon to probe node status.
```protobuf
message HealthRequest {
  string source = 1;        // Caller identifier (e.g. "health-monitor")
}

message HealthResponse {
  string server_id        = 1; // Identity of responding server
  bool   healthy          = 2; // Health flag
  bool   is_leader        = 3; // Leadership status flag
  int64  handled_requests = 4; // Total requests processed since boot
  double uptime_seconds   = 5; // Seconds elapsed since server process start
}
```

#### AdminRequest & AdminResponse
Out-of-band control channel for operations and chaos testing.
```protobuf
message AdminRequest {
  string command = 1; // KILL | REVIVE | SET_LEADER | CLEAR_LEADER | SET_FAIL_RATE | STATUS
  string value   = 2; // Optional argument string (e.g. float for fail_rate)
}

message AdminResponse {
  string server_id = 1; // Target server identifier
  bool   ok        = 2; // Execution outcome
  string message   = 3; // Outcome details or error message
}
```

---

## 2. Server Business Logic Operations

The `ComputeServicer._compute()` method supports six operations:

| Operation | Expected Payload | Output Format | Error Behavior | Purpose |
|:---|:---|:---|:---|:---|
| `echo` | Any string | `echo:<payload>` | None | Baseline connectivity and latency measurement |
| `sum` | Comma-delimited ints (`"1,2,3"`) | Sum integer (`6`) | `PermanentError` on non-integers | Arithmetic verification |
| `square` | Integer string (`"12"`) | Square integer (`144`) | `PermanentError` on non-integer | Single-parameter arithmetic |
| `slow` | Duration in seconds (`"1.5"`) | `"slept 1.5s"` | Sleep capped at 10.0s | Deadline and timeout verification |
| `flaky` | Required failures count (`"2"`) | `"flaky recovered..."` | `TransientError` until count exceeded | Linear backoff retry verification |
| `boom` | Any or empty | None | Always raises `PermanentError` | Unrecoverable error verification |

---

## 3. Dashboard REST API Specifications

The Flask dashboard (`dashboard/app.py`) provides REST endpoints for browser control and automation.

### 3.1 Status & Telemetry: `GET /api/status`
Returns complete cluster state snapshot.

**Response Structure (200 OK):**
```json
{
  "events": [
    {
      "level": "SUCCESS",
      "message": "server-1 recovered and re-added to the pool",
      "time": "18:25:30"
    }
  ],
  "leader": "server-1",
  "online_count": 3,
  "server_count": 3,
  "servers": [
    {
      "address": "127.0.0.1:50051",
      "failures": 0,
      "is_leader": true,
      "last_check": 1740000000.5,
      "last_error": "",
      "latency_ms": 1.2,
      "reported_handled": 42,
      "requests": 14,
      "server_id": "server-1",
      "status": "ONLINE",
      "successes": 14
    }
  ],
  "totals": {
    "failed": 0,
    "failovers": 0,
    "requests": 42,
    "retries": 0,
    "success": 42
  },
  "uptime_seconds": 128.4
}
```

---

### 3.2 Request Dispatch: `POST /api/request`
Dispatches one or more requests through the fault-tolerant client proxy.

**Request Payload:**
```json
{
  "operation": "sum",
  "payload": "10,20,30",
  "count": 3
}
```

**Response Structure (200 OK):**
```json
{
  "ok": true,
  "results": [
    {
      "attempts": [
        {
          "address": "127.0.0.1:50051",
          "attempt": 1,
          "code": "OK",
          "duration_ms": 2.1,
          "message": "60",
          "outcome": "SUCCESS",
          "server_id": "server-1"
        }
      ],
      "error_type": "",
      "message": "handled by server-1",
      "operation": "sum",
      "payload": "10,20,30",
      "request_id": "a1b2c3d4",
      "result": "60",
      "server_id": "server-1",
      "success": true,
      "total_ms": 2.4
    }
  ]
}
```

---

### 3.3 Server Lifecycle Control: `POST /api/servers/<server_id>/<action>`
Simulates a crash (`kill`) or recovers a node (`revive`).

- **Parameters**: `server_id` (`server-1`, `server-2`, `server-3`), `action` (`kill`, `revive`).
- **Response**: `{"ok": true, "message": "server marked DOWN", "server_id": "server-1"}`.

---

### 3.4 Transient Fault Injection: `POST /api/servers/<server_id>/fail_rate`
Injects transient error probability on a target replica.

**Request Payload:**
```json
{
  "rate": 0.5
}
```
**Response**: `{"ok": true, "message": "transient failure rate = 0.5"}`.

---

### 3.5 Leader Operations

- **Kill Active Leader**: `POST /api/leader/kill`
  - Evicts current leader and triggers immediate election.
  - **Response**: `{"ok": true, "old_leader": "server-1", "new_leader": "server-2"}`.
- **Force Election**: `POST /api/leader/elect`
  - Re-evaluates lowest-index online rule.
  - **Response**: `{"ok": true, "leader": "server-1"}`.

---

### 3.6 Reset Metrics: `POST /api/reset`
Clears request counters, logs, and resets the round-robin pointer.
- **Response**: `{"ok": true}`.
