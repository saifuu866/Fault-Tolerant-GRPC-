# Summary: Fault Tolerance and Dashboard Serialization Fixes

## 1. Overview and Scope
This document summarizes critical bug fixes applied to the Fault-Tolerant Distributed RPC Framework during verification of transient failure and client retry mechanisms. It connects directly with [docs/MASTER_CONTEXT.md](file:///e:/SEM%205/OSDS/CP/Fault-Tolerant-GRPC--main/docs/MASTER_CONTEXT.md), [docs/FAULT_TOLERANCE_AND_RESILIENCE.md](file:///e:/SEM%205/OSDS/CP/Fault-Tolerant-GRPC--main/docs/FAULT_TOLERANCE_AND_RESILIENCE.md), and [MISTAKES.md](file:///e:/SEM%205/OSDS/CP/Fault-Tolerant-GRPC--main/MISTAKES.md).

---

## 2. Issues Encountered and Technical Solutions

### Issue A: `grpc.StatusCode` Serialization Failure in Flask Dashboard
- **Problem**: When a transient fault or glitch occurred, `RpcClient._try_once` recorded `exc.code()` into `Attempt.code`. In Python, `exc.code()` returns a `grpc.StatusCode` enum instance. When `dashboard/app.py` attempted to serialize the list of attempts via `flask.jsonify({"results": results})`, Python's default JSON serializer raised `TypeError: Object of type StatusCode is not JSON serializable`. Flask generated a 500 HTML error page, which caused the browser's JavaScript client to fail with `SyntaxError: Unexpected token '<', "<!doctype "... is not valid JSON`.
- **Solution**:
  1. Updated `Attempt.__init__` and `Attempt.to_dict` in `client/rpc_client.py` to ensure `code` is always coerced to a string (`code.name if hasattr(code, "name") else str(code or "")`).
  2. Updated `api()` in `dashboard/static/script.js` to inspect `res.ok` before attempting JSON deserialization, preventing HTML error pages from raising syntax exceptions.

### Issue B: Multi-Node Retry Tracking in `flaky` Operation
- **Problem**: The `flaky` operation in `servers/server.py` simulates temporary glitches by rejecting the first $N$ requests of a given `request_id` before succeeding. Each server process tracked attempts using an isolated in-memory dictionary (`self._flaky_counters`). When a transient error occurred on `server-1`, the client's round-robin load balancer correctly routed the retry to `server-2`. Because `server-2` had not previously seen that `request_id`, it initialized `count = 1` and rejected the request again. Across a 3-node cluster, every attempt was evaluated as attempt #1, causing `flaky` requests to exhaust `MAX_ATTEMPTS = 3` and terminate in `RETRY_LIMIT`.
- **Solution**:
  1. The client already propagates the global attempt number via `request.attempt` in `ComputeRequest` (`proto/service.proto`).
  2. Updated `servers/server.py` to evaluate `effective_attempt = request.attempt if getattr(request, "attempt", 0) > 0 else count`. When attempt #2 reaches `server-2`, it correctly recognizes `effective_attempt = 2 > 1` and returns success.

---

## 3. Verification and Live Audit
- **End-to-End Browser Verification**:
  - Sent `flaky` with payload `1` against the running cluster.
  - Attempt #1 targeted `server-2`, failing as expected with `StatusCode.ABORTED` (`temporary glitch (1/1)`).
  - Client paused for 0.4s backoff and routed Attempt #2 to `server-3`.
  - Attempt #2 succeeded on `server-3` with `flaky recovered after 1 failure(s)`.
  - The execution trace and metrics updated cleanly without serialisation errors or popup exceptions.
