# Mistake Log

### gRPC StatusCode Object Not JSON Serializable in Flask Dashboard
**Problem:** When transient errors occurred or were simulated via glitch rates, `grpc.StatusCode` enum objects were stored in `Attempt.code`. Passing them uncoerced to `jsonify` raised `TypeError: Object of type StatusCode is not JSON serializable`, yielding an HTTP 500 HTML response that broke `fetch(...).json()` in the browser with `SyntaxError: Unexpected token '<'`.
**Context:** `client/rpc_client.py` (`Attempt` class) and `dashboard/app.py` (`/api/request`).
**Solution:** Coerce `code` to a string in `Attempt.__init__` and `Attempt.to_dict()` via `code.name if hasattr(code, "name") else str(code or "")`, and safely handle non-200 responses in `dashboard/static/script.js`.
**Date:** 2026-10-22

### Flaky Simulation Always Exhausted Retries Across Multi-Server Cluster
**Problem:** In `servers/server.py`, the `flaky` operation counted failures using an in-memory dictionary `self._flaky_counters`. Because each server is an isolated process and the client rotates retried attempts to other servers via round-robin, each server evaluated the request as attempt #1 (`count = 1 <= needed`). As a result, every attempt in a multi-server retry chain failed, causing `flaky` with payload `1` to unexpectedly hit `RETRY_LIMIT`.
**Context:** `servers/server.py` (`ComputeServicer.Execute` for `flaky` operation).
**Solution:** Checked `request.attempt` (`effective_attempt = request.attempt if getattr(request, "attempt", 0) > 0 else count`) so that global attempt numbers sent by the client are respected across all cluster nodes.
**Date:** 2026-10-22
