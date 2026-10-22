"""Fault-tolerant RPC client: load balancing + retry + failover in one place.

Per request algorithm
---------------------
for attempt in 1..MAX_ATTEMPTS:
    if attempt > 1: sleep(RETRY_DELAY * (attempt-1))          # linear backoff
    server = failover.pick(exclude = servers already tried)   # round robin
    if no server: continue (maybe one recovers before the next attempt)
    try Execute(server)
        -> success: return
    except RpcError:
        classify the status code:
          * UNAVAILABLE / DEADLINE_EXCEEDED -> connection failure
                 => mark server OFFLINE (failover) and retry on another server
          * ABORTED / RESOURCE_EXHAUSTED / UNKNOWN -> transient app failure
                 => retry (server stays ONLINE)
          * everything else -> permanent error => stop immediately
"""
import argparse
import logging
import os
import sys
import time
import uuid

import grpc

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)

from proto_stubs import service_pb2, service_pb2_grpc                 # noqa: E402
from coordinator.cluster import ServerRegistry                        # noqa: E402
from coordinator.failover_manager import FailoverManager              # noqa: E402
from coordinator.health_monitor import HealthMonitor                  # noqa: E402
from coordinator.leader_election import LeaderElector                 # noqa: E402
from coordinator.load_balancer import RoundRobinLoadBalancer          # noqa: E402
from servers.server_config import (MAX_ATTEMPTS, RETRY_DELAY,         # noqa: E402
                                   RPC_TIMEOUT)

LOG = logging.getLogger("rpc-client")

# Status codes that mean "the server/connection is broken" -> trigger failover.
CONNECTION_ERRORS = {grpc.StatusCode.UNAVAILABLE, grpc.StatusCode.DEADLINE_EXCEEDED}
# Status codes that mean "temporary, try again" but the server is still healthy.
TRANSIENT_ERRORS = {grpc.StatusCode.ABORTED, grpc.StatusCode.RESOURCE_EXHAUSTED,
                    grpc.StatusCode.UNKNOWN, grpc.StatusCode.INTERNAL}


class Attempt:
    """One try of one request (shown in the terminal and on the dashboard)."""

    def __init__(self, number, server_id, address, outcome, code="", message="",
                 duration_ms=0.0):
        self.number = number
        self.server_id = server_id
        self.address = address
        self.outcome = outcome          # SUCCESS | RETRY | FAILED | NO_SERVER
        self.code = code.name if hasattr(code, "name") else str(code or "")
        self.message = str(message or "")
        self.duration_ms = duration_ms

    def to_dict(self):
        code_str = self.code.name if hasattr(self.code, "name") else str(self.code or "")
        return {"attempt": self.number, "server_id": self.server_id,
                "address": self.address, "outcome": self.outcome,
                "code": code_str, "message": self.message,
                "duration_ms": round(self.duration_ms, 1)}

    def __str__(self):
        return (f"attempt {self.number} -> {self.server_id or '-'} "
                f"[{self.outcome}] {self.code} {self.message}")


class RpcResult:
    def __init__(self, request_id, operation, payload):
        self.request_id = request_id
        self.operation = operation
        self.payload = payload
        self.success = False
        self.server_id = None
        self.result = ""
        self.message = ""
        self.error_type = ""            # RETRY_LIMIT | PERMANENT | NO_SERVER
        self.attempts = []
        self.total_ms = 0.0

    def to_dict(self):
        return {"request_id": self.request_id, "operation": self.operation,
                "payload": self.payload, "success": self.success,
                "server_id": self.server_id, "result": self.result,
                "message": self.message, "error_type": self.error_type,
                "total_ms": round(self.total_ms, 1),
                "attempts": [a.to_dict() for a in self.attempts]}


class RpcClient:
    def __init__(self, registry, load_balancer=None, failover=None,
                 max_attempts=MAX_ATTEMPTS, retry_delay=RETRY_DELAY,
                 timeout=RPC_TIMEOUT, stub_factory=None, logger=None):
        self.registry = registry
        self.load_balancer = load_balancer or RoundRobinLoadBalancer(registry)
        self.failover = failover or FailoverManager(registry, self.load_balancer)
        self.max_attempts = max_attempts
        self.retry_delay = retry_delay
        self.timeout = timeout
        self.log = logger or LOG
        self._channels = {}
        self._stub_factory = stub_factory or self._default_stub

    # ----------------------------------------------------------- transport
    def _default_stub(self, address):
        channel = self._channels.get(address)
        if channel is None:
            channel = grpc.insecure_channel(address)
            self._channels[address] = channel
        return service_pb2_grpc.ComputeServiceStub(channel)

    def close(self):
        for channel in self._channels.values():
            try:
                channel.close()
            except Exception:
                pass
        self._channels.clear()

    # --------------------------------------------------------------- call
    def call(self, operation="echo", payload="", request_id=None):
        request_id = request_id or uuid.uuid4().hex[:8]
        result = RpcResult(request_id, operation, payload)
        self.registry.record_request()
        started = time.perf_counter()
        tried = set()

        for attempt_no in range(1, self.max_attempts + 1):
            if attempt_no > 1:
                self.registry.record_retry()
                delay = self.retry_delay * (attempt_no - 1)
                self.log.info("[%s] retrying in %.2fs (attempt %d/%d)",
                              request_id, delay, attempt_no, self.max_attempts)
                time.sleep(delay)

            server = self.failover.pick(exclude=tried)
            if server is None:
                # maybe every healthy server already failed for this request
                server = self.failover.pick(exclude=None)
            if server is None:
                attempt = Attempt(attempt_no, None, "", "NO_SERVER",
                                  "NO_SERVER", "no healthy backend available")
                result.attempts.append(attempt)
                self.log.error("[%s] %s", request_id, attempt)
                continue

            tried.add(server.server_id)
            attempt = self._try_once(server, request_id, operation, payload, attempt_no)
            result.attempts.append(attempt)

            if attempt.outcome == "SUCCESS":
                result.success = True
                result.server_id = server.server_id
                result.result = attempt.message
                result.message = f"handled by {server.server_id}"
                break
            if attempt.outcome == "FAILED":                 # permanent error
                result.error_type = "PERMANENT"
                result.message = f"{attempt.code}: {attempt.message}"
                break

        result.total_ms = (time.perf_counter() - started) * 1000.0

        if result.success:
            self.registry.record_success(result.server_id)
            self.registry.log_event(
                "SUCCESS",
                f"request {request_id} ({operation}) OK on {result.server_id} "
                f"after {len(result.attempts)} attempt(s)")
        else:
            self.registry.record_final_failure()
            if not result.error_type:
                result.error_type = ("NO_SERVER"
                                     if all(a.outcome == "NO_SERVER" for a in result.attempts)
                                     else "RETRY_LIMIT")
                last = result.attempts[-1] if result.attempts else None
                result.message = (f"{last.code}: {last.message}" if last
                                  else "no attempt was made")
            self.registry.log_event(
                "ERROR",
                f"request {request_id} ({operation}) FAILED [{result.error_type}] "
                f"after {len(result.attempts)} attempt(s): {result.message}")
        return result

    # ----------------------------------------------------------- one try
    def _try_once(self, server, request_id, operation, payload, attempt_no):
        self.registry.record_attempt(server.server_id)
        request = service_pb2.ComputeRequest(
            request_id=request_id, operation=operation,
            payload=str(payload), attempt=attempt_no)
        started = time.perf_counter()
        try:
            stub = self._stub_factory(server.address)
            response = stub.Execute(request, timeout=self.timeout)
        except grpc.RpcError as exc:
            elapsed = (time.perf_counter() - started) * 1000.0
            code = self._code(exc)
            details = self._details(exc)
            self.registry.record_attempt_failure(server.server_id, f"{code}: {details}")

            if code in CONNECTION_ERRORS:
                # connection-level problem -> take the server out of the pool
                self.failover.mark_failed(server.server_id, f"{code}: {details}")
                outcome = "RETRY"
            elif code in TRANSIENT_ERRORS:
                outcome = "RETRY"
            else:
                outcome = "FAILED"                      # permanent, do not retry

            attempt = Attempt(attempt_no, server.server_id, server.address,
                              outcome, code, details, elapsed)
            self.log.warning("[%s] %s", request_id, attempt)
            return attempt
        except Exception as exc:                        # pragma: no cover
            elapsed = (time.perf_counter() - started) * 1000.0
            attempt = Attempt(attempt_no, server.server_id, server.address,
                              "RETRY", "CLIENT_ERROR", str(exc), elapsed)
            self.log.error("[%s] %s", request_id, attempt)
            return attempt

        elapsed = (time.perf_counter() - started) * 1000.0
        attempt = Attempt(attempt_no, server.server_id, server.address,
                          "SUCCESS", "OK", response.result, elapsed)
        self.log.info("[%s] %s", request_id, attempt)
        return attempt

    @staticmethod
    def _code(exc):
        try:
            return exc.code()
        except Exception:
            return grpc.StatusCode.UNKNOWN

    @staticmethod
    def _details(exc):
        try:
            return exc.details() or ""
        except Exception:
            return str(exc)


# --------------------------------------------------------------------------
# Standalone CLI:  python client/rpc_client.py --operation sum --payload 1,2,3 -n 6
# --------------------------------------------------------------------------
def main():
    parser = argparse.ArgumentParser(description="Fault-tolerant RPC client")
    parser.add_argument("--operation", "-o", default="echo",
                        help="echo | sum | square | slow | flaky | boom")
    parser.add_argument("--payload", "-p", default="hello")
    parser.add_argument("--count", "-n", type=int, default=6)
    parser.add_argument("--delay", "-d", type=float, default=0.2,
                        help="seconds between requests")
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO,
                        format="%(asctime)s [client] %(levelname)-7s %(message)s",
                        datefmt="%H:%M:%S")

    registry = ServerRegistry()
    elector = LeaderElector(registry)
    monitor = HealthMonitor(registry, leader_elector=elector)
    monitor.check_all()          # one synchronous sweep so we know who is up
    monitor.check_all()          # second sweep promotes RECOVERING -> ONLINE
    monitor.start()

    client = RpcClient(registry)
    print("\nAvailable servers:",
          [s.server_id for s in registry.available()] or "NONE")
    print("Current leader   :", registry.leader_id, "\n")

    ok = 0
    try:
        for i in range(1, args.count + 1):
            result = client.call(args.operation, args.payload)
            ok += 1 if result.success else 0
            flag = "OK " if result.success else "FAIL"
            print(f"[{flag}] #{i} id={result.request_id} server={result.server_id} "
                  f"result={result.result or result.message} "
                  f"attempts={len(result.attempts)} ({result.total_ms:.0f} ms)")
            time.sleep(args.delay)
    except KeyboardInterrupt:
        pass
    finally:
        monitor.stop()
        client.close()

    totals = registry.snapshot()["totals"]
    print("\n--- summary ---")
    print(f"requests={totals['requests']} success={totals['success']} "
          f"failed={totals['failed']} retries={totals['retries']} "
          f"failovers={totals['failovers']}")
    for state in registry.all():
        print(f"  {state.server_id:<9} {state.status:<10} "
              f"handled={state.successes} failures={state.failures}")


if __name__ == "__main__":
    main()
