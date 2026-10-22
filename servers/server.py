"""Backend gRPC server.

Each instance runs on its own port and implements ComputeService.
It can also *simulate* a crash (Admin KILL) so that failover can be demonstrated
without actually killing the OS process.

Usage:
    python servers/server.py --id server-1
    python servers/server.py --id server-2
    python servers/server.py --id server-3
"""
import argparse
import logging
import os
import random
import sys
import threading
import time
from concurrent import futures

import grpc

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)

from proto_stubs import service_pb2, service_pb2_grpc          # noqa: E402
from servers.server_config import DEFAULT_HOST, get_server     # noqa: E402


class PermanentError(Exception):
    """Application error that must NOT be retried (bad input, unknown op...)."""


class TransientError(Exception):
    """Temporary error that SHOULD be retried (server is still healthy)."""


class ComputeServicer(service_pb2_grpc.ComputeServiceServicer):
    """Implements the three RPCs plus the simulated-failure switch."""

    def __init__(self, server_id, logger):
        self.server_id = server_id
        self.log = logger
        self._lock = threading.Lock()
        self.alive = True          # False  => behaves like a crashed server
        self.is_leader = False
        self.fail_rate = 0.0       # probability of injecting a transient error
        self.handled = 0
        self.started_at = time.time()
        self._flaky_counters = {}  # request_id -> how many times it already failed

    # ------------------------------------------------------------------ RPCs
    def Execute(self, request, context):
        started = time.perf_counter()

        with self._lock:
            alive, fail_rate = self.alive, self.fail_rate

        if not alive:
            self.log.warning("DOWN: rejecting %s (simulated crash)", request.request_id)
            context.abort(grpc.StatusCode.UNAVAILABLE,
                          f"{self.server_id} is DOWN (simulated failure)")

        if fail_rate > 0 and random.random() < fail_rate:
            self.log.warning("injected transient failure for %s", request.request_id)
            context.abort(grpc.StatusCode.ABORTED,
                          f"{self.server_id}: injected transient failure")

        operation = (request.operation or "echo").strip().lower()
        try:
            result = self._compute(operation, request)
        except PermanentError as err:
            self.log.error("permanent error for %s: %s", request.request_id, err)
            # INVALID_ARGUMENT tells the client: do not retry, this will never work.
            context.abort(grpc.StatusCode.INVALID_ARGUMENT, str(err))
        except TransientError as err:
            self.log.warning("transient error for %s: %s", request.request_id, err)
            # ABORTED tells the client: retry, but the server itself is fine.
            context.abort(grpc.StatusCode.ABORTED, str(err))

        with self._lock:
            self.handled += 1
            handled = self.handled

        elapsed = (time.perf_counter() - started) * 1000.0
        self.log.info("OK  %-10s op=%-6s attempt=%d -> %s (total handled=%d)",
                      request.request_id, operation, request.attempt, result, handled)
        return service_pb2.ComputeResponse(
            request_id=request.request_id,
            server_id=self.server_id,
            result=str(result),
            status="OK",
            message=f"processed by {self.server_id}",
            processing_ms=elapsed,
            attempt=request.attempt,
        )

    def HealthCheck(self, request, context):
        with self._lock:
            alive, leader, handled = self.alive, self.is_leader, self.handled
        if not alive:
            # A "crashed" server must look unreachable to the health monitor.
            context.abort(grpc.StatusCode.UNAVAILABLE,
                          f"{self.server_id} is DOWN (simulated failure)")
        return service_pb2.HealthResponse(
            server_id=self.server_id,
            healthy=True,
            is_leader=leader,
            handled_requests=handled,
            uptime_seconds=time.time() - self.started_at,
        )

    def Admin(self, request, context):
        """Control channel. Works even while the server is 'crashed',
        otherwise we could never revive it from the dashboard."""
        command = (request.command or "").strip().upper()
        message = ""
        ok = True
        with self._lock:
            if command == "KILL":
                self.alive = False
                message = "server marked DOWN (simulated crash)"
            elif command == "REVIVE":
                self.alive = True
                self._flaky_counters.clear()
                message = "server marked UP"
            elif command == "SET_LEADER":
                self.is_leader = True
                message = "this server is now the leader"
            elif command == "CLEAR_LEADER":
                self.is_leader = False
                message = "leadership cleared"
            elif command == "SET_FAIL_RATE":
                try:
                    rate = float(request.value)
                except ValueError:
                    ok, message = False, "value must be a float between 0 and 1"
                else:
                    self.fail_rate = min(max(rate, 0.0), 1.0)
                    message = f"transient failure rate = {self.fail_rate}"
            elif command == "STATUS":
                message = (f"alive={self.alive} leader={self.is_leader} "
                           f"handled={self.handled} fail_rate={self.fail_rate}")
            else:
                ok, message = False, f"unknown command '{command}'"
        self.log.info("ADMIN %s -> %s", command, message)
        return service_pb2.AdminResponse(server_id=self.server_id, ok=ok, message=message)

    # ------------------------------------------------------- business logic
    def _compute(self, operation, request):
        """Tiny 'business logic'. Each branch is deliberately simple."""
        payload = request.payload or ""

        if operation == "echo":
            return f"echo:{payload}"

        if operation == "sum":
            try:
                numbers = [int(x) for x in payload.replace(" ", "").split(",") if x]
            except ValueError:
                raise PermanentError("sum expects comma separated integers, e.g. '1,2,3'")
            return sum(numbers)

        if operation == "square":
            try:
                value = int(payload)
            except ValueError:
                raise PermanentError("square expects one integer")
            return value * value

        if operation == "slow":
            try:
                seconds = float(payload) if payload else 0.5
            except ValueError:
                raise PermanentError("slow expects a number of seconds")
            seconds = min(seconds, 10.0)
            time.sleep(seconds)
            return f"slept {seconds}s"

        if operation == "flaky":
            # Fails the first N attempts of a given request_id, then succeeds.
            try:
                needed = int(payload) if payload else 1
            except ValueError:
                raise PermanentError("flaky expects an integer failure count")
            with self._lock:
                count = self._flaky_counters.get(request.request_id, 0) + 1
                self._flaky_counters[request.request_id] = count
            effective_attempt = request.attempt if getattr(request, "attempt", 0) > 0 else count
            if effective_attempt <= needed:
                raise TransientError(
                    f"{self.server_id}: temporary glitch ({effective_attempt}/{needed})")
            return f"flaky recovered after {effective_attempt - 1} failure(s)"

        if operation == "boom":
            raise PermanentError("operation 'boom' always fails permanently")

        raise PermanentError(f"unknown operation '{operation}'")


def serve(server_id, host, port, workers=8):
    logging.basicConfig(
        level=logging.INFO,
        format=f"%(asctime)s [{server_id}] %(levelname)-7s %(message)s",
        datefmt="%H:%M:%S",
    )
    log = logging.getLogger(server_id)

    grpc_server = grpc.server(futures.ThreadPoolExecutor(max_workers=workers))
    servicer = ComputeServicer(server_id, log)
    service_pb2_grpc.add_ComputeServiceServicer_to_server(servicer, grpc_server)

    bind = f"{host}:{port}"
    try:
        grpc_server.add_insecure_port(bind)
    except Exception as exc:  # pragma: no cover
        log.error("cannot bind %s: %s", bind, exc)
        return 1

    grpc_server.start()
    log.info("listening on %s (Ctrl+C to stop = real crash simulation)", bind)
    try:
        grpc_server.wait_for_termination()
    except KeyboardInterrupt:
        log.info("shutting down...")
        grpc_server.stop(grace=1.0)
    return 0


def main():
    parser = argparse.ArgumentParser(description="Fault-tolerant RPC backend server")
    parser.add_argument("--id", default="server-1", help="server id from server_config.py")
    parser.add_argument("--host", default=None)
    parser.add_argument("--port", type=int, default=None)
    args = parser.parse_args()

    cfg = get_server(args.id) or {}
    host = args.host or cfg.get("host", DEFAULT_HOST)
    port = args.port or cfg.get("port")
    if port is None:
        parser.error(f"unknown server id '{args.id}' - pass --port explicitly")
    return serve(args.id, host, port)


if __name__ == "__main__":
    sys.exit(main())
