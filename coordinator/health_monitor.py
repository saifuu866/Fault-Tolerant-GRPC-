"""Health monitor: a background thread that pings every backend.

State machine per server
------------------------
    OFFLINE --(RECOVERY_THRESHOLD good probes)--> ONLINE
       ^                     |
       |              1 good probe -> RECOVERING (probation, no traffic yet)
       |
    ONLINE --(FAILURE_THRESHOLD failed probes)--> OFFLINE
"""
import logging
import threading
import time

import grpc

from proto_stubs import service_pb2, service_pb2_grpc
from coordinator.cluster import (STATUS_OFFLINE, STATUS_ONLINE, STATUS_RECOVERING)
from servers.server_config import (FAILURE_THRESHOLD, HEALTH_INTERVAL,
                                   HEALTH_TIMEOUT, RECOVERY_THRESHOLD)

LOG = logging.getLogger("health-monitor")


def default_stub_factory():
    """Returns a callable address -> stub, reusing one channel per address."""
    channels = {}
    lock = threading.Lock()

    def factory(address):
        with lock:
            channel = channels.get(address)
            if channel is None:
                channel = grpc.insecure_channel(address)
                channels[address] = channel
            return service_pb2_grpc.ComputeServiceStub(channel)

    factory.channels = channels
    return factory


class HealthMonitor(threading.Thread):
    def __init__(self, registry, interval=HEALTH_INTERVAL, timeout=HEALTH_TIMEOUT,
                 failure_threshold=FAILURE_THRESHOLD,
                 recovery_threshold=RECOVERY_THRESHOLD,
                 leader_elector=None, stub_factory=None):
        super().__init__(daemon=True, name="health-monitor")
        self.registry = registry
        self.interval = interval
        self.timeout = timeout
        self.failure_threshold = failure_threshold
        self.recovery_threshold = recovery_threshold
        self.leader_elector = leader_elector
        self.stub_factory = stub_factory or default_stub_factory()
        self._stop_event = threading.Event()

    # ------------------------------------------------------------------ run
    def run(self):
        LOG.info("health monitor started (every %.1fs)", self.interval)
        while not self._stop_event.is_set():
            try:
                self.check_all()
            except Exception as exc:                      # never kill the thread
                LOG.exception("health sweep error: %s", exc)
            self._stop_event.wait(self.interval)
        LOG.info("health monitor stopped")

    def stop(self):
        self._stop_event.set()

    # ------------------------------------------------------------ one sweep
    def check_all(self):
        """One full sweep over all servers (also callable synchronously)."""
        for state in self.registry.all():
            self._probe(state)
        if self.leader_elector is not None:
            self.leader_elector.ensure_leader()

    def _probe(self, state):
        start = time.perf_counter()
        try:
            stub = self.stub_factory(state.address)
            response = stub.HealthCheck(
                service_pb2.HealthRequest(source="health-monitor"),
                timeout=self.timeout)
        except grpc.RpcError as exc:
            self._on_failure(state, self._describe(exc))
        except Exception as exc:                          # bad address, etc.
            self._on_failure(state, str(exc))
        else:
            state.latency_ms = (time.perf_counter() - start) * 1000.0
            state.reported_handled = response.handled_requests
            self._on_success(state)
        finally:
            state.last_check = time.time()

    def _on_success(self, state):
        state.consecutive_fail = 0
        state.consecutive_ok += 1
        if state.status == STATUS_ONLINE:
            return
        if state.consecutive_ok >= self.recovery_threshold:
            self.registry.set_status(state.server_id, STATUS_ONLINE)
            self.registry.log_event(
                "SUCCESS", f"{state.server_id} recovered and re-added to the pool")
        else:
            self.registry.set_status(state.server_id, STATUS_RECOVERING)

    def _on_failure(self, state, error):
        state.consecutive_ok = 0
        state.consecutive_fail += 1
        if state.consecutive_fail >= self.failure_threshold:
            if state.status != STATUS_OFFLINE:
                self.registry.set_status(state.server_id, STATUS_OFFLINE, error=error)
                self.registry.log_event(
                    "ERROR", f"health check failed for {state.server_id}: {error}")
            else:
                state.last_error = error

    @staticmethod
    def _describe(exc):
        try:
            return f"{exc.code().name}: {exc.details()}"
        except Exception:                                 # pragma: no cover
            return str(exc)
