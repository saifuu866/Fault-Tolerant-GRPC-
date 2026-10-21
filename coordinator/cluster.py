"""Shared, thread-safe state of the cluster: server registry + metrics + events.

Every component (health monitor, load balancer, client, dashboard) reads and
writes through this single object, which is why the dashboard always shows the
*real* state of the system.
"""
import os
import sys
import threading
import time
from collections import OrderedDict, deque

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)

from servers.server_config import SERVERS  # noqa: E402

STATUS_ONLINE = "ONLINE"
STATUS_OFFLINE = "OFFLINE"
STATUS_RECOVERING = "RECOVERING"
STATUS_UNKNOWN = "UNKNOWN"


class ServerState:
    """Everything the coordinator knows about one backend server."""

    def __init__(self, server_id, host, port):
        self.server_id = server_id
        self.host = host
        self.port = port
        self.status = STATUS_UNKNOWN
        self.requests = 0        # attempts routed to this server
        self.successes = 0
        self.failures = 0
        self.last_error = ""
        self.last_check = 0.0
        self.latency_ms = 0.0
        self.is_leader = False
        self.consecutive_ok = 0
        self.consecutive_fail = 0
        self.reported_handled = 0

    @property
    def address(self):
        return f"{self.host}:{self.port}"

    def to_dict(self):
        return {
            "server_id": self.server_id,
            "address": self.address,
            "status": self.status,
            "requests": self.requests,
            "successes": self.successes,
            "failures": self.failures,
            "last_error": self.last_error,
            "latency_ms": round(self.latency_ms, 1),
            "is_leader": self.is_leader,
            "reported_handled": self.reported_handled,
            "last_check": self.last_check,
        }


class ServerRegistry:
    """Thread-safe registry + global counters + rolling event log."""

    def __init__(self, servers=None, event_log_size=120):
        self._lock = threading.RLock()
        self._servers = OrderedDict()
        for cfg in (servers or SERVERS):
            self._servers[cfg["id"]] = ServerState(cfg["id"], cfg["host"], cfg["port"])
        self.events = deque(maxlen=event_log_size)
        self.totals = {"requests": 0, "success": 0, "failed": 0,
                       "retries": 0, "failovers": 0}
        self.leader_id = None
        self.started_at = time.time()

    # ------------------------------------------------------------- lookups
    def ids(self):
        with self._lock:
            return list(self._servers.keys())

    def get(self, server_id):
        with self._lock:
            return self._servers.get(server_id)

    def all(self):
        with self._lock:
            return list(self._servers.values())

    def available(self):
        """Servers that may receive traffic (ONLINE only).
        RECOVERING servers are on probation and are kept out of the pool."""
        with self._lock:
            return [s for s in self._servers.values() if s.status == STATUS_ONLINE]

    # -------------------------------------------------------------- events
    def log_event(self, level, message):
        with self._lock:
            self.events.appendleft({
                "time": time.strftime("%H:%M:%S"),
                "level": level,
                "message": message,
            })

    # -------------------------------------------------------------- status
    def set_status(self, server_id, status, error=""):
        """Returns True if the status actually changed (used to avoid log spam)."""
        with self._lock:
            state = self._servers.get(server_id)
            if state is None:
                return False
            changed = state.status != status
            state.status = status
            if error:
                state.last_error = error
            if changed:
                level = {"ONLINE": "SUCCESS", "OFFLINE": "ERROR"}.get(status, "WARN")
                self.log_event(level, f"{server_id} -> {status}"
                                      + (f" ({error})" if error else ""))
            return changed

    def set_leader(self, server_id):
        with self._lock:
            self.leader_id = server_id
            for state in self._servers.values():
                state.is_leader = (state.server_id == server_id)

    # ------------------------------------------------------------- metrics
    def record_request(self):
        with self._lock:
            self.totals["requests"] += 1

    def record_attempt(self, server_id):
        with self._lock:
            state = self._servers.get(server_id)
            if state:
                state.requests += 1

    def record_success(self, server_id):
        with self._lock:
            self.totals["success"] += 1
            state = self._servers.get(server_id)
            if state:
                state.successes += 1
                state.last_error = ""

    def record_attempt_failure(self, server_id, error):
        with self._lock:
            state = self._servers.get(server_id)
            if state:
                state.failures += 1
                state.last_error = error

    def record_retry(self):
        with self._lock:
            self.totals["retries"] += 1

    def record_failover(self):
        with self._lock:
            self.totals["failovers"] += 1

    def record_final_failure(self):
        with self._lock:
            self.totals["failed"] += 1

    def reset_metrics(self):
        with self._lock:
            for key in self.totals:
                self.totals[key] = 0
            for state in self._servers.values():
                state.requests = state.successes = state.failures = 0
                state.last_error = ""
            self.events.clear()
            self.log_event("INFO", "metrics reset")

    # ------------------------------------------------------------ snapshot
    def snapshot(self):
        with self._lock:
            return {
                "servers": [s.to_dict() for s in self._servers.values()],
                "totals": dict(self.totals),
                "leader": self.leader_id,
                "events": list(self.events),
                "uptime_seconds": round(time.time() - self.started_at, 1),
                "online_count": len([s for s in self._servers.values()
                                     if s.status == STATUS_ONLINE]),
                "server_count": len(self._servers),
            }
