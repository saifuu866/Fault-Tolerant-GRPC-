"""Unit tests for load balancing, retry behaviour, failover, health monitoring
and leader election.

They use FAKE gRPC stubs, so no backend server has to be running:
    python -m unittest discover -s tests -v
"""
import os
import sys
import unittest

import grpc

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)

from proto_stubs import service_pb2                                   # noqa: E402
from client.rpc_client import RpcClient                               # noqa: E402
from coordinator.cluster import (STATUS_OFFLINE, STATUS_ONLINE,       # noqa: E402
                                 STATUS_RECOVERING, ServerRegistry)
from coordinator.failover_manager import FailoverManager              # noqa: E402
from coordinator.health_monitor import HealthMonitor                  # noqa: E402
from coordinator.leader_election import LeaderElector                 # noqa: E402
from coordinator.load_balancer import RoundRobinLoadBalancer          # noqa: E402

TEST_SERVERS = [
    {"id": "server-1", "host": "127.0.0.1", "port": 50051},
    {"id": "server-2", "host": "127.0.0.1", "port": 50052},
    {"id": "server-3", "host": "127.0.0.1", "port": 50053},
]
ADDRESS_TO_ID = {f"127.0.0.1:{s['port']}": s["id"] for s in TEST_SERVERS}


# --------------------------------------------------------------------- fakes
class FakeRpcError(grpc.RpcError):
    def __init__(self, code, details):
        super().__init__(details)
        self._code = code
        self._details = details

    def code(self):
        return self._code

    def details(self):
        return self._details


class FakeStub:
    """Behaviour is driven by a per-address 'script' dictionary."""

    def __init__(self, server_id, behaviour, calls):
        self.server_id = server_id
        self.behaviour = behaviour          # callable(request) -> response | raises
        self.calls = calls

    def Execute(self, request, timeout=None):
        self.calls.append((self.server_id, request.request_id, request.attempt))
        return self.behaviour(self.server_id, request)

    def HealthCheck(self, request, timeout=None):
        return self.behaviour(self.server_id, request)


def make_factory(behaviour, calls):
    def factory(address):
        return FakeStub(ADDRESS_TO_ID[address], behaviour, calls)
    return factory


def ok_response(server_id, request):
    return service_pb2.ComputeResponse(
        request_id=request.request_id, server_id=server_id,
        result=f"{server_id}:done", status="OK", attempt=request.attempt)


def build_cluster(all_online=True):
    registry = ServerRegistry(servers=TEST_SERVERS)
    if all_online:
        for state in registry.all():
            registry.set_status(state.server_id, STATUS_ONLINE)
    load_balancer = RoundRobinLoadBalancer(registry)
    failover = FailoverManager(registry, load_balancer)
    return registry, load_balancer, failover


# ------------------------------------------------------------ load balancing
class TestLoadBalancing(unittest.TestCase):
    def test_round_robin_cycles_through_all_servers(self):
        registry, lb, _ = build_cluster()
        picked = [lb.select().server_id for _ in range(6)]
        self.assertEqual(picked, ["server-1", "server-2", "server-3",
                                  "server-1", "server-2", "server-3"])

    def test_offline_servers_are_skipped(self):
        registry, lb, _ = build_cluster()
        registry.set_status("server-2", STATUS_OFFLINE)
        picked = [lb.select().server_id for _ in range(4)]
        self.assertNotIn("server-2", picked)
        self.assertEqual(picked, ["server-1", "server-3", "server-1", "server-3"])

    def test_recovering_server_is_not_in_the_pool(self):
        registry, lb, _ = build_cluster()
        registry.set_status("server-1", STATUS_RECOVERING)
        picked = {lb.select().server_id for _ in range(4)}
        self.assertEqual(picked, {"server-2", "server-3"})

    def test_select_returns_none_when_everything_is_down(self):
        registry, lb, _ = build_cluster()
        for state in registry.all():
            registry.set_status(state.server_id, STATUS_OFFLINE)
        self.assertIsNone(lb.select())

    def test_requests_are_distributed_evenly(self):
        registry, lb, failover = build_cluster()
        calls = []
        client = RpcClient(registry, lb, failover,
                           stub_factory=make_factory(ok_response, calls))
        for _ in range(6):
            self.assertTrue(client.call("echo", "x").success)
        counts = {s.server_id: s.successes for s in registry.all()}
        self.assertEqual(counts, {"server-1": 2, "server-2": 2, "server-3": 2})


# -------------------------------------------------------------------- retry
class TestRetry(unittest.TestCase):
    def test_retries_until_success(self):
        registry, lb, failover = build_cluster()
        calls = []
        state = {"n": 0}

        def behaviour(server_id, request):
            state["n"] += 1
            if state["n"] <= 2:                      # two transient failures
                raise FakeRpcError(grpc.StatusCode.ABORTED, "temporary glitch")
            return ok_response(server_id, request)

        client = RpcClient(registry, lb, failover, retry_delay=0.01,
                           stub_factory=make_factory(behaviour, calls))
        result = client.call("flaky", "2")

        self.assertTrue(result.success)
        self.assertEqual(len(result.attempts), 3)
        self.assertEqual([a.outcome for a in result.attempts],
                         ["RETRY", "RETRY", "SUCCESS"])
        self.assertEqual(registry.snapshot()["totals"]["retries"], 2)

    def test_stops_after_retry_limit(self):
        registry, lb, failover = build_cluster()

        def behaviour(server_id, request):
            raise FakeRpcError(grpc.StatusCode.ABORTED, "still broken")

        client = RpcClient(registry, lb, failover, retry_delay=0.01,
                           stub_factory=make_factory(behaviour, []))
        result = client.call("flaky", "9")

        self.assertFalse(result.success)
        self.assertEqual(result.error_type, "RETRY_LIMIT")
        self.assertEqual(len(result.attempts), 3)          # MAX_ATTEMPTS
        self.assertEqual(registry.snapshot()["totals"]["failed"], 1)

    def test_permanent_error_is_not_retried(self):
        registry, lb, failover = build_cluster()

        def behaviour(server_id, request):
            raise FakeRpcError(grpc.StatusCode.INVALID_ARGUMENT, "bad input")

        client = RpcClient(registry, lb, failover, retry_delay=0.01,
                           stub_factory=make_factory(behaviour, []))
        result = client.call("boom", "")

        self.assertFalse(result.success)
        self.assertEqual(result.error_type, "PERMANENT")
        self.assertEqual(len(result.attempts), 1)          # no retry at all

    def test_transient_failure_does_not_take_server_offline(self):
        registry, lb, failover = build_cluster()

        def behaviour(server_id, request):
            if server_id == "server-1":
                raise FakeRpcError(grpc.StatusCode.ABORTED, "glitch")
            return ok_response(server_id, request)

        client = RpcClient(registry, lb, failover, retry_delay=0.01,
                           stub_factory=make_factory(behaviour, []))
        self.assertTrue(client.call("echo", "x").success)
        self.assertEqual(registry.get("server-1").status, STATUS_ONLINE)


# ------------------------------------------------------------------ failover
class TestFailover(unittest.TestCase):
    def test_request_is_redirected_to_a_healthy_server(self):
        registry, lb, failover = build_cluster()

        def behaviour(server_id, request):
            if server_id == "server-1":
                raise FakeRpcError(grpc.StatusCode.UNAVAILABLE, "connection refused")
            return ok_response(server_id, request)

        client = RpcClient(registry, lb, failover, retry_delay=0.01,
                           stub_factory=make_factory(behaviour, []))
        result = client.call("echo", "x")

        self.assertTrue(result.success)
        self.assertEqual(result.server_id, "server-2")
        self.assertEqual(registry.get("server-1").status, STATUS_OFFLINE)
        self.assertGreaterEqual(registry.snapshot()["totals"]["failovers"], 1)

    def test_failed_server_leaves_the_pool_for_later_requests(self):
        registry, lb, failover = build_cluster()

        def behaviour(server_id, request):
            if server_id == "server-1":
                raise FakeRpcError(grpc.StatusCode.UNAVAILABLE, "down")
            return ok_response(server_id, request)

        client = RpcClient(registry, lb, failover, retry_delay=0.01,
                           stub_factory=make_factory(behaviour, []))
        client.call("echo", "a")
        used = {client.call("echo", str(i)).server_id for i in range(4)}
        self.assertNotIn("server-1", used)

    def test_failure_when_no_server_is_available(self):
        registry, lb, failover = build_cluster()

        def behaviour(server_id, request):
            raise FakeRpcError(grpc.StatusCode.UNAVAILABLE, "down")

        client = RpcClient(registry, lb, failover, retry_delay=0.01,
                           stub_factory=make_factory(behaviour, []))
        result = client.call("echo", "x")
        self.assertFalse(result.success)
        self.assertTrue(all(s.status == STATUS_OFFLINE for s in registry.all()))


# ------------------------------------------------------------ health monitor
class TestHealthMonitor(unittest.TestCase):
    def _monitor(self, registry, healthy_ids):
        def behaviour(server_id, request):
            if server_id not in healthy_ids:
                raise FakeRpcError(grpc.StatusCode.UNAVAILABLE, "no route")
            return service_pb2.HealthResponse(server_id=server_id, healthy=True)
        return HealthMonitor(registry, stub_factory=make_factory(behaviour, []))

    def test_detects_a_dead_server(self):
        registry, _, _ = build_cluster()
        monitor = self._monitor(registry, {"server-1", "server-3"})
        monitor.check_all()
        self.assertEqual(registry.get("server-2").status, STATUS_OFFLINE)
        self.assertEqual(registry.get("server-1").status, STATUS_ONLINE)

    def test_recovery_goes_through_recovering_then_online(self):
        registry, _, _ = build_cluster(all_online=False)
        healthy = set()
        monitor = self._monitor(registry, healthy)

        monitor.check_all()
        self.assertEqual(registry.get("server-1").status, STATUS_OFFLINE)

        healthy.add("server-1")                      # the server comes back
        monitor.check_all()
        self.assertEqual(registry.get("server-1").status, STATUS_RECOVERING)
        monitor.check_all()
        self.assertEqual(registry.get("server-1").status, STATUS_ONLINE)


# ---------------------------------------------------------- leader election
class TestLeaderElection(unittest.TestCase):
    def test_elects_first_online_server(self):
        registry, _, _ = build_cluster()
        sent = []
        elector = LeaderElector(registry,
                                admin_sender=lambda a, c, v="": (sent.append((a, c)), (True, "ok"))[1])
        self.assertEqual(elector.ensure_leader(), "server-1")
        self.assertTrue(registry.get("server-1").is_leader)

    def test_new_leader_elected_when_leader_dies(self):
        registry, _, _ = build_cluster()
        elector = LeaderElector(registry, admin_sender=lambda a, c, v="": (True, "ok"))
        elector.ensure_leader()
        registry.set_status("server-1", STATUS_OFFLINE)
        self.assertEqual(elector.ensure_leader(), "server-2")
        self.assertFalse(registry.get("server-1").is_leader)
        self.assertTrue(registry.get("server-2").is_leader)

    def test_no_leader_when_all_servers_are_down(self):
        registry, _, _ = build_cluster()
        elector = LeaderElector(registry, admin_sender=lambda a, c, v="": (True, "ok"))
        elector.ensure_leader()
        for state in registry.all():
            registry.set_status(state.server_id, STATUS_OFFLINE)
        self.assertIsNone(elector.ensure_leader())


if __name__ == "__main__":
    unittest.main(verbosity=2)
