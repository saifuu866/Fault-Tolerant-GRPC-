"""Flask dashboard: live system state + failure/recovery controls.

It hosts the coordinator in-process:
    ServerRegistry -> HealthMonitor -> LeaderElector
    RoundRobinLoadBalancer -> FailoverManager -> RpcClient
so everything shown on the page is the real state of the running cluster.
"""
import logging
import os
import sys

from flask import Flask, jsonify, render_template, request

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)

from client.rpc_client import RpcClient                                # noqa: E402
from coordinator.admin_client import send_admin                        # noqa: E402
from coordinator.cluster import ServerRegistry                         # noqa: E402
from coordinator.failover_manager import FailoverManager               # noqa: E402
from coordinator.health_monitor import HealthMonitor                   # noqa: E402
from coordinator.leader_election import LeaderElector                  # noqa: E402
from coordinator.load_balancer import RoundRobinLoadBalancer           # noqa: E402
from servers.server_config import DASHBOARD_HOST, DASHBOARD_PORT       # noqa: E402

logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s [dashboard] %(levelname)-7s %(message)s",
                    datefmt="%H:%M:%S")
LOG = logging.getLogger("dashboard")

app = Flask(__name__)

# ---------------------------------------------------------------- wiring
registry = ServerRegistry()
load_balancer = RoundRobinLoadBalancer(registry)
failover = FailoverManager(registry, load_balancer, logger=LOG)
elector = LeaderElector(registry)
monitor = HealthMonitor(registry, leader_elector=elector)
client = RpcClient(registry, load_balancer=load_balancer, failover=failover, logger=LOG)

monitor.check_all()      # immediate first picture of the cluster
monitor.start()
registry.log_event("INFO", "dashboard started, health monitor running")


# ---------------------------------------------------------------- routes
@app.route("/")
def index():
    return render_template("index.html")


@app.route("/api/status")
def api_status():
    """Full snapshot used by the auto-refreshing UI."""
    return jsonify(registry.snapshot())


@app.route("/api/request", methods=["POST"])
def api_request():
    """Send one or more RPCs through the fault-tolerant client."""
    data = request.get_json(silent=True) or {}
    operation = (data.get("operation") or "echo").strip()
    payload = str(data.get("payload", ""))
    try:
        count = max(1, min(int(data.get("count", 1)), 20))
    except (TypeError, ValueError):
        count = 1

    results = [client.call(operation, payload).to_dict() for _ in range(count)]
    return jsonify({"ok": True, "results": results})


@app.route("/api/servers/<server_id>/<action>", methods=["POST"])
def api_server_action(server_id, action):
    """action = kill | revive  (simulated crash / recovery)."""
    state = registry.get(server_id)
    if state is None:
        return jsonify({"ok": False, "message": f"unknown server '{server_id}'"}), 404

    action = action.lower()
    if action not in ("kill", "revive"):
        return jsonify({"ok": False, "message": "action must be kill or revive"}), 400

    command = "KILL" if action == "kill" else "REVIVE"
    ok, message = send_admin(state.address, command)

    if action == "kill":
        # take it out of the pool straight away, do not wait for the next probe
        registry.set_status(server_id, "OFFLINE", error="killed from dashboard")
        registry.log_event("WARN", f"SIMULATION: {server_id} killed from dashboard")
        if registry.leader_id == server_id:
            elector.force_reelection()
    else:
        registry.log_event("INFO", f"SIMULATION: revive sent to {server_id}")
        monitor.check_all()      # speed up the recovery transition

    return jsonify({"ok": ok, "message": message, "server_id": server_id})


@app.route("/api/servers/<server_id>/fail_rate", methods=["POST"])
def api_fail_rate(server_id):
    """Inject a probability of transient (retryable) errors on one server."""
    state = registry.get(server_id)
    if state is None:
        return jsonify({"ok": False, "message": "unknown server"}), 404
    data = request.get_json(silent=True) or {}
    rate = data.get("rate", 0)
    ok, message = send_admin(state.address, "SET_FAIL_RATE", rate)
    registry.log_event("WARN", f"{server_id} transient failure rate set to {rate}")
    return jsonify({"ok": ok, "message": message})


@app.route("/api/leader/kill", methods=["POST"])
def api_kill_leader():
    leader_id = registry.leader_id
    if not leader_id:
        return jsonify({"ok": False, "message": "there is no leader right now"}), 400
    state = registry.get(leader_id)
    send_admin(state.address, "KILL")
    registry.set_status(leader_id, "OFFLINE", error="leader killed from dashboard")
    registry.log_event("WARN", f"SIMULATION: leader {leader_id} killed")
    new_leader = elector.force_reelection()
    return jsonify({"ok": True, "old_leader": leader_id, "new_leader": new_leader})


@app.route("/api/leader/elect", methods=["POST"])
def api_force_election():
    return jsonify({"ok": True, "leader": elector.force_reelection()})


@app.route("/api/reset", methods=["POST"])
def api_reset():
    registry.reset_metrics()
    load_balancer.reset()
    return jsonify({"ok": True})


def main():
    LOG.info("dashboard on http://%s:%s", DASHBOARD_HOST, DASHBOARD_PORT)
    # use_reloader=False -> otherwise the monitor thread would start twice
    app.run(host=DASHBOARD_HOST, port=DASHBOARD_PORT, debug=False, use_reloader=False,
            threaded=True)


if __name__ == "__main__":
    main()
