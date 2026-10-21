"""Coordinator-managed leader election (DEMONSTRATION ONLY).

This is NOT a consensus protocol (no Raft/Paxos, no quorum, no split-brain
protection).  The coordinator simply keeps the leader's identity and applies a
deterministic rule:

    leader = the first ONLINE server in the configured order

If the current leader stops answering health checks, the next ONLINE server is
promoted and informed with an Admin(SET_LEADER) RPC.
"""
import logging

from coordinator.admin_client import send_admin

LOG = logging.getLogger("leader-election")


class LeaderElector:
    def __init__(self, registry, admin_sender=None):
        self.registry = registry
        self.admin_sender = admin_sender or send_admin

    def ensure_leader(self):
        """Called after every health sweep. Returns the current leader id."""
        current_id = self.registry.leader_id
        current = self.registry.get(current_id) if current_id else None

        if current is not None and current.status == "ONLINE":
            return current_id                                   # nothing to do

        candidates = self.registry.available()
        if not candidates:
            if current_id is not None:
                self.registry.set_leader(None)
                self.registry.log_event("ERROR", "no leader: all servers are down")
            return None

        new_leader = candidates[0]
        if current_id and current_id != new_leader.server_id:
            # best effort - the old leader is probably unreachable
            self.admin_sender(self.registry.get(current_id).address, "CLEAR_LEADER")

        ok, message = self.admin_sender(new_leader.address, "SET_LEADER")
        self.registry.set_leader(new_leader.server_id)
        if current_id:
            self.registry.log_event(
                "WARN", f"ELECTION: {current_id} lost leadership -> "
                        f"{new_leader.server_id} elected")
        else:
            self.registry.log_event(
                "INFO", f"ELECTION: {new_leader.server_id} elected as leader")
        LOG.info("leader = %s (notify ok=%s %s)", new_leader.server_id, ok, message)
        return new_leader.server_id

    def force_reelection(self):
        """Drop the current leader locally and elect again immediately."""
        old = self.registry.leader_id
        if old:
            self.registry.set_leader(None)
            self.registry.log_event("WARN", f"leader {old} dropped, re-electing")
        return self.ensure_leader()
