"""Failover manager.

Responsibilities
----------------
1. `pick()`  - ask the load balancer for a healthy server, excluding the ones
               that already failed during the current request.
2. `mark_failed()` - when a connection-level error happens, immediately take the
               server out of the pool (status OFFLINE).  The health monitor will
               put it back automatically once it answers health checks again.
"""
from coordinator.cluster import STATUS_OFFLINE


class FailoverManager:
    def __init__(self, registry, load_balancer, logger=None):
        self.registry = registry
        self.load_balancer = load_balancer
        self.logger = logger

    def pick(self, exclude=None):
        """Next healthy server for this request (None = nothing available)."""
        return self.load_balancer.select(exclude=exclude)

    def mark_failed(self, server_id, error):
        """Take a server out of rotation after a connection failure."""
        changed = self.registry.set_status(server_id, STATUS_OFFLINE, error=error)
        self.registry.record_failover()
        if changed:
            self.registry.log_event(
                "ERROR", f"FAILOVER: {server_id} removed from pool ({error})")
        if self.logger:
            self.logger.warning("failover: %s marked OFFLINE (%s)", server_id, error)
        return changed
