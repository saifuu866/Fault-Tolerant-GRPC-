"""Round-robin load balancer.

Algorithm
---------
Servers are kept in a fixed order (server-1, server-2, server-3...).
A pointer remembers where the previous selection stopped.  On each call we walk
the list starting at the pointer and return the *first* server that is
(a) ONLINE and (b) not in the `exclude` set (servers already tried for this
request).  The pointer then moves one slot past the chosen server.

Walking instead of a plain `pointer % len` is what lets us *skip* unavailable
servers while still giving a fair, repeating rotation.
"""
import threading

from coordinator.cluster import STATUS_ONLINE


class RoundRobinLoadBalancer:
    def __init__(self, registry):
        self._registry = registry
        self._pointer = 0
        self._lock = threading.Lock()

    def select(self, exclude=None):
        """Return a ServerState or None if nothing is available."""
        excluded = set(exclude or ())
        with self._lock:
            ids = self._registry.ids()
            count = len(ids)
            if count == 0:
                return None
            for offset in range(count):
                index = (self._pointer + offset) % count
                state = self._registry.get(ids[index])
                if state is None:
                    continue
                if state.status == STATUS_ONLINE and state.server_id not in excluded:
                    self._pointer = (index + 1) % count
                    return state
        return None

    def reset(self):
        with self._lock:
            self._pointer = 0
