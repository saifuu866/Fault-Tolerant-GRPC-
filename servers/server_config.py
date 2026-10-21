"""Cluster definition + all tunable constants (single source of truth)."""

DEFAULT_HOST = "127.0.0.1"

# The three backend servers. Add more entries here and everything else adapts.
SERVERS = [
    {"id": "server-1", "host": DEFAULT_HOST, "port": 50051},
    {"id": "server-2", "host": DEFAULT_HOST, "port": 50052},
    {"id": "server-3", "host": DEFAULT_HOST, "port": 50053},
]

# ---- client / retry tuning ----
MAX_ATTEMPTS = 3        # 1 initial try + up to 2 retries  (= "retry up to 3 times")
RETRY_DELAY = 0.4       # seconds; multiplied by the attempt number (linear backoff)
RPC_TIMEOUT = 3.0       # per-attempt deadline in seconds

# ---- health monitor tuning ----
HEALTH_INTERVAL = 2.0   # seconds between health sweeps
HEALTH_TIMEOUT = 1.0    # health RPC deadline
FAILURE_THRESHOLD = 1   # consecutive failed probes -> OFFLINE
RECOVERY_THRESHOLD = 2  # consecutive good probes   -> RECOVERING -> ONLINE

# ---- dashboard ----
DASHBOARD_HOST = "127.0.0.1"
DASHBOARD_PORT = 5000


def get_server(server_id):
    """Return the config dict of a server id, or None."""
    for cfg in SERVERS:
        if cfg["id"] == server_id:
            return cfg
    return None


def address_of(cfg):
    return f"{cfg['host']}:{cfg['port']}"
