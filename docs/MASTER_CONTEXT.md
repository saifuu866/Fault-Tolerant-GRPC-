# Master Context: Fault-Tolerant Distributed RPC Framework

## 1. Executive Summary

The Fault-Tolerant Distributed RPC Framework is an active-active, high-availability distributed computing platform written in Python using gRPC and Protocol Buffers. It demonstrates production-grade distributed resilience patterns without requiring external infrastructure, container engines, or cloud dependencies.

The system addresses the Single Point of Failure (SPOF) problem by replicating business logic across three isolated backend server processes and placing them behind an intelligent coordination and client proxy layer.

Key capabilities include:
- Client-side round-robin load balancing with dynamic node filtering.
- Automated error classification distinguishing connection, transient, and permanent failures.
- Linear backoff retry loops with request-id stability for idempotency.
- Instant failover upon connection failure with automatic exclusion list routing.
- Continuous background health probing with a two-stage probationary recovery state machine.
- Coordinator-managed deterministic leader election.
- Real-time web observability and chaos engineering control dashboard.

---

## 2. Repository Layout & File Manifest

```
Fault-Tolerant-GRPC--main/
├── proto/
│   └── service.proto              # Protocol Buffers interface definition (ComputeService)
├── generated/
│   ├── service_pb2.py             # Generated Python protobuf serialization classes
│   └── service_pb2_grpc.py        # Generated gRPC client stubs and servicer classes
├── proto_stubs.py                 # Centralized import shim with path resolution
├── generate_protos.py             # Protobuf compiler execution script
├── servers/
│   ├── server_config.py           # Single source of truth: topology, addresses, and timeouts
│   └── server.py                  # Backend server implementation with simulated failure hooks
├── coordinator/
│   ├── cluster.py                 # Thread-safe server registry, metrics, and circular event log
│   ├── load_balancer.py           # Round-robin selection algorithm with exclusion filters
│   ├── failover_manager.py        # Fast-failover dispatch and node eviction coordinator
│   ├── health_monitor.py          # Background daemon thread probing replicas every 2.0s
│   ├── leader_election.py         # Deterministic coordinator-driven leader election
│   └── admin_client.py            # gRPC client helper for out-of-band admin RPC commands
├── client/
│   └── rpc_client.py              # Fault-tolerant client with retry loops, backoff, and CLI runner
├── dashboard/
│   ├── app.py                     # Flask web server hosting the in-process coordinator
│   ├── templates/
│   │   └── index.html             # Single-page observability dashboard interface
│   └── static/
│       ├── style.css              # Custom dashboard styling and visual themes
│       └── script.js              # Auto-polling client logic and REST API bindings
├── tests/
│   └── test_fault_tolerance.py    # Unit test suite using in-memory mock gRPC stubs
├── scripts/                       # Convenience automation scripts (.bat and .ps1)
│   ├── compile_protos.*
│   ├── run_cluster.*
│   ├── demo_client.*
│   └── run_tests.*
├── run.py                         # Single-command unified cluster and dashboard orchestrator
├── requirements.txt               # Dependency definitions (grpcio, protobuf, flask)
└── docs/                          # Comprehensive technical documentation suite
    ├── MASTER_CONTEXT.md          # Primary index and architectural summary (this file)
    ├── SYSTEM_ARCHITECTURE.md     # In-depth topology, components, and thread safety
    ├── RUNTIME_AND_WORKFLOWS.md   # Step-by-step sequence diagrams and lifecycles
    ├── FAULT_TOLERANCE_AND_RESILIENCE.md # Failure modes, retries, and health states
    ├── API_AND_PROTOCOLS.md       # Protobuf schemas, gRPC methods, and REST endpoints
    ├── OPERATING_SYSTEMS_MAPPING.md # Mapping to classical operating system concepts
    └── OPERATIONS_AND_TESTING.md  # Runbooks, test harness, and CI/CD pipelines
```

---

## 3. High-Level Component Interaction

```
                      +---------------------------------------+
                      |         Flask Dashboard (:5000)       |
                      |   Auto-polling HTTP UI + Admin APIs   |
                      +-------------------+-------------------+
                                          | In-process instantiation
    +-------------------------------------v------------------------------------+
    |                            COORDINATOR KERNEL                            |
    |                                                                          |
    |  ServerRegistry (Guarded by RLock)                                       |
    |    - Node status: ONLINE, OFFLINE, RECOVERING, UNKNOWN                   |
    |    - Request / Success / Failure counters & rolling event log            |
    |    - Active leader identity                                              |
    |                                                                          |
    |  RoundRobinLoadBalancer       FailoverManager        LeaderElector       |
    |    - Pointer rotation           - Node eviction        - First-online    |
    |    - Excluded set filter        - Attempt retry          deterministic   |
    |                                                          promotion       |
    |  HealthMonitor Daemon (Daemon Thread, 2.0s interval)                     |
    |    - Probes HealthCheck() RPC across all configured backends             |
    |    - Governs two-stage recovery (OFFLINE -> RECOVERING -> ONLINE)        |
    +-------------------------------------^------------------------------------+
                                          |
                               +----------+----------+
                               |      RpcClient      |
                               |  - Retry loops (3x) |
                               |  - Linear backoff   |
                               |  - Error classifier |
                               +----------+----------+
                                          |
                +-------------------------+-------------------------+
                |                         |                         |
        gRPC (HTTP/2)             gRPC (HTTP/2)             gRPC (HTTP/2)
                |                         |                         |
        +-------v-------+         +-------v-------+         +-------v-------+
        |   server-1    |         |   server-2    |         |   server-3    |
        |  127.0.0.1    |         |  127.0.0.1    |         |  127.0.0.1    |
        |   :50051      |         |   :50052      |         |   :50053      |
        | ThreadPool(8) |         | ThreadPool(8) |         | ThreadPool(8) |
        +---------------+         +---------------+         +---------------+
```

---

## 4. Key Configuration Parameters

All operational constants are centrally defined in `servers/server_config.py`:

| Parameter | Default Value | Role and Description |
|:---|:---|:---|
| `SERVERS` | 3 nodes (`:50051`-`:50053`) | Cluster topology and listening addresses. |
| `MAX_ATTEMPTS` | 3 attempts | 1 initial attempt plus up to 2 retries per RPC request. |
| `RETRY_DELAY` | 0.4 seconds | Base delay for linear backoff multiplier: `delay = RETRY_DELAY * (attempt - 1)`. |
| `RPC_TIMEOUT` | 3.0 seconds | Per-attempt client deadline before triggering DEADLINE_EXCEEDED. |
| `HEALTH_INTERVAL` | 2.0 seconds | Polling cadence of the background HealthMonitor daemon thread. |
| `HEALTH_TIMEOUT` | 1.0 seconds | Maximum time allotted for a single HealthCheck RPC probe. |
| `FAILURE_THRESHOLD` | 1 probe | Consecutive probe failures required to mark an ONLINE server OFFLINE. |
| `RECOVERY_THRESHOLD` | 2 probes | Consecutive successful probes required to promote RECOVERING to ONLINE. |
| `DASHBOARD_HOST` | 127.0.0.1 | Interface bound by the Flask observability web dashboard. |
| `DASHBOARD_PORT` | 5000 | Port exposed by the Flask observability web dashboard. |

---

## 5. Documentation Navigation Guide

Detailed technical documentation is partitioned into focused modules:

1. **[System Architecture](SYSTEM_ARCHITECTURE.md)**: Deep dive into the three-tier topology, component roles, thread concurrency model, and locking mechanisms.
2. **[Runtime and Workflows](RUNTIME_AND_WORKFLOWS.md)**: Step-by-step sequence diagrams covering bootstrapping, normal execution, transient retries, crash failover, and leader election.
3. **[Fault Tolerance and Resilience](FAULT_TOLERANCE_AND_RESILIENCE.md)**: Comprehensive guide on error classification, backoff mathematics, health state transitions, and chaos injection.
4. **[API and Protocols](API_AND_PROTOCOLS.md)**: Specifications for `service.proto`, gRPC status code mappings, and Flask dashboard REST endpoints.
5. **[Operating Systems Mapping](OPERATING_SYSTEMS_MAPPING.md)**: Academic mapping linking framework mechanisms directly to classical OS concepts (IPC, scheduling, virtualization, synchronization).
6. **[Operations and Testing](OPERATIONS_AND_TESTING.md)**: Runbooks for developers, CLI usage instructions, testing patterns using mock stubs, and CI/CD pipelines.
