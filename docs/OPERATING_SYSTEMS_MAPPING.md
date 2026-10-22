# Operating Systems Foundations and Conceptual Mapping

## 1. Overview of Operating Systems Mapping

This project serves as a concrete bridge between core Operating Systems principles and distributed systems engineering. Distributed systems can be understood as an extension of operating systems where the bus is replaced by an unreliable network and memory is partitioned across disjoint machines.

---

## 2. Principle Mapping Matrix

| Operating System Concept | Local OS Implementation | Distributed Implementation in this Framework | Source File Reference |
|:---|:---|:---|:---|
| **Inter-Process Communication (IPC)** | UNIX pipes, FIFOs, message queues, shared memory | gRPC over HTTP/2 using TCP sockets | `proto/service.proto`, `client/rpc_client.py` |
| **System Calls (Syscalls)** | Trap instructions (`INT 0x80` / `syscall`), user-to-kernel switch | Remote Procedure Calls (`stub.Execute()`), user-to-remote-process switch | `client/rpc_client.py`, `servers/server.py` |
| **Data Marshalling & Packing** | Register passing, C struct alignment, stack frames | Protocol Buffers (proto3) binary serialization | `generated/service_pb2.py` |
| **Process Isolation** | MMU paging, separate virtual memory address spaces | Distinct OS processes bound to isolated TCP ports (`50051`-`50053`) | `run.py`, `servers/server.py` |
| **Multithreading & Concurrency** | Kernel threads (`pthreads`), thread scheduling | `concurrent.futures.ThreadPoolExecutor(max_workers=8)` | `servers/server.py` |
| **Mutual Exclusion & Locks** | Spinlocks, semaphores, mutexes (`pthread_mutex_t`) | Re-entrant locks (`threading.RLock`), mutexes (`threading.Lock`) | `coordinator/cluster.py`, `servers/server.py` |
| **CPU Scheduling** | Round-Robin CPU scheduling (Time-slice / Quantum) | Round-Robin Load Balancing across healthy replicas | `coordinator/load_balancer.py` |
| **Watchdog Timers / Heartbeats** | Hardware watchdog timers, timer interrupts | Background `HealthMonitor` daemon thread waking every 2.0s | `coordinator/health_monitor.py` |
| **Bounded Waiting & Timeouts** | Alarm signals (`SIGALRM`), `select()` / `poll()` timeouts | Explicit gRPC deadlines (`timeout=RPC_TIMEOUT`) | `client/rpc_client.py` |
| **Distributed Mutual Exclusion** | Dekker's algorithm, Peterson's algorithm, hardware test-and-set | Coordinator-driven deterministic Leader Election | `coordinator/leader_election.py` |

---

## 3. Deep-Dive Conceptual Analysis

### 3.1 Syscalls vs Remote Procedure Calls
- **OS System Call**: An application executing in User Mode requires a privileged kernel service (e.g., `read()` from disk). The CPU generates a software interrupt or trap instruction, saving registers, switching to Kernel Mode, executing the handler via the Interrupt Descriptor Table (IDT), and returning to User Mode. The calling thread is blocked while awaiting the kernel result.
- **Distributed RPC**: The application requires work from a separate process. The client calls a local stub function (`stub.Execute(request)`). The stub serializes arguments into binary wire format, transmits them over an HTTP/2 connection, blocks the calling thread, waits for the remote server process to compute the result, unpacks the response, and returns execution to the caller. RPC simulates local function invocation across network boundaries.

### 3.2 Process Isolation and Memory Safety
- In an operating system, the Memory Management Unit (MMU) uses page tables to isolate virtual address spaces. If process A dereferences a null pointer or experiences a segmentation fault, process B remains unharmed.
- In this framework, rather than running three threads inside a single Python process, three independent processes are spawned (`server.py --id server-1`, etc.). If `server-1` experiences high latency, heap corruption, or a sudden crash, `server-2` and `server-3` continue operating without shared-state contamination.

### 3.3 Round-Robin Scheduling: CPU vs Server Replicas
- **OS Round-Robin CPU Scheduling**: The kernel maintains a ready queue of runnable processes. The scheduler allocates the CPU to the head process for a fixed time quantum $q$. If the process does not terminate within $q$, a timer interrupt fires, triggering a context switch to the next process in the ready queue.
- **Distributed Round-Robin Load Balancing**: The coordinator maintains an ordered list of backend servers. Instead of time slices, the unit of allocation is a discrete client request. The load balancer advances its pointer after each assignment:
  $$\text{NextIndex} = (\text{CurrentIndex} + 1) \pmod N$$
  If a server is flagged `OFFLINE` or `RECOVERING`, it is skipped in the rotation—analogous to the OS scheduler skipping processes in a `BLOCKED` or `WAITING` state.

### 3.4 Concurrency Control and Race Prevention
- **Critical Section Problem**: Concurrent access to shared mutable data can produce nondeterministic state corruption.
- In `coordinator/cluster.py`, multiple threads access the `ServerRegistry`:
  - Flask web server worker threads servicing `/api/request` or `/api/status`.
  - The background `HealthMonitor` thread updating node status and latencies.
- **Lock Implementation**:
  ```python
  class ServerRegistry:
      def __init__(self):
          self._lock = threading.RLock()
  ```
  `RLock` allows a single thread to acquire the lock multiple times recursively without self-deadlock, ensuring atomic updates across registry lookups, metric increments, and circular event log appends.

### 3.5 Watchdogs and Periodic Heartbeat Daemons
- **Hardware Watchdogs**: High-reliability embedded systems employ hardware timers that count down to zero. The OS kernel must periodically "kick" or reset the timer. If the kernel freezes or deadlocks, the timer expires and forces a hardware reboot.
- **Distributed Health Monitor**: The `HealthMonitor` daemon thread acts as a software watchdog. Every 2.0s, it sends a `HealthCheck` probe. If a replica fails to answer within the 1.0s deadline, the watchdog marks the node `OFFLINE`, evicting it before subsequent client requests can experience failure.
