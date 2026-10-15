# A Failure-Resilient Distributed Mutual Exclusion Framework for Blood Unit Allocation in Multi-Branch Blood Banks

[![Python Version](https://img.shields.io/badge/python-3.10%20%7C%203.11%20%7C%203.12%20%7C%203.13-blue.svg)](https://www.python.org/)
[![Benchmarking](https://img.shields.io/badge/benchmarking-repeatable-blue.svg)]()
[![License](https://img.shields.io/badge/license-MIT-green.svg)]()

A reproducible distributed systems research and engineering framework evaluating distributed mutual exclusion (DME) algorithms, failure detection heuristics, and consistent inventory allocation across independent network-connected branch blood banks.

---

> [!CAUTION]
> **RESEARCH PROTOTYPE DISCLAIMER — NOT FOR CLINICAL USE**  
> This software is an academic computer science research prototype designed for evaluating distributed algorithms, network message complexity, coordination latency, and fault semantics. It uses synthetic test datasets and **is not certified or approved for clinical decision support, medical transfusion management, or patient healthcare**. Real clinical transfusion mandates immunohematology antibody screening, laboratory crossmatching, and bedside verification.

---

## 1. Quickstart & Complete Setup Guide

Follow these steps to set up and run the entire project on your local machine (Windows, macOS, or Linux).

### Prerequisites
* **Python 3.10+** (Tested on Python 3.10, 3.11, 3.12, 3.13). Check your version:
  ```bash
  python --version   # or python3 --version
  ```
* **pip** and `venv` (standard in modern Python distributions).
* **Git** (optional, if cloning via VCS).
* **Docker & Docker Compose** (optional, only needed if running in containerized mode).

---

### Step 1: Clone or Navigate to the Repository

```bash
cd distributed-blood-bank
```

---

### Step 2: Create and Activate a Python Virtual Environment

#### On Windows (PowerShell):
```powershell
# Create virtual environment
python -m venv venv

# If PowerShell script execution is restricted, allow it for this session:
Set-ExecutionPolicy -Scope Process -ExecutionPolicy Bypass

# Activate virtual environment
.\venv\Scripts\Activate.ps1
```

#### On Windows (Command Prompt `cmd`):
```cmd
python -m venv venv
venv\Scripts\activate.bat
```

#### On macOS / Linux (bash / zsh):
```bash
python3 -m venv venv
source venv/bin/activate
```

---

### Step 3: Install Dependencies

With the virtual environment active, install all required dependencies:

```bash
# Upgrade pip
pip install --upgrade pip

# Install project dependencies
pip install -r requirements.txt
```

*(Alternatively, you can install the project in editable development mode: `pip install -e ".[dev]"`)*

---

### Step 4: Run the Complete Automated Test Suite

Verify that all **121 unit and integration tests** pass offline without external network or Docker dependencies:

```bash
python -m pytest tests/unit tests/integration -v
```

Expected output:
```text
============================= 121 passed in ~8s =============================
```

---

### Step 5: Run the Live Interactive Demo

We have provided a ready-to-run interactive demo script that boots independent TCP nodes on loopback, initializes a synthetic blood inventory, executes concurrent allocation requests under Suzuki–Kasami mutual exclusion, and checks peer liveness:

```bash
python demo_allocation.py
```

This will output:
1. Two independent branch nodes starting on local TCP ports (`15001` and `15002`).
2. Authoritative inventory initialization with scarce blood units.
3. Concurrent allocation requests dispatched across sockets.
4. Serialized critical section execution with verifiable non-overlapping entry/exit timestamps.
5. Atomic allocation commitment without double allocation.
6. Failure detector status and heartbeat diagnostics.

---

## 2. Running Branch Nodes Locally (Native CLI)

You can run multiple independent branch-node processes in separate terminal windows using native TCP sockets.

### 4-Node Cluster Example

Open **4 separate terminal windows** (with the virtual environment activated in each):

* **Terminal 1 (Branch 0 — Initial Token Holder):**
  ```powershell
  python -m src.node.branch_node --node-id branch-0 --config config/defaults.yaml --initial-token-holder branch-0 --enable-failure-detection
  ```
* **Terminal 2 (Branch 1):**
  ```powershell
  python -m src.node.branch_node --node-id branch-1 --config config/defaults.yaml --enable-failure-detection
  ```
* **Terminal 3 (Branch 2):**
  ```powershell
  python -m src.node.branch_node --node-id branch-2 --config config/defaults.yaml --enable-failure-detection
  ```
* **Terminal 4 (Branch 3):**
  ```powershell
  python -m src.node.branch_node --node-id branch-3 --config config/defaults.yaml --enable-failure-detection
  ```

### CLI Command Options

| Argument | Description | Default |
|---|---|---|
| `--node-id` | **Required.** Node identifier (must match `node_id` in config) | N/A |
| `--config` | Path to YAML configuration file | `config/defaults.yaml` |
| `--initial-token-holder` | Initial token holder node ID (for Suzuki–Kasami / Raymond) | `None` |
| `--enable-failure-detection`| Enable periodic background heartbeats and failure detector | `False` |

---

### Diagnostic Health Endpoints

Each branch node serves HTTP health and diagnostic endpoints on `health_port` (`node.port + 1000`):

| Endpoint | Method | Purpose |
|---|---|---|
| `/health/live` | GET | Liveness probe (`200 {"status": "alive"}`) |
| `/health/ready` | GET | Readiness probe (`200 {"status": "ready"}` or `503`) |
| `/status` | GET | Complete JSON diagnostic status (counters, algorithm state, failure detector, transport) |

You can query them in your browser or with `curl`:
```bash
curl http://localhost:10001/status
curl http://localhost:10001/health/live
```

Example `/status` JSON response:
```json
{
  "node_id": "branch-0",
  "incarnation_id": "2640e021",
  "algorithm": "suzuki_kasami",
  "runtime": {
    "is_running": true,
    "is_ready": true,
    "uptime_seconds": 15.2,
    "messages_received": 14,
    "messages_sent": 14,
    "cs_entry_count": 2,
    "cs_exit_count": 2
  },
  "protocol": {
    "state": "IDLE",
    "has_token": true,
    "sequence_number": 2
  },
  "failure_detector": {
    "monitored_peers": ["branch-1", "branch-2", "branch-3"],
    "peer_status": {
      "branch-1": "HEALTHY",
      "branch-2": "HEALTHY",
      "branch-3": "HEALTHY"
    }
  }
}
```

---

## 3. Running with Docker Compose

If you have Docker and Docker Compose installed:

```bash
# 1. Build and launch all 4 branch nodes in isolated containers
docker compose up --build -d

# 2. Check cluster container status
docker compose ps

# 3. View live logs from branch-0
docker compose logs -f branch-0

# 4. Check HTTP health diagnostic
curl http://localhost:10001/status

# 5. Stop the cluster cleanly
docker compose down
```

### Docker Network Port Mapping

| Service | Container IP (Internal) | Host TCP Port | Host Health HTTP Port |
|---|---|---|---|
| `branch-0` | `branch-0:9001` | `9001` | `10001` |
| `branch-1` | `branch-1:9002` | `9002` | `10002` |
| `branch-2` | `branch-2:9003` | `9003` | `10003` |
| `branch-3` | `branch-3:9004` | `9004` | `10004` |

---

## 4. Architectural Overview & Project Phases

The framework is organized into layered, modular components:

```text
distributed-blood-bank/
├── pyproject.toml              # Build metadata, dependencies, pytest configuration
├── requirements.txt            # Direct pip dependencies
├── demo_allocation.py          # Ready-to-run interactive TCP allocation demonstration
├── Dockerfile                  # Container definition for branch nodes
├── docker-compose.yml          # 4-node cluster configuration
├── config/
│   ├── defaults.yaml           # Local loopback node & timeout configuration
│   ├── docker_defaults.yaml    # Container cluster configuration
│   └── experiments.yaml        # Matrix experiment configurations
├── src/
│   ├── common/                 # ProtocolMessage, configuration, framing, TCP transport
│   ├── simulation/             # Discrete-event engine and safety monitor
│   ├── algorithms/             # Production DME implementations:
│   │   ├── ricart_agrawala.py  # Permission-based algorithm (Phase 3)
│   │   ├── suzuki_kasami.py    # Broadcast token-based algorithm (Phase 4)
│   │   └── raymond_tree.py     # Tree-based token algorithm (Phase 4)
│   ├── node/                   # Real distributed runtime:
│   │   ├── branch_node.py      # Independent branch process & TCP coordinator (Phase 5)
│   │   ├── failure_detector.py # Chandra-Toueg suspicion failure detector (Phase 7)
│   │   ├── health.py           # Diagnostic HTTP health server (Phase 5)
│   │   └── state.py            # NodeRuntimeState metrics & interval tracking
│   ├── blood_bank/             # Domain inventory & allocation (Phases 6 & 7):
│       ├── models.py           # BloodUnit, BloodGroup, ComponentType, UnitStatus
│       ├── compatibility.py    # Synthetic ABO/Rh compatibility rules
│       ├── inventory.py        # Authoritative BloodBankInventory & uncertain reservations
│       ├── synthetic_data.py   # Deterministic synthetic inventory generator
│       └── allocation_service.py # Protected AllocationService coordinating DME
├── src/experiments/            # Seeded benchmark controller and results export
├── k8s/                        # Kubernetes manifest generator and deployment notes
└── tests/
    ├── unit/                   # Unit tests for protocols, experiments, and safety audit
    └── integration/            # Real TCP socket integration tests
```

### Phase Summary:
* **Phase 1:** Discrete-event simulation engine with Lamport clock and safety invariant monitor (`simultaneous_critical_section`, `duplicate_entry`, `exit_without_entry`).
* **Phase 2:** Strict YAML configuration (`config/defaults.yaml`), versioned wire messages (`ProtocolMessage`), and abstract protocol interfaces (`IMutualExclusionProtocol`).
* **Phase 3:** Production implementation of Ricart–Agrawala permission-based mutual exclusion.
* **Phase 4:** Production implementations of Suzuki–Kasami broadcast token and Raymond's tree-based token algorithms.
* **Phase 5:** Real distributed runtime: streaming newline-delimited TCP framing, `AsyncTcpServer`, `AsyncTcpTransport`, and `HealthServer`.
* **Phase 6:** Synthetic blood-bank inventory domain models, synthetic ABO/Rh compatibility engine, atomic transitions (`AVAILABLE` $\to$ `RESERVED` $\to$ `ALLOCATED`), and protected DME allocation operations.
* **Phase 7:** Chandra–Toueg suspicion-based failure detection (`HEALTHY`, `SUSPECTED`, `CONFIRMED_FAILED`), heartbeat loops, safe fail-closed recovery policies, prohibition of timeout token regeneration, and uncertain reservation reconciliation.

---

## 5. Algorithm Comparison

| Metric / Property | Ricart–Agrawala | Suzuki–Kasami | Raymond's Tree |
|---|---|---|---|
| **Paradigm** | Permission-based (Quorum of $N-1$) | Token-based (Broadcast Request) | Token-based (Logical Tree) |
| **Communication Topology** | Fully connected mesh | Fully connected mesh | Acyclic spanning tree |
| **Messages per Entry (Low Contention)** | $2(N - 1)$ | $N$ (or $0$ if holding token) | $O(\log N)$ on average ($2 \times \text{depth}$) |
| **Messages per Entry (High Contention)** | $2(N - 1)$ | $1$ (token handoff) | $\approx 1$ to $4$ hops |
| **State per Node** | Lamport clock, deferred set | Sequence array $RN[N]$, token $LN[N]$ & queue | Neighbors, $holder$ pointer, request queue |
| **Outbound Message Types** | `REQUEST`, `REPLY` | `REQUEST`, `TOKEN_TRANSFER` | `REQUEST`, `TOKEN_TRANSFER` |
| **Tie-Breaking Rule** | $(timestamp, node\_id)$ | Request sequence number ($RN[i] == LN[i] + 1$) | FIFO queue order at tree nodes |

---

## 6. Failure Detection & Safe Recovery (Phase 7)

### Failure Detector States
* **`HEALTHY`**: Active communication received within `heartbeat_timeout_ms`.
* **`SUSPECTED`**: Silence exceeds `heartbeat_timeout_ms` (network delay, packet drop, or potential crash).
* **`CONFIRMED_FAILED`**: Silence exceeds `confirmation_timeout_ms` or fatal transport drop.

### Safety Invariants & Policies
1. **No Token Regeneration on Timeout**: Unilateral token generation on timeout is strictly prohibited to prevent multiple circulating tokens and lethal double allocations.
2. **Fail-Closed Execution**: If participating peers are suspected or dead, allocations fail closed (`AllocationStatus.FAILED_CLOSED`) rather than bypassing missing nodes.
3. **Incarnation Epoch Fencing**: Restarted nodes start with a fresh `incarnation_id` and cannot inherit stale privileges from prior sessions.
4. **Uncertain Reservations**: Units reserved prior to a crash remain locked in `RESERVED` status. The system never silently releases reservations on timeout; explicit administrative reconciliation (`reconcile_uncertain_reservation` with `COMMIT` or `ABORT`) is required.

---

## 7. Running the Test Suites

The test suite is divided into focused modules:

```powershell
# 1. Run the complete test suite
python -m pytest tests/unit tests/integration -v

# 2. Run only Phase 6 Inventory and Allocation tests
python -m pytest tests/unit/test_inventory.py tests/unit/test_allocation.py tests/integration/test_allocation_distributed.py -v

# 3. Run only Phase 7 Failure Detector & Fault Recovery tests
python -m pytest tests/unit/test_failure_detector.py tests/unit/test_failure_recovery.py -v

# 4. Run real TCP socket multi-process integration tests
python -m pytest tests/integration -v
```

---

## 8. Repeatable Algorithm Benchmarking

The benchmark runner compares the **discrete-event simulation implementations** of Ricart–Agrawala, Suzuki–Kasami, and Raymond under the same seeded offered-arrival schedule across 10 independent trials per configuration. It exports raw event CSVs, message JSONL files, per-run JSON summaries, an aggregate results CSV, an aggregate summary CSV with **mean ± std** for every metric, and PNG charts.

### Running the suites

```powershell
# Reliable 5-process baseline
python -m src.experiments.runner --suite baseline --output-dir results

# Scalability: 5, 10, 20, 50 logical processes
python -m src.experiments.runner --suite scalability --output-dir results

# Network delay sensitivity (1, 5, 10, 50 ms)
python -m src.experiments.runner --suite network_delay --output-dir results

# Message-loss resilience (0 %, 1 %, 5 %)
python -m src.experiments.runner --suite message_loss --output-dir results
```

Each command **regenerates charts automatically** — no separate plotting step is needed.  
Charts are saved to `results/<suite>_<metric>.png` and show **±1 σ shaded bands** across 10 trials.

Override request count or simulation deadline if needed:

```powershell
python -m src.experiments.runner --suite baseline --requests-per-process 10 --max-simulation-time-ms 60000 --output-dir results
```

### Generated files

| File | Contents |
|---|---|
| `results/<suite>_results.csv` | One row per run — all raw metrics |
| `results/<suite>_summary.csv` | One row per `(algorithm, process_count, delay, loss_rate)` — `_mean` and `_std` columns for every numeric metric |
| `results/<suite>_mean_wait_ms.png` | Mean protocol wait time vs. process count (loss=0 %, ±1σ bands) |
| `results/<suite>_throughput_ops_per_sec.png` | Throughput vs. process count (loss=0 %, ±1σ bands) |
| `results/<suite>_messages_sent.png` | Message count vs. process count (loss=0 %, ±1σ bands) |
| `results/benchmark_report.md` | Full human-readable report with all tables |

### Per-run metrics

The `<suite>_results.csv` includes these columns (among others):

| Column | Description |
|---|---|
| `mean_wait_ms` | Mean protocol wait time across all operations in the run |
| `wait_std_ms` | Std dev of per-operation wait times (within run) |
| `wait_cv` | Coefficient of variation (`std/mean`) — fairness proxy; lower = more uniform |
| `p95_wait_ms` | 95th-percentile wait time |
| `throughput_ops_per_sec` | Completed operations ÷ elapsed simulated time |
| `messages_sent` / `messages_delivered` / `messages_dropped` | Network message counts |
| `max_concurrent_cs` | Peak number of processes simultaneously inside CS (must be 1 for correct algorithms) |
| `stalled_requests` | Operations not completed before the simulation deadline |
| `run_status` | `completed`, `incomplete`, or `safety_violation` |

### Summary aggregation

The `<suite>_summary.csv` groups by `(algorithm, process_count, configured_delay, loss_rate)` so different network conditions are **never averaged together**. Wait/throughput means are computed only from `completed` runs; `n_incomplete` and `n_safety_violation` are counted separately.

### Simulator vs. Kubernetes runtime

The benchmark runner measures the **discrete-event simulator**, not wall-clock performance of Kubernetes pods. The existing TCP runtime can be deployed as independent pods using the manifest generator in `k8s/`; see [`k8s/README.md`](k8s/README.md). A live-cluster workload controller and cross-pod metrics collector are not yet implemented — do not describe the simulator's CSV as Kubernetes runtime measurements.


---

## 9. Troubleshooting & FAQs

* **PowerShell script execution error (`cannot be loaded because running scripts is disabled`):**
  Run: `Set-ExecutionPolicy -Scope Process -ExecutionPolicy Bypass` in your PowerShell window before activating `venv`.
* **Port conflicts on ports 9001–9004 or 15001–15002:**
  Ensure no background node processes are running, or change `port` and `health_port` values in `config/defaults.yaml`.
* **Running offline:**
  All tests run completely offline on local loopback (`127.0.0.1`) without requiring an active internet connection.
