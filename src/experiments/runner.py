"""Repeatable benchmark runner for the existing discrete-event DME simulator.

Note: this runner benchmarks the simulator implementation. Kubernetes manifests in
this repository deploy the separate TCP runtime; these two modes are not conflated.
"""
from __future__ import annotations

import argparse
import csv
import json
import math
import random
import statistics
import time
import uuid
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any

import yaml

from src.simulation.engine import ExperimentConfig, Simulation
from src.simulation.ricart_agrawala import SimulationRicartAgrawala
from src.simulation.suzuki_kasami import SimulationSuzukiKasami
from src.simulation.raymond_tree import SimulationRaymond


ALGORITHMS = {
    "ricart_agrawala": SimulationRicartAgrawala,
    "suzuki_kasami": SimulationSuzukiKasami,
    "raymond_tree": SimulationRaymond,
}
CSV_COLUMNS = [
    "experiment_id", "algorithm", "process_count", "seed",
    "configured_delay", "loss_rate", "requests", "completed_operations",
    "messages_sent", "messages_delivered", "messages_dropped",
    "mean_wait_ms", "wait_std_ms", "wait_cv", "p95_wait_ms",
    "throughput_ops_per_sec", "safety_violations", "stalled_requests",
    "run_status",
]


@dataclass(frozen=True)
class RunSpec:
    algorithm: str
    process_count: int
    seed: int = 42
    configured_delay: float = 5.0
    loss_rate: float = 0.0
    requests_per_process: int = 5
    mean_think_time_ms: float = 20.0
    critical_section_ms: float = 2.0
    max_simulation_time_ms: float = 30000.0


def _percentile(values: list[float], percentile: float) -> float:
    """Nearest-rank percentile, returning 0 for an empty sample."""
    if not values:
        return 0.0
    ordered = sorted(values)
    index = max(0, math.ceil((percentile / 100.0) * len(ordered)) - 1)
    return float(ordered[index])


def run_experiment(spec: RunSpec, output_dir: str | Path) -> dict[str, Any]:
    """Run one deterministic workload and persist its raw event/message logs."""
    if spec.algorithm not in ALGORITHMS:
        raise ValueError(f"Unsupported algorithm: {spec.algorithm}")
    if spec.process_count < 2:
        raise ValueError("process_count must be at least 2")
    if spec.requests_per_process < 1:
        raise ValueError("requests_per_process must be positive")
    if spec.configured_delay < 0 or not 0 <= spec.loss_rate <= 1:
        raise ValueError("delay must be non-negative and loss_rate must be in [0, 1]")

    out = Path(output_dir)
    out.mkdir(parents=True, exist_ok=True)
    experiment_id = f"{spec.algorithm}-{spec.process_count}-{spec.seed}-{uuid.uuid4().hex[:8]}"

    config = ExperimentConfig(
        process_count=spec.process_count,
        simulation_time=spec.max_simulation_time_ms,
        mean_network_delay=spec.configured_delay,
        message_loss_rate=spec.loss_rate,
        critical_section_time=spec.critical_section_ms,
        mean_think_time=spec.mean_think_time_ms,
        random_seed=spec.seed,
    )
    sim = Simulation(config)
    algorithm = ALGORITHMS[spec.algorithm](sim)

    # Build the same offered arrival schedule for all algorithms with this seed.
    # Arrivals while a process is busy queue locally; each process has at most one
    # outstanding protocol request, which is the workload contract for this model.
    rng = random.Random(spec.seed)
    arrivals: dict[int, list[float]] = {}
    for pid in range(spec.process_count):
        t = 0.0
        times = []
        for _ in range(spec.requests_per_process):
            t += rng.expovariate(1.0 / max(spec.mean_think_time_ms, 0.001))
            times.append(t)
        arrivals[pid] = times

    pending: dict[int, list[float]] = {pid: [] for pid in range(spec.process_count)}
    accepted: dict[int, int] = {pid: 0 for pid in range(spec.process_count)}
    total_expected = spec.process_count * spec.requests_per_process
    original_log = sim.log

    def try_start(pid: int) -> None:
        process = sim.processes[pid]
        if pending[pid] and process.active and not process.requesting and not process.in_critical_section:
            arrival_time = pending[pid].pop(0)
            accepted[pid] += 1
            original_log(pid, "WORKLOAD_REQUEST_START", {
                "offered_at_ms": arrival_time,
                "request_number": accepted[pid],
                "queue_delay_ms": max(0.0, sim.time - arrival_time),
            })
            algorithm.request_cs(pid)

    def log_with_workload_hook(pid: int, event_type: str, details: dict[str, Any] | None = None) -> None:
        original_log(pid, event_type, details)
        if event_type == "EXIT_CS":
            # Defer until the algorithm completes its release-state cleanup.
            sim.scheduler.schedule(sim.time, lambda p=pid: try_start(p),
                                   description=f"next workload request for process {pid}")

    sim.log = log_with_workload_hook  # type: ignore[method-assign]

    for pid, times in arrivals.items():
        for arrival in times:
            def arrive(p: int = pid, at: float = arrival) -> None:
                pending[p].append(at)
                pending[p].sort()
                original_log(p, "WORKLOAD_ARRIVAL", {"offered_at_ms": at})
                try_start(p)
            sim.scheduler.schedule(arrival, arrive, description=f"workload arrival p{pid}")

    wall_start = time.perf_counter()
    sim.run(until=spec.max_simulation_time_ms)
    wall_duration = time.perf_counter() - wall_start

    events = sim.logger.to_dataframe()
    events.insert(0, "experiment_id", experiment_id)
    events.insert(1, "algorithm", spec.algorithm)
    events.insert(2, "process_count", spec.process_count)
    events.insert(3, "seed", spec.seed)
    events.to_csv(out / f"{experiment_id}_events.csv", index=False)

    message_log = sim.network.message_log
    with (out / f"{experiment_id}_messages.jsonl").open("w", encoding="utf-8") as f:
        for item in message_log:
            f.write(json.dumps(item, default=str) + "\n")

    entry_times = {
        int(row["process_id"]): float(row["time"])
        for _, row in events[events["event_type"] == "ENTER_CS"].iterrows()
    }
    # Algorithm wait times are protocol wait after the request is issued.
    wait_times = [float(v) for v in getattr(algorithm, "wait_times", [])]
    exits = events[events["event_type"] == "EXIT_CS"]
    completed = len(exits)
    violations = len(sim.safety.violations)
    stalled = max(0, total_expected - completed)
    if violations:
        status = "safety_violation"
    elif completed == total_expected:
        status = "completed"
    else:
        status = "incomplete"

    if completed:
        first_request = float(events.loc[events["event_type"] == "WORKLOAD_ARRIVAL", "time"].min())
        last_exit = float(exits["time"].max())
        elapsed_seconds = max((last_exit - first_request) / 1000.0, 1e-9)
    else:
        elapsed_seconds = max(spec.max_simulation_time_ms / 1000.0, 1e-9)

    # Compute max concurrent critical sections (independent safety/contention audit).
    cs_ev = events[events["event_type"].isin(["ENTER_CS", "EXIT_CS"])][["time", "event_type"]]
    cs_ev = cs_ev.sort_values("time")
    concurrent = 0
    max_concurrent_cs = 0
    for _, ev in cs_ev.iterrows():
        if ev["event_type"] == "ENTER_CS":
            concurrent += 1
            max_concurrent_cs = max(max_concurrent_cs, concurrent)
        else:
            concurrent = max(concurrent - 1, 0)

    row: dict[str, Any] = {
        "experiment_id": experiment_id,
        "algorithm": spec.algorithm,
        "process_count": spec.process_count,
        "seed": spec.seed,
        "configured_delay": spec.configured_delay,
        "loss_rate": spec.loss_rate,
        "requests": total_expected,
        "completed_operations": completed,
        "messages_sent": sim.network.messages_sent,
        "messages_delivered": sim.network.messages_delivered,
        "messages_dropped": sim.network.messages_dropped,
        "mean_wait_ms": statistics.fmean(wait_times) if wait_times else 0.0,
        "wait_std_ms": statistics.stdev(wait_times) if len(wait_times) > 1 else 0.0,
        "wait_cv": (statistics.stdev(wait_times) / statistics.fmean(wait_times)) if len(wait_times) > 1 else 0.0,
        "p95_wait_ms": _percentile(wait_times, 95),
        "throughput_ops_per_sec": completed / elapsed_seconds,
        "safety_violations": violations,
        "stalled_requests": stalled,
        "run_status": status,
        "max_concurrent_cs": max_concurrent_cs,
        "wall_runtime_seconds": wall_duration,
        "simulated_runtime_ms": float(sim.time),
        "requests_per_process": spec.requests_per_process,
        "critical_section_ms": spec.critical_section_ms,
        "mean_think_time_ms": spec.mean_think_time_ms,
    }
    (out / f"{experiment_id}_summary.json").write_text(
        json.dumps({"spec": asdict(spec), "metrics": row,
                    "safety_details": sim.safety.violations}, indent=2),
        encoding="utf-8",
    )
    return row


def load_suite(config_path: str | Path, suite_name: str) -> list[RunSpec]:
    """Expand a named YAML suite into deterministic run specifications."""
    config = yaml.safe_load(Path(config_path).read_text(encoding="utf-8"))
    suites = config.get("suites", {})
    if suite_name not in suites:
        raise ValueError(f"Unknown suite {suite_name!r}; available: {', '.join(suites)}")
    suite = suites[suite_name]
    defaults = config.get("workload", {})
    specs = []
    for n in suite.get("node_counts", [5]):
        for algorithm in suite.get("algorithms", list(ALGORITHMS)):
            for delay in suite.get("network_delays_ms", [5.0]):
                for loss in suite.get("loss_rates", [0.0]):
                    for seed in suite.get("seeds", [42]):
                        specs.append(RunSpec(
                            algorithm=algorithm,
                            process_count=int(n),
                            seed=int(seed),
                            configured_delay=float(delay),
                            loss_rate=float(loss),
                            requests_per_process=int(defaults.get("requests_per_process", 5)),
                            mean_think_time_ms=float(defaults.get("mean_think_time_ms", 20.0)),
                            critical_section_ms=float(defaults.get("critical_section_ms", 2.0)),
                            max_simulation_time_ms=float(defaults.get("max_simulation_time_ms", 30000.0)),
                        ))
    return specs


def main() -> None:
    parser = argparse.ArgumentParser(description="Run reproducible DME benchmark experiments")
    parser.add_argument("--config", default="config/experiments.yaml")
    parser.add_argument("--suite", default="baseline")
    parser.add_argument("--output-dir", default="results")
    parser.add_argument("--requests-per-process", type=int, default=None,
                        help="Override the suite's request count per process")
    parser.add_argument("--max-simulation-time-ms", type=float, default=None,
                        help="Override the suite's simulation deadline")
    args = parser.parse_args()

    specs = load_suite(args.config, args.suite)
    rows = []
    for i, spec in enumerate(specs, 1):
        if args.requests_per_process is not None:
            spec = RunSpec(**{**asdict(spec), "requests_per_process": args.requests_per_process})
        if args.max_simulation_time_ms is not None:
            spec = RunSpec(**{**asdict(spec), "max_simulation_time_ms": args.max_simulation_time_ms})
        print(f"[{i}/{len(specs)}] {spec.algorithm} n={spec.process_count} seed={spec.seed} "
              f"delay={spec.configured_delay}ms loss={spec.loss_rate}")
        rows.append(run_experiment(spec, args.output_dir))

    out = Path(args.output_dir)
    all_fields = list(dict.fromkeys(CSV_COLUMNS + [k for row in rows for k in row]))
    with (out / f"{args.suite}_results.csv").open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=all_fields)
        writer.writeheader()
        writer.writerows(rows)

    # Aggregate repeated trials grouped by all configuration dimensions.
    try:
        import pandas as pd
        df = pd.DataFrame(rows)
        group_cols = ["algorithm", "process_count", "configured_delay", "loss_rate"]
        numeric_cols = [
            "completed_operations", "messages_sent", "messages_delivered", "messages_dropped",
            "mean_wait_ms", "wait_std_ms", "wait_cv", "p95_wait_ms",
            "throughput_ops_per_sec", "safety_violations", "stalled_requests", "max_concurrent_cs",
        ]
        wait_cols = {"mean_wait_ms", "wait_std_ms", "wait_cv", "p95_wait_ms", "throughput_ops_per_sec"}
        records = []
        for keys, grp in df.groupby(group_cols):
            rec = dict(zip(group_cols, keys))
            rec["n_trials"] = len(grp)
            rec["n_completed"] = int((grp["run_status"] == "completed").sum())
            rec["n_incomplete"] = int((grp["run_status"] == "incomplete").sum())
            rec["n_safety_violation"] = int((grp["run_status"] == "safety_violation").sum())
            comp = grp[grp["run_status"] == "completed"]
            for col in numeric_cols:
                if col not in grp.columns:
                    continue
                src = comp if col in wait_cols else grp
                vals = src[col].dropna()
                rec[f"{col}_mean"] = float(vals.mean()) if len(vals) else float("nan")
                rec[f"{col}_std"] = float(vals.std(ddof=1)) if len(vals) > 1 else float("nan")
            records.append(rec)
        summary = pd.DataFrame(records)
        summary.to_csv(out / f"{args.suite}_summary.csv", index=False)
        try:
            import matplotlib
            matplotlib.use("Agg")
            import matplotlib.pyplot as plt
            # Only plot loss_rate==0 rows to avoid mixing conditions
            plot_df = summary[summary["loss_rate"] == 0.0].copy()
            for base in ("mean_wait_ms", "throughput_ops_per_sec", "messages_sent"):
                metric = f"{base}_mean"
                std_col = f"{base}_std"
                if metric not in plot_df.columns:
                    continue
                fig, ax = plt.subplots()
                for algorithm, group in plot_df.groupby("algorithm"):
                    group = group.sort_values("process_count")
                    ax.plot(group["process_count"], group[metric], marker="o", label=algorithm)
                    if std_col in group.columns:
                        ax.fill_between(
                            group["process_count"],
                            group[metric] - group[std_col].fillna(0),
                            group[metric] + group[std_col].fillna(0),
                            alpha=0.15,
                        )
                ax.set_xlabel("Process count")
                ax.set_ylabel(base.replace("_", " ").title())
                ax.set_title(f"{args.suite.title()}: {base.replace(chr(95), chr(32)).title()} (loss=0%)")
                ax.legend()
                ax.grid(True, alpha=0.25)
                fig.tight_layout()
                fig.savefig(out / f"{args.suite}_{base}.png", dpi=140)
                plt.close(fig)
        except Exception as exc:
            print(f"Chart export skipped: {exc}")
    except Exception as exc:
        print(f"Summary export skipped: {exc}")

    print(f"\nWrote {len(rows)} runs to {out.resolve()}")
    print("Statuses:", {status: sum(r["run_status"] == status for r in rows)
                         for status in sorted({r["run_status"] for r in rows})})


if __name__ == "__main__":
    main()
