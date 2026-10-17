"""Run all 570 configurations across the 4 benchmark suites and regenerate report."""
import time
from pathlib import Path
import subprocess
import sys

from scripts.generate_report import generate_report

SUITES = ["baseline", "scalability", "network_delay", "message_loss"]

def main():
    start_total = time.perf_counter()
    results_dir = Path("results")
    results_dir.mkdir(parents=True, exist_ok=True)

    for suite in SUITES:
        print(f"\n==========================================")
        print(f"Starting suite: {suite}")
        print(f"==========================================")
        cmd = [sys.executable, "-m", "src.experiments.runner", "--suite", suite, "--output-dir", str(results_dir)]
        t0 = time.perf_counter()
        res = subprocess.run(cmd, check=True)
        t_elapsed = time.perf_counter() - t0
        print(f"Finished suite {suite} in {t_elapsed:.2f}s")

    print("\nRegenerating results/benchmark_report.md...")
    generate_report(results_dir)
    total_elapsed = time.perf_counter() - start_total
    print(f"\nAll 570 runs and report regeneration completed in {total_elapsed:.2f}s.")

if __name__ == "__main__":
    main()
