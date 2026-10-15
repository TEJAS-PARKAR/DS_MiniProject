import pytest
from src.experiments.runner import RunSpec, run_experiment, load_suite


@pytest.mark.parametrize("algorithm", ["ricart_agrawala", "suzuki_kasami", "raymond_tree"])
def test_runner_completes_reliable_baseline_and_writes_raw_logs(tmp_path, algorithm):
    row = run_experiment(RunSpec(
        algorithm=algorithm, process_count=5, seed=12,
        configured_delay=1, loss_rate=0, requests_per_process=2,
        mean_think_time_ms=5, critical_section_ms=1,
        max_simulation_time_ms=5000,
    ), tmp_path)
    assert row["requests"] == 10
    assert row["completed_operations"] == 10
    assert row["run_status"] == "completed"
    assert row["safety_violations"] == 0
    assert row["stalled_requests"] == 0
    assert (tmp_path / f'{row["experiment_id"]}_events.csv').exists()
    assert (tmp_path / f'{row["experiment_id"]}_messages.jsonl').exists()
    assert (tmp_path / f'{row["experiment_id"]}_summary.json').exists()


def test_suite_includes_required_process_counts():
    specs = load_suite("config/experiments.yaml", "scalability")
    assert {s.process_count for s in specs} == {5, 10, 20, 50}
    assert {s.algorithm for s in specs} == {"ricart_agrawala", "suzuki_kasami", "raymond_tree"}
