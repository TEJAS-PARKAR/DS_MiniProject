import pytest
from src.simulation.safety import detect_overlapping_intervals


def test_injected_overlapping_cs_intervals_are_detected():
    overlaps = detect_overlapping_intervals([
        {"process_id": 0, "entry_time": 1.0, "exit_time": 4.0},
        {"process_id": 1, "entry_time": 3.0, "exit_time": 5.0},
    ])
    assert len(overlaps) == 1
    assert overlaps[0]["type"] == "overlapping_intervals"
    assert overlaps[0]["overlap_start"] == 3.0
    assert overlaps[0]["overlap_end"] == 4.0


def test_touching_intervals_are_not_overlapping():
    assert detect_overlapping_intervals([
        {"process_id": 0, "entry_time": 1, "exit_time": 2},
        {"process_id": 1, "entry_time": 2, "exit_time": 3},
    ]) == []


def test_invalid_interval_is_rejected():
    with pytest.raises(ValueError):
        detect_overlapping_intervals([
            {"process_id": 0, "entry_time": 3, "exit_time": 2},
        ])
