from scripts.opendota_pipeline_utils import assign_recency_window


def test_assign_recency_window_maps_2023_epoch_to_2023_2024() -> None:
    start_time_epoch_seconds = 1704067200  # 2024-01-01 UTC
    assigned_window = assign_recency_window(start_time_epoch_seconds)
    assert assigned_window == "2023_2024"


def test_assign_recency_window_maps_2025_epoch_to_2025_2026() -> None:
    start_time_epoch_seconds = 1735689600  # 2025-01-01 UTC
    assigned_window = assign_recency_window(start_time_epoch_seconds)
    assert assigned_window == "2025_2026"


def test_assign_recency_window_maps_old_epoch_outside_target() -> None:
    start_time_epoch_seconds = 1609459200  # 2021-01-01 UTC
    assigned_window = assign_recency_window(start_time_epoch_seconds)
    assert assigned_window == "outside_target_windows"
