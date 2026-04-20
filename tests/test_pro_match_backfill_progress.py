from scripts.ingest_pro_matches_bounded import (
    build_backfill_progress_summary,
    count_selected_rows_by_window,
    determine_active_backfill_window,
)


def test_determine_active_backfill_window_prioritizes_current_window_until_full() -> None:
    selected_counts_by_window = {
        "2025_2026": 150,
        "2023_2024": 0,
    }
    target_counts_by_window = {
        "2025_2026": 300,
        "2023_2024": 300,
    }

    active_backfill_window = determine_active_backfill_window(
        selected_counts_by_window=selected_counts_by_window,
        target_counts_by_window=target_counts_by_window,
    )

    assert active_backfill_window == "2025_2026"


def test_determine_active_backfill_window_switches_to_historical_window_after_current_fills() -> None:
    selected_counts_by_window = {
        "2025_2026": 300,
        "2023_2024": 12,
    }
    target_counts_by_window = {
        "2025_2026": 300,
        "2023_2024": 300,
    }

    active_backfill_window = determine_active_backfill_window(
        selected_counts_by_window=selected_counts_by_window,
        target_counts_by_window=target_counts_by_window,
    )

    assert active_backfill_window == "2023_2024"


def test_count_selected_rows_by_window_counts_existing_manifest_rows() -> None:
    selected_rows_by_match_id = {
        1: {"match_id": 1, "start_time": 1704067200, "source_stream": "pro_matches", "recency_window": "2023_2024"},
        2: {"match_id": 2, "start_time": 1735689600, "source_stream": "pro_matches", "recency_window": "2025_2026"},
        3: {"match_id": 3, "start_time": 1735776000, "source_stream": "pro_matches", "recency_window": "2025_2026"},
    }

    selected_counts_by_window = count_selected_rows_by_window(selected_rows_by_match_id)

    assert selected_counts_by_window == {
        "2023_2024": 1,
        "2025_2026": 2,
    }


def test_build_backfill_progress_summary_tracks_resume_cursor_and_active_window() -> None:
    selected_counts_by_window = {
        "2023_2024": 40,
        "2025_2026": 300,
    }
    target_counts_by_window = {
        "2023_2024": 300,
        "2025_2026": 300,
    }

    progress_summary = build_backfill_progress_summary(
        selected_counts_by_window=selected_counts_by_window,
        target_counts_by_window=target_counts_by_window,
        next_less_than_match_id=8123456789,
        request_count=60,
        oldest_page_start_time=1700000000,
        newest_page_start_time=1776000000,
    )

    assert progress_summary["active_backfill_window"] == "2023_2024"
    assert progress_summary["next_less_than_match_id"] == 8123456789
    assert progress_summary["completed_all_targets"] is False
