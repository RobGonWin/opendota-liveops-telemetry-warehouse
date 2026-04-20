from scripts.ingest_pro_matches_bounded import (
    build_pro_match_manifest_rows,
    get_next_less_than_match_id,
)


def test_build_pro_match_manifest_rows_filters_to_target_windows() -> None:
    pro_matches_page = [
        {"match_id": 10, "start_time": 1704067200},
        {"match_id": 11, "start_time": 1735689600},
        {"match_id": 12, "start_time": 1609459200},
    ]

    manifest_rows = build_pro_match_manifest_rows(pro_matches_page)

    assert len(manifest_rows) == 2
    assert manifest_rows[0]["recency_window"] == "2023_2024"
    assert manifest_rows[1]["recency_window"] == "2025_2026"


def test_get_next_less_than_match_id_returns_smallest_page_match_id() -> None:
    pro_matches_page = [
        {"match_id": 1005, "start_time": 1704067200},
        {"match_id": 1002, "start_time": 1704067300},
        {"match_id": 1009, "start_time": 1704067400},
    ]

    next_less_than_match_id = get_next_less_than_match_id(pro_matches_page)

    assert next_less_than_match_id == 1002
