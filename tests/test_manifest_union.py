from scripts.opendota_pipeline_utils import merge_match_manifest_rows


def test_merge_match_manifest_rows_prefers_pro_match_source() -> None:
    manifest_rows = [
        {
            "match_id": 1001,
            "start_time": 1735689600,
            "source_stream": "public_matches",
            "recency_window": "2025_2026",
        },
        {
            "match_id": 1001,
            "start_time": 1735689600,
            "source_stream": "pro_matches",
            "recency_window": "2025_2026",
        },
    ]

    merged_rows = merge_match_manifest_rows(manifest_rows)

    assert len(merged_rows) == 1
    assert merged_rows[0]["source_stream"] == "pro_matches"


def test_merge_match_manifest_rows_keeps_newest_record_with_same_source() -> None:
    manifest_rows = [
        {
            "match_id": 1002,
            "start_time": 1704067200,
            "source_stream": "public_matches",
            "recency_window": "2023_2024",
        },
        {
            "match_id": 1002,
            "start_time": 1704153600,
            "source_stream": "public_matches",
            "recency_window": "2023_2024",
        },
    ]

    merged_rows = merge_match_manifest_rows(manifest_rows)

    assert len(merged_rows) == 1
    assert merged_rows[0]["start_time"] == 1704153600
