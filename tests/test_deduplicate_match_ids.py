from scripts.opendota_pipeline_utils import deduplicate_match_ids


def test_deduplicate_match_ids_keeps_newest_observation() -> None:
    public_matches = [
        {"match_id": 1, "start_time": 100},
        {"match_id": 1, "start_time": 110},
        {"match_id": 2, "start_time": 200},
    ]

    deduplicated_records = deduplicate_match_ids(public_matches)
    deduplicated_by_match_id = {record.match_id: record for record in deduplicated_records}

    assert len(deduplicated_records) == 2
    assert deduplicated_by_match_id[1].start_time == 110
    assert deduplicated_by_match_id[2].start_time == 200


def test_deduplicate_match_ids_ignores_incomplete_rows() -> None:
    public_matches = [
        {"match_id": 1, "start_time": 100},
        {"start_time": 110},
        {"match_id": 2},
    ]

    deduplicated_records = deduplicate_match_ids(public_matches)

    assert len(deduplicated_records) == 1
    assert deduplicated_records[0].match_id == 1
