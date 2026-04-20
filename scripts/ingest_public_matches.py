"""Bounded ingestion of OpenDota public matches.

Pulls recent match IDs and assigns recency windows for downstream detail ingestion.
"""

from __future__ import annotations

import argparse
import csv
import json
from pathlib import Path

try:
    from scripts.opendota_pipeline_utils import (
        build_match_manifest_row,
        deduplicate_match_ids,
        fetch_json_with_backoff,
    )
except ImportError:  # pragma: no cover - supports direct script execution
    from opendota_pipeline_utils import (
        build_match_manifest_row,
        deduplicate_match_ids,
        fetch_json_with_backoff,
    )

PUBLIC_MATCHES_URL = "https://api.opendota.com/api/publicMatches"


def fetch_public_matches(limit: int, timeout_seconds: int = 20) -> list[dict]:
    """Fetch a bounded sample from the public matches endpoint."""

    response_payload = fetch_json_with_backoff(
        PUBLIC_MATCHES_URL,
        timeout_seconds=timeout_seconds,
    )

    deduplicated_matches = deduplicate_match_ids(response_payload)
    bounded_matches = deduplicated_matches[:limit]

    records = []
    for match in bounded_matches:
        record = build_match_manifest_row(
            match_id=match.match_id,
            start_time=match.start_time,
            source_stream="public_matches",
        )
        records.append(record)

    return records


def write_public_match_snapshots(records: list[dict], output_prefix: str) -> None:
    """Persist raw and normalized snapshots for reproducibility."""

    raw_json_path = Path(f"data/raw/opendota/{output_prefix}_public_matches.json")
    normalized_csv_path = Path(f"data/staging/{output_prefix}_public_match_ids.csv")

    raw_json_path.parent.mkdir(parents=True, exist_ok=True)
    normalized_csv_path.parent.mkdir(parents=True, exist_ok=True)

    raw_json_path.write_text(json.dumps(records, indent=2), encoding="utf-8")

    with normalized_csv_path.open("w", newline="", encoding="utf-8") as csv_file:
        field_names = ["match_id", "start_time", "source_stream", "recency_window"]
        writer = csv.DictWriter(csv_file, fieldnames=field_names)
        writer.writeheader()
        writer.writerows(records)



def main() -> None:
    parser = argparse.ArgumentParser(description="Ingest bounded public match IDs.")
    parser.add_argument(
        "--limit",
        type=int,
        default=500,
        help="Maximum number of unique match IDs to keep",
    )
    parser.add_argument(
        "--output-prefix",
        default="initial_slice",
        help="File name prefix used for output artifacts",
    )
    args = parser.parse_args()

    public_match_records = fetch_public_matches(limit=args.limit)
    write_public_match_snapshots(public_match_records, args.output_prefix)
    print(f"Saved {len(public_match_records)} public match records.")


if __name__ == "__main__":
    main()
