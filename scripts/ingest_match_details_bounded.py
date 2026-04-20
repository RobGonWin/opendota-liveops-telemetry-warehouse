"""Fetch match detail payloads for one or more bounded manifest files."""

from __future__ import annotations

import argparse
import csv
import json
import time
from datetime import datetime, timezone
from pathlib import Path

import requests

try:
    from scripts.opendota_pipeline_utils import fetch_json_with_backoff, merge_match_manifest_rows
except ImportError:  # pragma: no cover - supports direct script execution
    from opendota_pipeline_utils import fetch_json_with_backoff, merge_match_manifest_rows

MATCH_DETAILS_URL_TEMPLATE = "https://api.opendota.com/api/matches/{match_id}"


def load_manifest_rows(manifest_csv_paths: list[Path], limit: int) -> list[dict]:
    """Load and deduplicate manifest rows from one or more CSV snapshots."""

    loaded_rows: list[dict] = []
    for manifest_csv_path in manifest_csv_paths:
        with manifest_csv_path.open("r", encoding="utf-8") as csv_file:
            reader = csv.DictReader(csv_file)
            for row in reader:
                loaded_rows.append(row)

    deduplicated_rows = merge_match_manifest_rows(loaded_rows)
    bounded_rows = deduplicated_rows[:limit]
    return bounded_rows


def fetch_match_payload(
    match_id: int,
    timeout_seconds: int = 25,
    max_retries: int = 3,
    retry_sleep_seconds: float = 1.0,
) -> dict:
    """Fetch a single match payload from OpenDota."""

    match_url = MATCH_DETAILS_URL_TEMPLATE.format(match_id=match_id)
    payload = fetch_json_with_backoff(
        match_url,
        timeout_seconds=timeout_seconds,
        max_retries=max_retries,
        retry_sleep_seconds=retry_sleep_seconds,
    )
    return payload


def build_match_detail_record(manifest_row: dict, match_payload: dict) -> dict:
    """Wrap a raw match payload with manifest metadata for downstream loading."""

    ingested_at = datetime.now(tz=timezone.utc).isoformat()
    detail_record = {
        "match_id": int(manifest_row["match_id"]),
        "start_time": int(manifest_row["start_time"]),
        "source_stream": str(manifest_row["source_stream"]),
        "recency_window": str(manifest_row["recency_window"]),
        "ingested_at": ingested_at,
        "payload": match_payload,
    }
    return detail_record


def write_payloads(payload_records: list[dict], output_path: Path) -> None:
    """Write JSON Lines payload snapshot for reproducible loading."""

    output_path.parent.mkdir(parents=True, exist_ok=True)
    with output_path.open("w", encoding="utf-8") as output_file:
        for payload_record in payload_records:
            output_file.write(json.dumps(payload_record))
            output_file.write("\n")


def parse_manifest_csv_paths(manifest_csv_argument: str) -> list[Path]:
    """Parse a comma-separated manifest path list."""

    manifest_csv_paths = [
        Path(csv_path.strip())
        for csv_path in manifest_csv_argument.split(",")
        if csv_path.strip()
    ]
    return manifest_csv_paths


def main() -> None:
    parser = argparse.ArgumentParser(description="Ingest bounded OpenDota match details.")
    parser.add_argument(
        "--manifest-csv-paths",
        default="data/staging/initial_slice_public_match_ids.csv",
        help="Comma-separated CSV paths containing match manifests",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=500,
        help="Maximum match payloads to fetch",
    )
    parser.add_argument(
        "--sleep-seconds",
        type=float,
        default=0.1,
        help="Pause between requests to reduce rate limit pressure",
    )
    parser.add_argument(
        "--max-retries",
        type=int,
        default=3,
        help="Maximum retries for one match detail request when OpenDota rate limits or transiently fails",
    )
    parser.add_argument(
        "--retry-sleep-seconds",
        type=float,
        default=1.0,
        help="Base sleep used between retries for one match detail request",
    )
    parser.add_argument(
        "--output-path",
        default="data/raw/opendota/initial_slice_match_details.jsonl",
        help="Output JSONL path",
    )
    args = parser.parse_args()

    manifest_csv_paths = parse_manifest_csv_paths(args.manifest_csv_paths)
    manifest_rows = load_manifest_rows(
        manifest_csv_paths=manifest_csv_paths,
        limit=args.limit,
    )

    payload_records: list[dict] = []
    for manifest_row in manifest_rows:
        match_id = int(manifest_row["match_id"])
        try:
            match_payload = fetch_match_payload(
                match_id=match_id,
                max_retries=args.max_retries,
                retry_sleep_seconds=args.retry_sleep_seconds,
            )
            payload_record = build_match_detail_record(
                manifest_row=manifest_row,
                match_payload=match_payload,
            )
            payload_records.append(payload_record)
        except requests.HTTPError as request_error:
            print(f"Skipping {match_id}: {request_error}")
        time.sleep(args.sleep_seconds)

    write_payloads(payload_records=payload_records, output_path=Path(args.output_path))
    print(f"Saved {len(payload_records)} match payloads to {args.output_path}.")


if __name__ == "__main__":
    main()
