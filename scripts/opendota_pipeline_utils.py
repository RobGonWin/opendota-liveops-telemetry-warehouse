"""Utility helpers for bounded OpenDota ingestion and window assignment."""

from __future__ import annotations

import time
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any

import requests


WINDOW_2023_2024_START = datetime(2023, 1, 1, tzinfo=timezone.utc)
WINDOW_2023_2024_END = datetime(2024, 12, 31, 23, 59, 59, tzinfo=timezone.utc)
WINDOW_2025_2026_START = datetime(2025, 1, 1, tzinfo=timezone.utc)
TARGET_WINDOWS = ("2023_2024", "2025_2026")
RETRYABLE_STATUS_CODES = {429, 500, 502, 503, 504}
SOURCE_STREAM_PRIORITY = {
    "pro_matches": 0,
    "public_matches": 1,
    "unknown": 2,
}


@dataclass(frozen=True)
class MatchRecord:
    """Represents a public match row used for ingestion orchestration."""

    match_id: int
    start_time: int


def fetch_json_with_backoff(
    url: str,
    *,
    params: dict[str, Any] | None = None,
    timeout_seconds: int = 20,
    max_retries: int = 3,
    retry_sleep_seconds: float = 1.0,
) -> Any:
    """Fetch JSON from OpenDota with lightweight retry handling for rate limits."""

    latest_response: requests.Response | None = None
    for attempt_number in range(max_retries + 1):
        latest_response = requests.get(url, params=params, timeout=timeout_seconds)
        should_retry = (
            latest_response.status_code in RETRYABLE_STATUS_CODES
            and attempt_number < max_retries
        )
        if should_retry:
            retry_delay_seconds = retry_sleep_seconds * (attempt_number + 1)
            time.sleep(retry_delay_seconds)
            continue

        latest_response.raise_for_status()
        response_payload = latest_response.json()
        return response_payload

    if latest_response is None:
        raise RuntimeError("OpenDota request failed before a response was received.")

    latest_response.raise_for_status()
    raise RuntimeError("OpenDota request exhausted retries without returning JSON.")


def assign_recency_window(start_time_epoch_seconds: int) -> str:
    """Map match start time to named recency windows used by this project."""

    match_start_time = datetime.fromtimestamp(start_time_epoch_seconds, tz=timezone.utc)

    is_in_2023_2024_window = (
        WINDOW_2023_2024_START <= match_start_time <= WINDOW_2023_2024_END
    )
    if is_in_2023_2024_window:
        assigned_window = "2023_2024"
        return assigned_window

    is_in_2025_2026_window = match_start_time >= WINDOW_2025_2026_START
    if is_in_2025_2026_window:
        assigned_window = "2025_2026"
        return assigned_window

    assigned_window = "outside_target_windows"
    return assigned_window


def deduplicate_match_ids(public_matches: list[dict[str, Any]]) -> list[MatchRecord]:
    """Keep one record per match ID using the newest observed start_time."""

    newest_match_by_id: dict[int, MatchRecord] = {}

    for public_match in public_matches:
        has_required_fields = "match_id" in public_match and "start_time" in public_match
        if not has_required_fields:
            continue

        candidate_match_id = int(public_match["match_id"])
        candidate_start_time = int(public_match["start_time"])
        candidate_record = MatchRecord(
            match_id=candidate_match_id,
            start_time=candidate_start_time,
        )

        existing_record = newest_match_by_id.get(candidate_match_id)
        if existing_record is None:
            newest_match_by_id[candidate_match_id] = candidate_record
            continue

        is_newer_observation = candidate_start_time >= existing_record.start_time
        if is_newer_observation:
            newest_match_by_id[candidate_match_id] = candidate_record

    deduplicated_matches = list(newest_match_by_id.values())
    return deduplicated_matches


def build_match_manifest_row(
    *,
    match_id: int,
    start_time: int,
    source_stream: str,
) -> dict[str, Any]:
    """Build a normalized manifest row for downstream detail fetching."""

    recency_window = assign_recency_window(start_time)
    manifest_row = {
        "match_id": int(match_id),
        "start_time": int(start_time),
        "source_stream": source_stream,
        "recency_window": recency_window,
    }
    return manifest_row


def is_target_window(recency_window: str) -> bool:
    """Return whether a recency window is in the LiveOps comparison scope."""

    is_supported_window = recency_window in TARGET_WINDOWS
    return is_supported_window


def merge_match_manifest_rows(manifest_rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Deduplicate manifest rows, preferring pro matches over public matches."""

    preferred_rows_by_match_id: dict[int, dict[str, Any]] = {}

    for manifest_row in manifest_rows:
        has_required_fields = (
            "match_id" in manifest_row
            and "start_time" in manifest_row
            and "source_stream" in manifest_row
            and "recency_window" in manifest_row
        )
        if not has_required_fields:
            continue

        candidate_match_id = int(manifest_row["match_id"])
        candidate_start_time = int(manifest_row["start_time"])
        candidate_source_stream = str(manifest_row["source_stream"])
        candidate_window = str(manifest_row["recency_window"])
        candidate_priority = SOURCE_STREAM_PRIORITY.get(
            candidate_source_stream,
            SOURCE_STREAM_PRIORITY["unknown"],
        )

        normalized_candidate = {
            "match_id": candidate_match_id,
            "start_time": candidate_start_time,
            "source_stream": candidate_source_stream,
            "recency_window": candidate_window,
        }

        existing_row = preferred_rows_by_match_id.get(candidate_match_id)
        if existing_row is None:
            preferred_rows_by_match_id[candidate_match_id] = normalized_candidate
            continue

        existing_priority = SOURCE_STREAM_PRIORITY.get(
            str(existing_row["source_stream"]),
            SOURCE_STREAM_PRIORITY["unknown"],
        )
        should_replace_existing = candidate_priority < existing_priority
        is_same_priority_newer_record = (
            candidate_priority == existing_priority
            and candidate_start_time >= int(existing_row["start_time"])
        )
        if should_replace_existing or is_same_priority_newer_record:
            preferred_rows_by_match_id[candidate_match_id] = normalized_candidate

    deduplicated_rows = sorted(
        preferred_rows_by_match_id.values(),
        key=lambda row: (int(row["start_time"]), int(row["match_id"])),
        reverse=True,
    )
    return deduplicated_rows
