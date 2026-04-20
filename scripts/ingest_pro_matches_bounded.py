"""Ingest bounded pro match manifests for the LiveOps comparison windows."""

from __future__ import annotations

import argparse
import csv
import json
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

try:
    from scripts.opendota_pipeline_utils import (
        TARGET_WINDOWS,
        build_match_manifest_row,
        fetch_json_with_backoff,
        is_target_window,
    )
except ImportError:  # pragma: no cover - supports direct script execution
    from opendota_pipeline_utils import (
        TARGET_WINDOWS,
        build_match_manifest_row,
        fetch_json_with_backoff,
        is_target_window,
    )

PRO_MATCHES_URL = "https://api.opendota.com/api/proMatches"
WINDOW_ORDER = ("2025_2026", "2023_2024")
WINDOW_START_EPOCH = {
    "2025_2026": int(datetime(2025, 1, 1, tzinfo=timezone.utc).timestamp()),
    "2023_2024": int(datetime(2023, 1, 1, tzinfo=timezone.utc).timestamp()),
}


def fetch_pro_matches_page(
    *,
    less_than_match_id: int | None = None,
    timeout_seconds: int = 20,
    max_retries: int = 3,
    retry_sleep_seconds: float = 1.0,
) -> list[dict]:
    """Fetch one page from the proMatches endpoint."""

    params: dict[str, Any] = {}
    if less_than_match_id is not None:
        params["less_than_match_id"] = less_than_match_id

    pro_matches_page = fetch_json_with_backoff(
        PRO_MATCHES_URL,
        params=params,
        timeout_seconds=timeout_seconds,
        max_retries=max_retries,
        retry_sleep_seconds=retry_sleep_seconds,
    )
    return pro_matches_page


def build_pro_match_manifest_rows(pro_matches_page: list[dict]) -> list[dict]:
    """Normalize a pro match page into manifest rows for target windows only."""

    manifest_rows: list[dict] = []
    for pro_match in pro_matches_page:
        match_id = pro_match.get("match_id")
        start_time = pro_match.get("start_time")
        if match_id is None or start_time is None:
            continue

        manifest_row = build_match_manifest_row(
            match_id=int(match_id),
            start_time=int(start_time),
            source_stream="pro_matches",
        )
        if not is_target_window(manifest_row["recency_window"]):
            continue

        manifest_rows.append(manifest_row)

    return manifest_rows


def get_next_less_than_match_id(pro_matches_page: list[dict]) -> int | None:
    """Use the smallest match ID in the page to continue paging backwards."""

    page_match_ids = [
        int(pro_match["match_id"])
        for pro_match in pro_matches_page
        if pro_match.get("match_id") is not None
    ]
    if not page_match_ids:
        return None

    next_less_than_match_id = min(page_match_ids)
    return next_less_than_match_id


def get_oldest_start_time(pro_matches_page: list[dict]) -> int | None:
    """Get the oldest start time observed in a page."""

    page_start_times = [
        int(pro_match["start_time"])
        for pro_match in pro_matches_page
        if pro_match.get("start_time") is not None
    ]
    if not page_start_times:
        return None

    oldest_start_time = min(page_start_times)
    return oldest_start_time


def get_newest_start_time(pro_matches_page: list[dict]) -> int | None:
    """Get the newest start time observed in a page."""

    page_start_times = [
        int(pro_match["start_time"])
        for pro_match in pro_matches_page
        if pro_match.get("start_time") is not None
    ]
    if not page_start_times:
        return None

    newest_start_time = max(page_start_times)
    return newest_start_time


def has_met_target_counts(
    selected_counts_by_window: dict[str, int],
    target_counts_by_window: dict[str, int],
) -> bool:
    """Return whether the bounded window targets have been satisfied."""

    for target_window in TARGET_WINDOWS:
        selected_count = selected_counts_by_window.get(target_window, 0)
        target_count = target_counts_by_window.get(target_window, 0)
        if selected_count < target_count:
            return False

    return True


def load_existing_manifest_rows(manifest_csv_path: Path) -> list[dict]:
    """Load an existing manifest CSV so a backfill can resume instead of restarting."""

    if not manifest_csv_path.exists():
        return []

    loaded_rows: list[dict] = []
    with manifest_csv_path.open("r", encoding="utf-8") as csv_file:
        reader = csv.DictReader(csv_file)
        for row in reader:
            loaded_rows.append(
                {
                    "match_id": int(row["match_id"]),
                    "start_time": int(row["start_time"]),
                    "source_stream": row["source_stream"],
                    "recency_window": row["recency_window"],
                }
            )
    return loaded_rows


def load_existing_raw_rows(raw_json_path: Path) -> list[dict]:
    """Load an existing raw proMatches snapshot."""

    if not raw_json_path.exists():
        return []

    loaded_rows = json.loads(raw_json_path.read_text(encoding="utf-8"))
    return loaded_rows


def build_selected_rows_by_match_id(manifest_rows: list[dict]) -> dict[int, dict]:
    """Index selected manifest rows by match ID."""

    selected_rows_by_match_id = {
        int(manifest_row["match_id"]): {
            "match_id": int(manifest_row["match_id"]),
            "start_time": int(manifest_row["start_time"]),
            "source_stream": str(manifest_row["source_stream"]),
            "recency_window": str(manifest_row["recency_window"]),
        }
        for manifest_row in manifest_rows
    }
    return selected_rows_by_match_id


def count_selected_rows_by_window(selected_rows_by_match_id: dict[int, dict]) -> dict[str, int]:
    """Count selected manifest rows by recency window."""

    selected_counts_by_window = {window: 0 for window in TARGET_WINDOWS}
    for manifest_row in selected_rows_by_match_id.values():
        recency_window = str(manifest_row["recency_window"])
        if recency_window in selected_counts_by_window:
            selected_counts_by_window[recency_window] += 1

    return selected_counts_by_window


def determine_active_backfill_window(
    selected_counts_by_window: dict[str, int],
    target_counts_by_window: dict[str, int],
) -> str | None:
    """Pick the highest-priority window that still needs more matches."""

    for window in WINDOW_ORDER:
        selected_count = selected_counts_by_window.get(window, 0)
        target_count = target_counts_by_window.get(window, 0)
        if selected_count < target_count:
            return window

    return None


def build_backfill_progress_summary(
    *,
    selected_counts_by_window: dict[str, int],
    target_counts_by_window: dict[str, int],
    next_less_than_match_id: int | None,
    request_count: int,
    oldest_page_start_time: int | None,
    newest_page_start_time: int | None,
) -> dict[str, Any]:
    """Build a progress summary for resumable historical backfills."""

    active_backfill_window = determine_active_backfill_window(
        selected_counts_by_window=selected_counts_by_window,
        target_counts_by_window=target_counts_by_window,
    )
    progress_summary = {
        "selected_counts_by_window": selected_counts_by_window,
        "target_counts_by_window": target_counts_by_window,
        "active_backfill_window": active_backfill_window,
        "next_less_than_match_id": next_less_than_match_id,
        "request_count": request_count,
        "oldest_page_start_time": oldest_page_start_time,
        "newest_page_start_time": newest_page_start_time,
        "completed_all_targets": has_met_target_counts(
            selected_counts_by_window=selected_counts_by_window,
            target_counts_by_window=target_counts_by_window,
        ),
    }
    return progress_summary


def write_progress_summary(progress_summary: dict[str, Any], output_prefix: str) -> None:
    """Persist a resumable progress checkpoint for historical backfills."""

    progress_summary_path = Path(f"outputs/manifests/{output_prefix}_pro_match_progress.json")
    progress_summary_path.parent.mkdir(parents=True, exist_ok=True)
    progress_summary_path.write_text(
        json.dumps(progress_summary, indent=2),
        encoding="utf-8",
    )


def collect_bounded_pro_match_manifests(
    *,
    target_counts_by_window: dict[str, int],
    max_requests: int,
    sleep_seconds: float,
    max_retries: int,
    retry_sleep_seconds: float,
    start_less_than_match_id: int | None,
    output_prefix: str,
    should_resume: bool,
) -> tuple[list[dict], list[dict], dict[str, Any]]:
    """Page through proMatches until each target window is sufficiently covered."""

    manifest_csv_path = Path(f"data/staging/{output_prefix}_pro_match_manifest.csv")
    raw_json_path = Path(f"data/raw/opendota/{output_prefix}_pro_matches.json")
    existing_manifest_rows = load_existing_manifest_rows(manifest_csv_path) if should_resume else []
    existing_raw_rows = load_existing_raw_rows(raw_json_path) if should_resume else []

    selected_rows_by_match_id = build_selected_rows_by_match_id(existing_manifest_rows)
    selected_counts_by_window = count_selected_rows_by_window(selected_rows_by_match_id)
    collected_raw_rows = list(existing_raw_rows)
    less_than_match_id = start_less_than_match_id
    request_count = 0
    oldest_page_start_time: int | None = None
    newest_page_start_time: int | None = None

    if should_resume and less_than_match_id is None:
        progress_summary_path = Path(f"outputs/manifests/{output_prefix}_pro_match_progress.json")
        if progress_summary_path.exists():
            progress_summary = json.loads(progress_summary_path.read_text(encoding="utf-8"))
            saved_less_than_match_id = progress_summary.get("next_less_than_match_id")
            if saved_less_than_match_id is not None:
                less_than_match_id = int(saved_less_than_match_id)

    for _ in range(max_requests):
        active_backfill_window = determine_active_backfill_window(
            selected_counts_by_window=selected_counts_by_window,
            target_counts_by_window=target_counts_by_window,
        )
        if active_backfill_window is None:
            break

        pro_matches_page = fetch_pro_matches_page(
            less_than_match_id=less_than_match_id,
            max_retries=max_retries,
            retry_sleep_seconds=retry_sleep_seconds,
        )
        request_count += 1
        if not pro_matches_page:
            break

        collected_raw_rows.extend(pro_matches_page)
        page_oldest_start_time = get_oldest_start_time(pro_matches_page)
        page_newest_start_time = get_newest_start_time(pro_matches_page)
        if page_oldest_start_time is not None:
            oldest_page_start_time = page_oldest_start_time
        if page_newest_start_time is not None and newest_page_start_time is None:
            newest_page_start_time = page_newest_start_time

        manifest_rows = build_pro_match_manifest_rows(pro_matches_page)
        for manifest_row in manifest_rows:
            match_id = int(manifest_row["match_id"])
            recency_window = str(manifest_row["recency_window"])
            already_selected = match_id in selected_rows_by_match_id
            has_window_capacity = (
                selected_counts_by_window[recency_window]
                < target_counts_by_window[recency_window]
            )
            if already_selected or not has_window_capacity:
                continue

            selected_rows_by_match_id[match_id] = manifest_row
            selected_counts_by_window[recency_window] += 1

        if has_met_target_counts(
            selected_counts_by_window=selected_counts_by_window,
            target_counts_by_window=target_counts_by_window,
        ):
            break

        less_than_match_id = get_next_less_than_match_id(pro_matches_page)
        if less_than_match_id is None:
            break

        active_backfill_window = determine_active_backfill_window(
            selected_counts_by_window=selected_counts_by_window,
            target_counts_by_window=target_counts_by_window,
        )
        if active_backfill_window == "2023_2024" and page_oldest_start_time is not None:
            has_reached_2023_window_boundary = page_oldest_start_time <= WINDOW_START_EPOCH["2025_2026"]
            if not has_reached_2023_window_boundary:
                time.sleep(sleep_seconds)
                continue

        time.sleep(sleep_seconds)

    selected_manifest_rows = sorted(
        selected_rows_by_match_id.values(),
        key=lambda row: (row["recency_window"], -int(row["start_time"])),
    )
    progress_summary = build_backfill_progress_summary(
        selected_counts_by_window=selected_counts_by_window,
        target_counts_by_window=target_counts_by_window,
        next_less_than_match_id=less_than_match_id,
        request_count=request_count,
        oldest_page_start_time=oldest_page_start_time,
        newest_page_start_time=newest_page_start_time,
    )
    return selected_manifest_rows, collected_raw_rows, progress_summary


def write_manifest_outputs(
    *,
    manifest_rows: list[dict],
    raw_rows: list[dict],
    output_prefix: str,
) -> None:
    """Persist raw and normalized pro match snapshots."""

    raw_json_path = Path(f"data/raw/opendota/{output_prefix}_pro_matches.json")
    manifest_csv_path = Path(f"data/staging/{output_prefix}_pro_match_manifest.csv")

    raw_json_path.parent.mkdir(parents=True, exist_ok=True)
    manifest_csv_path.parent.mkdir(parents=True, exist_ok=True)

    raw_json_path.write_text(json.dumps(raw_rows, indent=2), encoding="utf-8")

    with manifest_csv_path.open("w", newline="", encoding="utf-8") as csv_file:
        field_names = ["match_id", "start_time", "source_stream", "recency_window"]
        writer = csv.DictWriter(csv_file, fieldnames=field_names)
        writer.writeheader()
        writer.writerows(manifest_rows)


def main() -> None:
    parser = argparse.ArgumentParser(description="Ingest bounded pro match manifests.")
    parser.add_argument(
        "--target-2023-2024",
        type=int,
        default=300,
        help="Target number of pro matches in the 2023_2024 window",
    )
    parser.add_argument(
        "--target-2025-2026",
        type=int,
        default=300,
        help="Target number of pro matches in the 2025_2026 window",
    )
    parser.add_argument(
        "--max-requests",
        type=int,
        default=60,
        help="Maximum number of proMatches page requests to make in this run",
    )
    parser.add_argument(
        "--sleep-seconds",
        type=float,
        default=0.2,
        help="Pause between page requests to reduce rate limit pressure",
    )
    parser.add_argument(
        "--max-retries",
        type=int,
        default=3,
        help="Maximum retries for one page request when OpenDota rate limits or transiently fails",
    )
    parser.add_argument(
        "--retry-sleep-seconds",
        type=float,
        default=1.0,
        help="Base sleep used between retries for one page request",
    )
    parser.add_argument(
        "--start-less-than-match-id",
        type=int,
        default=None,
        help="Resume paging from a known older match_id cursor instead of restarting at the latest page",
    )
    parser.add_argument(
        "--resume-existing",
        action="store_true",
        help="Resume from existing manifest/raw/progress files for the same output prefix",
    )
    parser.add_argument(
        "--output-prefix",
        default="historical_slice",
        help="File name prefix used for output artifacts",
    )
    args = parser.parse_args()

    target_counts_by_window = {
        "2023_2024": args.target_2023_2024,
        "2025_2026": args.target_2025_2026,
    }
    manifest_rows, raw_rows, progress_summary = collect_bounded_pro_match_manifests(
        target_counts_by_window=target_counts_by_window,
        max_requests=args.max_requests,
        sleep_seconds=args.sleep_seconds,
        max_retries=args.max_retries,
        retry_sleep_seconds=args.retry_sleep_seconds,
        start_less_than_match_id=args.start_less_than_match_id,
        output_prefix=args.output_prefix,
        should_resume=args.resume_existing,
    )
    write_manifest_outputs(
        manifest_rows=manifest_rows,
        raw_rows=raw_rows,
        output_prefix=args.output_prefix,
    )
    write_progress_summary(progress_summary, args.output_prefix)
    print(
        "Saved "
        f"{len(manifest_rows)} pro match manifest rows. "
        f"Progress: {json.dumps(progress_summary)}"
    )


if __name__ == "__main__":
    main()
