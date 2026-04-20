"""Normalize match payloads into reproducible tabular snapshots.

Outputs CSV snapshots for match-level and player-match-level analysis.
"""

from __future__ import annotations

import argparse
import csv
import json
from pathlib import Path

try:
    from scripts.opendota_pipeline_utils import assign_recency_window
except ImportError:  # pragma: no cover - supports direct script execution
    from opendota_pipeline_utils import assign_recency_window


def load_json_lines(payload_path: Path) -> list[dict]:
    """Load JSONL payloads from raw ingestion outputs."""

    payloads: list[dict] = []
    with payload_path.open("r", encoding="utf-8") as payload_file:
        for line in payload_file:
            cleaned_line = line.strip()
            if not cleaned_line:
                continue
            payloads.append(json.loads(cleaned_line))
    return payloads


def parse_match_detail_record(payload_record: dict) -> tuple[dict, str, str]:
    """Support both enriched JSONL envelopes and legacy raw payload lines."""

    has_enriched_envelope = "payload" in payload_record
    if has_enriched_envelope:
        match_payload = payload_record["payload"]
        source_stream = str(payload_record.get("source_stream", "unknown"))
        recency_window = str(
            payload_record.get(
                "recency_window",
                assign_recency_window(int(match_payload.get("start_time") or 0)),
            )
        )
        return match_payload, source_stream, recency_window

    match_payload = payload_record
    source_stream = "unknown"
    recency_window = assign_recency_window(int(match_payload.get("start_time") or 0))
    return match_payload, source_stream, recency_window


def build_match_rows(payload_records: list[dict]) -> list[dict]:
    """Create one row per match."""

    match_rows: list[dict] = []
    for payload_record in payload_records:
        payload, source_stream, recency_window = parse_match_detail_record(payload_record)
        match_row = {
            "source_stream": source_stream,
            "recency_window": recency_window,
            "match_id": payload.get("match_id"),
            "start_time": payload.get("start_time"),
            "duration_seconds": payload.get("duration"),
            "radiant_win": payload.get("radiant_win"),
            "lobby_type": payload.get("lobby_type"),
            "game_mode": payload.get("game_mode"),
            "patch": payload.get("patch"),
            "region": payload.get("region"),
            "radiant_score": payload.get("radiant_score"),
            "dire_score": payload.get("dire_score"),
            "league_id": payload.get("leagueid"),
            "league_name": payload.get("league_name"),
        }
        match_rows.append(match_row)
    return match_rows


def build_player_rows(payload_records: list[dict]) -> list[dict]:
    """Create one row per player in each match."""

    player_rows: list[dict] = []
    for payload_record in payload_records:
        payload, source_stream, recency_window = parse_match_detail_record(payload_record)
        match_id = payload.get("match_id")
        players = payload.get("players") or []
        for player in players:
            player_row = {
                "source_stream": source_stream,
                "recency_window": recency_window,
                "match_id": match_id,
                "account_id": player.get("account_id"),
                "player_slot": player.get("player_slot"),
                "hero_id": player.get("hero_id"),
                "kills": player.get("kills"),
                "deaths": player.get("deaths"),
                "assists": player.get("assists"),
                "is_radiant": (player.get("player_slot") or 0) < 128,
                "win": player.get("win"),
                "lane_role": player.get("lane_role"),
                "rank_tier": player.get("rank_tier"),
                "leaver_status": player.get("leaver_status"),
                "total_gold": player.get("total_gold"),
                "total_xp": player.get("total_xp"),
            }
            player_rows.append(player_row)
    return player_rows


def write_csv(rows: list[dict], output_path: Path) -> None:
    """Write rows as CSV with explicit schema ordering."""

    output_path.parent.mkdir(parents=True, exist_ok=True)
    if not rows:
        output_path.write_text("", encoding="utf-8")
        return

    field_names = list(rows[0].keys())
    with output_path.open("w", newline="", encoding="utf-8") as csv_file:
        writer = csv.DictWriter(csv_file, fieldnames=field_names)
        writer.writeheader()
        writer.writerows(rows)



def main() -> None:
    parser = argparse.ArgumentParser(description="Normalize OpenDota match payloads.")
    parser.add_argument(
        "--input-path",
        default="data/raw/opendota/initial_slice_match_details.jsonl",
        help="Input JSONL path",
    )
    parser.add_argument(
        "--match-output-csv",
        default="data/curated/match_snapshot.csv",
        help="Output path for match-level CSV",
    )
    parser.add_argument(
        "--player-output-csv",
        default="data/curated/player_match_snapshot.csv",
        help="Output path for player-match-level CSV",
    )
    args = parser.parse_args()

    payload_records = load_json_lines(Path(args.input_path))
    match_rows = build_match_rows(payload_records)
    player_rows = build_player_rows(payload_records)

    write_csv(match_rows, Path(args.match_output_csv))
    write_csv(player_rows, Path(args.player_output_csv))

    print(
        f"Saved {len(match_rows)} match rows and {len(player_rows)} player rows "
        "to curated snapshots."
    )


if __name__ == "__main__":
    main()
