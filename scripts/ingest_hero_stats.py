"""Ingest the current OpenDota heroStats snapshot."""

from __future__ import annotations

import argparse
import json
from pathlib import Path

try:
    from scripts.opendota_pipeline_utils import fetch_json_with_backoff
except ImportError:  # pragma: no cover - supports direct script execution
    from opendota_pipeline_utils import fetch_json_with_backoff

HERO_STATS_URL = "https://api.opendota.com/api/heroStats"


def fetch_hero_stats(timeout_seconds: int = 20) -> list[dict]:
    """Fetch the current heroStats payload."""

    hero_stats_payload = fetch_json_with_backoff(
        HERO_STATS_URL,
        timeout_seconds=timeout_seconds,
    )
    return hero_stats_payload


def build_hero_stats_rows(hero_stats_payload: list[dict]) -> list[dict]:
    """Build minimal validation rows from the heroStats payload."""

    hero_stats_rows: list[dict] = []
    for hero_stat in hero_stats_payload:
        hero_id = hero_stat.get("id")
        localized_name = hero_stat.get("localized_name")
        if hero_id is None or localized_name is None:
            continue

        hero_stats_row = {
            "hero_id": int(hero_id),
            "hero_name": localized_name,
            "primary_attribute": hero_stat.get("primary_attr"),
            "attack_type": hero_stat.get("attack_type"),
            "pro_pick": hero_stat.get("pro_pick"),
            "pro_win": hero_stat.get("pro_win"),
        }
        hero_stats_rows.append(hero_stats_row)

    return hero_stats_rows


def write_hero_stats_snapshot(hero_stats_payload: list[dict], output_path: Path) -> None:
    """Persist the raw heroStats payload for warehouse loading."""

    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(json.dumps(hero_stats_payload, indent=2), encoding="utf-8")


def main() -> None:
    parser = argparse.ArgumentParser(description="Ingest the OpenDota heroStats snapshot.")
    parser.add_argument(
        "--output-path",
        default="data/raw/opendota/hero_stats_snapshot.json",
        help="Output path for the raw heroStats payload",
    )
    args = parser.parse_args()

    hero_stats_payload = fetch_hero_stats()
    hero_stats_rows = build_hero_stats_rows(hero_stats_payload)
    write_hero_stats_snapshot(hero_stats_payload, Path(args.output_path))
    print(f"Saved {len(hero_stats_rows)} heroStats rows to {args.output_path}.")


if __name__ == "__main__":
    main()
