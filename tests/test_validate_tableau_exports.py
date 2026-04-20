import csv
from pathlib import Path

from scripts.validate_tableau_exports import build_validation_report


def write_csv(csv_path: Path, rows: list[dict[str, str]]) -> None:
    field_names = list(rows[0].keys())
    with csv_path.open("w", encoding="utf-8", newline="") as csv_file:
        writer = csv.DictWriter(csv_file, fieldnames=field_names)
        writer.writeheader()
        writer.writerows(rows)


def build_valid_export_set(output_directory: Path) -> None:
    write_csv(
        output_directory / "mart_liveops_summary.csv",
        [
            {
                "RECENCY_WINDOW": "2023_2024",
                "MATCH_COUNT": "300",
                "PLAYER_ROW_COUNT": "3000",
                "KNOWN_ACCOUNT_COUNT": "900",
                "DISTINCT_HERO_COUNT": "120",
                "AVG_MATCH_DURATION_MINUTES": "35.0",
                "RADIANT_WIN_RATE": "0.5",
                "PATCH_COUNT": "3",
                "LEAGUE_COUNT": "10",
            },
            {
                "RECENCY_WINDOW": "2025_2026",
                "MATCH_COUNT": "300",
                "PLAYER_ROW_COUNT": "3000",
                "KNOWN_ACCOUNT_COUNT": "950",
                "DISTINCT_HERO_COUNT": "121",
                "AVG_MATCH_DURATION_MINUTES": "34.0",
                "RADIANT_WIN_RATE": "0.49",
                "PATCH_COUNT": "4",
                "LEAGUE_COUNT": "12",
            },
        ],
    )
    write_csv(
        output_directory / "mart_returning_player_proxy.csv",
        [
            {
                "RECENCY_WINDOW": "2023_2024",
                "KNOWN_ACCOUNT_COUNT": "900",
                "RETURNING_ACCOUNT_COUNT": "250",
                "NET_NEW_ACCOUNT_COUNT": "650",
                "RETURNING_ACCOUNT_RATE": "0.2778",
            },
            {
                "RECENCY_WINDOW": "2025_2026",
                "KNOWN_ACCOUNT_COUNT": "950",
                "RETURNING_ACCOUNT_COUNT": "280",
                "NET_NEW_ACCOUNT_COUNT": "670",
                "RETURNING_ACCOUNT_RATE": "0.2947",
            },
        ],
    )
    write_csv(
        output_directory / "mart_hero_meta_shift.csv",
        [
            {
                "HERO_WINDOW_KEY": "1-2023_2024",
                "RECENCY_WINDOW": "2023_2024",
                "HERO_ID": "1",
                "HERO_NAME": "Anti-Mage",
                "PRIMARY_ATTRIBUTE": "agi",
                "ATTACK_TYPE": "Melee",
                "ROLE_LIST": '["Carry"]',
                "HERO_PICKS": "22",
                "WIN_RATE": "0.52",
                "PICK_RATE": "0.0073",
                "PREVIOUS_WINDOW_PICK_RATE": "0.0073",
                "CURRENT_WINDOW_PICK_RATE": "0.0080",
                "PREVIOUS_WINDOW_WIN_RATE": "0.52",
                "CURRENT_WINDOW_WIN_RATE": "0.50",
                "PICK_RATE_DELTA_VS_PRIOR_WINDOW": "0.0007",
                "WIN_RATE_DELTA_VS_PRIOR_WINDOW": "-0.02",
                "VOLATILITY_SCORE": "0.0207",
                "STABILITY_SEGMENT": "watchlist",
            },
            {
                "HERO_WINDOW_KEY": "1-2025_2026",
                "RECENCY_WINDOW": "2025_2026",
                "HERO_ID": "1",
                "HERO_NAME": "Anti-Mage",
                "PRIMARY_ATTRIBUTE": "agi",
                "ATTACK_TYPE": "Melee",
                "ROLE_LIST": '["Carry"]',
                "HERO_PICKS": "24",
                "WIN_RATE": "0.50",
                "PICK_RATE": "0.0080",
                "PREVIOUS_WINDOW_PICK_RATE": "0.0073",
                "CURRENT_WINDOW_PICK_RATE": "0.0080",
                "PREVIOUS_WINDOW_WIN_RATE": "0.52",
                "CURRENT_WINDOW_WIN_RATE": "0.50",
                "PICK_RATE_DELTA_VS_PRIOR_WINDOW": "0.0007",
                "WIN_RATE_DELTA_VS_PRIOR_WINDOW": "-0.02",
                "VOLATILITY_SCORE": "0.0207",
                "STABILITY_SEGMENT": "watchlist",
            },
        ],
    )


def test_build_validation_report_passes_for_two_window_export_set(tmp_path: Path) -> None:
    build_valid_export_set(tmp_path)

    validation_report = build_validation_report(tmp_path)

    assert validation_report["is_valid"] is True
    assert validation_report["errors"] == []


def test_build_validation_report_fails_when_windowed_mart_is_missing_target_window(
    tmp_path: Path,
) -> None:
    build_valid_export_set(tmp_path)
    write_csv(
        tmp_path / "mart_liveops_summary.csv",
        [
            {
                "RECENCY_WINDOW": "2025_2026",
                "MATCH_COUNT": "300",
                "PLAYER_ROW_COUNT": "3000",
                "KNOWN_ACCOUNT_COUNT": "950",
                "DISTINCT_HERO_COUNT": "121",
                "AVG_MATCH_DURATION_MINUTES": "34.0",
                "RADIANT_WIN_RATE": "0.49",
                "PATCH_COUNT": "4",
                "LEAGUE_COUNT": "12",
            }
        ],
    )

    validation_report = build_validation_report(tmp_path)

    assert validation_report["is_valid"] is False
    assert "mart_liveops_summary.csv is missing target windows: 2023_2024" in validation_report["errors"]


def test_build_validation_report_fails_when_hero_delta_fields_are_blank(tmp_path: Path) -> None:
    build_valid_export_set(tmp_path)
    write_csv(
        tmp_path / "mart_hero_meta_shift.csv",
        [
            {
                "HERO_WINDOW_KEY": "1-2023_2024",
                "RECENCY_WINDOW": "2023_2024",
                "HERO_ID": "1",
                "HERO_NAME": "Anti-Mage",
                "PRIMARY_ATTRIBUTE": "agi",
                "ATTACK_TYPE": "Melee",
                "ROLE_LIST": '["Carry"]',
                "HERO_PICKS": "22",
                "WIN_RATE": "0.52",
                "PICK_RATE": "0.0073",
                "PREVIOUS_WINDOW_PICK_RATE": "",
                "CURRENT_WINDOW_PICK_RATE": "",
                "PREVIOUS_WINDOW_WIN_RATE": "",
                "CURRENT_WINDOW_WIN_RATE": "",
                "PICK_RATE_DELTA_VS_PRIOR_WINDOW": "",
                "WIN_RATE_DELTA_VS_PRIOR_WINDOW": "",
                "VOLATILITY_SCORE": "",
                "STABILITY_SEGMENT": "",
            },
            {
                "HERO_WINDOW_KEY": "1-2025_2026",
                "RECENCY_WINDOW": "2025_2026",
                "HERO_ID": "1",
                "HERO_NAME": "Anti-Mage",
                "PRIMARY_ATTRIBUTE": "agi",
                "ATTACK_TYPE": "Melee",
                "ROLE_LIST": '["Carry"]',
                "HERO_PICKS": "24",
                "WIN_RATE": "0.50",
                "PICK_RATE": "0.0080",
                "PREVIOUS_WINDOW_PICK_RATE": "",
                "CURRENT_WINDOW_PICK_RATE": "",
                "PREVIOUS_WINDOW_WIN_RATE": "",
                "CURRENT_WINDOW_WIN_RATE": "",
                "PICK_RATE_DELTA_VS_PRIOR_WINDOW": "",
                "WIN_RATE_DELTA_VS_PRIOR_WINDOW": "",
                "VOLATILITY_SCORE": "",
                "STABILITY_SEGMENT": "",
            },
        ],
    )

    validation_report = build_validation_report(tmp_path)

    assert validation_report["is_valid"] is False
    assert (
        "mart_hero_meta_shift.csv does not contain a 2025_2026 hero row with populated prior/current delta fields."
        in validation_report["errors"]
    )
