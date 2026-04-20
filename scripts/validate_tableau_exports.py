"""Validate curated mart exports before Tableau workbook assembly.

This script checks that the exported CSV marts support the planned
two-window Tableau workbook without requiring ad hoc workbook logic.
"""

from __future__ import annotations

import argparse
import csv
import json
from pathlib import Path

TARGET_WINDOWS = ("2023_2024", "2025_2026")
WINDOWED_MART_REQUIREMENTS = {
    "mart_liveops_summary.csv": {
        "required_columns": (
            "RECENCY_WINDOW",
            "MATCH_COUNT",
            "PLAYER_ROW_COUNT",
            "KNOWN_ACCOUNT_COUNT",
            "DISTINCT_HERO_COUNT",
            "AVG_MATCH_DURATION_MINUTES",
            "RADIANT_WIN_RATE",
            "PATCH_COUNT",
            "LEAGUE_COUNT",
        ),
        "window_column": "RECENCY_WINDOW",
    },
    "mart_returning_player_proxy.csv": {
        "required_columns": (
            "RECENCY_WINDOW",
            "KNOWN_ACCOUNT_COUNT",
            "RETURNING_ACCOUNT_COUNT",
            "NET_NEW_ACCOUNT_COUNT",
            "RETURNING_ACCOUNT_RATE",
        ),
        "window_column": "RECENCY_WINDOW",
    },
}
HERO_META_SHIFT_REQUIREMENTS = {
    "required_columns": (
        "HERO_WINDOW_KEY",
        "RECENCY_WINDOW",
        "HERO_ID",
        "HERO_NAME",
        "PRIMARY_ATTRIBUTE",
        "ATTACK_TYPE",
        "ROLE_LIST",
        "HERO_PICKS",
        "WIN_RATE",
        "PICK_RATE",
        "PREVIOUS_WINDOW_PICK_RATE",
        "CURRENT_WINDOW_PICK_RATE",
        "PREVIOUS_WINDOW_WIN_RATE",
        "CURRENT_WINDOW_WIN_RATE",
        "PICK_RATE_DELTA_VS_PRIOR_WINDOW",
        "WIN_RATE_DELTA_VS_PRIOR_WINDOW",
        "VOLATILITY_SCORE",
        "STABILITY_SEGMENT",
    ),
    "window_column": "RECENCY_WINDOW",
    "delta_columns": (
        "PREVIOUS_WINDOW_PICK_RATE",
        "CURRENT_WINDOW_PICK_RATE",
        "PREVIOUS_WINDOW_WIN_RATE",
        "CURRENT_WINDOW_WIN_RATE",
        "PICK_RATE_DELTA_VS_PRIOR_WINDOW",
        "WIN_RATE_DELTA_VS_PRIOR_WINDOW",
        "VOLATILITY_SCORE",
        "STABILITY_SEGMENT",
    ),
}


def read_csv_rows(csv_path: Path) -> tuple[list[str], list[dict[str, str]]]:
    """Read a CSV file into header + row dictionaries."""

    with csv_path.open("r", encoding="utf-8", newline="") as csv_file:
        reader = csv.DictReader(csv_file)
        field_names = reader.fieldnames or []
        rows = list(reader)
    return field_names, rows


def find_missing_columns(field_names: list[str], required_columns: tuple[str, ...]) -> list[str]:
    """Return required columns that are not present."""

    missing_columns = [column_name for column_name in required_columns if column_name not in field_names]
    return missing_columns


def find_missing_target_windows(
    rows: list[dict[str, str]],
    *,
    window_column: str,
) -> list[str]:
    """Return target windows that are absent from a mart export."""

    available_windows = {
        str(row.get(window_column, "")).strip()
        for row in rows
        if str(row.get(window_column, "")).strip()
    }
    missing_windows = [
        target_window for target_window in TARGET_WINDOWS if target_window not in available_windows
    ]
    return missing_windows


def has_complete_delta_fields(hero_rows: list[dict[str, str]]) -> bool:
    """Return whether at least one hero has the required prior/current delta fields."""

    for hero_row in hero_rows:
        row_window = str(hero_row.get("RECENCY_WINDOW", "")).strip()
        if row_window != "2025_2026":
            continue

        has_all_delta_values = all(str(hero_row.get(column_name, "")).strip() for column_name in HERO_META_SHIFT_REQUIREMENTS["delta_columns"])
        if has_all_delta_values:
            return True

    return False


def validate_windowed_mart(
    mart_path: Path,
    *,
    required_columns: tuple[str, ...],
    window_column: str,
) -> list[str]:
    """Validate one recency-window-level mart export."""

    validation_errors: list[str] = []
    if not mart_path.exists():
        validation_errors.append(f"Missing exported mart: {mart_path}")
        return validation_errors

    field_names, rows = read_csv_rows(mart_path)
    missing_columns = find_missing_columns(field_names, required_columns)
    if missing_columns:
        validation_errors.append(
            f"{mart_path.name} is missing required columns: {', '.join(missing_columns)}"
        )

    missing_windows = find_missing_target_windows(rows, window_column=window_column)
    if missing_windows:
        validation_errors.append(
            f"{mart_path.name} is missing target windows: {', '.join(missing_windows)}"
        )

    return validation_errors


def validate_hero_meta_shift_mart(mart_path: Path) -> list[str]:
    """Validate the hero-level mart used by the Tableau workbook."""

    validation_errors: list[str] = []
    if not mart_path.exists():
        validation_errors.append(f"Missing exported mart: {mart_path}")
        return validation_errors

    field_names, rows = read_csv_rows(mart_path)
    missing_columns = find_missing_columns(
        field_names,
        HERO_META_SHIFT_REQUIREMENTS["required_columns"],
    )
    if missing_columns:
        validation_errors.append(
            f"{mart_path.name} is missing required columns: {', '.join(missing_columns)}"
        )

    missing_windows = find_missing_target_windows(
        rows,
        window_column=HERO_META_SHIFT_REQUIREMENTS["window_column"],
    )
    if missing_windows:
        validation_errors.append(
            f"{mart_path.name} is missing target windows: {', '.join(missing_windows)}"
        )

    if not has_complete_delta_fields(rows):
        validation_errors.append(
            f"{mart_path.name} does not contain a 2025_2026 hero row with populated prior/current delta fields."
        )

    return validation_errors


def build_validation_report(output_directory: Path) -> dict[str, object]:
    """Build a machine-readable validation report for the exported marts."""

    validation_errors: list[str] = []
    for mart_name, requirements in WINDOWED_MART_REQUIREMENTS.items():
        mart_errors = validate_windowed_mart(
            output_directory / mart_name,
            required_columns=requirements["required_columns"],
            window_column=str(requirements["window_column"]),
        )
        validation_errors.extend(mart_errors)

    hero_meta_shift_path = output_directory / "mart_hero_meta_shift.csv"
    validation_errors.extend(validate_hero_meta_shift_mart(hero_meta_shift_path))

    validation_report = {
        "output_directory": str(output_directory),
        "target_windows": list(TARGET_WINDOWS),
        "is_valid": len(validation_errors) == 0,
        "errors": validation_errors,
    }
    return validation_report


def main() -> None:
    parser = argparse.ArgumentParser(description="Validate Tableau-ready curated mart exports.")
    parser.add_argument(
        "--output-directory",
        default="outputs/curated",
        help="Directory containing exported curated mart CSV files",
    )
    parser.add_argument(
        "--report-path",
        default="outputs/manifests/tableau_export_validation.json",
        help="Path used to save the JSON validation report",
    )
    args = parser.parse_args()

    output_directory = Path(args.output_directory)
    validation_report = build_validation_report(output_directory)

    report_path = Path(args.report_path)
    report_path.parent.mkdir(parents=True, exist_ok=True)
    report_path.write_text(json.dumps(validation_report, indent=2), encoding="utf-8")

    print(json.dumps(validation_report, indent=2))
    if not validation_report["is_valid"]:
        raise SystemExit("Tableau export validation failed.")


if __name__ == "__main__":
    main()
