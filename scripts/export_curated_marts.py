"""Export curated Snowflake marts to CSV for Tableau Public fallback usage."""

from __future__ import annotations

import argparse
import csv
import os
from pathlib import Path

import snowflake.connector
from dotenv import load_dotenv

try:
    from scripts.load_opendota_to_snowflake import build_connection_settings
except ImportError:  # pragma: no cover - supports direct script execution
    from load_opendota_to_snowflake import build_connection_settings

def build_mart_export_queries(schema_name: str) -> dict[str, str]:
    """Build the curated mart export queries for one Snowflake schema."""

    mart_export_queries = {
        "mart_liveops_summary": f"select * from {schema_name}.mart_liveops_summary order by recency_window",
        "mart_hero_meta_shift": f"select * from {schema_name}.mart_hero_meta_shift order by hero_id, recency_window",
        "mart_returning_player_proxy": f"select * from {schema_name}.mart_returning_player_proxy order by recency_window",
    }
    return mart_export_queries


def export_query_to_csv(
    connection: snowflake.connector.SnowflakeConnection,
    *,
    sql_query: str,
    output_path: Path,
) -> None:
    """Run a query and export the result set as CSV."""

    output_path.parent.mkdir(parents=True, exist_ok=True)
    with connection.cursor() as cursor:
        cursor.execute(sql_query)
        column_names = [column_metadata[0] for column_metadata in cursor.description]
        rows = cursor.fetchall()

    with output_path.open("w", newline="", encoding="utf-8") as csv_file:
        writer = csv.writer(csv_file)
        writer.writerow(column_names)
        writer.writerows(rows)


def main() -> None:
    load_dotenv()

    parser = argparse.ArgumentParser(description="Export curated marts from Snowflake.")
    parser.add_argument(
        "--schema-name",
        default=os.getenv("SNOWFLAKE_SCHEMA", "opendota_liveops"),
        help="Snowflake schema used for the curated mart tables",
    )
    parser.add_argument(
        "--output-directory",
        default="outputs/curated",
        help="Directory used for exported mart CSV files",
    )
    args = parser.parse_args()

    connection_settings = build_connection_settings(schema_name=args.schema_name)
    connection = snowflake.connector.connect(**connection_settings)

    try:
        output_directory = Path(args.output_directory)
        mart_export_queries = build_mart_export_queries(args.schema_name)
        for mart_name, sql_query in mart_export_queries.items():
            output_path = output_directory / f"{mart_name}.csv"
            export_query_to_csv(
                connection,
                sql_query=sql_query,
                output_path=output_path,
            )
            print(f"Exported {mart_name} to {output_path}.")
    finally:
        connection.close()


if __name__ == "__main__":
    main()
