"""Load local OpenDota snapshots into Snowflake raw tables."""

from __future__ import annotations

import argparse
import json
import os
from datetime import datetime, timezone
from pathlib import Path

import snowflake.connector
from dotenv import load_dotenv


def execute_rows_individually(
    connection: snowflake.connector.SnowflakeConnection,
    *,
    insert_statement: str,
    rows: list[tuple],
) -> None:
    """Insert rows one at a time to avoid connector rewrite issues on JSON payloads."""

    with connection.cursor() as cursor:
        for row in rows:
            cursor.execute(insert_statement, row)


def require_environment_variable(variable_name: str) -> str:
    """Read a required Snowflake environment variable."""

    variable_value = os.getenv(variable_name)
    if not variable_value:
        raise RuntimeError(f"Missing required environment variable: {variable_name}")

    return variable_value


def build_connection_settings(schema_name: str) -> dict[str, str]:
    """Build Snowflake connection settings from environment variables."""

    authenticator = os.getenv("SNOWFLAKE_AUTHENTICATOR")
    normalized_authenticator = None
    if authenticator:
        if authenticator.lower() == "snowflake_jwt":
            normalized_authenticator = "SNOWFLAKE_JWT"
        else:
            normalized_authenticator = authenticator

    private_key_path = os.getenv("SNOWFLAKE_PRIVATE_KEY_PATH") or os.getenv(
        "SNOWFLAKE_PRIVATE_KEY_FILE"
    )
    private_key_passphrase = os.getenv("SNOWFLAKE_PRIVATE_KEY_PASSPHRASE")

    connection_settings = {
        "account": require_environment_variable("SNOWFLAKE_ACCOUNT"),
        "user": require_environment_variable("SNOWFLAKE_USER"),
        "warehouse": require_environment_variable("SNOWFLAKE_WAREHOUSE"),
        "database": require_environment_variable("SNOWFLAKE_DATABASE"),
        "schema": schema_name,
    }

    uses_private_key_auth = bool(private_key_path)
    if uses_private_key_auth:
        connection_settings["private_key_file"] = private_key_path
        if private_key_passphrase:
            connection_settings["private_key_pwd"] = private_key_passphrase
    else:
        connection_settings["password"] = require_environment_variable("SNOWFLAKE_PASSWORD")

    role = os.getenv("SNOWFLAKE_ROLE")
    if role:
        connection_settings["role"] = role

    passcode = os.getenv("SNOWFLAKE_PASSCODE")
    passcode_in_password = os.getenv("SNOWFLAKE_PASSCODE_IN_PASSWORD")

    if normalized_authenticator:
        connection_settings["authenticator"] = normalized_authenticator
    elif passcode or str(passcode_in_password).lower() == "true":
        connection_settings["authenticator"] = "username_password_mfa"

    if passcode and not uses_private_key_auth:
        connection_settings["passcode"] = passcode

    if str(passcode_in_password).lower() == "true" and not uses_private_key_auth:
        connection_settings["passcode_in_password"] = True

    return connection_settings


def ensure_raw_tables(
    connection: snowflake.connector.SnowflakeConnection,
    schema_name: str,
) -> None:
    """Create the raw ingestion tables used by the dbt models."""

    statements = [
        f"""
        create schema if not exists {schema_name}
        """,
        f"""
        create table if not exists {schema_name}.raw_matches (
            match_id number,
            start_time number,
            source_stream string,
            recency_window string,
            raw_payload variant,
            loaded_at timestamp_ntz
        )
        """,
        f"""
        create table if not exists {schema_name}.raw_hero_stats (
            hero_id number,
            raw_payload variant,
            loaded_at timestamp_ntz
        )
        """,
        f"""
        create table if not exists {schema_name}.raw_pro_matches (
            match_id number,
            start_time number,
            recency_window string,
            raw_payload variant,
            loaded_at timestamp_ntz
        )
        """,
    ]

    with connection.cursor() as cursor:
        for statement in statements:
            cursor.execute(statement)


def truncate_raw_tables(
    connection: snowflake.connector.SnowflakeConnection,
    schema_name: str,
) -> None:
    """Truncate raw tables before a replacement load."""

    statements = [
        f"truncate table if exists {schema_name}.raw_matches",
        f"truncate table if exists {schema_name}.raw_hero_stats",
        f"truncate table if exists {schema_name}.raw_pro_matches",
    ]

    with connection.cursor() as cursor:
        for statement in statements:
            cursor.execute(statement)


def load_match_payloads(
    connection: snowflake.connector.SnowflakeConnection,
    schema_name: str,
    match_payload_path: Path,
) -> int:
    """Load enriched match detail JSONL rows into raw_matches."""

    if not match_payload_path.exists():
        return 0

    match_rows: list[tuple[int, int, str, str, str, str]] = []
    loaded_at = datetime.now(tz=timezone.utc).isoformat()
    with match_payload_path.open("r", encoding="utf-8") as input_file:
        for line in input_file:
            cleaned_line = line.strip()
            if not cleaned_line:
                continue

            payload_record = json.loads(cleaned_line)
            match_payload = payload_record["payload"]
            match_rows.append(
                (
                    int(payload_record["match_id"]),
                    int(payload_record["start_time"]),
                    str(payload_record["source_stream"]),
                    str(payload_record["recency_window"]),
                    json.dumps(match_payload),
                    loaded_at,
                )
            )

    if not match_rows:
        return 0

    insert_statement = f"""
        insert into {schema_name}.raw_matches (
            match_id,
            start_time,
            source_stream,
            recency_window,
            raw_payload,
            loaded_at
        )
        select
            %s,
            %s,
            %s,
            %s,
            parse_json(%s),
            %s
    """
    execute_rows_individually(
        connection,
        insert_statement=insert_statement,
        rows=match_rows,
    )

    return len(match_rows)


def load_hero_stats(
    connection: snowflake.connector.SnowflakeConnection,
    schema_name: str,
    hero_stats_path: Path,
) -> int:
    """Load the heroStats JSON array into raw_hero_stats."""

    if not hero_stats_path.exists():
        return 0

    hero_stats_payload = json.loads(hero_stats_path.read_text(encoding="utf-8"))
    loaded_at = datetime.now(tz=timezone.utc).isoformat()
    hero_rows: list[tuple[int, str, str]] = []
    for hero_stat in hero_stats_payload:
        hero_id = hero_stat.get("id")
        if hero_id is None:
            continue
        hero_rows.append((int(hero_id), json.dumps(hero_stat), loaded_at))

    if not hero_rows:
        return 0

    insert_statement = f"""
        insert into {schema_name}.raw_hero_stats (
            hero_id,
            raw_payload,
            loaded_at
        )
        select
            %s,
            parse_json(%s),
            %s
    """
    execute_rows_individually(
        connection,
        insert_statement=insert_statement,
        rows=hero_rows,
    )

    return len(hero_rows)


def load_pro_matches(
    connection: snowflake.connector.SnowflakeConnection,
    schema_name: str,
    pro_matches_path: Path,
) -> int:
    """Load the raw pro match snapshot into raw_pro_matches."""

    if not pro_matches_path.exists():
        return 0

    pro_match_payload = json.loads(pro_matches_path.read_text(encoding="utf-8"))
    loaded_at = datetime.now(tz=timezone.utc).isoformat()
    pro_match_rows: list[tuple[int, int, str, str, str]] = []
    for pro_match in pro_match_payload:
        match_id = pro_match.get("match_id")
        start_time = pro_match.get("start_time")
        if match_id is None or start_time is None:
            continue

        recency_window = "outside_target_windows"
        if datetime.fromtimestamp(int(start_time), tz=timezone.utc).year >= 2025:
            recency_window = "2025_2026"
        elif datetime.fromtimestamp(int(start_time), tz=timezone.utc).year >= 2023:
            recency_window = "2023_2024"

        pro_match_rows.append(
            (
                int(match_id),
                int(start_time),
                recency_window,
                json.dumps(pro_match),
                loaded_at,
            )
        )

    if not pro_match_rows:
        return 0

    insert_statement = f"""
        insert into {schema_name}.raw_pro_matches (
            match_id,
            start_time,
            recency_window,
            raw_payload,
            loaded_at
        )
        select
            %s,
            %s,
            %s,
            parse_json(%s),
            %s
    """
    execute_rows_individually(
        connection,
        insert_statement=insert_statement,
        rows=pro_match_rows,
    )

    return len(pro_match_rows)


def main() -> None:
    load_dotenv()

    parser = argparse.ArgumentParser(description="Load OpenDota snapshots into Snowflake.")
    parser.add_argument(
        "--schema-name",
        default=os.getenv("SNOWFLAKE_SCHEMA", "opendota_liveops"),
        help="Snowflake schema used for the raw OpenDota tables",
    )
    parser.add_argument(
        "--match-payload-path",
        default="data/raw/opendota/initial_slice_match_details.jsonl",
        help="JSONL file containing enriched match payload records",
    )
    parser.add_argument(
        "--hero-stats-path",
        default="data/raw/opendota/hero_stats_snapshot.json",
        help="JSON file containing the heroStats snapshot",
    )
    parser.add_argument(
        "--pro-matches-path",
        default="data/raw/opendota/historical_slice_pro_matches.json",
        help="JSON file containing the raw proMatches snapshot",
    )
    parser.add_argument(
        "--replace-existing",
        action="store_true",
        help="Truncate raw tables before loading the local snapshots",
    )
    args = parser.parse_args()

    connection_settings = build_connection_settings(schema_name=args.schema_name)
    connection = snowflake.connector.connect(**connection_settings)

    try:
        ensure_raw_tables(connection, schema_name=args.schema_name)
        if args.replace_existing:
            truncate_raw_tables(connection, schema_name=args.schema_name)

        loaded_match_count = load_match_payloads(
            connection,
            schema_name=args.schema_name,
            match_payload_path=Path(args.match_payload_path),
        )
        loaded_hero_stats_count = load_hero_stats(
            connection,
            schema_name=args.schema_name,
            hero_stats_path=Path(args.hero_stats_path),
        )
        loaded_pro_match_count = load_pro_matches(
            connection,
            schema_name=args.schema_name,
            pro_matches_path=Path(args.pro_matches_path),
        )
        print(
            "Loaded "
            f"{loaded_match_count} raw match rows, "
            f"{loaded_hero_stats_count} hero rows, "
            f"and {loaded_pro_match_count} pro match rows."
        )
    finally:
        connection.close()


if __name__ == "__main__":
    main()
