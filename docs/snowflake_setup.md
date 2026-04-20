# Snowflake And dbt Setup

## 0. Install Snowflake CLI And Cortex Code CLI
Snowflake's current Cortex Code docs say Cortex Code CLI setup uses your Snowflake connection configuration and list Snowflake CLI as a prerequisite.

Install and verify in this order:

```powershell
# Install Snowflake CLI from the official Windows installer, then verify:
snow --help

# Install Cortex Code CLI from the official Snowflake PowerShell bootstrapper:
irm https://ai.snowflake.com/static/cc-scripts/install.ps1 | iex

# Open a new terminal, then verify:
cortex --help
```

Official references:
- Snowflake CLI install docs: https://docs.snowflake.com/en/developer-guide/snowflake-cli/installation/installation
- Cortex Code CLI docs: https://docs.snowflake.com/en/user-guide/cortex-code/cortex-code-cli
- Cortex Code CLI reference: https://docs.snowflake.com/en/user-guide/cortex-code/cli-reference

## 1. Create Local Credentials
1. Copy `.env.example` to `.env`.
2. Fill in:
   - `SNOWFLAKE_ACCOUNT`
   - `SNOWFLAKE_USER`
   - `SNOWFLAKE_WAREHOUSE`
   - `SNOWFLAKE_DATABASE`
   - optional `SNOWFLAKE_ROLE`
   - optional `SNOWFLAKE_AUTHENTICATOR`
   - optional `SNOWFLAKE_PASSWORD`
   - optional `SNOWFLAKE_PRIVATE_KEY_PATH`
   - optional `SNOWFLAKE_PRIVATE_KEY_PASSPHRASE`
   - optional `SNOWFLAKE_PASSCODE`
   - optional `SNOWFLAKE_PASSCODE_IN_PASSWORD`
3. Keep `SNOWFLAKE_SCHEMA=opendota_liveops` unless you intentionally want a different schema.

For MFA-enabled accounts, Snowflake's Python connector docs support:
- `SNOWFLAKE_PASSCODE=<current 6-digit TOTP>` with `username_password_mfa`
- `SNOWFLAKE_PASSCODE_IN_PASSWORD=true` if your password already includes the current passcode
- `SNOWFLAKE_AUTHENTICATOR=externalbrowser` if your Snowflake setup allows browser auth

For RSA key-pair authentication with `snowflake_jwt`, set:
- `SNOWFLAKE_AUTHENTICATOR=snowflake_jwt`
- `SNOWFLAKE_PRIVATE_KEY_PATH=C:/path/to/rsa_key.p8`
- `SNOWFLAKE_PRIVATE_KEY_PASSPHRASE=` only if the `.p8` file is encrypted

With key-pair auth, leave `SNOWFLAKE_PASSCODE` empty. `SNOWFLAKE_PASSWORD` is also typically unnecessary for the repo loader and dbt unless your local setup intentionally still uses it.

## 2. Use The Repo-Local dbt Profile
Set:

```powershell
$env:DBT_PROFILES_DIR = "dbt"
```

The committed `dbt/profiles.yml` reads from `.env`, so `dbt run` and `dbt test` will target the same Snowflake account as the loader scripts.

## 3. Optional Snowflake CLI / Cortex Code Connection Template
If you want one named Snowflake connection for `snow` and `cortex`, copy `snowflake/connections.toml.example` to:

```text
%USERPROFILE%\.snowflake\connections.toml
```

Then replace the placeholder values and keep the connection name as `opendota_liveops` or rename it consistently.

For Cortex Code CLI, Snowflake's current CLI reference shows a connection block using either:
- `authenticator = "externalbrowser"` for browser-based SSO
- `password = "<PAT>"` for PAT authentication

Do not keep both values active in the same connection block.

After the file exists, both CLIs can reuse it:

```powershell
snow connection test -c opendota_liveops
cortex -c opendota_liveops -w .
```

## 4. First Bounded Run
This section contains the full bounded ingestion, normalization, warehouse load, and export sequence referenced from the README.

Run these commands from the repo root:

```powershell
python scripts/head_preflight_opendota.py https://api.opendota.com/api/publicMatches
python scripts/ingest_public_matches.py --limit 200 --output-prefix public_slice
python scripts/ingest_pro_matches_bounded.py --target-2023-2024 300 --target-2025-2026 300 --output-prefix historical_slice
# If the first run only fills the 2025_2026 window, continue the backfill from the saved cursor:
python scripts/ingest_pro_matches_bounded.py --target-2023-2024 300 --target-2025-2026 300 --output-prefix historical_slice --resume-existing
python scripts/ingest_hero_stats.py
python scripts/ingest_match_details_bounded.py --manifest-csv-paths data/staging/public_slice_public_match_ids.csv,data/staging/historical_slice_pro_match_manifest.csv --limit 800 --output-path data/raw/opendota/bounded_match_details.jsonl
python scripts/normalize_match_payloads.py --input-path data/raw/opendota/bounded_match_details.jsonl --match-output-csv data/curated/match_snapshot.csv --player-output-csv data/curated/player_match_snapshot.csv
python scripts/load_opendota_to_snowflake.py --match-payload-path data/raw/opendota/bounded_match_details.jsonl --hero-stats-path data/raw/opendota/hero_stats_snapshot.json --pro-matches-path data/raw/opendota/historical_slice_pro_matches.json --replace-existing
dbt run
dbt test
python scripts/export_curated_marts.py
python scripts/validate_tableau_exports.py
```

## 5. Tableau Cloud Handoff
- Add the Tableau variables in `.env` using the template in `.env.example`.
- Follow [docs/tableau_cloud_setup.md](tableau_cloud_setup.md) for the publish order and Tableau MCP mapping.
- Treat Snowflake as the dynamic source of truth. Use the CSV exports only as the Tableau Public or manual-upload fallback.

## 6. Expected Snowflake Objects
- `opendota_liveops.raw_matches`
- `opendota_liveops.raw_hero_stats`
- `opendota_liveops.raw_pro_matches`
- `opendota_liveops.dim_heroes`
- `opendota_liveops.fct_match`
- `opendota_liveops.fct_player_match`
- `opendota_liveops.mart_liveops_summary`
- `opendota_liveops.mart_hero_meta_shift`
- `opendota_liveops.mart_returning_player_proxy`
