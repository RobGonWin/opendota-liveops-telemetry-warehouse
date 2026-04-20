# OpenDota LiveOps Analytics Pipeline

Analytics project that ingests bounded OpenDota telemetry, loads it into Snowflake, models it with dbt, and publishes Tableau-ready marts for a LiveOps analytics workflow.

**Business question:** Which heroes look stable versus volatile across the `2023_2024` and `2025_2026` comparison windows, and what should a LiveOps or product team monitor next?

**Stack:** Python, Snowflake, dbt, Tableau-style curated exports, bounded public OpenDota data

## Demo Snapshot
![OpenDota LiveOps dim heroes](https://github.com/user-attachments/assets/38a82cad-dd1d-42fd-b541-7a595b19a0a7 "Image of Snowflake LiveOps dim heroes snapshot")

![OpenDota LiveOps raw matches](https://github.com/user-attachments/assets/4be02292-e6b5-4d96-a82e-3e92bcf5b722 "Image of Snowflake LiveOps raw matches snapshot")

## What This Project Proves
- Bounded, reproducible API ingestion
- Warehouse loading and dbt-based transformation discipline
- Validation-aware export contracts for a presentation layer
- Stakeholder-facing framing around a concrete LiveOps decision
- Reviewable sample artifacts that make the workflow inspectable without a full rerun

## Current Demo State
This repository is in a working demo state. It includes:

- Bounded ingestion scripts for `publicMatches`, `proMatches`, `matches/{match_id}`, and `heroStats`
- Resumable historical pro-match backfill with saved progress manifests
- Snowflake raw-layer loading for match, hero, and pro-match snapshots
- dbt staging, core, and mart models
- Curated Tableau-facing CSV exports and export validation
- Python tests, dbt schema tests, and stakeholder-facing documentation

The public repo keeps bounded sample artifacts so reviewers can inspect the pipeline outputs without re-running the full stack:

- Raw and staging sample artifacts are checked in under [data/raw/opendota](data/raw/opendota) and [data/staging](data/staging).
- Normalized analytical snapshots are checked in under [data/curated](data/curated).
- Curated Tableau-facing exports are checked in under [outputs/curated](outputs/curated).
- The Tableau workbook structure is documented in [tableau/workbook_layout.md](tableau/workbook_layout.md).
- The stakeholder narrative is documented in [docs/liveops_stakeholder_memo.md](docs/liveops_stakeholder_memo.md).

The checked-in files in `outputs/curated/` are an intentionally lightweight `2025_2026` sample export for the public repo. The ingestion pipeline, Snowflake/dbt models, validation tests, and normalized snapshots in `data/curated/` support the full `2023_2024` vs `2025_2026` comparison when the export step is rerun against a refreshed warehouse build.

## Stakeholder Summary
For non-technical reviewers, this project is a compact LiveOps analytics stack:

- It pulls a bounded slice of gameplay telemetry from OpenDota rather than implying unlimited ingestion.
- It converts noisy match-level API payloads into reviewable warehouse tables and decision-ready summary marts.
- It frames the output around product questions: what changed, which heroes moved the most, and what action should come next.
- It supports a Tableau presentation layer, with CSV exports included as a fallback when a live Snowflake-connected workbook is not being used.

The main business output is a hero stability readout that groups heroes into `stable`, `watchlist`, or `volatile`, plus a returning-player proxy that helps separate balance issues from broader engagement questions.

## Why A Team Would Care
A LiveOps, product analytics, or data platform team could use this pattern to turn noisy public telemetry into reviewable warehouse tables, compare bounded historical windows, and support balance or engagement decisions without hiding business logic in workbook-only calculations.

## Technical Summary
- Source data comes from OpenDota `publicMatches`, `proMatches`, `matches/{match_id}`, and `heroStats`.
- The warehouse model separates staging, core facts and dimensions, and mart-level outputs instead of mixing logic into one script.
- Historical backfill is resumable through saved progress manifests and bounded target-window counts.
- The presentation layer uses curated CSV exports backed by explicit validation rather than workbook-only calculations.
- Key outputs include `mart_liveops_summary`, `mart_hero_meta_shift`, and `mart_returning_player_proxy`.

## Architecture
### Source Data
- `publicMatches`: bounded proof of recent public telemetry ingestion
- `proMatches`: historical comparison spine for the two recency windows
- `matches/{match_id}`: match- and player-level detail used for warehouse facts
- `heroStats`: current hero metadata and benchmark enrichment

### Pipeline Flow
1. Run a `HEAD` preflight check against the OpenDota endpoint.
2. Ingest bounded public and pro match snapshots.
3. Build manifest files with `source_stream` and `recency_window` labels.
4. Fetch bounded match detail payloads from the manifest union.
5. Normalize and load raw payloads into Snowflake.
6. Build dbt staging, core, and mart models.
7. Export curated marts for Tableau consumption.
8. Validate the exports before workbook assembly.

### Data Model
- `stg_matches`: staged match-level records
- `stg_hero_stats`: staged hero metadata
- `dim_heroes`: one row per hero
- `fct_match`: one row per match
- `fct_player_match`: one row per player per match
- `mart_liveops_summary`: one row per recency window
- `mart_hero_meta_shift`: one row per hero per recency window
- `mart_returning_player_proxy`: one row per recency window

## Why The Modeling Matters
This repo is structured as analytics engineering work rather than a dashboard-only demo.

- The ingestion layer is bounded and resumable, which makes the project reproducible.
- The warehouse layer separates staging, facts, dimensions, and marts instead of mixing all logic into one script.
- The Tableau layer is fed by curated outputs so business logic stays in SQL and Python, not buried in workbook calculations.
- The validation layer checks whether the exported marts are actually usable for the intended dashboard story.

## Tableau Story
The intended presentation surface is a Tableau workbook with three views:

- KPI overview by recency window
- Hero meta shift scatter and ranked volatility views
- Action board combining hero volatility with a returning-player proxy

Workbook design notes live in [tableau/workbook_layout.md](tableau/workbook_layout.md), and Tableau Cloud setup guidance lives in [docs/tableau_cloud_setup.md](docs/tableau_cloud_setup.md).

No published Tableau URL is checked into the repo right now. The repo includes the workbook specification, export contract, and Cloud setup path, but not a committed public workbook link.

## Repository Layout
```text
.
|-- data/
|   |-- curated/
|   |-- raw/
|   `-- staging/
|-- docs/
|-- models/
|   |-- core/
|   |-- marts/
|   `-- staging/
|-- outputs/
|   |-- curated/
|   `-- manifests/
|-- scripts/
|-- snowflake/
|-- tableau/
`-- tests/
```

## Key Technical Assets
### Ingestion And Orchestration
- [scripts/head_preflight_opendota.py](scripts/head_preflight_opendota.py)
- [scripts/ingest_public_matches.py](scripts/ingest_public_matches.py)
- [scripts/ingest_pro_matches_bounded.py](scripts/ingest_pro_matches_bounded.py)
- [scripts/ingest_match_details_bounded.py](scripts/ingest_match_details_bounded.py)
- [scripts/ingest_hero_stats.py](scripts/ingest_hero_stats.py)
- [scripts/opendota_pipeline_utils.py](scripts/opendota_pipeline_utils.py)

### Warehouse And Export
- [scripts/normalize_match_payloads.py](scripts/normalize_match_payloads.py)
- [scripts/load_opendota_to_snowflake.py](scripts/load_opendota_to_snowflake.py)
- [scripts/export_curated_marts.py](scripts/export_curated_marts.py)
- [scripts/validate_tableau_exports.py](scripts/validate_tableau_exports.py)

### dbt Models
- [models/staging/stg_matches.sql](models/staging/stg_matches.sql)
- [models/staging/stg_hero_stats.sql](models/staging/stg_hero_stats.sql)
- [models/core/dim_heroes.sql](models/core/dim_heroes.sql)
- [models/core/fct_match.sql](models/core/fct_match.sql)
- [models/core/fct_player_match.sql](models/core/fct_player_match.sql)
- [models/marts/mart_liveops_summary.sql](models/marts/mart_liveops_summary.sql)
- [models/marts/mart_hero_meta_shift.sql](models/marts/mart_hero_meta_shift.sql)
- [models/marts/mart_returning_player_proxy.sql](models/marts/mart_returning_player_proxy.sql)

## Validation
The project includes both code-level and data-model validation.

- Python unit tests cover deduplication, window assignment, retry/backoff behavior, manifest unions, export validation, and ingestion edge cases.
- dbt tests cover key uniqueness, non-null critical fields, accepted recency windows, stability segment values, and fact-to-dimension relationships.
- [scripts/validate_tableau_exports.py](scripts/validate_tableau_exports.py) checks whether exported marts satisfy the intended two-window Tableau contract when a full warehouse-backed export is produced.

## Quickstart
```bash
python scripts/head_preflight_opendota.py https://api.opendota.com/api/publicMatches
python scripts/ingest_public_matches.py --limit 200 --output-prefix public_slice
dbt run
python scripts/export_curated_marts.py
python scripts/validate_tableau_exports.py
```

For the full bounded ingestion, normalization, Snowflake load, and Tableau handoff sequence, see [docs/snowflake_setup.md](docs/snowflake_setup.md) and [docs/tableau_cloud_setup.md](docs/tableau_cloud_setup.md).

## Environment Notes
- Python dependencies live in [requirements.txt](requirements.txt).
- Snowflake environment variables are documented in [.env.example](.env.example).
- Snowflake setup details live in [docs/snowflake_setup.md](docs/snowflake_setup.md).
- Tableau environment and MCP mapping details live in [docs/tableau_cloud_setup.md](docs/tableau_cloud_setup.md) and [tableau/tableau_mcp_env.example](tableau/tableau_mcp_env.example).

## Hiring Signal
This repo demonstrates bounded API ingestion, resumable backfill logic, Snowflake loading, dbt-based metric modeling, Tableau-ready export validation, and stakeholder framing around a concrete LiveOps question rather than a dashboard-only exercise.
