# Project Brief: OpenDota LiveOps Meta Stability Warehouse

## Objective
Build a bounded and reproducible gameplay telemetry pipeline that answers one LiveOps decision question with Snowflake-backed dbt marts and Tableau-ready exports.

## Dataset
- OpenDota `publicMatches` for bounded public telemetry proof
- OpenDota `proMatches` for the cross-window historical spine
- OpenDota `matches/{match_id}` for match and player detail rows
- OpenDota `heroStats` for current hero metadata and benchmark enrichment

## Business Question
Which heroes are stable versus volatile across `2023_2024` and `2025_2026`, and which should a PM or LiveOps lead monitor, tune, or test next?

## Data Grain
- Raw match detail ingestion: one JSON payload per match
- `fct_match`: one row per match
- `fct_player_match`: one row per player per match
- `mart_hero_meta_shift`: one row per hero per recency window
- `mart_liveops_summary`: one row per recency window
- `mart_returning_player_proxy`: one row per recency window

## Transformations
1. Build public and pro match manifests with explicit `source_stream` and `recency_window`.
2. Persist resumable pro match progress checkpoints so historical backfills can continue from a saved cursor instead of restarting at the newest page.
3. Fetch bounded match detail payloads using the manifest union.
4. Load `raw_matches`, `raw_hero_stats`, and `raw_pro_matches` into Snowflake.
5. Stage hero and match data in dbt.
6. Build core facts and dimensions.
7. Publish Tableau-ready marts and CSV exports.

## Validation Plan
- Unit tests for recency windows, manifest preference logic, pro match manifest filtering, and heroStats normalization
- dbt tests for primary keys, not-null fields, accepted values, and fact-to-dimension relationships
- Export sanity checks for row counts and recency window membership

## Deliverables
- bounded ingestion scripts
- Snowflake loader
- dbt project with staging, core, and mart models
- curated CSV exports for Tableau Public fallback
- stakeholder memo and workbook layout
