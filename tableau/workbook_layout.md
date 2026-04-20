# Tableau Workbook Layout

## Workbook Metadata
- Workbook name: `opendota_liveops_meta_stability`
- Primary published datasource name: `opendota_liveops_curated_marts`
- Publishing order: publish datasource first, then build and publish the workbook
- Primary route: Tableau Cloud backed by Snowflake
- Fallback route: upload the CSV exports in `outputs/curated/`

## Datasource Assembly
- `mart_liveops_summary.csv` or `opendota_liveops.mart_liveops_summary`
- `mart_hero_meta_shift.csv` or `opendota_liveops.mart_hero_meta_shift`
- `mart_returning_player_proxy.csv` or `opendota_liveops.mart_returning_player_proxy`
- Keep the marts separate in Tableau and relate them on `RECENCY_WINDOW`

## Dashboard 1: LiveOps KPI Overview
- Window filter (`2023_2024`, `2025_2026`)
- KPI cards: total matches, total player rows, known accounts, average match duration
- Window summary table from `mart_liveops_summary`
- Short note explaining the public-ingestion proof versus pro-match historical spine

### Sheets
- `kpi_match_count_card`: `SUM(MATCH_COUNT)` from `mart_liveops_summary`
- `kpi_player_row_count_card`: `SUM(PLAYER_ROW_COUNT)` from `mart_liveops_summary`
- `kpi_known_account_count_card`: `SUM(KNOWN_ACCOUNT_COUNT)` from `mart_liveops_summary`
- `kpi_avg_match_duration_card`: `AVG(AVG_MATCH_DURATION_MINUTES)` from `mart_liveops_summary`
- `window_summary_table`: one row per `RECENCY_WINDOW` with match, account, patch, and league metrics

### Filters
- `RECENCY_WINDOW`

## Dashboard 2: Hero Meta Shift
- Hero-level pick-rate delta vs win-rate delta scatter from `mart_hero_meta_shift`
- Hero bars ranked by volatility score
- Highlight table for top `stable`, `watchlist`, and `volatile` heroes

### Sheets
- `hero_delta_scatter`: columns = `PICK_RATE_DELTA_VS_PRIOR_WINDOW`, rows = `WIN_RATE_DELTA_VS_PRIOR_WINDOW`, detail = `HERO_NAME`, color = `STABILITY_SEGMENT`
- `hero_volatility_ranked_bars`: bars ranked by `VOLATILITY_SCORE`
- `hero_segment_highlight_table`: `HERO_NAME`, `STABILITY_SEGMENT`, `CURRENT_WINDOW_PICK_RATE`, `CURRENT_WINDOW_WIN_RATE`

### Filters
- `RECENCY_WINDOW`
- `PRIMARY_ATTRIBUTE`
- `ATTACK_TYPE`
- `STABILITY_SEGMENT`

## Dashboard 3: Stability vs Volatility Action Board
- Quadrant view: stability index vs outcome impact
- Filters for primary attribute, attack type, and recency window
- Returning-player proxy summary from `mart_returning_player_proxy`
- Suggested PM action cards for monitor, tune, and experiment paths

### Sheets
- `hero_action_quadrant`: columns = `VOLATILITY_SCORE`, rows = `CURRENT_WINDOW_WIN_RATE`, detail = `HERO_NAME`, color = `STABILITY_SEGMENT`
- `returning_player_proxy_table`: one row per `RECENCY_WINDOW` from `mart_returning_player_proxy`
- `recommended_action_text`: dashboard text or shape card tied to `STABILITY_SEGMENT`

### Filters
- `RECENCY_WINDOW`
- `PRIMARY_ATTRIBUTE`
- `ATTACK_TYPE`

## Workbook Rules
- Treat `2025_2026` as the current comparison window and `2023_2024` as the prior comparison window.
- Build hero delta views from `mart_hero_meta_shift` only; do not recreate the delta logic in Tableau.
- If a workbook step requires a field not already exported by the marts, treat that as a data-model gap and fix the export layer instead of adding workbook-only logic.
