# Stakeholder Memo: Hero Meta Stability vs Volatility

## Audience
LiveOps lead, product manager, and analytics engineering partner.

## Decision
Use the hero stability and volatility readout to decide which heroes should be monitored, which should be considered for balance intervention, and which are strong candidates for follow-up experimentation.

## Data Spine
- `publicMatches` proves the bounded public telemetry collection path.
- `proMatches` is the historical spine for the `2023_2024` and `2025_2026` comparison.
- `matches/{match_id}` supplies player and match detail rows used in the warehouse models.
- `heroStats` adds current hero metadata and benchmark context.

## What The Dashboard Should Show
- window-level KPI movement across the two target windows
- hero-level pick-rate and win-rate shifts
- a stability segment for each hero: `stable`, `watchlist`, or `volatile`
- a returning-player proxy based on known account IDs observed across target windows

## Interpretation Rules
- `stable`: small movement in both pick rate and win rate, generally appropriate for monitoring rather than immediate intervention
- `watchlist`: meaningful but not extreme movement, appropriate for targeted investigation
- `volatile`: clear movement in pick rate or win rate, appropriate for balance review or design follow-up

## Recommended PM Actions
1. Monitor stable high-usage heroes for centralization risk.
2. Investigate volatile rising heroes for patch-driven overperformance or unhealthy concentration.
3. Review volatile declining heroes for underuse, weak outcomes, or progression friction.
4. Pair hero volatility findings with the returning-player proxy before prioritizing a balance or exposure experiment.

## Suggested Follow-Up Experiment
Run one targeted balance or exposure adjustment for a volatile hero cohort and evaluate:
1. pick-rate normalization
2. win-rate compression toward the healthy band
3. returning-player proxy movement in the subsequent observation window

## Caveat
The returning-player metric is intentionally labeled as a proxy because anonymous accounts limit full retention measurement in public gameplay telemetry.
