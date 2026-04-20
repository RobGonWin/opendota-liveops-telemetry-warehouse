-- mart_liveops_summary
-- Grain: one row per recency_window

with in_scope_matches as (
    select
        match_id,
        source_stream,
        recency_window,
        duration_seconds,
        radiant_win,
        patch,
        league_id
    from {{ ref('fct_match') }}
    where recency_window in ('2023_2024', '2025_2026')
),

in_scope_player_matches as (
    select
        player_match_key,
        match_id,
        account_id,
        hero_id
    from {{ ref('fct_player_match') }}
    where recency_window in ('2023_2024', '2025_2026')
),

window_match_metrics as (
    select
        recency_window,
        count(*) as match_count,
        avg(duration_seconds) / 60.0 as avg_match_duration_minutes,
        avg(case when radiant_win then 1 else 0 end) as radiant_win_rate,
        count(distinct patch) as patch_count,
        count(distinct league_id) as league_count
    from in_scope_matches
    group by recency_window
),

window_player_metrics as (
    select
        fm.recency_window,
        count(*) as player_row_count,
        count(distinct ipm.account_id) as known_account_count,
        count(distinct ipm.hero_id) as distinct_hero_count
    from in_scope_player_matches as ipm
    inner join in_scope_matches as fm
        on ipm.match_id = fm.match_id
    group by fm.recency_window
)

select
    wmm.recency_window,
    wmm.match_count,
    wpm.player_row_count,
    wpm.known_account_count,
    wpm.distinct_hero_count,
    wmm.avg_match_duration_minutes,
    wmm.radiant_win_rate,
    wmm.patch_count,
    wmm.league_count
from window_match_metrics as wmm
inner join window_player_metrics as wpm
    on wmm.recency_window = wpm.recency_window
