-- mart_hero_meta_shift
-- Grain: one row per hero_id x recency_window
-- This model measures pick-rate and win-rate movement across target windows.

with player_matches as (
    select
        fpm.match_id,
        fpm.hero_id,
        fpm.win,
        fm.recency_window
    from {{ ref('fct_player_match') }} as fpm
    inner join {{ ref('fct_match') }} as fm
        on fpm.match_id = fm.match_id
),

in_scope_player_matches as (
    select
        match_id,
        hero_id,
        win,
        recency_window
    from player_matches
    where recency_window in ('2023_2024', '2025_2026')
),

window_totals as (
    select
        recency_window,
        count(*) as total_player_rows
    from in_scope_player_matches
    group by recency_window
),

hero_window_metrics as (
    select
        pm.recency_window,
        pm.hero_id,
        count(*) as hero_picks,
        avg(case when pm.win = 1 then 1 else 0 end) as win_rate
    from in_scope_player_matches as pm
    group by pm.recency_window, pm.hero_id
),

current_window_metrics as (
    select
        hero_id,
        hero_picks as current_window_hero_picks,
        pick_rate as current_window_pick_rate,
        win_rate as current_window_win_rate
    from (
        select
            hwm.recency_window,
            hwm.hero_id,
            hwm.hero_picks,
            hwm.win_rate,
            hwm.hero_picks / nullif(wt.total_player_rows, 0) as pick_rate
        from hero_window_metrics as hwm
        inner join window_totals as wt
            on hwm.recency_window = wt.recency_window
    ) as metrics
    where recency_window = '2025_2026'
),

previous_window_metrics as (
    select
        hero_id,
        hero_picks as previous_window_hero_picks,
        pick_rate as previous_window_pick_rate,
        win_rate as previous_window_win_rate
    from (
        select
            hwm.recency_window,
            hwm.hero_id,
            hwm.hero_picks,
            hwm.win_rate,
            hwm.hero_picks / nullif(wt.total_player_rows, 0) as pick_rate
        from hero_window_metrics as hwm
        inner join window_totals as wt
            on hwm.recency_window = wt.recency_window
    ) as metrics
    where recency_window = '2023_2024'
),

hero_deltas as (
    select
        current_window_metrics.hero_id,
        current_window_metrics.current_window_hero_picks,
        previous_window_metrics.previous_window_hero_picks,
        current_window_metrics.current_window_pick_rate,
        previous_window_metrics.previous_window_pick_rate,
        current_window_metrics.current_window_win_rate,
        previous_window_metrics.previous_window_win_rate,
        current_window_metrics.current_window_pick_rate - previous_window_metrics.previous_window_pick_rate as pick_rate_delta_vs_prior_window,
        current_window_metrics.current_window_win_rate - previous_window_metrics.previous_window_win_rate as win_rate_delta_vs_prior_window,
        abs(current_window_metrics.current_window_pick_rate - previous_window_metrics.previous_window_pick_rate)
            + abs(current_window_metrics.current_window_win_rate - previous_window_metrics.previous_window_win_rate) as volatility_score,
        case
            when abs(current_window_metrics.current_window_pick_rate - previous_window_metrics.previous_window_pick_rate) < 0.01
                and abs(current_window_metrics.current_window_win_rate - previous_window_metrics.previous_window_win_rate) < 0.01
                then 'stable'
            when abs(current_window_metrics.current_window_pick_rate - previous_window_metrics.previous_window_pick_rate) >= 0.03
                or abs(current_window_metrics.current_window_win_rate - previous_window_metrics.previous_window_win_rate) >= 0.03
                then 'volatile'
            else 'watchlist'
        end as stability_segment
    from current_window_metrics
    inner join previous_window_metrics
        on current_window_metrics.hero_id = previous_window_metrics.hero_id
)

select
    concat(hwm.hero_id::string, '-', hwm.recency_window) as hero_window_key,
    hwm.recency_window,
    hwm.hero_id,
    d.hero_name,
    d.primary_attribute,
    d.attack_type,
    d.role_list,
    hwm.hero_picks,
    hwm.win_rate,
    wt.total_player_rows,
    hwm.hero_picks / nullif(wt.total_player_rows, 0) as pick_rate,
    hd.previous_window_hero_picks,
    hd.current_window_hero_picks,
    hd.previous_window_pick_rate,
    hd.current_window_pick_rate,
    hd.previous_window_win_rate,
    hd.current_window_win_rate,
    hd.pick_rate_delta_vs_prior_window,
    hd.win_rate_delta_vs_prior_window,
    hd.volatility_score,
    hd.stability_segment
from hero_window_metrics as hwm
inner join window_totals as wt
    on hwm.recency_window = wt.recency_window
left join {{ ref('dim_heroes') }} as d
    on hwm.hero_id = d.hero_id
left join hero_deltas as hd
    on hwm.hero_id = hd.hero_id
