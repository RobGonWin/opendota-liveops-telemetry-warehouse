-- mart_returning_player_proxy
-- Grain: one row per recency_window
-- Returning player is a proxy based on known account IDs observed in an earlier target window.

with in_scope_player_matches as (
    select
        fpm.account_id,
        fm.recency_window,
        fm.start_time
    from {{ ref('fct_player_match') }} as fpm
    inner join {{ ref('fct_match') }} as fm
        on fpm.match_id = fm.match_id
    where fm.recency_window in ('2023_2024', '2025_2026')
      and fpm.account_id is not null
),

first_seen_account_match as (
    select
        account_id,
        min(start_time) as first_seen_start_time
    from in_scope_player_matches
    group by account_id
),

window_start_boundaries as (
    select '2023_2024' as recency_window, to_timestamp_ntz('2023-01-01 00:00:00') as window_start_at
    union all
    select '2025_2026' as recency_window, to_timestamp_ntz('2025-01-01 00:00:00') as window_start_at
),

account_window_status as (
    select
        ipm.recency_window,
        ipm.account_id,
        max(
            case
                when to_timestamp_ntz(fsa.first_seen_start_time) < wsb.window_start_at then 1
                else 0
            end
        ) as is_returning_account
    from in_scope_player_matches as ipm
    inner join first_seen_account_match as fsa
        on ipm.account_id = fsa.account_id
    inner join window_start_boundaries as wsb
        on ipm.recency_window = wsb.recency_window
    group by ipm.recency_window, ipm.account_id
)

select
    recency_window,
    count(distinct account_id) as known_account_count,
    count(distinct case when is_returning_account = 1 then account_id end) as returning_account_count,
    count(distinct case when is_returning_account = 0 then account_id end) as net_new_account_count,
    count(distinct case when is_returning_account = 1 then account_id end)
        / nullif(count(distinct account_id), 0) as returning_account_rate
from account_window_status
group by recency_window
