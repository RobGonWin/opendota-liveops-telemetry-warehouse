-- fct_match
-- Grain: one row per match_id

with candidate_matches as (
    select
        match_id,
        source_stream,
        recency_window,
        start_time,
        match_started_at_utc,
        match_date,
        duration_seconds,
        radiant_win,
        lobby_type,
        game_mode,
        patch,
        region,
        radiant_score,
        dire_score,
        league_id,
        league_name,
        loaded_at,
        case
            when source_stream = 'pro_matches' then 1
            when source_stream = 'public_matches' then 2
            else 3
        end as source_priority
    from {{ ref('stg_matches') }}
),

ranked_matches as (
    select
        *,
        row_number() over (
            partition by match_id
            order by source_priority asc, loaded_at desc
        ) as source_rank
    from candidate_matches
)

select
    match_id,
    source_stream,
    recency_window,
    start_time,
    match_started_at_utc,
    match_date,
    duration_seconds,
    radiant_win,
    lobby_type,
    game_mode,
    patch,
    region,
    radiant_score,
    dire_score,
    league_id,
    league_name,
    loaded_at
from ranked_matches
where source_rank = 1
