-- stg_matches
-- Grain: one row per match_id x source_stream

with source_matches as (
    select
        match_id as raw_match_id,
        source_stream,
        recency_window,
        raw_payload:match_id::number as match_id,
        raw_payload:start_time::number as start_time,
        raw_payload:duration::number as duration_seconds,
        raw_payload:radiant_win::boolean as radiant_win,
        raw_payload:lobby_type::number as lobby_type,
        raw_payload:game_mode::number as game_mode,
        raw_payload:patch::number as patch,
        raw_payload:region::number as region,
        raw_payload:radiant_score::number as radiant_score,
        raw_payload:dire_score::number as dire_score,
        raw_payload:leagueid::number as league_id,
        raw_payload:league_name::string as league_name,
        loaded_at
    from {{ source('raw', 'raw_matches') }}
),

valid_matches as (
    select
        match_id,
        source_stream,
        recency_window,
        start_time,
        to_timestamp_ntz(start_time) as match_started_at_utc,
        to_date(to_timestamp_ntz(start_time)) as match_date,
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
    from source_matches
    where match_id is not null
)

select *
from valid_matches
