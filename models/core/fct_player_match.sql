-- fct_player_match
-- Grain: one row per match_id x player_slot

with ranked_match_payloads as (
    select
        match_id,
        source_stream,
        recency_window,
        raw_payload,
        loaded_at,
        case
            when source_stream = 'pro_matches' then 1
            when source_stream = 'public_matches' then 2
            else 3
        end as source_priority,
        row_number() over (
            partition by match_id
            order by
                case
                    when source_stream = 'pro_matches' then 1
                    when source_stream = 'public_matches' then 2
                    else 3
                end asc,
                loaded_at desc
        ) as source_rank
    from {{ source('raw', 'raw_matches') }}
),

selected_match_payloads as (
    select
        match_id,
        source_stream,
        recency_window,
        raw_payload
    from ranked_match_payloads
    where source_rank = 1
),

flattened_players as (
    select
        selected_match_payloads.match_id,
        selected_match_payloads.source_stream,
        selected_match_payloads.recency_window,
        player.value:account_id::number as account_id,
        player.value:player_slot::number as player_slot,
        player.value:hero_id::number as hero_id,
        player.value:kills::number as kills,
        player.value:deaths::number as deaths,
        player.value:assists::number as assists,
        player.value:win::number as win,
        player.value:lane_role::number as lane_role,
        player.value:rank_tier::number as rank_tier,
        player.value:leaver_status::number as leaver_status,
        player.value:total_gold::number as total_gold,
        player.value:total_xp::number as total_xp
    from selected_match_payloads,
    lateral flatten(input => selected_match_payloads.raw_payload:players) as player
),

valid_player_matches as (
    select
        concat(match_id::string, '-', player_slot::string) as player_match_key,
        match_id,
        source_stream,
        recency_window,
        account_id,
        player_slot,
        hero_id,
        kills,
        deaths,
        assists,
        case
            when player_slot < 128 then true
            else false
        end as is_radiant,
        win,
        lane_role,
        rank_tier,
        leaver_status,
        total_gold,
        total_xp
    from flattened_players
    where match_id is not null
      and player_slot is not null
      and hero_id is not null
      and hero_id > 0
)

select
    player_match_key,
    match_id,
    source_stream,
    recency_window,
    account_id,
    player_slot,
    hero_id,
    kills,
    deaths,
    assists,
    is_radiant,
    win,
    lane_role,
    rank_tier,
    leaver_status,
    total_gold,
    total_xp
from valid_player_matches
