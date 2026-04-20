-- stg_hero_stats
-- Grain: one row per hero_id

with source_hero_stats as (
    select
        hero_id as raw_hero_id,
        raw_payload:id::number as hero_id,
        raw_payload:localized_name::string as hero_name,
        raw_payload:primary_attr::string as primary_attribute,
        raw_payload:attack_type::string as attack_type,
        raw_payload:roles::string as role_list,
        raw_payload:pro_pick::number as pro_pick,
        raw_payload:pro_win::number as pro_win,
        raw_payload:pro_ban::number as pro_ban,
        raw_payload:turbo_picks::number as turbo_picks,
        raw_payload:turbo_wins::number as turbo_wins,
        loaded_at
    from {{ source('raw', 'raw_hero_stats') }}
),

valid_hero_stats as (
    select
        hero_id,
        hero_name,
        primary_attribute,
        attack_type,
        role_list,
        pro_pick,
        pro_win,
        pro_ban,
        turbo_picks,
        turbo_wins,
        loaded_at
    from source_hero_stats
    where hero_id is not null
)

select *
from valid_hero_stats
