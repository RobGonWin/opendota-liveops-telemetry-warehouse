-- dim_heroes
-- Grain: one row per hero_id

with hero_stats as (
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
    from {{ ref('stg_hero_stats') }}
),

latest_hero_snapshot as (
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
        loaded_at,
        row_number() over (
            partition by hero_id
            order by loaded_at desc
        ) as hero_snapshot_rank
    from hero_stats
)

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
from latest_hero_snapshot
where hero_snapshot_rank = 1
