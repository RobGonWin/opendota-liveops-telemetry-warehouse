from scripts.ingest_hero_stats import build_hero_stats_rows


def test_build_hero_stats_rows_keeps_required_hero_fields() -> None:
    hero_stats_payload = [
        {
            "id": 1,
            "localized_name": "Anti-Mage",
            "primary_attr": "agi",
            "attack_type": "Melee",
            "pro_pick": 30,
            "pro_win": 15,
        },
        {
            "id": None,
            "localized_name": "Missing Hero",
        },
    ]

    hero_stats_rows = build_hero_stats_rows(hero_stats_payload)

    assert hero_stats_rows == [
        {
            "hero_id": 1,
            "hero_name": "Anti-Mage",
            "primary_attribute": "agi",
            "attack_type": "Melee",
            "pro_pick": 30,
            "pro_win": 15,
        }
    ]
