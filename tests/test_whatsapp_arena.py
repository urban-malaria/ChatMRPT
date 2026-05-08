from __future__ import annotations

from app.whatsapp import arena


def test_format_battle_error_is_user_facing():
    message = arena.format_battle({"error": "Arena disabled"})

    assert "Arena is not available" in message
    assert "Arena disabled" in message


def test_format_vote_result_continues_or_finishes():
    next_round = arena.format_vote_result({
        "continue": True,
        "response_a": "A response",
        "response_b": "B response",
        "current_round": 2,
    })
    complete = arena.format_vote_result({
        "winner": "model-a",
        "final_ranking": ["model-a", "model-b"],
    })

    assert "round 2" in next_round
    assert "Arena complete" in complete


def test_normalize_vote_maps_whatsapp_choices():
    assert arena.normalize_vote("A") == "left"
    assert arena.normalize_vote("b") == "right"
    assert arena.normalize_vote("tie") == "tie"
