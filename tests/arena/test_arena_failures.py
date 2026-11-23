"""Arena must not show provider error messages as model answers."""

import asyncio
from unittest.mock import MagicMock

import pytest

from app.arena.manager import ArenaManager, is_error_response


@pytest.mark.parametrize("text", [
    'Groq error: {"error":{"code":"model_not_found"}}',
    "OpenAI error: all candidate models failed.",
    "Error: connection refused",
    "",
    "   ",
    None,
])
def test_error_responses_detected(text):
    assert is_error_response(text)


@pytest.mark.parametrize("text", [
    "ChatMRPT ranks wards by malaria risk using composite scores.",
    "Errors in TPR data can come from incomplete facility reports.",
])
def test_real_answers_not_flagged(text):
    assert not is_error_response(text)


def _manager_with_responses(responses):
    manager = ArenaManager.__new__(ArenaManager)
    manager.storage = MagicMock()
    manager.available_models = {
        "model-x": {"provider": "groq"},
        "model-y": {"provider": "groq"},
    }
    manager.model_usage_stats = {}
    manager._get_model_response = lambda model_id, msg: (responses[model_id], 10.0)
    return manager


def test_start_battle_aborts_when_a_model_fails():
    manager = _manager_with_responses({
        "model-x": 'Groq error: {"code":"model_not_found"}',
        "model-y": "A real answer.",
    })
    result = manager.start_battle("How does ChatMRPT work?", "s1")
    assert "error" in result
    manager.storage.store_progressive_battle.assert_not_called()


def test_start_battle_succeeds_when_models_answer():
    manager = _manager_with_responses({
        "model-x": "Answer one.",
        "model-y": "Answer two.",
    })
    result = manager.start_battle("How does ChatMRPT work?", "s1")
    assert "error" not in result
    assert {result["response_a"], result["response_b"]} == {"Answer one.", "Answer two."}


def test_pattern_routing_is_default(monkeypatch):
    from app.api.analysis import chat_routing

    monkeypatch.delenv("SEMANTIC_ROUTER_ENABLED", raising=False)
    assert not chat_routing._is_semantic_router_enabled()
    decision = asyncio.run(chat_routing.route_with_mistral("How does ChatMRPT work?", {}))
    assert decision == "can_answer"


def test_later_round_failure_shows_plain_message(monkeypatch):
    from app.arena import manager as manager_module

    manager = _manager_with_responses({
        "model-x": "Answer one.",
        "model-y": "Answer two.",
        "model-z": 'Groq error: {"code":"rate_limit_exceeded"}',
    })
    manager.available_models["model-z"] = {"provider": "groq"}
    battle = manager_module.ProgressiveBattleSession(
        session_id="s1", user_message="q",
        all_models=["model-x", "model-y", "model-z"],
        remaining_models=["model-x", "model-y", "model-z"],
    )
    battle.current_pair = battle.get_next_matchup()
    battle.all_responses = {"model-x": "Answer one.", "model-y": "Answer two."}
    manager.storage.get_progressive_battle.return_value = battle
    manager.elo_system = manager_module.ELORatingSystem()
    manager.preference_stats = {"left": 0, "right": 0, "tie": 0, "both_bad": 0}
    monkeypatch.setattr(manager, "_save_stats", lambda: None)
    monkeypatch.setattr(manager, "_log_battle_to_db", lambda *a, **k: None, raising=False)

    result = manager.submit_vote("s1", "left")
    responses = [result.get("response_a"), result.get("response_b")]
    assert manager_module.MODEL_UNAVAILABLE_MESSAGE in responses
    assert not any(r and r.startswith("Groq error") for r in responses)


def test_rate_limited_model_is_retried_once(monkeypatch):
    from app.arena import manager as manager_module

    calls = []

    class FakeAdapter:
        def __init__(self, **kwargs):
            pass

        def generate(self, **kwargs):
            calls.append(1)
            return 'Groq error: Rate limit reached' if len(calls) == 1 else "Real answer."

    monkeypatch.setattr(manager_module, "LLMAdapter", FakeAdapter)
    monkeypatch.setattr(manager_module, "RATE_LIMIT_RETRY_SECONDS", 0)
    manager = ArenaManager.__new__(ArenaManager)
    manager.available_models = {"model-x": {"provider": "groq"}}
    response, _ = manager._get_model_response("model-x", "q")
    assert response == "Real answer." and len(calls) == 2
