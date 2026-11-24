"""Arena (several models, user votes) is only for general malaria questions."""
import pytest

from app.api.analysis.arena_helpers import is_arena_eligible_message

SINGLE_ANSWER = [
    "how does this work?",
    "what can you do?",
    "I don't have any data, what should I do?",
    "what data do I need?",
    "how do I start the analysis",
    "can I upload an Excel file?",
    "show me a map of my wards",
    "What is ChatMRPT used for?",
    "where do I find the sample data",
]

ARENA = [
    "How do insecticide-treated nets reduce malaria transmission?",
    "What is seasonal malaria chemoprevention?",
    "Why are children under five most at risk of severe malaria?",
]


@pytest.mark.parametrize("question", SINGLE_ANSWER)
def test_questions_about_the_tool_get_one_answer(question):
    assert not is_arena_eligible_message(question)


@pytest.mark.parametrize("question", ARENA)
def test_general_malaria_questions_go_to_arena(question):
    assert is_arena_eligible_message(question)


def test_first_message_never_goes_to_arena():
    assert not is_arena_eligible_message(ARENA[0], is_first_message=True)


def test_greetings_and_confirmations_get_one_answer():
    assert not is_arena_eligible_message("hello there")
    assert not is_arena_eligible_message("ok")
