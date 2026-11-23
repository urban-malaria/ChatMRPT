"""Every chat path builds its ChatMRPT explanations from the knowledge library."""

from langchain_core.prompts import ChatPromptTemplate

from app.knowledge import get
from app.knowledge.library import NOT_COVERED_RULE

BANNED = ["forecasting", "transmission potential", "two main tabs", "tpr > 50%"]


def _assert_clean(text: str):
    lowered = text.lower()
    for phrase in BANNED:
        assert phrase not in lowered, phrase


def test_arena_prompt_uses_library():
    from app.arena.prompts import get_arena_system_prompt

    prompt = get_arena_system_prompt("How does ChatMRPT work?")
    assert NOT_COVERED_RULE in prompt
    assert "What it does not do" in prompt
    _assert_clean(prompt)


def test_arena_prompt_includes_tpr_definition_when_asked():
    from app.arena.prompts import get_arena_system_prompt

    assert "Test Positivity Rate" in get_arena_system_prompt("What does TPR mean?")


def test_general_chat_prompt_uses_library():
    from app.agent.prompt_builder import PromptBuilder

    prompt = PromptBuilder().build({}, None, "How does ChatMRPT work?")
    assert NOT_COVERED_RULE in prompt
    assert "What it does not do" in prompt
    assert "{knowledge}" not in prompt
    _assert_clean(prompt)


def test_agent_prompt_renders_with_library():
    from app.agent.prompts.system_prompt import MAIN_SYSTEM_PROMPT

    template = ChatPromptTemplate.from_messages([("system", MAIN_SYSTEM_PROMPT), ("placeholder", "{messages}")])
    text = template.invoke({"messages": []}).to_messages()[0].content
    assert NOT_COVERED_RULE in text
    assert "lookup_knowledge" in text
    _assert_clean(text)


def test_lookup_knowledge_tool():
    from app.agent.tools.knowledge_tool import lookup_knowledge

    assert "Kaiser-Meyer-Olkin" in lookup_knowledge.invoke({"topic": "why was PCA skipped"})
    assert "not certain" in lookup_knowledge.invoke({"topic": "quantum computing"})


def test_whatsapp_definitions_come_from_library():
    from app.whatsapp import responses

    assert responses.identity_response() == get("about-chatmrpt").summary
    assert responses.burden_definition_response() == get("malaria-burden").summary
    assert responses.tpr_definition_response().startswith(get("tpr").summary)
    assert responses.itn_planning_response().startswith(get("itn-planning").summary)
