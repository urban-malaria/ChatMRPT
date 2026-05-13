from types import SimpleNamespace

from langchain_core.messages import AIMessage

from app.agent.agent import DataAnalysisAgent
from app.agent.canonical_response import select_final_message


def test_select_final_message_prefers_deterministic_canonical_response():
    state = {
        "canonical_responses": [
            {
                "tool_name": "run_risk_analysis",
                "message": "Canonical risk analysis complete.",
                "success": True,
                "requires_user_input": False,
                "priority": 100,
                "sequence": 1,
            }
        ]
    }

    message, meta = select_final_message(state, AIMessage(content="LLM paraphrase."))

    assert message == "Canonical risk analysis complete."
    assert meta["source"] == "canonical"
    assert meta["tool_name"] == "run_risk_analysis"


def test_select_final_message_ignores_non_allowlisted_canonical_response():
    state = {
        "canonical_responses": [
            {
                "tool_name": "create_variable_map",
                "message": "Map created.",
                "success": True,
                "priority": 100,
                "sequence": 1,
            }
        ]
    }

    message, meta = select_final_message(state, AIMessage(content="Explain the map."))

    assert message == "Explain the map."
    assert meta["source"] == "ai_final"


def test_select_final_message_prioritizes_user_input_prompt():
    state = {
        "canonical_responses": [
            {
                "tool_name": "run_risk_analysis",
                "message": "Risk analysis complete.",
                "success": True,
                "requires_user_input": False,
                "priority": 100,
                "sequence": 1,
            },
            {
                "tool_name": "plan_itn_distribution",
                "message": "Please provide total nets and household size.",
                "success": True,
                "requires_user_input": True,
                "priority": 120,
                "sequence": 2,
            },
        ]
    }

    message, meta = select_final_message(state, AIMessage(content="What would you like next?"))

    assert message == "Please provide total nets and household size."
    assert meta["requires_user_input"] is True
    assert meta["tool_name"] == "plan_itn_distribution"


def test_tools_node_returns_canonical_and_plot_deltas_from_mutated_state():
    agent = object.__new__(DataAnalysisAgent)
    agent.session_id = "session-1"

    def invoke(state):
        state["canonical_responses"].append({
            "tool_name": "plan_itn_distribution",
            "message": "ITN canonical",
            "success": True,
            "priority": 105,
            "sequence": 1,
        })
        state["output_plots"].append("instance/uploads/session-1/map.html")
        return {"messages": []}

    agent.tool_node = SimpleNamespace(invoke=invoke)
    state = {
        "messages": [],
        "canonical_responses": [],
        "output_plots": [],
        "tool_call_count": 0,
        "consecutive_error_count": 0,
    }

    result = DataAnalysisAgent._tools_node(agent, state)

    assert result["canonical_responses"] == [{
        "tool_name": "plan_itn_distribution",
        "message": "ITN canonical",
        "success": True,
        "priority": 105,
        "sequence": 1,
    }]
    assert result["output_plots"] == ["instance/uploads/session-1/map.html"]
    assert result["tool_call_count"] == 1
