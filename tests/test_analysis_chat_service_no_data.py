from __future__ import annotations

from app.services import analysis_chat_service


class FakeStateManager:
    def __init__(self, session_id):
        self.session_id = session_id

    def get_state(self):
        return {}

    def is_tpr_workflow_active(self):
        return False


def test_no_data_tpr_definition_is_answered_without_agent(monkeypatch):
    monkeypatch.setattr(analysis_chat_service, "ensure_analysis_session_available", lambda session_id: True)
    monkeypatch.setattr(analysis_chat_service, "build_general_workflow_context", lambda session_id: {
        "workflow": "data_analysis_v3",
        "stage": "no_data",
        "data_loaded": False,
        "session_id": session_id,
    })

    import app.agent.state_manager as state_manager_mod

    monkeypatch.setattr(state_manager_mod, "DataAnalysisStateManager", FakeStateManager)

    response = analysis_chat_service.run_analysis_message(
        "session-1",
        "what is TPR in malaria surveillance?",
    )

    assert response["success"] is True
    assert "Test Positivity Rate" in response["message"]
    assert "positive malaria tests / total malaria tests" in response["message"]
    assert response["workflow"] == "no_data_help"
