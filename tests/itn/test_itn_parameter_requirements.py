from flask import Flask, session


def test_plan_itn_distribution_schema_allows_missing_user_parameters():
    from app.agent.tools.map_tools import plan_itn_distribution

    schema = plan_itn_distribution.args_schema.model_json_schema()
    required = schema.get("required", [])

    assert "total_nets" not in required
    assert "avg_household_size" not in required


def test_update_itn_distribution_requires_initial_user_parameters():
    from app.api.itn_routes import update_itn_distribution

    app = Flask(__name__)
    app.secret_key = "test-secret"

    with app.test_request_context("/api/itn/update-distribution", method="POST", json={}):
        session["session_id"] = "test-session"

        response, status_code = update_itn_distribution()

    assert status_code == 400
    payload = response.get_json()
    assert payload["status"] == "requires_user_input"
    assert payload["requires_user_input"] is True
    assert set(payload["missing_parameters"]) == {"total_nets", "avg_household_size"}
