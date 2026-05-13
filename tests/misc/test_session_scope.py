from flask import Flask, g, session


def test_effective_session_id_scopes_conversation_without_replacing_base_session():
    from app.utils.session_scope import get_effective_session_id

    app = Flask(__name__)
    app.secret_key = "test-secret"

    with app.test_request_context("/"):
        session["session_id"] = "base-session"
        session["base_session_id"] = "base-session"
        g.conversation_id = "conv-a"

        effective_session_id = get_effective_session_id()

        assert effective_session_id == "base-session__conv-a"
        assert session["session_id"] == "base-session"
        assert session["base_session_id"] == "base-session"
