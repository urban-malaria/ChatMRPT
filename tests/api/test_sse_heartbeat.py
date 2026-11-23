"""SSE heartbeat keeps long streams alive without changing their events."""

import time

import pytest
from flask import Flask, session

from app.utils.sse_heartbeat import with_heartbeat


def _slow_events():
    yield {"n": 1}
    time.sleep(0.35)
    yield {"n": 2}


def test_heartbeats_sent_while_waiting_and_events_kept_in_order():
    out = list(with_heartbeat(_slow_events, interval=0.1))
    events = [e for e in out if e is not None]
    assert events == [{"n": 1}, {"n": 2}]
    assert out.count(None) >= 2


def test_no_heartbeat_when_events_are_fast():
    assert list(with_heartbeat(lambda: iter([1, 2, 3]), interval=5)) == [1, 2, 3]


def test_errors_are_reraised():
    def failing():
        yield 1
        raise ValueError("boom")

    gen = with_heartbeat(failing, interval=5)
    assert next(gen) == 1
    with pytest.raises(ValueError, match="boom"):
        next(gen)


def test_worker_sees_same_flask_session():
    app = Flask(__name__)
    app.secret_key = "test"

    def uses_session():
        yield session.get("state_name")
        session["analysis_complete"] = True
        yield "written"

    with app.test_request_context("/"):
        session["state_name"] = "Example"
        out = list(with_heartbeat(uses_session, interval=5))
        assert out == ["Example", "written"]
        assert session["analysis_complete"] is True


def test_data_analysis_stream_route_sends_keepalive(monkeypatch):
    from unittest.mock import MagicMock

    import app.api.data_analysis_routes as routes
    import app.services.session_memory as session_memory
    import app.utils.sse_heartbeat as heartbeat

    def slow_workflow(session_id, message):
        yield {"type": "status", "status": "started"}
        time.sleep(0.4)  # e.g. burden calculation with no events
        yield {"type": "result", "data": {"message": "Malaria Burden Analysis Complete"}}

    monkeypatch.setattr(routes, "stream_analysis_events", slow_workflow)
    monkeypatch.setattr(routes, "interaction_core", MagicMock())
    monkeypatch.setattr(session_memory, "SessionMemory", MagicMock())
    monkeypatch.setattr(heartbeat, "HEARTBEAT_INTERVAL_SECONDS", 0.1)

    app = Flask(__name__)
    app.config.update(TESTING=True, DISABLE_AUTH=True, SECRET_KEY="test")
    app.register_blueprint(routes.data_analysis_v3_bp)

    resp = app.test_client().post(
        "/api/v1/data-analysis/chat/stream", json={"message": "u5", "session_id": "s1"}
    )
    body = resp.get_data(as_text=True)
    assert body.count(heartbeat.SSE_HEARTBEAT) >= 2
    assert "Malaria Burden Analysis Complete" in body
    assert body.rstrip().endswith("data: [DONE]")
    assert body.index("started") < body.index(heartbeat.SSE_HEARTBEAT) < body.index("Complete")
