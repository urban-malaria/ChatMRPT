from __future__ import annotations

import importlib.util
from pathlib import Path


def _load_module():
    module_path = Path(__file__).resolve().parents[1] / "app" / "whatsapp" / "session.py"
    spec = importlib.util.spec_from_file_location("whatsapp_session", module_path)
    module = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    spec.loader.exec_module(module)
    return module


whatsapp_session = _load_module()


class FakeRedis:
    def __init__(self):
        self.store = {}

    def get(self, key):
        return self.store.get(key)

    def setex(self, key, ttl, value):
        self.store[key] = value

    def expire(self, key, ttl):
        return None

    def delete(self, *keys):
        for key in keys:
            self.store.pop(key, None)


def test_session_id_and_upload_metadata_round_trip():
    redis = FakeRedis()
    mgr = whatsapp_session.WhatsAppSessionManager(redis)

    mgr.set_session_id("whatsapp:+123", "session-1")
    mgr.set_upload_metadata("whatsapp:+123", {"session_id": "session-1", "rows": 10})
    mgr.set_arena_state("whatsapp:+123", {"battle_id": "battle-1"})

    assert mgr.get_session_id("whatsapp:+123") == "session-1"
    assert mgr.get_upload_metadata("whatsapp:+123") == {"session_id": "session-1", "rows": 10}
    assert mgr.get_arena_state("whatsapp:+123") == {"battle_id": "battle-1"}


def test_clear_session_removes_upload_metadata():
    redis = FakeRedis()
    mgr = whatsapp_session.WhatsAppSessionManager(redis)

    mgr.set_session_id("whatsapp:+123", "session-1")
    mgr.append_history("whatsapp:+123", "user", "hello")
    mgr.set_upload_metadata("whatsapp:+123", {"session_id": "session-1"})
    mgr.set_arena_state("whatsapp:+123", {"battle_id": "battle-1"})

    mgr.clear_session("whatsapp:+123")

    assert mgr.get_session_id("whatsapp:+123") is None
    assert mgr.get_history("whatsapp:+123") == []
    assert mgr.get_upload_metadata("whatsapp:+123") is None
    assert mgr.get_arena_state("whatsapp:+123") is None
