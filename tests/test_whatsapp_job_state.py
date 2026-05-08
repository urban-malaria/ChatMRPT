from __future__ import annotations

import importlib.util
import json
from pathlib import Path


def _load_module():
    module_path = Path(__file__).resolve().parents[1] / "app" / "whatsapp" / "job_state.py"
    spec = importlib.util.spec_from_file_location("whatsapp_job_state", module_path)
    module = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    spec.loader.exec_module(module)
    return module


job_state = _load_module()


class FakeRedis:
    def __init__(self):
        self.store = {}

    def get(self, key):
        return self.store.get(key)

    def set(self, key, value, ex=None, nx=False):
        if nx and key in self.store:
            return False
        self.store[key] = value
        return True


def test_claim_ignores_processing_duplicate():
    redis = FakeRedis()

    assert job_state.claim_job(redis, "SM1", "whatsapp:+123", "analysis") is True
    assert job_state.claim_job(redis, "SM1", "whatsapp:+123", "analysis") is False


def test_failed_job_can_be_reclaimed_with_retry_count():
    redis = FakeRedis()

    assert job_state.claim_job(redis, "SM1", "whatsapp:+123", "analysis") is True
    job_state.finish_job(redis, "SM1", "failed", error="boom")

    assert job_state.claim_job(redis, "SM1", "whatsapp:+123", "analysis") is True
    payload = json.loads(redis.store[job_state.job_key("SM1")])
    assert payload["retry_count"] == 1
    assert payload["status"] == "processing"


def test_whatsapp_rq_job_id_uses_only_rq_safe_characters():
    from app.whatsapp.queue import make_whatsapp_job_id

    job_id = make_whatsapp_job_id("SM123abc", "analysis")

    assert job_id.startswith("wa_analysis_SM123abc_")
    assert ":" not in job_id
    assert all(ch.isalnum() or ch in {"_", "-"} for ch in job_id)
