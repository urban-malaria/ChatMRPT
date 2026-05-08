from __future__ import annotations

import importlib.util
from pathlib import Path

import pytest


def _load_module():
    module_path = Path(__file__).resolve().parents[1] / "app" / "whatsapp" / "media.py"
    spec = importlib.util.spec_from_file_location("whatsapp_media", module_path)
    module = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    spec.loader.exec_module(module)
    return module


whatsapp_media = _load_module()


class FakeResponse:
    def __init__(self, chunks, headers=None):
        self._chunks = chunks
        self.headers = headers or {}

    def raise_for_status(self):
        return None

    def iter_content(self, chunk_size):
        yield from self._chunks


def test_download_twilio_media_uses_auth_and_streams(monkeypatch):
    calls = []

    def fake_get(url, auth=None, timeout=None, stream=None):
        calls.append({"url": url, "auth": auth, "timeout": timeout, "stream": stream})
        return FakeResponse(
            [b"abc", b"def"],
            {"Content-Type": "text/csv; charset=utf-8", "Content-Length": "6"},
        )

    monkeypatch.setenv("TWILIO_ACCOUNT_SID", "AC123")
    monkeypatch.setenv("TWILIO_AUTH_TOKEN", "secret")
    monkeypatch.setattr(whatsapp_media.requests, "get", fake_get)

    file_bytes, content_type = whatsapp_media.download_twilio_media("https://media.example/test")

    assert file_bytes == b"abcdef"
    assert content_type == "text/csv"
    assert calls == [{
        "url": "https://media.example/test",
        "auth": ("AC123", "secret"),
        "timeout": 60,
        "stream": True,
    }]


def test_download_twilio_media_rejects_oversized_content_length(monkeypatch):
    def fake_get(url, auth=None, timeout=None, stream=None):
        return FakeResponse([], {"Content-Length": "5"})

    monkeypatch.setenv("WHATSAPP_MAX_UPLOAD_BYTES", "4")
    monkeypatch.setattr(whatsapp_media.requests, "get", fake_get)

    with pytest.raises(ValueError, match="too large"):
        whatsapp_media.download_twilio_media("https://media.example/test")


def test_download_twilio_media_rejects_oversized_stream(monkeypatch):
    def fake_get(url, auth=None, timeout=None, stream=None):
        return FakeResponse([b"abc", b"def"], {})

    monkeypatch.setenv("WHATSAPP_MAX_UPLOAD_BYTES", "4")
    monkeypatch.setattr(whatsapp_media.requests, "get", fake_get)

    with pytest.raises(ValueError, match="too large"):
        whatsapp_media.download_twilio_media("https://media.example/test")
