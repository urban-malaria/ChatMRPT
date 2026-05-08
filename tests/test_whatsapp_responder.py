from __future__ import annotations

import importlib.util
import sys
import types
from pathlib import Path


class AppContext:
    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False


class FakeApp:
    def __init__(self, upload_root):
        self.config = {"UPLOAD_FOLDER": str(upload_root)}
        self.instance_path = str(upload_root.parent)

    def app_context(self):
        return AppContext()


def _install_fake_app_modules(monkeypatch, result, local_path, calls):
    app_pkg = types.ModuleType("app")
    app_pkg.__path__ = []
    whatsapp_pkg = types.ModuleType("app.whatsapp")
    whatsapp_pkg.__path__ = []
    formatter_mod = types.ModuleType("app.whatsapp.formatter")
    formatter_mod.chunk_text = lambda text: [text]
    formatter_mod.format_error = lambda: "error"
    observability_mod = types.ModuleType("app.whatsapp.observability")
    observability_mod.log_event = lambda *args, **kwargs: None

    services_pkg = types.ModuleType("app.services")
    services_pkg.__path__ = []
    analysis_mod = types.ModuleType("app.services.analysis_chat_service")
    analysis_mod.run_analysis_message = lambda session_id, message: result

    instance_sync_mod = types.ModuleType("app.services.instance_sync")
    instance_sync_mod.ensure_session_available = lambda session_id: calls.append(("ensure", session_id)) or True
    instance_sync_mod.sync_session_after_upload = lambda session_id: calls.append(("sync", session_id))

    utils_pkg = types.ModuleType("app.utils")
    utils_pkg.__path__ = []
    s3_mod = types.ModuleType("app.utils.s3_map_storage")
    s3_mod.resolve_visualization_file = lambda viz, session_id, upload_root: local_path
    s3_mod.upload_public = lambda path, key: calls.append(("upload", path, key)) or "https://s3.example/map.html"
    utils_pkg.s3_map_storage = s3_mod

    for name, module in {
        "app": app_pkg,
        "app.whatsapp": whatsapp_pkg,
        "app.whatsapp.formatter": formatter_mod,
        "app.whatsapp.observability": observability_mod,
        "app.services": services_pkg,
        "app.services.analysis_chat_service": analysis_mod,
        "app.services.instance_sync": instance_sync_mod,
        "app.utils": utils_pkg,
        "app.utils.s3_map_storage": s3_mod,
    }.items():
        monkeypatch.setitem(sys.modules, name, module)


def _load_responder():
    module_path = Path(__file__).resolve().parents[1] / "app" / "whatsapp" / "responder.py"
    spec = importlib.util.spec_from_file_location("whatsapp_responder", module_path)
    module = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    spec.loader.exec_module(module)
    return module


def test_run_whatsapp_analysis_sends_text_map_and_syncs(tmp_path, monkeypatch):
    local_map = tmp_path / "uploads" / "session-1" / "visualizations" / "map.html"
    local_map.parent.mkdir(parents=True)
    local_map.write_text("<html>map</html>", encoding="utf-8")
    calls = []
    result = {
        "success": True,
        "message": "Analysis result",
        "visualizations": [{"title": "Risk Map", "url": "/serve_viz_file/session-1/visualizations/map.html"}],
    }
    _install_fake_app_modules(monkeypatch, result, local_map, calls)
    responder = _load_responder()

    sent = []

    def send_fn(sender, messages, app):
        sent.append((sender, messages))
        return True

    response = responder.run_whatsapp_analysis_and_respond(
        user_message="map malaria burden",
        sender="whatsapp:+123",
        session_id="session-1",
        send_fn=send_fn,
        app=FakeApp(tmp_path / "uploads"),
    )

    assert response["success"] is True
    assert sent == [
        ("whatsapp:+123", ["Analysis result"]),
        ("whatsapp:+123", ["Risk Map\nhttps://s3.example/map.html"]),
    ]
    assert ("ensure", "session-1") in calls
    assert ("sync", "session-1") in calls
    assert calls[-2][0] == "upload"
