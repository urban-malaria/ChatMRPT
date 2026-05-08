from __future__ import annotations

import importlib.util
from pathlib import Path


def _load_module():
    module_path = Path(__file__).resolve().parents[1] / "app" / "utils" / "s3_map_storage.py"
    spec = importlib.util.spec_from_file_location("s3_map_storage", module_path)
    module = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    spec.loader.exec_module(module)
    return module


s3_map_storage = _load_module()


class StubS3Client:
    def __init__(self):
        self.calls = []

    def upload_file(self, local_path, bucket, key, ExtraArgs=None):
        self.calls.append({
            "local_path": local_path,
            "bucket": bucket,
            "key": key,
            "ExtraArgs": ExtraArgs or {},
        })


def test_quote_key_escapes_each_segment():
    assert (
        s3_map_storage._quote_key("maps/session 1/a+b/test map.html")
        == "maps/session%201/a%2Bb/test%20map.html"
    )


def test_upload_public_uses_maps_prefix_and_no_acl(tmp_path, monkeypatch):
    html = tmp_path / "test map.html"
    html.write_text("<html>map</html>", encoding="utf-8")
    client = StubS3Client()

    monkeypatch.setenv("S3_UPLOADS_BUCKET", "chatmrpt-uploads")
    monkeypatch.setenv("AWS_REGION", "us-east-2")
    monkeypatch.setattr(s3_map_storage, "_get_s3_client", lambda: client)

    url = s3_map_storage.upload_public(str(html), "maps/test session/test map.html")

    assert url == (
        "https://chatmrpt-uploads.s3.us-east-2.amazonaws.com/"
        "maps/test%20session/test%20map.html"
    )
    assert len(client.calls) == 1
    call = client.calls[0]
    assert call["bucket"] == "chatmrpt-uploads"
    assert call["key"] == "maps/test session/test map.html"
    assert "ACL" not in call["ExtraArgs"]
    assert call["ExtraArgs"]["ContentType"] == "text/html"


def test_upload_public_refuses_non_maps_prefix(tmp_path, monkeypatch):
    html = tmp_path / "map.html"
    html.write_text("<html>map</html>", encoding="utf-8")
    client = StubS3Client()

    monkeypatch.setenv("S3_UPLOADS_BUCKET", "chatmrpt-uploads")
    monkeypatch.setattr(s3_map_storage, "_get_s3_client", lambda: client)

    assert s3_map_storage.upload_public(str(html), "private/test.html") is None
    assert client.calls == []


def test_resolve_visualization_file_shapes(tmp_path):
    session_id = "session-123"
    upload_root = tmp_path / "uploads"
    viz_dir = upload_root / session_id / "visualizations"
    viz_dir.mkdir(parents=True)
    html = viz_dir / "map one.html"
    html.write_text("<html>map</html>", encoding="utf-8")

    assert s3_map_storage.resolve_visualization_file(
        {"file_path": str(html)}, session_id, upload_root
    ) == html.resolve()
    assert s3_map_storage.resolve_visualization_file(
        {"path": "visualizations/map one.html"}, session_id, upload_root
    ) == html.resolve()
    assert s3_map_storage.resolve_visualization_file(
        {"url": f"/serve_viz_file/{session_id}/visualizations/map%20one.html?x=1"},
        session_id,
        upload_root,
    ) == html.resolve()
    assert s3_map_storage.resolve_visualization_file(
        {"url": "visualizations/map%20one.html"}, session_id, upload_root
    ) == html.resolve()
    assert s3_map_storage.resolve_visualization_file(
        {"url": "https://example.com/visualizations/map%20one.html"},
        session_id,
        upload_root,
    ) == html.resolve()


def test_resolve_visualization_file_rejects_outside_session(tmp_path):
    session_id = "session-123"
    upload_root = tmp_path / "uploads"
    (upload_root / session_id).mkdir(parents=True)
    outside = tmp_path / "outside.html"
    outside.write_text("<html>outside</html>", encoding="utf-8")

    assert s3_map_storage.resolve_visualization_file(
        {"file_path": str(outside)}, session_id, upload_root
    ) is None
