from flask import Flask
import pytest

from app.visualization.explain_resolver import (
    ExplainResolutionError,
    resolve_explain_target,
)


@pytest.fixture()
def app_ctx(tmp_path, monkeypatch):
    app = Flask(__name__)
    app.config["UPLOAD_FOLDER"] = str(tmp_path / "uploads")
    app.config["ENABLE_EXPLAIN_S3_URLS"] = True
    monkeypatch.setenv("S3_UPLOADS_BUCKET", "chatmrpt-uploads")
    with app.app_context():
        yield app


def test_resolves_serve_viz_file_inside_session(app_ctx, tmp_path):
    upload_root = tmp_path / "uploads"
    viz_file = upload_root / "session-1" / "visualizations" / "risk.html"
    viz_file.parent.mkdir(parents=True)
    viz_file.write_text("<html></html>")

    target = resolve_explain_target(
        viz_url="/serve_viz_file/session-1/visualizations/risk.html",
        viz_path=None,
        visualization_path=None,
        request_session_id="session-1",
    )

    assert target.session_id == "session-1"
    assert target.local_path == str(viz_file.resolve())
    assert target.source == "serve_viz_file"


def test_rejects_wrong_session(app_ctx, tmp_path):
    upload_root = tmp_path / "uploads"
    viz_file = upload_root / "session-2" / "visualizations" / "risk.html"
    viz_file.parent.mkdir(parents=True)
    viz_file.write_text("<html></html>")

    with pytest.raises(ExplainResolutionError, match="current session"):
        resolve_explain_target(
            viz_url="/serve_viz_file/session-2/visualizations/risk.html",
            viz_path=None,
            visualization_path=None,
            request_session_id="session-1",
        )


def test_rejects_path_traversal(app_ctx):
    with pytest.raises(ExplainResolutionError, match="unsafe|escapes"):
        resolve_explain_target(
            viz_url="/serve_viz_file/session-1/visualizations/%2e%2e/secret.html",
            viz_path=None,
            visualization_path=None,
            request_session_id="session-1",
        )


def test_resolves_public_s3_map_url(app_ctx, tmp_path):
    session_dir = tmp_path / "uploads" / "session-1"
    session_dir.mkdir(parents=True)

    target = resolve_explain_target(
        viz_url="https://chatmrpt-uploads.s3.us-east-2.amazonaws.com/maps/session-1/risk.html",
        viz_path=None,
        visualization_path=None,
        request_session_id="session-1",
    )

    assert target.session_id == "session-1"
    assert target.source == "s3_public_map"
    assert target.s3_url.endswith("/maps/session-1/risk.html")
