"""Explain button: the right chart file and chart type reach the explainer."""

from types import SimpleNamespace

import pytest
from flask import Flask

from app.visualization.explainer import resolve_viz_type


@pytest.mark.parametrize("sent_type, filename, expected", [
    ("vulnerability_map", "vulnerability_map_composite_20261123_203440_1795466080_0a26b18e.html", "vulnerability_map_composite"),
    ("vulnerability_map", "vulnerability_map_pca_20261123_203440.html", "vulnerability_map_pca"),
    ("pca_map", "", "vulnerability_map_pca"),
    ("itn_map", "itn_distribution_map_20261123_203858.html", "itn_map"),
    ("visualization", "Burden_distribution_map_20261123_203246.html", "tpr_map"),
    ("tpr_map", "tpr_distribution_map_adamawa.html", "tpr_map"),
    ("visualization", "rainfall_distribution_map_20261123_201641.html", "variable_distribution"),
    ("visualization", "composite_map_page_1_20261123.html", "composite_score_maps"),
    ("box_plot", "box_plot_ranking_top20_x.html", "box_plot"),
    ("visualization", "urban_extent_vuln_50_x.html", "urban_extent_map"),
    ("settlement_map", "settlement_selector.html", "settlement_map"),
    ("data_analysis", "data_analysis_123.html", "data_analysis"),
])
def test_resolve_viz_type(sent_type, filename, expected):
    assert resolve_viz_type(sent_type, f"/x/visualizations/{filename}" if filename else None) == expected


def _app(tmp_path, monkeypatch, calls):
    import app.api.visualization_routes as routes
    import app.visualization.explainer as explainer_module

    class FakeExplainer:
        def explain_visualization(self, viz_path, viz_type=None, session_id=None):
            calls.append((viz_path, viz_type, session_id))
            return "Explanation"

    monkeypatch.setattr(explainer_module, "get_universal_viz_explainer", lambda llm_manager=None: FakeExplainer())
    app = Flask(__name__, instance_path=str(tmp_path / "instance"))
    app.config.update(TESTING=True, DISABLE_AUTH=True, SECRET_KEY="t",
                      UPLOAD_FOLDER=str(tmp_path / "instance" / "uploads"))
    app.services = SimpleNamespace(llm_manager=None, interaction_logger=None)
    app.register_blueprint(routes.viz_bp)
    return app


def test_viz_url_alone_is_resolved_to_session_file(tmp_path, monkeypatch):
    calls = []
    app = _app(tmp_path, monkeypatch, calls)
    viz = tmp_path / "instance" / "uploads" / "sid1" / "visualizations" / "itn_distribution_map_1.html"
    viz.parent.mkdir(parents=True)
    viz.write_text("<html></html>")
    r = app.test_client().post("/explain_visualization", json={
        "viz_url": "https://example.org/serve_viz_file/sid1/visualizations/itn_distribution_map_1.html?x=1",
        "viz_type": "itn_map", "session_id": "sid1"})
    assert r.status_code == 200, r.get_json()
    assert r.get_json()["explanation"] == "Explanation"
    assert calls and calls[0][0].endswith("sid1/visualizations/itn_distribution_map_1.html")


def test_paths_outside_uploads_are_rejected(tmp_path, monkeypatch):
    calls = []
    app = _app(tmp_path, monkeypatch, calls)
    r = app.test_client().post("/explain_visualization", json={
        "viz_url": "/serve_viz_file/sid1/../../../../etc/passwd", "session_id": "sid1"})
    assert r.status_code == 400 and not calls
