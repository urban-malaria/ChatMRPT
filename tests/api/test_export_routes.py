from pathlib import Path

from flask import Flask

from app.api.export_routes import export_bp


def _app(tmp_path: Path) -> Flask:
    app_root = tmp_path / "app"
    app_root.mkdir()
    app = Flask(__name__, root_path=str(app_root))
    app.config.update(TESTING=True, DISABLE_AUTH=True, SECRET_KEY="test")
    app.register_blueprint(export_bp)
    return app


def test_export_list_and_download_use_friendly_analysis_names(tmp_path):
    app = _app(tmp_path)
    session_id = "session-123"
    uploads = tmp_path / "instance" / "uploads" / session_id
    uploads.mkdir(parents=True)
    (uploads / "raw_data.csv").write_text("WardName,Burden\nAlpha,12\n", encoding="utf-8")
    (uploads / "raw_shapefile.zip").write_bytes(b"zip")
    (uploads / "population_data.csv").write_text("ward,population_total\nAlpha,1000\n", encoding="utf-8")
    (uploads / "data_dictionary.csv").write_text("raw_column,display_name\nBurden,Malaria burden\n", encoding="utf-8")

    client = app.test_client()
    listed = client.get(f"/export/list/{session_id}")

    assert listed.status_code == 200
    files = {item["filename"]: item for item in listed.get_json()["files"]}
    assert "ward_malaria_burden_environmental_variables.csv" in files
    assert "ward_boundaries_burden_environmental_variables.zip" in files
    assert "ward_population_denominators.csv" in files
    assert "data_dictionary.csv" in files

    downloaded = client.get(f"/export/download/{session_id}/ward_malaria_burden_environmental_variables.csv")

    assert downloaded.status_code == 200
    assert downloaded.data == b"WardName,Burden\nAlpha,12\n"
    assert "ward_malaria_burden_environmental_variables.csv" in downloaded.headers["Content-Disposition"]
