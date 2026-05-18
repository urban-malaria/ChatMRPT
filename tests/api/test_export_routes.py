import zipfile
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


def _write_bundle(uploads: Path, bundle_name: str, content: bytes = b"bundle") -> Path:
    bundle = uploads / bundle_name
    with zipfile.ZipFile(bundle, 'w') as zf:
        zf.writestr("README.txt", "test bundle")
        zf.writestr("ward_malaria_burden_environmental_variables.csv", "WardName,Burden\nAlpha,12\n")
    return bundle


def test_bundle_zip_appears_in_export_list(tmp_path):
    """list_exports surfaces the bundle ZIP, not 4 individual files."""
    app = _app(tmp_path)
    session_id = "session-bundle"
    uploads = tmp_path / "instance" / "uploads" / session_id
    uploads.mkdir(parents=True)
    bundle_name = "malaria_burden_analysis_Kwara_2026-05-18.zip"
    _write_bundle(uploads, bundle_name)

    client = app.test_client()
    listed = client.get(f"/export/list/{session_id}")

    assert listed.status_code == 200
    files = {item["filename"]: item for item in listed.get_json()["files"]}
    assert bundle_name in files
    assert files[bundle_name]["type"] == "zip"
    assert files[bundle_name]["category"] == "tpr"


def test_bundle_zip_is_downloadable(tmp_path):
    """The bundle ZIP can be downloaded via the download endpoint."""
    app = _app(tmp_path)
    session_id = "session-dl"
    uploads = tmp_path / "instance" / "uploads" / session_id
    uploads.mkdir(parents=True)
    bundle_name = "malaria_burden_analysis_Lagos_2026-05-18.zip"
    _write_bundle(uploads, bundle_name)

    client = app.test_client()
    resp = client.get(f"/export/download/{session_id}/{bundle_name}")

    assert resp.status_code == 200
    assert resp.content_type == "application/zip"
    assert bundle_name in resp.headers.get("Content-Disposition", "")


def test_latest_bundle_chosen_when_multiple_exist(tmp_path):
    """When multiple bundles exist, the lexicographically last (newest date) is listed."""
    app = _app(tmp_path)
    session_id = "session-multi"
    uploads = tmp_path / "instance" / "uploads" / session_id
    uploads.mkdir(parents=True)
    _write_bundle(uploads, "malaria_burden_analysis_Kwara_2026-05-01.zip")
    _write_bundle(uploads, "malaria_burden_analysis_Kwara_2026-05-18.zip")

    client = app.test_client()
    listed = client.get(f"/export/list/{session_id}")
    files = {item["filename"]: item for item in listed.get_json()["files"]}

    assert "malaria_burden_analysis_Kwara_2026-05-18.zip" in files
    assert "malaria_burden_analysis_Kwara_2026-05-01.zip" not in files


def test_no_individual_burden_files_in_export_list(tmp_path):
    """Individual burden files are NOT listed even if they exist on disk."""
    app = _app(tmp_path)
    session_id = "session-individual"
    uploads = tmp_path / "instance" / "uploads" / session_id
    uploads.mkdir(parents=True)
    (uploads / "raw_data.csv").write_text("WardName,Burden\nAlpha,12\n", encoding="utf-8")
    (uploads / "population_data.csv").write_text("ward,pop\nAlpha,1000\n", encoding="utf-8")
    (uploads / "data_dictionary.csv").write_text("col,name\nBurden,Malaria\n", encoding="utf-8")

    client = app.test_client()
    listed = client.get(f"/export/list/{session_id}")
    files = {item["filename"]: item for item in listed.get_json()["files"]}

    assert "ward_malaria_burden_environmental_variables.csv" not in files
    assert "ward_population_denominators.csv" not in files
    assert "data_dictionary.csv" not in files
