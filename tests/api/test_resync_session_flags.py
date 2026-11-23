"""Session data flags follow what is on disk for the current chat's session."""

from flask import Flask, session

import app.api.analysis.utils as utils


class _FakeUploadService:
    def __init__(self, base_dir):
        self.base_dir = str(base_dir)

    def set_session_flags(self, store, session_id, *, context, updates):
        for key, value in updates.items():
            store[key] = value


def _app():
    app = Flask(__name__)
    app.secret_key = "test"
    return app


def test_flags_from_old_chat_are_cleared_when_new_chat_has_no_data(tmp_path, monkeypatch):
    monkeypatch.setattr(utils, "UploadService", lambda: _FakeUploadService(tmp_path))
    with _app().test_request_context("/"):
        session.update(data_loaded=True, csv_loaded=True, shapefile_loaded=True, analysis_complete=True)
        updates = utils.resync_session_flags("new-chat-session")
        assert updates == {"data_loaded": False, "csv_loaded": False,
                           "shapefile_loaded": False, "analysis_complete": False}
        assert not session["data_loaded"] and not session["csv_loaded"]


def test_flags_set_when_session_has_data(tmp_path, monkeypatch):
    folder = tmp_path / "upload-session"
    folder.mkdir()
    (folder / "raw_data.csv").write_text("WardName,Burden\nA,1\n")
    (folder / "raw_shapefile.zip").write_bytes(b"zip")
    monkeypatch.setattr(utils, "UploadService", lambda: _FakeUploadService(tmp_path))
    with _app().test_request_context("/"):
        updates = utils.resync_session_flags("upload-session")
        assert updates == {"csv_loaded": True, "shapefile_loaded": True, "data_loaded": True}


def test_empty_folder_with_no_flags_changes_nothing(tmp_path, monkeypatch):
    (tmp_path / "s").mkdir()
    monkeypatch.setattr(utils, "UploadService", lambda: _FakeUploadService(tmp_path))
    with _app().test_request_context("/"):
        assert utils.resync_session_flags("s") == {}
