"""Tests for ITN tool visualization path handling."""

import sys
import types


def _install_extension_stub(module_name, class_name):
    module = types.ModuleType(module_name)

    class _Stub:  # pragma: no cover - simple import placeholder
        def __init__(self, *args, **kwargs):
            pass

    setattr(module, class_name, _Stub)
    sys.modules.setdefault(module_name, module)


_install_extension_stub("flask_compress", "Compress")
_install_extension_stub("flask_login", "LoginManager")
_install_extension_stub("flask_session", "Session")

from app.planning.itn_tools import _web_path_to_local_path


def test_itn_web_path_is_converted_to_agent_file_path():
    session_id = "session-123"
    web_path = f"/serve_viz_file/{session_id}/visualizations/itn_distribution_map.html"

    assert (
        _web_path_to_local_path(web_path, session_id)
        == "instance/uploads/session-123/visualizations/itn_distribution_map.html"
    )


def test_non_session_web_path_is_not_converted():
    assert _web_path_to_local_path("/export/download/session-123/file.csv", "session-123") is None
