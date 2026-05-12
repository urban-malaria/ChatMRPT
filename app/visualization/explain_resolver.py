"""Safe visualization resolution for explanation requests."""

from __future__ import annotations

import logging
import os
from dataclasses import dataclass
from pathlib import Path
from urllib.parse import unquote, urlparse

from flask import current_app

logger = logging.getLogger(__name__)


class ExplainResolutionError(ValueError):
    """Raised when a visualization reference cannot be safely resolved."""


@dataclass(frozen=True)
class ExplainTarget:
    """Canonical visualization target passed to the explainer."""

    session_id: str
    viz_path: str
    source: str
    local_path: str | None = None
    s3_url: str | None = None
    restored_session: bool = False


def _uploads_root() -> Path:
    return Path(current_app.config.get("UPLOAD_FOLDER", "instance/uploads")).resolve()


def _inside(child: Path, parent: Path) -> bool:
    try:
        child.resolve().relative_to(parent.resolve())
        return True
    except ValueError:
        return False


def _reject_unsafe_parts(parts: list[str]) -> None:
    if not parts:
        raise ExplainResolutionError("Visualization path is empty")
    if any(part in {"", ".", ".."} for part in parts):
        raise ExplainResolutionError("Visualization path contains unsafe segments")
    if any("/" in part or "\\" in part for part in parts):
        raise ExplainResolutionError("Visualization path contains invalid separators")


def _session_root(session_id: str) -> Path:
    if not session_id or "/" in session_id or "\\" in session_id or session_id in {".", ".."}:
        raise ExplainResolutionError("Invalid session id")
    return (_uploads_root() / session_id).resolve()


def _ensure_session_available(session_id: str) -> bool:
    session_dir = _session_root(session_id)
    if session_dir.exists():
        return True

    try:
        from app.services.instance_sync import ensure_session_available

        restored = bool(ensure_session_available(session_id))
        logger.info(
            "explain_resolution_instance_sync",
            extra={"session_id": session_id, "restored": restored},
        )
        return restored
    except Exception as exc:
        logger.warning(
            "explain_resolution_instance_sync_failed",
            extra={"session_id": session_id, "error": str(exc)},
        )
        return False


def _resolve_session_file(session_id: str, rel_parts: list[str]) -> tuple[Path | None, bool]:
    _reject_unsafe_parts(rel_parts)
    root = _session_root(session_id)
    restored = False
    if not root.exists():
        restored = _ensure_session_available(session_id)
    candidate = (root / Path(*rel_parts)).resolve()
    if not _inside(candidate, root):
        raise ExplainResolutionError("Visualization path escapes the session folder")
    if candidate.is_file():
        return candidate, restored
    return None, restored


def _same_session(viz_session_id: str, request_session_id: str | None) -> str:
    if request_session_id and viz_session_id != request_session_id:
        raise ExplainResolutionError("Visualization does not belong to the current session")
    return viz_session_id


def _resolve_serve_viz_path(path: str, request_session_id: str | None) -> ExplainTarget:
    parts = [unquote(part) for part in path.strip("/").split("/")]
    if len(parts) < 3 or parts[0] != "serve_viz_file":
        raise ExplainResolutionError("Invalid visualization route")

    viz_session_id = _same_session(parts[1], request_session_id)
    rel_parts = parts[2:]
    local_path, restored = _resolve_session_file(viz_session_id, rel_parts)
    if local_path is None:
        raise ExplainResolutionError("Visualization file not found for this session")

    return ExplainTarget(
        session_id=viz_session_id,
        viz_path=str(local_path),
        local_path=str(local_path),
        source="serve_viz_file",
        restored_session=restored,
    )


def _resolve_static_path(path: str) -> ExplainTarget:
    parts = [unquote(part) for part in path.strip("/").split("/")]
    if len(parts) < 3 or parts[:2] != ["static", "visualizations"]:
        raise ExplainResolutionError("Invalid static visualization path")
    rel_parts = parts[2:]
    _reject_unsafe_parts(rel_parts)

    static_root = (Path(current_app.root_path) / "static" / "visualizations").resolve()
    candidate = (static_root / Path(*rel_parts)).resolve()
    if not _inside(candidate, static_root) or not candidate.is_file():
        raise ExplainResolutionError("Static visualization file not found")

    return ExplainTarget(
        session_id="",
        viz_path=str(candidate),
        local_path=str(candidate),
        source="static_visualization",
    )


def _resolve_pickle_path(path: str, request_session_id: str | None) -> ExplainTarget:
    if not request_session_id:
        raise ExplainResolutionError("Session ID required for plotly figure explanations")

    parts = [unquote(part) for part in path.strip("/").split("/")]
    if len(parts) < 4 or parts[:3] != ["images", "plotly_figures", "pickle"]:
        raise ExplainResolutionError("Invalid plotly figure path")
    filename = parts[-1]
    _reject_unsafe_parts([filename])
    local_path, restored = _resolve_session_file(request_session_id, ["visualizations", filename])
    if local_path is None:
        raise ExplainResolutionError("Plotly figure file not found for this session")

    return ExplainTarget(
        session_id=request_session_id,
        viz_path=str(local_path),
        local_path=str(local_path),
        source="plotly_pickle",
        restored_session=restored,
    )


def _s3_bucket() -> str:
    return os.getenv("S3_UPLOADS_BUCKET", "").strip()


def _resolve_s3_url(parsed, request_session_id: str | None) -> ExplainTarget:
    if not current_app.config.get("ENABLE_EXPLAIN_S3_URLS", True):
        raise ExplainResolutionError("S3 visualization explanation is disabled")

    bucket = _s3_bucket()
    host = parsed.netloc.lower()
    path = unquote(parsed.path.lstrip("/"))

    if bucket:
        virtual_host = f"{bucket}.s3."
        path_style = host.startswith("s3.") and path.startswith(f"{bucket}/")
        if host.startswith(virtual_host):
            key = path
        elif path_style:
            key = path.split("/", 1)[1]
        else:
            raise ExplainResolutionError("S3 URL bucket is not configured for ChatMRPT")
    elif ".s3." in host:
        key = path
    else:
        raise ExplainResolutionError("Unsupported S3 URL")

    parts = [part for part in key.split("/") if part]
    _reject_unsafe_parts(parts)
    if len(parts) < 3 or parts[0] != "maps":
        raise ExplainResolutionError("S3 visualization must be under maps/{session_id}/")

    viz_session_id = _same_session(parts[1], request_session_id)
    restored = _ensure_session_available(viz_session_id)
    return ExplainTarget(
        session_id=viz_session_id,
        viz_path=parsed.geturl(),
        s3_url=parsed.geturl(),
        source="s3_public_map",
        restored_session=restored,
    )


def resolve_explain_target(
    *,
    viz_url: str | None,
    viz_path: str | None,
    visualization_path: str | None,
    request_session_id: str | None,
) -> ExplainTarget:
    """Resolve incoming explain request fields into one safe target.

    The canonical input is ``viz_url``. Legacy ``viz_path`` and
    ``visualization_path`` remain supported only through the same validation
    rules.
    """
    raw = (viz_url or viz_path or visualization_path or "").strip()
    if not raw:
        raise ExplainResolutionError("Visualization path is required")

    parsed = urlparse(raw)
    path = parsed.path if (parsed.scheme or parsed.netloc) else raw.split("?", 1)[0]

    logger.info(
        "explain_resolution_started",
        extra={
            "source_url": bool(viz_url),
            "request_session_id": request_session_id,
            "path": path[:300],
        },
    )

    if parsed.scheme in {"http", "https"} and ".s3." in parsed.netloc.lower():
        return _resolve_s3_url(parsed, request_session_id)

    if path.startswith("/serve_viz_file/") or path.startswith("serve_viz_file/"):
        return _resolve_serve_viz_path(path, request_session_id)

    if path.startswith("/images/plotly_figures/pickle/") or path.startswith("images/plotly_figures/pickle/"):
        return _resolve_pickle_path(path, request_session_id)

    if path.startswith("/static/visualizations/") or path.startswith("static/visualizations/"):
        return _resolve_static_path(path)

    # Legacy relative path support. Only allow paths under the current session.
    if not request_session_id:
        raise ExplainResolutionError("Session ID required for relative visualization paths")
    rel_parts = [unquote(part) for part in path.strip("/").split("/")]
    if rel_parts and rel_parts[0] == request_session_id:
        rel_parts = rel_parts[1:]
    local_path, restored = _resolve_session_file(request_session_id, rel_parts)
    if local_path is None:
        raise ExplainResolutionError("Visualization file not found for this session")

    return ExplainTarget(
        session_id=request_session_id,
        viz_path=str(local_path),
        local_path=str(local_path),
        source="relative_session_path",
        restored_session=restored,
    )
