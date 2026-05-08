"""Public S3 storage helpers for WhatsApp map links.

Private session data remains on local EC2 disks and is synchronized by
``app.services.instance_sync``. This module is only for public HTML map
artifacts under ``maps/{session_id}/...``.
"""

from __future__ import annotations

import logging
import mimetypes
import os
from pathlib import Path
from urllib.parse import quote, unquote, urlparse

logger = logging.getLogger(__name__)

_DEFAULT_REGION = "us-east-2"


def _quote_key(key: str) -> str:
    """URL-escape an S3 key one path segment at a time."""
    return "/".join(quote(part, safe="") for part in key.split("/"))


def _clean_s3_key(s3_key: str) -> str:
    key = (s3_key or "").strip().lstrip("/")
    parts = [part for part in key.split("/") if part not in {"", ".", ".."}]
    return "/".join(parts)


def _get_s3_client():
    import boto3

    region = os.getenv("AWS_REGION") or os.getenv("AWS_DEFAULT_REGION") or _DEFAULT_REGION
    return boto3.client("s3", region_name=region)


def _public_url(bucket: str, key: str) -> str:
    region = os.getenv("AWS_REGION") or os.getenv("AWS_DEFAULT_REGION") or _DEFAULT_REGION
    return f"https://{bucket}.s3.{region}.amazonaws.com/{_quote_key(key)}"


def upload_public(local_path: str, s3_key: str) -> str | None:
    """Upload public map HTML and return a direct S3 URL.

    The bucket policy, not object ACLs, must make ``maps/*`` public. This helper
    intentionally never sends an ACL in the S3 request.
    """
    bucket = os.getenv("S3_UPLOADS_BUCKET", "").strip()
    if not bucket:
        logger.warning("S3_UPLOADS_BUCKET is not configured; skipping public map upload")
        return None

    key = _clean_s3_key(s3_key)
    if not key.startswith("maps/"):
        logger.error("Refusing to upload public map outside maps/ prefix: %s", s3_key)
        return None

    path = Path(local_path)
    if not path.is_file():
        logger.warning("Public map upload skipped; local file does not exist: %s", local_path)
        return None

    content_type = mimetypes.guess_type(str(path))[0] or "application/octet-stream"
    extra_args = {
        "ContentType": content_type,
        "CacheControl": "public, max-age=86400",
    }

    try:
        client = _get_s3_client()
        client.upload_file(str(path), bucket, key, ExtraArgs=extra_args)
    except Exception as exc:
        logger.exception("Failed to upload public map %s to s3://%s/%s: %s", path, bucket, key, exc)
        return None

    public_url = _public_url(bucket, key)
    logger.info("Uploaded public map: %s -> %s", path, public_url)
    return public_url


def _session_root(session_id: str, upload_root: str | os.PathLike[str]) -> Path:
    return (Path(upload_root) / session_id).resolve()


def _inside_session(path: Path, session_root: Path) -> bool:
    try:
        path.resolve().relative_to(session_root)
        return True
    except ValueError:
        return False


def _existing_session_file(candidate: Path, session_root: Path) -> Path | None:
    resolved = candidate.resolve()
    if resolved.is_file() and _inside_session(resolved, session_root):
        return resolved
    return None


def _path_without_query(raw: str) -> str:
    parsed = urlparse(raw)
    if parsed.scheme or parsed.netloc:
        return unquote(parsed.path)
    return unquote(raw.split("?", 1)[0])


def resolve_visualization_file(
    viz: dict,
    session_id: str,
    upload_root: str | os.PathLike[str],
) -> Path | None:
    """Resolve a visualization object to a local session file.

    Handles current visualization shapes used by the web app and planned
    WhatsApp adapter: ``file_path``, ``path``, ``/serve_viz_file/...`` URLs,
    and ``visualizations/{filename}`` relative URLs.
    """
    if not isinstance(viz, dict) or not session_id:
        return None

    session_root = _session_root(session_id, upload_root)

    raw_path = viz.get("file_path") or viz.get("path")
    if raw_path:
        raw_path = str(raw_path)
        if raw_path.startswith("/serve_viz_file/"):
            viz = {**viz, "url": raw_path}
        else:
            path_text = _path_without_query(raw_path)
            candidate = Path(path_text)
            if candidate.is_absolute():
                found = _existing_session_file(candidate, session_root)
            else:
                cleaned = path_text.lstrip("/")
                if cleaned.startswith(f"instance/uploads/{session_id}/"):
                    candidate = Path(cleaned)
                elif cleaned.startswith(f"uploads/{session_id}/"):
                    candidate = Path("instance") / cleaned
                else:
                    candidate = session_root / cleaned
                found = _existing_session_file(candidate, session_root)
            if found:
                return found

    url = str(viz.get("url") or "")
    if not url:
        return None

    path = _path_without_query(url)
    marker = f"/serve_viz_file/{session_id}/"
    if marker in path:
        rel = path.split(marker, 1)[1].lstrip("/")
        found = _existing_session_file(session_root / rel, session_root)
        if found:
            return found

    if path.startswith("/serve_viz_file/"):
        parts = [part for part in path.strip("/").split("/") if part]
        if len(parts) >= 3 and parts[0] == "serve_viz_file" and parts[1] == session_id:
            rel = "/".join(parts[2:])
            found = _existing_session_file(session_root / rel, session_root)
            if found:
                return found

    rel_path = path.lstrip("/")
    if rel_path.startswith("visualizations/"):
        found = _existing_session_file(session_root / rel_path, session_root)
        if found:
            return found

    filename = Path(rel_path).name
    if filename:
        for rel_dir in ("visualizations", ""):
            found = _existing_session_file(session_root / rel_dir / filename, session_root)
            if found:
                return found

    return None
