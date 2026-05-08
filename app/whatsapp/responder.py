"""WhatsApp analysis responder.

This module adapts the shared Data Analysis V3 chat service to WhatsApp. It does
not call DataAnalysisAgent directly.
"""

from __future__ import annotations

import logging
from pathlib import Path
from typing import Any

from app.whatsapp.formatter import chunk_text, format_error
from app.whatsapp.observability import log_event

logger = logging.getLogger(__name__)


def _send_or_raise(send_fn, sender: str, messages: list[str], app) -> None:
    if not send_fn(sender, messages, app):
        raise RuntimeError("Failed to send WhatsApp analysis response")


def run_whatsapp_analysis_and_respond(
    *,
    user_message: str,
    sender: str,
    session_id: str,
    send_fn,
    app,
) -> dict[str, Any]:
    """Run ChatMRPT analysis and send text plus public map links to WhatsApp."""
    with app.app_context():
        from app.services.analysis_chat_service import run_analysis_message
        from app.services.instance_sync import ensure_session_available, sync_session_after_upload
        from app.utils import s3_map_storage

        upload_root = app.config.get("UPLOAD_FOLDER", str(Path(app.instance_path) / "uploads"))
        ensure_session_available(session_id)

        try:
            log_event("analysis_start", sender=sender, session_id=session_id)
            result = run_analysis_message(session_id=session_id, message=user_message)
            log_event("analysis_end", sender=sender, session_id=session_id, success=True)
        except Exception:
            log_event("analysis_end", logging.ERROR, sender=sender, session_id=session_id, success=False)
            send_fn(sender, [format_error()], app)
            raise

        if not isinstance(result, dict):
            result = {"success": True, "message": str(result), "visualizations": []}

        if not result.get("success", True):
            message = result.get("error") or result.get("message") or format_error()
            warning = f"Warning: {message}"
            _send_or_raise(send_fn, sender, [warning], app)
            return {
                "success": False,
                "message": warning,
                "result": result,
                "public_visualizations": [],
            }

        message = result.get("message") or "Analysis complete. Ask me a follow-up question."
        _send_or_raise(send_fn, sender, chunk_text(message), app)

        public_visualizations = []
        for viz in result.get("visualizations") or []:
            local_path = s3_map_storage.resolve_visualization_file(viz, session_id, upload_root)
            if not local_path:
                logger.warning("Could not resolve visualization for WhatsApp: %s", viz)
                continue

            public_url = s3_map_storage.upload_public(
                str(local_path),
                f"maps/{session_id}/{local_path.name}",
            )
            if not public_url:
                log_event("s3_map_upload_failure", logging.ERROR, sender=sender, session_id=session_id, local_path=str(local_path))
                continue

            title = viz.get("title") or "Map"
            _send_or_raise(send_fn, sender, [f"{title}\n{public_url}"], app)
            public_visualizations.append({
                "title": title,
                "url": public_url,
                "local_path": str(local_path),
            })

        try:
            sync_session_after_upload(session_id)
        except Exception:
            log_event("instance_sync_failure", logging.ERROR, sender=sender, session_id=session_id)
            raise
        return {
            "success": True,
            "message": message,
            "result": result,
            "public_visualizations": public_visualizations,
        }
