"""
Conversation History API routes.

Provides endpoints for listing past conversations and resuming them.
"""

from __future__ import annotations

import logging
import os
import re

from flask import Blueprint, jsonify, session, current_app

from app.auth.decorators import require_auth
from app.services.conversation_history import get_user_id

logger = logging.getLogger(__name__)

# Only accept UUID-formatted session IDs to prevent path traversal
_UUID_RE = re.compile(r"^[a-f0-9]{8}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{12}$")

conversations_bp = Blueprint("conversations", __name__, url_prefix="/api")


def _get_service():
    """Return the ConversationHistoryService from the app, or create one on the fly."""
    if hasattr(current_app, "conversation_history"):
        return current_app.conversation_history

    # Lazy initialisation (first request)
    from app.services.conversation_history import ConversationHistoryService

    redis_client = current_app.config.get("SESSION_REDIS")
    svc = ConversationHistoryService(redis_client=redis_client)
    current_app.conversation_history = svc
    return svc


# ------------------------------------------------------------------
# GET /api/conversations
# ------------------------------------------------------------------

@conversations_bp.route("/conversations", methods=["GET"])
@require_auth
def list_conversations():
    """Return recent conversations for the authenticated user."""
    try:
        user_id = get_user_id()
        svc = _get_service()
        conversations = svc.list_conversations(user_id)
        return jsonify({"success": True, "conversations": conversations})
    except Exception as exc:
        logger.error("Error listing conversations: %s", exc, exc_info=True)
        return jsonify({"success": False, "message": str(exc)}), 500


# ------------------------------------------------------------------
# POST /api/conversations/<session_id>/resume
# ------------------------------------------------------------------

@conversations_bp.route("/conversations/<session_id>/resume", methods=["POST"])
@require_auth
def resume_conversation(session_id: str):
    """Resume a previous conversation.

    Verifies ownership, loads messages from SessionMemory files,
    reconstructs Flask session state from the filesystem, and returns
    everything the frontend needs to restore the conversation.
    """
    # Validate session_id format to prevent path traversal
    if not _UUID_RE.match(session_id):
        return jsonify({"success": False, "message": "Invalid session ID"}), 400

    try:
        user_id = get_user_id()
        svc = _get_service()

        # Ownership check: sorted-set membership is sufficient proof.
        # The meta user_id field may be absent if register_conversation was
        # never called (e.g. session created via file upload, not page load).
        if not svc.conversation_belongs_to_user(user_id, session_id):
            return jsonify({"success": False, "message": "Conversation not found"}), 404

        # --- Load messages from SessionMemory JSON files ---
        messages = _load_messages(session_id)

        # --- Reconstruct session state from filesystem ---
        session_state = _reconstruct_session_state(session_id)

        # --- Switch Flask session to the resumed session ---
        # If there's a child upload session, set session_id to it so the
        # backend can find uploaded files. Keep base_session_id as the original.
        upload_sid = session_state.get("upload_session_id")
        session["session_id"] = upload_sid or session_id
        session["base_session_id"] = session_id
        session["data_loaded"] = session_state.get("data_loaded", False)
        session["csv_loaded"] = session_state.get("csv_loaded", False)
        session["shapefile_loaded"] = session_state.get("shapefile_loaded", False)
        session["analysis_complete"] = session_state.get("analysis_complete", False)
        session["csv_filename"] = session_state.get("csv_filename")
        session["shapefile_filename"] = session_state.get("shapefile_filename")
        session["conversation_history"] = []
        session["message_count"] = len([m for m in messages if m.get("type") == "user"])
        session.modified = True

        logger.info("Resumed conversation %s for user %s (%d messages)", session_id, user_id, len(messages))

        return jsonify({
            "success": True,
            "session_id": session_id,
            "messages": messages,
            "session_state": session_state,
        })

    except Exception as exc:
        logger.error("Error resuming conversation %s: %s", session_id, exc, exc_info=True)
        return jsonify({"success": False, "message": str(exc)}), 500


# ------------------------------------------------------------------
# Helpers
# ------------------------------------------------------------------

def _load_messages(session_id: str) -> list:
    """Load messages from the SessionMemory JSON file on disk."""
    import json

    memory_path = os.path.join(
        current_app.instance_path, "memory", f"{session_id}_memory.json"
    )
    if not os.path.exists(memory_path):
        return []

    try:
        with open(memory_path, "r") as f:
            data = json.load(f)

        raw_messages = data.get("conversation_history", [])
        messages = []
        for msg in raw_messages:
            msg_type = msg.get("type", "")
            # Only return user and assistant messages to the frontend
            if msg_type not in ("user", "assistant"):
                continue
            entry = {
                "type": msg_type,
                "content": msg.get("content", ""),
                "timestamp": msg.get("timestamp", ""),
            }
            # Restore visualizations from metadata if present
            viz = (msg.get("metadata") or {}).get("visualizations")
            if viz:
                entry["visualizations"] = viz
            messages.append(entry)
        return messages

    except Exception as exc:
        logger.warning("Could not load messages for %s: %s", session_id, exc)
        return []


def _reconstruct_session_state(session_id: str) -> dict:
    """Reconstruct session flags by inspecting the upload directory.

    If this session has a child upload session (stored in memory file as
    upload_session_id), also check that folder for files.
    """
    state = {
        "has_files": False,
        "csv_loaded": False,
        "shapefile_loaded": False,
        "data_loaded": False,
        "analysis_complete": False,
        "csv_filename": None,
        "shapefile_filename": None,
        "tpr_active": False,
        "upload_session_id": None,
    }

    # Determine which directories to scan: the base session + any child upload session
    dirs_to_check = [os.path.join(current_app.instance_path, "uploads", session_id)]

    # Check if the memory file records a child upload session
    import json as _json
    memory_path = os.path.join(current_app.instance_path, "memory", f"{session_id}_memory.json")
    if os.path.exists(memory_path):
        try:
            with open(memory_path, "r") as _f:
                mem_data = _json.load(_f)
            child_sid = mem_data.get("upload_session_id")
            if child_sid:
                state["upload_session_id"] = child_sid
                child_dir = os.path.join(current_app.instance_path, "uploads", child_sid)
                dirs_to_check.append(child_dir)
        except Exception:
            pass

    for upload_dir in dirs_to_check:
        if not os.path.isdir(upload_dir):
            continue

        try:
            files = os.listdir(upload_dir)
        except OSError:
            continue

        for fname in files:
            lower = fname.lower()
            if lower.endswith((".csv", ".xlsx", ".xls")):
                state["csv_loaded"] = True
                state["csv_filename"] = fname
                state["has_files"] = True
            elif lower.endswith((".zip", ".shp")):
                state["shapefile_loaded"] = True
                state["shapefile_filename"] = fname
                state["has_files"] = True

        # Check analysis-complete marker
        if os.path.exists(os.path.join(upload_dir, ".analysis_complete")):
            state["analysis_complete"] = True

        # Check if TPR workflow was active
        agent_state_path = os.path.join(upload_dir, ".agent_state.json")
        if os.path.exists(agent_state_path):
            try:
                with open(agent_state_path, "r") as _f:
                    ws = _json.load(_f)
                if ws.get("tpr_workflow_active", False):
                    state["tpr_active"] = True
            except Exception:
                pass

    # CSV is the gate for data analysis; shapefile is optional (enriches maps)
    state["data_loaded"] = state["csv_loaded"]

    return state
