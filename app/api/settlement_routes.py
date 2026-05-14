"""API routes for manual settlement classification."""

from __future__ import annotations

import logging
import os
from pathlib import Path

from flask import Blueprint, current_app, jsonify, request, session

from app.auth.decorators import require_auth
from app.settlement import SettlementClassificationService

logger = logging.getLogger(__name__)

settlement_bp = Blueprint("settlement", __name__, url_prefix="/api/settlement")


def _auth_disabled() -> bool:
    return (
        os.environ.get("DISABLE_AUTH", "false").lower() == "true"
        or bool(current_app.config.get("DISABLE_AUTH", False))
    )


def _authorize_session(session_id: str):
    if _auth_disabled():
        return None

    allowed = {
        value
        for value in (
            session.get("session_id"),
            session.get("base_session_id"),
        )
        if value
    }
    if session_id not in allowed:
        return jsonify({
            "success": False,
            "message": "You are not authorized to access this settlement classification session.",
        }), 403
    return None


def _service(session_id: str) -> SettlementClassificationService:
    upload_root = current_app.config.get("UPLOAD_FOLDER", "instance/uploads")
    root = Path(upload_root)
    if not root.is_absolute():
        root = Path(current_app.root_path).parent / root
    return SettlementClassificationService(session_id, upload_root=str(root))


@settlement_bp.route("/<session_id>/status", methods=["GET"])
@require_auth
def settlement_status(session_id: str):
    auth_error = _authorize_session(session_id)
    if auth_error:
        return auth_error
    try:
        return jsonify(_service(session_id).status())
    except Exception as exc:
        logger.error("Settlement status failed: %s", exc, exc_info=True)
        return jsonify({"success": False, "message": str(exc)}), 400


@settlement_bp.route("/<session_id>/wards", methods=["GET"])
@require_auth
def settlement_wards(session_id: str):
    auth_error = _authorize_session(session_id)
    if auth_error:
        return auth_error
    try:
        method = request.args.get("method", "composite")
        wards = _service(session_id).list_wards(include_rankings=True, method=method)
        return jsonify({"success": True, "session_id": session_id, "wards": wards, "count": len(wards)})
    except Exception as exc:
        logger.error("Settlement ward list failed: %s", exc, exc_info=True)
        return jsonify({"success": False, "message": str(exc), "wards": []}), 400


@settlement_bp.route("/<session_id>/boundaries", methods=["GET"])
@require_auth
def settlement_boundaries(session_id: str):
    auth_error = _authorize_session(session_id)
    if auth_error:
        return auth_error
    try:
        method = request.args.get("method", "composite")
        return jsonify(_service(session_id).load_boundaries_geojson(method=method))
    except Exception as exc:
        logger.error("Settlement boundaries failed: %s", exc, exc_info=True)
        return jsonify({"success": False, "message": str(exc)}), 400


@settlement_bp.route("/<session_id>/classifications", methods=["POST"])
@require_auth
def create_classification(session_id: str):
    auth_error = _authorize_session(session_id)
    if auth_error:
        return auth_error
    try:
        payload = request.get_json(silent=True) or {}
        result = _service(session_id).create_classification(
            ward_names=payload.get("ward_names"),
            ward_ids=payload.get("ward_ids"),
            top_n=payload.get("top_n"),
            method=payload.get("method", "composite"),
            cell_size_m=int(payload.get("cell_size_m", 500)),
            include_no_buildings=bool(payload.get("include_no_buildings", True)),
        )
        return jsonify({"success": True, **result})
    except Exception as exc:
        logger.error("Settlement classification create failed: %s", exc, exc_info=True)
        return jsonify({"success": False, "message": str(exc)}), 400


@settlement_bp.route("/<session_id>/classifications/<classification_id>", methods=["GET"])
@require_auth
def get_classification(session_id: str, classification_id: str):
    auth_error = _authorize_session(session_id)
    if auth_error:
        return auth_error
    try:
        metadata = _service(session_id).get_classification(classification_id)
        return jsonify({"success": True, "classification": metadata})
    except FileNotFoundError as exc:
        return jsonify({"success": False, "message": str(exc)}), 404
    except Exception as exc:
        logger.error("Settlement classification metadata failed: %s", exc, exc_info=True)
        return jsonify({"success": False, "message": str(exc)}), 400


@settlement_bp.route("/<session_id>/classifications/<classification_id>/grid", methods=["GET"])
@require_auth
def get_classification_grid(session_id: str, classification_id: str):
    auth_error = _authorize_session(session_id)
    if auth_error:
        return auth_error
    try:
        return jsonify(_service(session_id).load_grid_geojson(classification_id))
    except FileNotFoundError as exc:
        return jsonify({"success": False, "message": str(exc)}), 404
    except Exception as exc:
        logger.error("Settlement classification grid failed: %s", exc, exc_info=True)
        return jsonify({"success": False, "message": str(exc)}), 400


@settlement_bp.route("/<session_id>/classifications/<classification_id>/annotations", methods=["GET", "POST"])
@require_auth
def classification_annotations(session_id: str, classification_id: str):
    auth_error = _authorize_session(session_id)
    if auth_error:
        return auth_error
    try:
        service = _service(session_id)
        if request.method == "GET":
            return jsonify(service.load_annotations(classification_id))
        payload = request.get_json(silent=True) or {}
        return jsonify(service.save_annotation(classification_id, payload))
    except FileNotFoundError as exc:
        return jsonify({"success": False, "message": str(exc)}), 404
    except Exception as exc:
        logger.error("Settlement annotation operation failed: %s", exc, exc_info=True)
        return jsonify({"success": False, "message": str(exc)}), 400


@settlement_bp.route("/<session_id>/classifications/<classification_id>/export", methods=["POST"])
@require_auth
def export_classification(session_id: str, classification_id: str):
    auth_error = _authorize_session(session_id)
    if auth_error:
        return auth_error
    try:
        result = _service(session_id).export_classification(classification_id)
        return jsonify(result)
    except FileNotFoundError as exc:
        return jsonify({"success": False, "message": str(exc)}), 404
    except Exception as exc:
        logger.error("Settlement export failed: %s", exc, exc_info=True)
        return jsonify({"success": False, "message": str(exc)}), 400
