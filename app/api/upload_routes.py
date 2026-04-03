"""
File upload routes for ChatMRPT.

Provides standard dual-file upload (CSV/Excel + Shapefile), CSV-only upload,
and basic sample/download helpers. This module focuses on robust handling and
clear error responses to avoid opaque 500s during upload.
"""

from __future__ import annotations

import os
import time
import logging
from typing import Dict, Optional, Tuple

from flask import Blueprint, session, request, current_app, jsonify
from app.auth.decorators import require_auth
from werkzeug.utils import secure_filename

from app.utils.decorators import handle_errors, log_execution_time, validate_session
from app.utils.exceptions import DataProcessingError
from app.agent.encoding_handler import find_raw_data_file, read_raw_data

logger = logging.getLogger(__name__)

# Optional TPR imports
try:
    from ...tpr_module.integration.upload_detector import TPRUploadDetector
    # REMOVED: tpr_module not available import get_tpr_handler
    TPR_MODULE_AVAILABLE = True
except Exception:
    TPRUploadDetector = None  # type: ignore
    get_tpr_handler = None    # type: ignore
    TPR_MODULE_AVAILABLE = False


upload_bp = Blueprint("upload", __name__)

# Allowed extensions
ALLOWED_EXTENSIONS_CSV = {"csv", "txt", "xlsx", "xls"}
ALLOWED_EXTENSIONS_SHP = {"zip"}


def allowed_file(filename: str, allowed_extensions: set) -> bool:
    return "." in filename and filename.rsplit(".", 1)[1].lower() in allowed_extensions


class UploadTypeDetector:
    """Detects upload type: csv_shapefile, csv_only, or TPR variants."""

    def __init__(self) -> None:
        self.tpr_detector = TPRUploadDetector() if TPR_MODULE_AVAILABLE and TPRUploadDetector else None
        self._tpr_detection_info: Optional[dict] = None

    def detect_upload_type(self, files: Dict[str, object], csv_content=None) -> str:
        csv_file = files.get("csv_file")
        shapefile = files.get("shapefile")

        has_csv = bool(csv_file) and getattr(csv_file, "filename", "") != ""
        has_shp = bool(shapefile) and getattr(shapefile, "filename", "") != ""

        # TPR detection first if available
        if TPR_MODULE_AVAILABLE and self.tpr_detector and has_csv:
            try:
                tpr_type, tpr_info = self.tpr_detector.detect_tpr_upload(csv_file, shapefile, csv_content)  # type: ignore[arg-type]
                if self.tpr_detector.should_use_tpr_workflow(tpr_type):
                    self._tpr_detection_info = tpr_info
                    return tpr_type
            except Exception as e:
                logger.warning(f"TPR detection failed, falling back to standard: {e}")

        if has_csv and has_shp:
            return "csv_shapefile"
        if has_csv and not has_shp:
            return "csv_only"
        return "invalid"

    def get_upload_summary(self, upload_type: str, file_info: dict) -> dict:
        summaries = {
            "csv_shapefile": {
                "path": "Full Dataset Path",
                "description": "CSV data + Shapefile boundaries",
                "next_step": "Store raw data and generate summary",
            },
            "csv_only": {
                "path": "CSV-Only Path",
                "description": "CSV data without geographic boundaries",
                "next_step": "Data analysis without mapping capabilities",
            },
            "tpr_excel": {
                "path": "TPR Analysis Path",
                "description": "NMEP TPR Excel file for Test Positivity Rate calculation",
                "next_step": "Select state and configure TPR analysis parameters",
            },
            "tpr_shapefile": {
                "path": "TPR + Shapefile Path",
                "description": "NMEP TPR Excel + Custom shapefile boundaries",
                "next_step": "Select state for TPR analysis with custom boundaries",
            },
            "invalid": {
                "path": "Invalid Upload",
                "description": "No valid files detected",
                "next_step": "Please upload valid CSV and/or shapefile",
            },
        }

        return {
            "upload_type": upload_type,
            "file_info": file_info,
            **summaries.get(upload_type, summaries["invalid"]),
        }


@upload_bp.route("/upload_both_files", methods=["POST"])
@require_auth
@validate_session
@log_execution_time
def upload_both_files():
    """Standard upload entrypoint used by the frontend modal."""
    try:
        # ALWAYS generate a new session ID for each upload to prevent session reuse
        # This fixes the concurrent user data bleed issue
        import uuid
        session_id = str(uuid.uuid4())
        session['session_id'] = session_id
        session['base_session_id'] = session_id

        logger.info(f"🔄 Generated new session ID for upload: {session_id}")

        # Extract files
        csv_file = request.files.get("csv_file")
        shapefile = request.files.get("shapefile")

        # Basic validation
        if csv_file and csv_file.filename == "":
            csv_file = None
        if shapefile and shapefile.filename == "":
            shapefile = None
        if not csv_file and not shapefile:
            return (
                jsonify({"status": "error", "message": "No files selected for upload"}),
                400,
            )

        detector = UploadTypeDetector()
        upload_type = detector.detect_upload_type({"csv_file": csv_file, "shapefile": shapefile})

        file_info = {
            "csv_filename": getattr(csv_file, "filename", None),
            "shapefile_filename": getattr(shapefile, "filename", None),
            "session_id": session_id,
        }
        upload_summary = detector.get_upload_summary(upload_type, file_info)

        logger.info(
            f"Upload Type Detected: {upload_type} for session {session_id} | files: {file_info}"
        )

        if upload_type == "csv_shapefile":
            return handle_full_dataset_path(session_id, csv_file, shapefile, upload_summary)  # type: ignore[arg-type]
        if upload_type == "csv_only":
            return handle_csv_only_path(session_id, csv_file, upload_summary)  # type: ignore[arg-type]
        if upload_type in ["tpr_excel", "tpr_shapefile"] and TPR_MODULE_AVAILABLE:
            return handle_tpr_path(
                session_id, csv_file, shapefile, upload_type, detector._tpr_detection_info, upload_summary  # type: ignore[arg-type]
            )

        return (
            jsonify(
                {
                    "status": "error",
                    "message": f"Invalid upload combination: {upload_summary['description']}",
                }
            ),
            400,
        )
    except Exception as e:
        logger.error(f"Upload failed: {e}", exc_info=True)
        return jsonify({"status": "error", "message": str(e)}), 500


def _ensure_session_folder(session_id: str) -> str:
    session_folder = os.path.join(current_app.config["UPLOAD_FOLDER"], session_id)
    os.makedirs(session_folder, exist_ok=True)
    return session_folder


def clear_analysis_results(session_folder: str) -> int:
    """Remove stale analysis artifacts before storing new raw files."""
    import glob

    patterns = [
        "analysis_*.csv",
        "unified_dataset.*",
        "composite_scores*.csv",
        "vulnerability_rankings*.csv",
        "normalized_data.csv",
        "cleaned_data.csv",
        "*_map_*.html",
        "*_chart_*.html",
        "itn_distribution_*.csv",
        "model_*.csv",
        "*.geoparquet",
    ]
    removed = 0
    for pat in patterns:
        for path in glob.glob(os.path.join(session_folder, pat)):
            try:
                os.remove(path)
                removed += 1
            except Exception as e:
                logger.warning(f"Could not remove {path}: {e}")
    if removed:
        logger.info(f"Cleared {removed} old analysis files from session folder")
    return removed


def store_raw_data_files(session_folder: str, csv_file, shapefile) -> dict:
    """Save uploaded files as-is into the session folder."""
    try:
        stored = []
        clear_analysis_results(session_folder)

        if csv_file and allowed_file(csv_file.filename, ALLOWED_EXTENSIONS_CSV):
            # Preserve correct extension based on upload type
            ext = csv_file.filename.rsplit(".", 1)[1].lower() if "." in csv_file.filename else "csv"
            if ext in ("xlsx", "xls"):
                raw_filename = "raw_data.xlsx"
            else:
                raw_filename = "raw_data.csv"
            raw_path = os.path.join(session_folder, raw_filename)
            csv_file.save(raw_path)
            try:
                os.sync()  # type: ignore[attr-defined]
            except Exception:
                pass
            stored.append(raw_filename)
            logger.info(f"Stored raw data: {csv_file.filename} -> {raw_filename}")

        if shapefile and allowed_file(shapefile.filename, ALLOWED_EXTENSIONS_SHP):
            raw_shp = os.path.join(session_folder, "raw_shapefile.zip")
            shapefile.save(raw_shp)
            try:
                os.sync()  # type: ignore[attr-defined]
            except Exception:
                pass
            stored.append("raw_shapefile.zip")
            logger.info(
                f"Stored raw shapefile: {shapefile.filename} -> raw_shapefile.zip"
            )

        return {
            "status": "success",
            "message": "Raw data files stored successfully",
            "files_stored": len(stored),
            "stored_files": stored,
        }
    except Exception as e:
        logger.error(f"Error storing raw data files: {e}")
        return {"status": "error", "message": f"Failed to store raw data: {e}"}


def generate_dynamic_data_summary(session_id: str, session_folder: str, upload_type: str):
    """Create a light-weight summary of the uploaded data for UI feedback."""
    try:
        import pandas as pd

        summary = {
            "session_id": session_id,
            "upload_type": upload_type,
            "analysis_timestamp": pd.Timestamp.now().isoformat(),
        }

        raw_data_path = find_raw_data_file(session_folder)
        if raw_data_path:
            try:
                df = read_raw_data(session_folder)
            except Exception as e:
                logger.error(f"Failed to parse uploaded data: {e}")
                raise

            summary.update(
                {
                    "total_rows": int(len(df)),
                    "total_columns": int(len(df.columns)),
                    "column_names": list(df.columns),
                    "preview_rows": df.head(5).fillna("N/A").to_dict("records"),
                    "column_types": detect_column_types(df),
                    "data_completeness": calculate_data_completeness(df),
                    "data_quality_assessment": assess_data_quality(df),
                }
            )

        raw_shp = os.path.join(session_folder, "raw_shapefile.zip")
        if os.path.exists(raw_shp):
            summary["shapefile_info"] = {
                "filename": "raw_shapefile.zip",
                "size_mb": round(os.path.getsize(raw_shp) / (1024 * 1024), 2),
                "status": "stored",
            }

        logger.info(
            f"Generated data summary for session {session_id}: "
            f"rows={summary.get('total_rows')}, cols={summary.get('total_columns')}"
        )
        return summary
    except Exception as e:
        logger.error(f"Error generating data summary: {e}")
        return {"status": "error", "message": f"Failed to generate data summary: {e}"}


def detect_column_types(df) -> dict:
    types = {}
    for col in df.columns:
        try:
            if str(df[col].dtype) in ("int64", "float64"):
                types[col] = "numeric"
            elif str(df[col].dtype) == "object":
                unique_ratio = (df[col].nunique() / max(1, len(df)))
                types[col] = "categorical" if unique_ratio < 0.1 else "text"
            else:
                types[col] = "other"
        except Exception:
            types[col] = "other"
    return types


def calculate_data_completeness(df) -> dict:
    try:
        completeness = {}
        total_rows = max(1, len(df))
        for col in df.columns:
            non_null = int(df[col].count())
            completeness[col] = round((non_null / total_rows) * 100, 2)
        overall = round((df.count().sum() / (total_rows * max(1, len(df.columns)))) * 100, 2)
        return {"by_column": completeness, "overall": overall}
    except Exception:
        return {"by_column": {}, "overall": 0.0}


def assess_data_quality(df) -> dict:
    issues = []
    try:
        empty_cols = df.columns[df.isnull().all()].tolist()
        if empty_cols:
            issues.append(f"Empty columns: {empty_cols}")
        dup = int(df.duplicated().sum())
        if dup:
            issues.append(f"Duplicate rows: {dup}")
        ward_col = "ward_name" if "ward_name" in df.columns else ("WardName" if "WardName" in df.columns else None)
        if ward_col:
            missing_wards = int(df[ward_col].isnull().sum())
            if missing_wards:
                issues.append(f"Missing ward names: {missing_wards}")
    except Exception:
        pass
    score = max(0, 100 - (len(issues) * 20))
    return {"issues_found": len(issues), "issues": issues, "quality_score": score}


def handle_full_dataset_path(session_id: str, csv_file, shapefile, upload_summary: dict):
    start_ts = time.time()
    session_folder = _ensure_session_folder(session_id)
    logger.info(f"Starting Full Dataset Path for session {session_id}")

    # Store raw files
    raw_storage = store_raw_data_files(session_folder, csv_file, shapefile)
    if raw_storage.get("status") != "success":
        raise DataProcessingError(f"Failed to store raw data: {raw_storage.get('message')}")

    # Generate summary (non-fatal if fails; function returns an error dict)
    summary = generate_dynamic_data_summary(session_id, session_folder, "csv_shapefile")

    # Mark session flags
    session["upload_type"] = "csv_shapefile"
    session["raw_data_stored"] = True
    session["should_ask_analysis_permission"] = False
    session["csv_loaded"] = True
    session["shapefile_loaded"] = True
    session["data_loaded"] = True
    session.permanent = True  # Make session permanent
    session.modified = True  # CRITICAL: Force Redis session update

    logger.info(f"✅ Session flags set for {session_id}: csv_loaded=True, data_loaded=True")

    try:
        from app.conversation.session_state import SessionStateManager  # lazy import
        SessionStateManager().update_state(session_id, {
            "should_ask_analysis_permission": False,
            "data_loaded": True,
        })
    except Exception as e:
        logger.debug(f"SessionStateManager update failed: {e}")

    # Cross-instance sync to ensure other workers see uploaded files
    try:
        from app.services.instance_sync import sync_session_after_upload
        sync_session_after_upload(session_id)
    except Exception as e:
        logger.debug(f"Instance sync not performed: {e}")

    duration = time.time() - start_ts
    resp = jsonify({
        "status": "success",
        "upload_type": "csv_shapefile",
        "session_id": session_id,
        "upload_summary": upload_summary,
        "raw_storage": raw_storage,
        "data_summary": summary,
        "message": f"Full dataset uploaded successfully. {raw_storage.get('files_stored', 0)} files stored as raw data.",
        "next_step": "Data summary will be presented for your review and permission.",
        "upload_duration": f"{duration:.2f}s",
    })
    try:
        resp.set_cookie(
            "ChatMRPT-Session",
            value=session_id,
            max_age=3600,
            httponly=True,
            samesite="Lax",
            path="/",
        )
    except Exception:
        pass
    return resp


def store_raw_csv_only(session_folder: str, csv_file):
    try:
        clear_analysis_results(session_folder)
        if csv_file and allowed_file(csv_file.filename, ALLOWED_EXTENSIONS_CSV):
            # Preserve correct extension based on upload type
            ext = csv_file.filename.rsplit(".", 1)[1].lower() if "." in csv_file.filename else "csv"
            if ext in ("xlsx", "xls"):
                raw_filename = "raw_data.xlsx"
            else:
                raw_filename = "raw_data.csv"
            raw_path = os.path.join(session_folder, raw_filename)
            csv_file.save(raw_path)
            logger.info(f"Stored raw data: {csv_file.filename} -> {raw_filename}")
            return {
                "status": "success",
                "message": "Raw data file stored successfully",
                "files_stored": 1,
                "stored_files": [raw_filename],
            }
        return {"status": "error", "message": "Invalid data file"}
    except Exception as e:
        logger.error(f"Error storing raw CSV: {e}")
        return {"status": "error", "message": f"Failed to store raw CSV: {e}"}


def handle_csv_only_path(session_id: str, csv_file, upload_summary: dict):
    session_folder = _ensure_session_folder(session_id)
    logger.info(f"CSV-Only path for session {session_id}")

    raw_storage = store_raw_csv_only(session_folder, csv_file)
    if raw_storage.get("status") != "success":
        raise DataProcessingError(f"Failed to store raw CSV: {raw_storage.get('message')}")

    summary = generate_dynamic_data_summary(session_id, session_folder, "csv_only")

    session["upload_type"] = "csv_only"
    session["raw_data_stored"] = True
    session["should_ask_analysis_permission"] = False
    session["csv_loaded"] = True
    session["shapefile_loaded"] = False
    session["data_loaded"] = True
    session.permanent = True  # Make session permanent
    session.modified = True  # CRITICAL: Force Redis session update

    logger.info(f"✅ Session flags set for {session_id}: csv_loaded=True, data_loaded=True")

    # Cross-instance sync
    try:
        from app.services.instance_sync import sync_session_after_upload
        sync_session_after_upload(session_id)
    except Exception as e:
        logger.debug(f"Instance sync not performed: {e}")

    resp = jsonify({
        "status": "success",
        "upload_type": "csv_only",
        "session_id": session_id,
        "upload_summary": upload_summary,
        "raw_storage": raw_storage,
        "data_summary": summary,
        "message": "CSV uploaded successfully. Note: No mapping capabilities without shapefile.",
        "next_step": "Data summary will be presented for analysis without geographic visualization.",
    })
    try:
        resp.set_cookie(
            "ChatMRPT-Session",
            value=session_id,
            max_age=3600,
            httponly=True,
            samesite="Lax",
            path="/",
        )
    except Exception:
        pass
    return resp


def handle_tpr_path(session_id: str, csv_file, shapefile, upload_type: str, detection_info: dict, upload_summary: dict):
    start_ts = time.time()
    session_folder = _ensure_session_folder(session_id)
    logger.info(f"Starting TPR Path for session {session_id}, type: {upload_type}")

    try:
        excel_path = os.path.join(session_folder, "tpr_data.xlsx")
        csv_file.save(excel_path)
        shp_path = None
        if shapefile and upload_type == "tpr_shapefile":
            shp_path = os.path.join(session_folder, "custom_shapefile.zip")
            shapefile.save(shp_path)

        if hasattr(current_app, "services") and getattr(current_app.services, "interaction_logger", None):
            current_app.services.interaction_logger.log_file_upload(  # type: ignore[attr-defined]
                session_id=session_id,
                file_type="tpr_excel",
                file_name=getattr(csv_file, "filename", ""),
                file_size=os.path.getsize(excel_path),
                metadata={
                    "upload_type": upload_type,
                    "has_shapefile": shp_path is not None,
                    "tpr_year": (detection_info or {}).get("metadata", {}).get("year"),
                    "tpr_month": (detection_info or {}).get("metadata", {}).get("month"),
                },
            )
    except Exception as e:
        logger.error(f"Error storing TPR files: {e}")
        raise DataProcessingError(f"Failed to store TPR files: {e}")

    tpr_handler = get_tpr_handler(session_id)  # type: ignore[misc]
    result = tpr_handler.handle_tpr_upload(excel_path, shp_path, upload_type, detection_info)  # type: ignore[name-defined]
    if result.get("status") != "success":
        raise DataProcessingError(result.get("message", "TPR processing failed"))

    session["upload_type"] = upload_type
    session["tpr_workflow_active"] = True
    session["tpr_session_id"] = session_id
    session["data_loaded"] = True
    session["csv_loaded"] = True
    session["shapefile_loaded"] = bool(shp_path)
    session["should_ask_analysis_permission"] = False
    session.modified = True

    # Cross-instance sync
    try:
        from app.services.instance_sync import sync_session_after_upload
        sync_session_after_upload(session_id)
    except Exception as e:
        logger.debug(f"Instance sync not performed: {e}")

    duration = time.time() - start_ts
    if hasattr(current_app, "services") and getattr(current_app.services, "interaction_logger", None):
        current_app.services.interaction_logger.log_analysis_event(  # type: ignore[attr-defined]
            session_id=session_id,
            event_type="tpr_upload_completed",
            details={
                "upload_type": upload_type,
                "upload_duration": duration,
                "states_available": len(result.get("available_states", [])),
                "workflow_stage": result.get("stage", "unknown"),
            },
            success=True,
        )

    resp = jsonify({
        "status": "success",
        "upload_type": upload_type,
        "workflow": "tpr",
        "session_id": session_id,
        "upload_summary": upload_summary,
        "tpr_response": result.get("response", ""),
        "available_states": result.get("available_states", []),
        "metadata": result.get("metadata", {}),
        "next_step": result.get("next_step", "Please select a state to analyze"),
        "upload_duration": f"{duration:.2f}s",
        "message": "TPR file uploaded successfully. Starting TPR analysis workflow.",
    })
    try:
        resp.set_cookie(
            "ChatMRPT-Session",
            value=session_id,
            max_age=3600,
            httponly=True,
            samesite="Lax",
            path="/",
        )
    except Exception:
        pass
    return resp


# Legacy compatibility: keep /upload redirecting to main handler
@upload_bp.route("/upload", methods=["POST"])
@require_auth
@handle_errors
@validate_session
@log_execution_time
def upload():
    return upload_both_files()


@upload_bp.route("/load_sample_data", methods=["POST"])
@require_auth
@validate_session
@handle_errors
@log_execution_time
def load_sample_data():
    try:
        session_id = session.get("session_id")
        logger.info(f"Loading sample data for session {session_id}")
        return jsonify({"status": "success", "message": "Sample data loading not yet implemented"})
    except Exception as e:
        logger.error(f"Error loading sample data: {e}")
        return jsonify({"status": "error", "message": f"Failed to load sample data: {e}"})


@upload_bp.route("/api/download/processed-csv", methods=["GET"])
@require_auth
@validate_session
@handle_errors
def download_processed_csv():
    session_id = session.get("session_id")
    session_folder = os.path.join(current_app.config["UPLOAD_FOLDER"], session_id)
    csv_path = os.path.join(session_folder, "raw_data.csv")
    if not os.path.exists(csv_path):
        return (
            jsonify({
                "status": "error",
                "message": "No processed CSV data available. Please upload and process data first.",
            }),
            404,
        )
    try:
        from flask import send_file
        return send_file(csv_path, as_attachment=True, download_name=f"processed_data_{session_id}.csv", mimetype="text/csv")
    except Exception as e:
        logger.error(f"Error downloading CSV: {e}")
        return jsonify({"status": "error", "message": f"Failed to download CSV: {e}"}), 500


@upload_bp.route("/api/download/processed-shapefile", methods=["GET"])
@require_auth
@validate_session
@handle_errors
def download_processed_shapefile():
    session_id = session.get("session_id")
    session_folder = os.path.join(current_app.config["UPLOAD_FOLDER"], session_id)
    shp_path = os.path.join(session_folder, "raw_shapefile.zip")
    if not os.path.exists(shp_path):
        return (
            jsonify({
                "status": "error",
                "message": "No processed shapefile data available. Please upload and process data first.",
            }),
            404,
        )
    try:
        from flask import send_file
        return send_file(
            shp_path,
            as_attachment=True,
            download_name=f"processed_shapefile_{session_id}.zip",
            mimetype="application/zip",
        )
    except Exception as e:
        logger.error(f"Error downloading shapefile: {e}")
        return jsonify({"status": "error", "message": f"Failed to download shapefile: {e}"}), 500
