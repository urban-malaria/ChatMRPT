"""Runtime helpers that expose the standard risk-analysis workflow."""

from __future__ import annotations

import logging
from pathlib import Path
from typing import Any, Dict, List, Optional

from app.analysis.engine import AnalysisEngine
from app.data import DataHandler
from app.upload.upload_service import UploadService

logger = logging.getLogger(__name__)


class SessionDataMissing(RuntimeError):
    """Raised when a session cannot be analysed because data assets are missing."""


def _session_path(session_id: str) -> Path:
    """Return the upload directory for a session without creating new folders."""
    upload_service = UploadService()
    base_dir = Path(upload_service.base_dir)
    return base_dir / session_id


def _require_dataset(session_path: Path) -> None:
    """Ensure the session folder exists and contains at least one data asset."""
    if not session_path.exists():
        raise SessionDataMissing("Session folder not found. Please upload your data first.")

    has_csv = any(p.suffix.lower() in {".csv", ".xlsx", ".xls"} for p in session_path.iterdir())
    if not has_csv:
        raise SessionDataMissing("No CSV or Excel data detected for this session. Upload data before running analysis.")


def get_data_handler(session_id: str) -> DataHandler:
    """Create a DataHandler initialised with the session's stored assets."""
    session_path = _session_path(session_id)
    _require_dataset(session_path)
    handler = DataHandler(str(session_path))

    if handler.csv_data is None:
        raise SessionDataMissing("Uploaded data could not be loaded. Please re-upload your dataset.")

    return handler


def run_standard_analysis(session_id: str) -> Dict[str, Any]:
    """Execute the default composite analysis workflow for a session."""
    handler = get_data_handler(session_id)
    engine = AnalysisEngine(handler)
    return engine.run_standard_analysis(handler, session_id=session_id)


def run_custom_analysis(session_id: str, selected_variables: List[str]) -> Dict[str, Any]:
    """Execute composite analysis constrained to the provided variables."""
    if not selected_variables:
        raise ValueError("'selected_variables' must contain at least one column name")

    handler = get_data_handler(session_id)
    engine = AnalysisEngine(handler)
    return engine.run_custom_analysis(handler, selected_variables=selected_variables, session_id=session_id)


def run_pca_analysis(session_id: str, selected_variables: Optional[List[str]] = None) -> Dict[str, Any]:
    """Execute the standalone PCA workflow for a session."""
    handler = get_data_handler(session_id)
    engine = AnalysisEngine(handler)
    return engine.run_pca_analysis(session_id=session_id, variables=selected_variables)


def get_session_overview(session_id: str) -> Dict[str, Any]:
    """Summarise which artefacts are currently available for a session."""
    session_path = _session_path(session_id)

    if not session_path.exists():
        return {
            "status": "new_session",
            "session_id": session_id,
            "csv_loaded": False,
            "shapefile_loaded": False,
            "analysis_complete": False,
            "can_run_analysis": False,
            "available_actions": ["upload_data", "explain_concept"],
            "message": "New session – upload your CSV (and shapefile) to begin analysis.",
        }

    csv_files: List[str] = []
    shapefile_files: List[str] = []
    analysis_files: List[str] = []

    for entry in session_path.iterdir():
        if entry.is_dir() or entry.name.startswith('.'):
            continue
        suffix = entry.suffix.lower()
        if suffix in {'.csv', '.xlsx', '.xls'}:
            csv_files.append(entry.name)
        elif suffix in {'.zip', '.shp', '.geoparquet'}:
            shapefile_files.append(entry.name)
        if 'composite' in entry.name.lower() or 'unified' in entry.name.lower():
            analysis_files.append(entry.name)

    csv_loaded = bool(csv_files)
    shapefile_loaded = bool(shapefile_files)
    analysis_complete = bool(analysis_files)

    available_actions: List[str] = ["get_session_status", "explain_concept"]
    if not csv_loaded or not shapefile_loaded:
        available_actions.append("upload_data")
    if csv_loaded and shapefile_loaded and not analysis_complete:
        available_actions.extend(["run_composite_analysis", "run_pca_analysis"])
    elif analysis_complete:
        available_actions.extend([
            "create_composite_maps",
            "create_vulnerability_map",
            "create_box_plot_ranking",
            "list_available_maps",
        ])

    if csv_loaded and shapefile_loaded and analysis_complete:
        status = "analysis_complete"
        message = "Analysis complete. Visualisations and reports are ready."
    elif csv_loaded and shapefile_loaded:
        status = "ready_for_analysis"
        message = "Data loaded. You can now run the malaria risk analysis."
    elif csv_loaded:
        status = "needs_shapefile"
        message = "CSV data loaded. Upload a shapefile to enable spatial analysis."
    elif shapefile_loaded:
        status = "needs_csv"
        message = "Shapefile loaded. Upload CSV data to proceed with analysis."
    else:
        status = "needs_data"
        message = "Session exists but no data is available yet. Upload your dataset to continue."

    return {
        "status": status,
        "session_id": session_id,
        "csv_loaded": csv_loaded,
        "shapefile_loaded": shapefile_loaded,
        "analysis_complete": analysis_complete,
        "can_run_analysis": csv_loaded and shapefile_loaded,
        "available_actions": available_actions,
        "csv_files": csv_files,
        "shapefile_files": shapefile_files,
        "message": message,
    }
