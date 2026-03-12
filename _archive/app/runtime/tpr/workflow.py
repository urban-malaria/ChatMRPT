"""Runtime helpers that expose the LangGraph TPR workflow to other flows."""

from __future__ import annotations

import logging
from functools import lru_cache
from pathlib import Path
from typing import Any, Dict, Optional

import pandas as pd

from app.data_analysis_v3.core.state_manager import (
    ConversationStage,
    DataAnalysisStateManager,
)
from app.data_analysis_v3.core.tpr_data_analyzer import TPRDataAnalyzer
from app.data_analysis_v3.core.tpr_workflow_handler import TPRWorkflowHandler
from app.runtime.upload_service import UploadService

logger = logging.getLogger(__name__)


@lru_cache(maxsize=128)
def get_tpr_workflow_handler(session_id: str) -> TPRWorkflowHandler:
    """Return a cached TPR workflow handler backed by the LangGraph implementation."""
    logger.info("Creating TPRWorkflowHandler via runtime wrapper for session %s", session_id)
    state_manager = DataAnalysisStateManager(session_id)
    analyzer = TPRDataAnalyzer()
    handler = TPRWorkflowHandler(session_id, state_manager, analyzer)
    try:
        handler.load_state_from_manager()
        _ensure_dataset_loaded(session_id, handler)
    except Exception as exc:  # pragma: no cover - defensive logging
        logger.warning("Failed to hydrate TPR handler state for %s: %s", session_id, exc)
    return handler


def initialize_tpr_session(
    session_id: str,
    dataframe: pd.DataFrame,
    metadata: Optional[Dict[str, Any]] = None,
    shapefile_saved: bool = False,
) -> None:
    """Persist dataset and prime the runtime handler/state manager for a fresh workflow."""
    upload_service = UploadService()
    session_dir = upload_service.session_dir(session_id)

    # Persist canonical CSV outputs used by both workflows
    raw_path = session_dir / "raw_data.csv"
    analysis_path = session_dir / "data_analysis.csv"
    dataframe.to_csv(raw_path, index=False)
    dataframe.to_csv(analysis_path, index=False)

    # Update metadata cache so exploratory prompts have schema context
    try:
        from app.data_analysis_v3.core.metadata_cache import MetadataCache

        MetadataCache.update_file_metadata(session_id, str(analysis_path), analysis_path.name)
    except Exception as exc:  # pragma: no cover - best effort
        logger.warning("Failed to update metadata cache for %s: %s", session_id, exc)

    handler = get_tpr_workflow_handler(session_id)
    handler.set_data(dataframe)
    handler.set_stage(ConversationStage.INITIAL)

    # Persist workflow state hints
    state_updates: Dict[str, Any] = {
        "tpr_workflow_active": True,
        "data_loaded": True,
        "csv_loaded": True,
        "tpr_metadata": metadata or {},
        "tpr_shapefile_present": shapefile_saved,
        "tpr_dataset_path": str(analysis_path),
    }
    handler.state_manager.update_state(state_updates)

    # Mirror critical flags into Flask session when available
    try:
        from flask import session

        upload_service.set_session_flags(
            session,
            session_id,
            context="upload_tpr_runtime",
            updates={
                "tpr_workflow_active": True,
                "csv_loaded": True,
                "data_loaded": True,
                "shapefile_loaded": shapefile_saved,
                "upload_type": "tpr_excel" if not shapefile_saved else "tpr_shapefile",
                "previous_workflow": "tpr",
            },
        )
    except RuntimeError:
        # Running outside request context – nothing to mirror
        pass


def start_tpr_workflow(session_id: str) -> Dict[str, Any]:
    handler = get_tpr_workflow_handler(session_id)
    _ensure_dataset_loaded(session_id, handler)
    return handler.start_workflow()


def process_tpr_message(session_id: str, message: str) -> Dict[str, Any]:
    handler = get_tpr_workflow_handler(session_id)
    _ensure_dataset_loaded(session_id, handler)
    result = handler.handle_workflow(message)
    return result or {
        "success": False,
        "message": "I could not map that response to the TPR workflow. You can continue the workflow or say 'exit TPR'.",
        "workflow": "tpr",
    }


def cancel_tpr_workflow(session_id: str) -> Dict[str, Any]:
    handler = get_tpr_workflow_handler(session_id)
    handler.state_manager.mark_tpr_workflow_complete()
    handler.state_manager.update_state({"tpr_selections": {}})
    reset_tpr_handler_cache(session_id)
    return {
        "status": "success",
        "message": "TPR workflow cancelled. You can restart anytime by uploading your TPR data again.",
    }


def get_tpr_status(session_id: str) -> Dict[str, Any]:
    state_manager = DataAnalysisStateManager(session_id)
    selections = state_manager.get_tpr_selections()
    stage = state_manager.get_workflow_stage()
    return {
        "active": state_manager.is_tpr_workflow_active(),
        "stage": stage.value if isinstance(stage, ConversationStage) else stage,
        "selections": selections,
        "metadata": state_manager.get_field("tpr_metadata", {}),
    }


def reset_tpr_handler_cache(session_id: Optional[str] = None) -> None:
    """Clear cached handlers (all or a specific session)."""
    if session_id is None:
        get_tpr_workflow_handler.cache_clear()
        logger.info("Cleared all cached TPR workflow handlers")
        return

    if get_tpr_workflow_handler.cache_info().currsize == 0:
        return
    get_tpr_workflow_handler.cache_clear()
    logger.info("Cleared cached TPR workflow handler for session %s", session_id)


def _ensure_dataset_loaded(session_id: str, handler: TPRWorkflowHandler) -> None:
    """Ensure the handler has an in-memory dataframe loaded."""
    if handler.uploaded_data is not None:
        return

    upload_service = UploadService()
    dataset_path = upload_service.locate_latest_dataset(session_id)
    if not dataset_path:
        candidate = Path(upload_service.base_dir) / session_id / "raw_data.csv"
        dataset_path = candidate if candidate.exists() else None

    if not dataset_path:
        logger.warning("TPR handler for %s has no dataset available", session_id)
        return

    try:
        df = pd.read_csv(dataset_path)
        handler.set_data(df)
        logger.info("Loaded dataset for TPR handler from %s", dataset_path)
    except Exception as exc:  # pragma: no cover - defensive logging
        logger.warning("Failed to read dataset for %s: %s", session_id, exc)


__all__ = [
    "get_tpr_workflow_handler",
    "initialize_tpr_session",
    "start_tpr_workflow",
    "process_tpr_message",
    "cancel_tpr_workflow",
    "get_tpr_status",
    "reset_tpr_handler_cache",
]
