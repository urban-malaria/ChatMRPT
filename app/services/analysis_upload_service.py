"""Shared upload processing for Data Analysis V3.

This module owns file/artifact/state creation for data-analysis uploads. It does
not read or write Flask's browser session; routes remain responsible for UI
session flags and response formatting.
"""

from __future__ import annotations

import json
import logging
import os
import shutil
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any

from werkzeug.datastructures import FileStorage
from werkzeug.utils import secure_filename

from app.agent.metadata_cache import MetadataCache

logger = logging.getLogger(__name__)

ALLOWED_ANALYSIS_EXTENSIONS = {".csv", ".xlsx", ".xls", ".json", ".txt"}


@dataclass
class AnalysisUploadResult:
    session_id: str
    original_filename: str
    saved_path: Path
    standard_path: Path
    uploaded_csv_path: Path | None
    rows: int | str
    cols: int | str
    detected_type: str
    key_columns: list[str]
    column_schema: dict[str, Any]
    cleaning_report: Any | None
    metadata: dict[str, Any] | None
    file_size: int
    error: str | None = None


def is_allowed_analysis_file(filename: str) -> bool:
    """Return True when the filename has an upload extension supported by the UI."""
    return bool(filename and Path(filename).suffix.lower() in ALLOWED_ANALYSIS_EXTENSIONS)


def _standard_filename_for(safe_name: str) -> str:
    ext = Path(safe_name).suffix.lower()
    if ext == ".csv":
        return "data_analysis.csv"
    if ext in {".xlsx", ".xls"}:
        return "data_analysis.xlsx"
    if ext == ".json":
        return "data_analysis.json"
    return "data_analysis.txt"


def _save_file_obj(file_obj: Any, destination: Path) -> None:
    """Persist a FileStorage, bytes object, or readable file-like object."""
    if isinstance(file_obj, FileStorage) or hasattr(file_obj, "save"):
        file_obj.save(str(destination))
        return

    if isinstance(file_obj, (bytes, bytearray)):
        destination.write_bytes(bytes(file_obj))
        return

    if not hasattr(file_obj, "read"):
        raise ValueError("Unsupported upload object; expected FileStorage, bytes, or file-like object")

    with destination.open("wb") as out:
        while True:
            chunk = file_obj.read(1024 * 1024)
            if not chunk:
                break
            if isinstance(chunk, str):
                chunk = chunk.encode("utf-8")
            out.write(chunk)


def _clean_dataframe(df, schema: dict[str, Any], upload_dir: Path):
    """Apply DHIS2 cleaner and persist a cleaning report when configured."""
    cleaning_report = None
    try:
        from app.utils.dhis2_cleaner import (
            apply_rename_map_to_schema,
            clean_dhis2_export,
            get_cleaner_mode,
        )

        cleaner_mode = get_cleaner_mode()
        if cleaner_mode != "off":
            df, cleaning_report = clean_dhis2_export(df, mode=cleaner_mode)
            if getattr(cleaning_report, "column_rename_map", None):
                schema = apply_rename_map_to_schema(schema, cleaning_report.column_rename_map)
                logger.info(
                    "[DHIS2_CLEANER] Updated schema for %d renamed columns",
                    len(cleaning_report.column_rename_map),
                )

            report_path = upload_dir / "cleaning_report.json"
            report_path.write_text(
                json.dumps(cleaning_report.to_dict(), indent=2, default=str),
                encoding="utf-8",
            )

            if getattr(cleaning_report, "cleaning_applied", False):
                logger.info(
                    "[DHIS2_CLEANER] mode=%s detected=%s duplicates=%d mojibake=%d warnings=%d",
                    cleaner_mode,
                    cleaning_report.detected_as,
                    len(cleaning_report.duplicates_merged),
                    len(cleaning_report.mojibake_fixed),
                    len(cleaning_report.data_quality_warnings),
                )
    except Exception as exc:
        logger.exception("[DHIS2_CLEANER] Failed (using uncleaned data): %s", exc)

    return df, schema, cleaning_report


def _load_normalized_dataframe(path: Path, schema: dict[str, Any]):
    """Infer/read an upload into a DataFrame suitable for uploaded_data.csv."""
    ext = path.suffix.lower()

    try:
        from app.tpr.data_analyzer import TPRDataAnalyzer

        analyzer = TPRDataAnalyzer()
        return analyzer.infer_schema_from_file(str(path))
    except Exception as exc:
        logger.warning("Schema inference failed at upload (falling back to header=0): %s", exc)

    from app.agent.encoding_handler import EncodingHandler

    if ext in {".xlsx", ".xls"}:
        df = EncodingHandler.read_excel_with_encoding(str(path), header=0)
    elif ext == ".csv":
        df = EncodingHandler.read_csv_with_encoding(str(path))
    else:
        raise ValueError(f"Cannot normalize unsupported file type: {ext}")

    return df, schema


def process_analysis_upload(
    *,
    session_id: str,
    file_obj: Any,
    original_filename: str,
    upload_root: str,
) -> AnalysisUploadResult:
    """Create the same Data Analysis V3 upload artifacts/state as the web route."""
    safe_name = secure_filename(original_filename or "")
    if not safe_name:
        raise ValueError("Filename could not be sanitised")
    if not is_allowed_analysis_file(safe_name):
        allowed = ", ".join(sorted(ext.lstrip(".") for ext in ALLOWED_ANALYSIS_EXTENSIONS))
        raise ValueError(f"Invalid file type. Allowed types: {allowed}")

    upload_dir = Path(upload_root) / session_id
    upload_dir.mkdir(parents=True, exist_ok=True)

    saved_path = upload_dir / safe_name
    _save_file_obj(file_obj, saved_path)

    standard_path = upload_dir / _standard_filename_for(safe_name)
    if standard_path != saved_path:
        shutil.copy2(saved_path, standard_path)

    uploaded_csv_path: Path | None = upload_dir / "uploaded_data.csv"
    schema_at_upload: dict[str, Any] = {"header_row": 0}
    cleaning_report = None
    rows: int | str = "Unknown"
    cols: int | str = "Unknown"
    key_columns: list[str] = []

    if saved_path.suffix.lower() in {".csv", ".xlsx", ".xls"}:
        try:
            df_upload, schema_at_upload = _load_normalized_dataframe(saved_path, schema_at_upload)
            df_upload, schema_at_upload, cleaning_report = _clean_dataframe(
                df_upload, schema_at_upload, upload_dir
            )
            df_upload.to_csv(uploaded_csv_path, index=False)
            rows = int(df_upload.shape[0])
            cols = int(df_upload.shape[1])
            key_columns = [str(col) for col in list(df_upload.columns[:5])]
            logger.info(
                "Schema inferred at upload: header_row=%s shape=%s columns=%s",
                schema_at_upload.get("header_row"),
                df_upload.shape,
                list(df_upload.columns[:5]),
            )
        except Exception as exc:
            logger.warning("Could not save uploaded_data.csv at upload time: %s", exc)
            uploaded_csv_path = None
    else:
        uploaded_csv_path = None

    logger.info("Extracting metadata for %s", safe_name)
    metadata = MetadataCache.update_file_metadata(session_id, str(saved_path), safe_name)

    if standard_path != saved_path:
        MetadataCache.update_file_metadata(session_id, str(standard_path), standard_path.name)

    if uploaded_csv_path and uploaded_csv_path.exists():
        try:
            MetadataCache.update_file_metadata(session_id, str(uploaded_csv_path), "uploaded_data.csv")
            logger.info("Profiled uploaded_data.csv for session %s", session_id)
        except Exception as exc:
            logger.warning("Could not profile uploaded_data.csv: %s", exc)

    flag_file = upload_dir / ".data_analysis_mode"
    flag_file.write_text(f"{safe_name}\n{datetime.now().isoformat()}", encoding="utf-8")

    from app.conversation.workflow_state import WorkflowSource, WorkflowStage, WorkflowStateManager

    workflow_manager = WorkflowStateManager(session_id)
    workflow_manager.transition_workflow(
        from_source=WorkflowSource.STANDARD,
        to_source=WorkflowSource.DATA_ANALYSIS_V3,
        new_stage=WorkflowStage.UPLOADED,
        clear_markers=[".analysis_complete"],
    )
    logger.info("Transitioned to Data Analysis V3 workflow for session %s", session_id)

    from app.agent.state_manager import DataAnalysisStateManager

    DataAnalysisStateManager(session_id).update_state(
        {
            "workflow_transitioned": False,
            "tpr_completed": False,
            "column_schema": schema_at_upload,
        }
    )
    logger.info(
        "Cleared DataAnalysisStateManager flags, saved schema (header_row=%s) for session %s",
        schema_at_upload.get("header_row"),
        session_id,
    )

    detected_type = "tabular data"
    if cleaning_report and getattr(cleaning_report, "detected_as", None):
        detected_type = cleaning_report.detected_as
    elif metadata:
        detected_type = str(metadata.get("type") or detected_type)

    if rows == "Unknown" and metadata:
        rows = metadata.get("rows", rows)
    if cols == "Unknown" and metadata:
        cols = metadata.get("columns", cols)

    return AnalysisUploadResult(
        session_id=session_id,
        original_filename=safe_name,
        saved_path=saved_path,
        standard_path=standard_path,
        uploaded_csv_path=uploaded_csv_path if uploaded_csv_path and uploaded_csv_path.exists() else None,
        rows=rows,
        cols=cols,
        detected_type=detected_type,
        key_columns=key_columns,
        column_schema=schema_at_upload,
        cleaning_report=cleaning_report,
        metadata=metadata,
        file_size=os.path.getsize(saved_path),
    )
