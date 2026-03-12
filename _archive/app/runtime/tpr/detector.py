"""TPR upload detection utilities that operate independently of the legacy module."""

from __future__ import annotations

import logging
import os
import tempfile
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, Any, Optional, Tuple

import pandas as pd
from werkzeug.datastructures import FileStorage

from app.runtime.tpr.utils import is_tpr_data, validate_tpr_data

logger = logging.getLogger(__name__)


@dataclass
class TPRDetectionResult:
    upload_type: str
    detection_info: Dict[str, Any]


class TPRUploadDetector:
    """Lightweight detector for NMEP-style TPR Excel uploads."""

    def __init__(self) -> None:
        self._sample_rows = 500

    def detect(self, file: Optional[FileStorage], has_shapefile: bool = False) -> TPRDetectionResult:
        """Inspect the uploaded Excel file and determine if it matches TPR structure."""
        detection_info: Dict[str, Any] = {
            "is_tpr": False,
            "file_type": None,
            "has_shapefile": has_shapefile,
            "metadata": {},
            "validation": {"valid": False, "issues": []},
        }

        if not file or not getattr(file, "filename", ""):
            return TPRDetectionResult("standard", detection_info)

        filename = file.filename.lower()
        if not filename.endswith((".xlsx", ".xls")):
            return TPRDetectionResult("standard", detection_info)

        try:
            preview_df = self._load_preview_dataframe(file)
            if preview_df is None or preview_df.empty:
                logger.info("TPR detector: preview dataframe empty")
                return TPRDetectionResult("standard", detection_info)

            is_tpr, info = is_tpr_data(preview_df)
            detection_info["metadata"].update(self._extract_metadata(preview_df))
            detection_info["metadata"].update(info)

            if not is_tpr:
                logger.info("TPR detector: heuristics indicate non-TPR dataset")
                return TPRDetectionResult("standard", detection_info)

            valid, issues = validate_tpr_data(preview_df)
            detection_info["is_tpr"] = True
            detection_info["file_type"] = "nmep_excel"
            detection_info["validation"] = {"valid": valid, "issues": issues}

            upload_type = "tpr_shapefile" if has_shapefile else "tpr_excel"
            return TPRDetectionResult(upload_type, detection_info)

        except Exception as exc:  # pragma: no cover - defensive logging
            logger.warning("TPR detector failed: %s", exc)
            return TPRDetectionResult("standard", detection_info)
        finally:
            try:
                file.stream.seek(0)
            except Exception:
                pass

    # ------------------------------------------------------------------
    def _load_preview_dataframe(self, file: FileStorage) -> Optional[pd.DataFrame]:
        """Read the first few rows of the Excel file into a dataframe."""
        tmp_fd: Optional[tempfile.NamedTemporaryFile] = None
        try:
            file.stream.seek(0)
            tmp_fd = tempfile.NamedTemporaryFile(suffix=os.path.splitext(file.filename)[1], delete=False)
            tmp_fd.write(file.stream.read())
            tmp_fd.flush()
            df = pd.read_excel(tmp_fd.name, nrows=self._sample_rows)
            return df
        finally:
            try:
                file.stream.seek(0)
            except Exception:
                pass
            if tmp_fd is not None:
                try:
                    tmp_fd.close()
                    os.unlink(tmp_fd.name)
                except Exception:
                    pass

    def _extract_metadata(self, df: pd.DataFrame) -> Dict[str, Any]:
        """Collect basic metadata (states, years, months) from the preview dataframe."""
        metadata: Dict[str, Any] = {}

        # States
        state_col = next((col for col in df.columns if str(col).lower().strip().startswith("state")), None)
        if state_col:
            states = (
                df[state_col]
                .dropna()
                .astype(str)
                .str.replace(" State", "", case=False)
                .str.strip()
                .unique()
                .tolist()
            )
            metadata["states_available"] = states[:50]  # avoid overly long lists

        # Year / Month detection
        for col in df.columns:
            col_lower = str(col).lower()
            if "year" in col_lower and "year" not in metadata:
                metadata["year"] = self._first_non_null(df[col])
            if any(token in col_lower for token in ["month", "period"]):
                metadata["month"] = self._first_non_null(df[col])

        # Row estimate
        metadata["rows_sampled"] = len(df)
        metadata["columns"] = len(df.columns)
        return metadata

    @staticmethod
    def _first_non_null(series: pd.Series) -> Optional[str]:
        value = series.dropna().astype(str).head(1)
        return value.iloc[0] if not value.empty else None


__all__ = ["TPRUploadDetector", "TPRDetectionResult"]
