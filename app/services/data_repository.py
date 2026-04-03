"""
DataRepository: Centralized file I/O for session datasets.

- Avoids direct filesystem access in higher-level components
- Provides convenience helpers for raw/unified dataset loading
"""

from __future__ import annotations

import os
from pathlib import Path
from typing import Optional, Tuple, List

import pandas as pd


class DataRepository:
    """Repository for reading session-scoped data files."""

    def __init__(self, base_upload_folder: Optional[str] = None) -> None:
        self.base_upload_folder = base_upload_folder or os.environ.get(
            "UPLOAD_FOLDER", "instance/uploads"
        )

    def session_folder(self, session_id: str) -> Path:
        return Path(self.base_upload_folder) / session_id

    def exists(self, session_id: str, filename: str) -> bool:
        return (self.session_folder(session_id) / filename).exists()

    def list_data_files(self, session_id: str) -> List[str]:
        folder = self.session_folder(session_id)
        if not folder.exists():
            return []
        return [p.name for p in folder.iterdir() if p.is_file()]

    def load_raw(self, session_id: str) -> Optional[pd.DataFrame]:
        """Load the raw CSV uploaded for this session if present."""
        folder = self.session_folder(session_id)
        raw_csv = folder / "raw_data.csv"
        if raw_csv.exists():
            try:
                return pd.read_csv(raw_csv)
            except Exception:
                return None
        # Common alternate names used in the app
        alt = folder / "data_analysis.csv"
        if alt.exists():
            try:
                return pd.read_csv(alt)
            except Exception:
                return None
        return None

    def load_unified(self, session_id: str) -> Optional[pd.DataFrame]:
        """Load the unified dataset produced by analysis if present."""
        folder = self.session_folder(session_id)
        path = folder / "unified_dataset.csv"
        if path.exists():
            try:
                return pd.read_csv(path)
            except Exception:
                return None
        return None

    def has_any_data(self, session_id: str) -> bool:
        folder = self.session_folder(session_id)
        if not folder.exists():
            return False
        for ext in (".csv", ".xlsx", ".xls", ".json"):
            if any(p.suffix.lower() == ext for p in folder.iterdir() if p.is_file()):
                return True
        return False

