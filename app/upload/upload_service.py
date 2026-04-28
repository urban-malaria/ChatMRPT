"""Utilities to persist user uploads and keep session state in sync."""

from __future__ import annotations

import logging
import shutil
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, Optional, Any, Set

from flask import current_app
from werkzeug.datastructures import FileStorage
from werkzeug.utils import secure_filename

from app.agent.metadata_cache import MetadataCache

logger = logging.getLogger(__name__)

_ALLOWED_DEFAULT_EXTENSIONS: Set[str] = {".csv", ".xlsx", ".xls", ".json", ".txt"}


@dataclass
class UploadResult:
    """Outcome of saving an uploaded file."""

    session_id: str
    original_filename: str
    saved_path: Path
    alias_filename: Optional[str] = None
    alias_path: Optional[Path] = None
    metadata: Optional[Dict[str, Any]] = None
    alias_metadata: Optional[Dict[str, Any]] = None

    def to_dict(self) -> Dict[str, Any]:
        return {
            "session_id": self.session_id,
            "original_filename": self.original_filename,
            "saved_path": str(self.saved_path),
            "alias_filename": self.alias_filename,
            "alias_path": str(self.alias_path) if self.alias_path else None,
            "metadata": self.metadata,
            "alias_metadata": self.alias_metadata,
        }


class UploadService:
    """Centralised helper for storing uploads and setting workflow markers."""

    def __init__(self, base_upload_dir: Optional[str] = None) -> None:
        base_path = base_upload_dir or current_app.config.get("UPLOAD_FOLDER", "instance/uploads")
        self.base_dir = Path(base_path)
        self.base_dir.mkdir(parents=True, exist_ok=True)

    @staticmethod
    def is_extension_allowed(filename: str, allowed_extensions: Optional[Set[str]] = None) -> bool:
        if not filename or "." not in filename:
            return False
        allowed = allowed_extensions or _ALLOWED_DEFAULT_EXTENSIONS
        extension = Path(filename).suffix.lower()
        return bool(extension) and extension in allowed

    @staticmethod
    def canonical_name(filename: str, base: str) -> str:
        suffix = Path(filename).suffix.lower()
        if suffix not in _ALLOWED_DEFAULT_EXTENSIONS:
            suffix = ".txt"
        return f"{base}{suffix}"

    def session_dir(self, session_id: str) -> Path:
        path = self.base_dir / session_id
        path.mkdir(parents=True, exist_ok=True)
        return path

    def save_file(
        self,
        session_id: str,
        file_storage: FileStorage,
        *,
        canonical_basename: Optional[str] = None,
        update_metadata: bool = True,
    ) -> UploadResult:
        if not file_storage or not getattr(file_storage, "filename", ""):
            raise ValueError("No file provided for upload")

        safe_name = secure_filename(file_storage.filename)
        if not safe_name:
            raise ValueError("Filename could not be sanitised")

        session_path = self.session_dir(session_id)
        original_path = session_path / safe_name
        file_storage.save(original_path)

        metadata = None
        alias_metadata = None
        alias_path = None
        alias_filename = None

        if update_metadata:
            try:
                metadata = MetadataCache.update_file_metadata(session_id, str(original_path), safe_name)
            except Exception as exc:  # pragma: no cover - metadata best effort
                logger.warning("Metadata extraction failed for %s/%s: %s", session_id, safe_name, exc)

        if canonical_basename:
            alias_filename = canonical_basename
            alias_path = session_path / canonical_basename
            if alias_path != original_path:
                shutil.copy2(original_path, alias_path)
                if update_metadata:
                    try:
                        alias_metadata = MetadataCache.update_file_metadata(
                            session_id, str(alias_path), canonical_basename
                        )
                    except Exception as exc:  # pragma: no cover - metadata best effort
                        logger.warning(
                            "Metadata extraction failed for alias %s/%s: %s",
                            session_id,
                            canonical_basename,
                            exc,
                        )
            else:
                alias_metadata = metadata

        logger.info(
            "Upload saved for session %s | original=%s | alias=%s",
            session_id,
            safe_name,
            alias_filename or "-",
        )

        return UploadResult(
            session_id=session_id,
            original_filename=safe_name,
            saved_path=original_path,
            alias_filename=alias_filename,
            alias_path=alias_path,
            metadata=metadata,
            alias_metadata=alias_metadata,
        )

    def set_session_flags(
        self,
        session_store: Any,
        session_id: str,
        *,
        context: str,
        updates: Dict[str, Any],
    ) -> None:
        changes: Dict[str, Dict[str, Any]] = {}
        for key, new_value in updates.items():
            old_value = session_store.get(key)
            if old_value != new_value:
                changes[key] = {"old": old_value, "new": new_value}
            session_store[key] = new_value
        if hasattr(session_store, 'modified'):
            session_store.modified = True

        if changes:
            logger.info(
                "Session %s flags updated (%s): %s",
                session_id,
                context,
                {k: v for k, v in changes.items()},
            )

    def write_marker(self, session_id: str, marker_name: str, contents: str) -> Path:
        marker_path = self.session_dir(session_id) / marker_name
        marker_path.write_text(contents, encoding="utf-8")
        logger.debug("Marker %s created for session %s", marker_name, session_id)
        return marker_path

    def locate_latest_dataset(self, session_id: str) -> Optional[Path]:
        session_path = self.session_dir(session_id)
        candidates = sorted(
            session_path.glob("data_analysis.*"),
            key=lambda p: p.stat().st_mtime,
            reverse=True,
        )
        return candidates[0] if candidates else None
