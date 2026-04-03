"""
SessionContextService: Builds a canonical session context dictionary.

Responsibilities moved out of RequestInterpreter:
- Read Flask session data safely (when available)
- Sync flags from agent state file if present
- Check filesystem for uploaded data and analysis outputs
- Provide lightweight column/schema hints for prompt building
"""

from __future__ import annotations

import json
import os
from dataclasses import dataclass, asdict
from pathlib import Path
from typing import Any, Dict, Optional, List, Tuple

import pandas as pd

from .data_repository import DataRepository


@dataclass
class SessionContext:
    session_id: str
    data_loaded: bool = False
    csv_loaded: bool = False
    shapefile_loaded: bool = False
    analysis_complete: bool = False
    columns: List[str] = None
    ward_column: str = "WardName"
    current_data: str = "No data uploaded"
    variables_used: List[str] = None
    state_name: str = ""
    data_schema: Optional[str] = None

    def to_dict(self) -> Dict[str, Any]:
        d = asdict(self)
        # Avoid serializing huge lists into prompts
        if d.get("columns") and len(d["columns"]) > 100:
            d["columns"] = d["columns"][:100]
        if d.get("variables_used") and len(d["variables_used"]) > 50:
            d["variables_used"] = d["variables_used"][0:50]
        return d


class SessionContextService:
    def __init__(self, data_repo: Optional[DataRepository] = None) -> None:
        self.data_repo = data_repo or DataRepository()

    def _sync_from_agent_state(self, session_id: str, session_data: Dict[str, Any]) -> Dict[str, Any]:
        """Sync flags from agent state file when available."""
        upload_folder = os.environ.get("UPLOAD_FOLDER", "instance/uploads")
        folder = Path(upload_folder) / session_id
        agent_state_file = folder / ".agent_state.json"
        if not agent_state_file.exists():
            return session_data

        try:
            with open(agent_state_file, "r") as f:
                agent_state = json.load(f)
            # Trust agent state flags when files exist
            has_csv = (folder / "raw_data.csv").exists() or (folder / "data_analysis.csv").exists()
            has_shp = (folder / "raw_shapefile.zip").exists()
            if (agent_state.get("data_loaded") or agent_state.get("csv_loaded")) and has_csv:
                session_data = dict(session_data) if session_data else {}
                session_data.update({
                    "data_loaded": True,
                    "csv_loaded": True,
                    "shapefile_loaded": has_shp,
                })
        except Exception:
            # Be tolerant of bad files
            return session_data
        return session_data

    def get_context(self, session_id: str, session_data: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        # Accept external session_data, then enrich from agent state
        session_data = dict(session_data) if session_data else {}
        session_data = self._sync_from_agent_state(session_id, session_data)

        # Detect data availability
        has_any = self.data_repo.has_any_data(session_id)
        raw_df = self.data_repo.load_raw(session_id)
        unified_df = self.data_repo.load_unified(session_id)

        columns: List[str] = []
        if unified_df is not None:
            columns = list(unified_df.columns)
        elif raw_df is not None:
            columns = list(raw_df.columns)

        # Infer analysis_complete
        analysis_complete = bool(unified_df is not None)

        schema_summary = None
        schema_columns: List[Dict[str, Any]] = []
        reference_df = unified_df if unified_df is not None else raw_df

        if reference_df is not None and columns:
            schema_summary, schema_columns = self._build_schema_profile(reference_df)
            # Trim to avoid excessive payloads
            if schema_summary and len(schema_summary) > 1500:
                schema_summary = schema_summary[:1500]
            if len(schema_columns) > 80:
                schema_columns = schema_columns[:80]

            # Persist schema details for downstream memory-aware prompts
            try:
                from app.services.memory_service import get_memory_service
                mem = get_memory_service()
                if schema_summary:
                    mem.set_fact(session_id, 'dataset_schema_summary', schema_summary)
                if schema_columns:
                    mem.set_fact(session_id, 'dataset_schema_columns', schema_columns)
            except Exception:
                pass

        context = SessionContext(
            session_id=session_id,
            data_loaded=bool(session_data.get("data_loaded") or session_data.get("csv_loaded") or has_any),
            csv_loaded=bool(session_data.get("csv_loaded") or (raw_df is not None)),
            shapefile_loaded=bool(session_data.get("shapefile_loaded", False)),
            analysis_complete=analysis_complete or bool(session_data.get("analysis_complete")),
            columns=columns,
            ward_column=session_data.get("ward_column", "WardName"),
            current_data=f"{len(unified_df) if unified_df is not None else (len(raw_df) if raw_df is not None else 0)} rows",
            variables_used=session_data.get("variables_used", []) or [],
            state_name=session_data.get("state_name", ""),
            data_schema=session_data.get("data_schema")
        )

        context_dict = context.to_dict()
        if schema_summary:
            context_dict['schema_summary'] = schema_summary
        if schema_columns:
            context_dict['schema_columns'] = schema_columns

        return context_dict

    def _build_schema_profile(self, df: pd.DataFrame, max_columns: int = 80) -> Tuple[str, List[Dict[str, Any]]]:
        summary_lines: List[str] = []
        column_details: List[Dict[str, Any]] = []

        if df is None:
            return "", []

        sample = df.head(5)

        for idx, column in enumerate(df.columns):
            if idx >= max_columns:
                break
            series = df[column]
            dtype = str(series.dtype)
            non_null = int(series.notna().sum())
            unique = int(series.nunique(dropna=True))
            sample_values = [str(val)[:60] for val in sample[column].dropna().head(3).tolist()]
            example = ", ".join(sample_values) if sample_values else "(no sample)"
            summary_lines.append(f"- {column} [{dtype}] • non-null: {non_null} • unique: {unique} • sample: {example}")
            column_details.append({
                'name': str(column),
                'dtype': dtype,
                'non_null': non_null,
                'unique': unique,
                'sample_values': sample_values
            })

        summary_text = "\n".join(summary_lines)
        return summary_text, column_details
