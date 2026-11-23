"""Shared helpers for analysis routes."""

from __future__ import annotations

import asyncio

from pathlib import Path
from typing import Dict

from flask import current_app, session

from app.upload.upload_service import UploadService


STALE_DATA_FLAGS = ('data_loaded', 'csv_loaded', 'shapefile_loaded', 'analysis_complete')


def resync_session_flags(session_id: str) -> Dict[str, bool]:
    """Synchronise Flask session flags with on-disk uploads for the session."""

    upload_service = UploadService()
    session_path = Path(upload_service.base_dir) / session_id

    has_csv = False
    has_shp = False
    has_unified = False

    for entry in (session_path.rglob('*') if session_path.exists() else ()):
        if entry.is_dir() or entry.name.startswith('.'):  # skip directories/hidden files
            continue
        suffix = entry.suffix.lower()
        if suffix in {'.csv', '.xlsx', '.xls'}:
            has_csv = True
            if entry.name.lower().startswith('unified_dataset'):
                has_unified = True
        elif suffix == '.geoparquet' and 'unified_dataset' in entry.name.lower():
            has_unified = True
        elif suffix in {'.zip', '.shp'}:
            has_shp = True

    updates: Dict[str, bool] = {}
    if has_csv:
        updates['csv_loaded'] = True
    if has_shp:
        updates['shapefile_loaded'] = True
    if has_unified:
        updates['analysis_complete'] = True
    if has_csv or has_shp or has_unified:
        updates['data_loaded'] = True

    if not updates:
        # No data for this session on disk (e.g. a new chat opened after an
        # upload in another chat): clear flags left over from the old chat so
        # messages are not routed to the data agent.
        updates = {flag: False for flag in STALE_DATA_FLAGS if session.get(flag)}

    if updates:
        upload_service.set_session_flags(
            session,
            session_id,
            context='analysis_resync',
            updates=updates,
        )
    else:
        session.modified = True

    return updates



def run_async(coro):
    """Execute an async coroutine from sync Flask handlers."""
    try:
        return asyncio.run(coro)
    except RuntimeError:
        loop = asyncio.new_event_loop()
        try:
            return loop.run_until_complete(coro)
        finally:
            loop.close()

