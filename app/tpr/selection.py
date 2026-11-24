"""The facility level and age group the session's results are based on.

The guided workflow and switch_tpr_combination record the current selection. The risk analysis
records which selection it used, so ranking and bed-net answers can say when they are out of date.
"""
from __future__ import annotations

import json
import logging
import os
from typing import Dict, Optional

logger = logging.getLogger(__name__)

FACILITY_LABELS = {'primary': 'primary facilities', 'secondary': 'secondary facilities',
                   'tertiary': 'tertiary facilities', 'all': 'all facilities'}
AGE_LABELS = {'u5': 'under 5s', 'o5': 'over 5s', 'pw': 'pregnant women', 'all_ages': 'all ages', 'all': 'all ages'}


def _status_path(session_id: str) -> str:
    return os.path.join('instance', 'uploads', session_id, 'selection_status.json')


def current_selection(session_id: str) -> Dict[str, Optional[str]]:
    """The selection recorded by the workflow or the last switch."""
    try:
        from app.agent.state_manager import DataAnalysisStateManager
        selections = DataAnalysisStateManager(session_id).get_tpr_selections() or {}
    except Exception:
        selections = {}
    age = selections.get('age_group')
    return {'facility_level': selections.get('facility_level'),
            'age_group': 'all_ages' if age == 'all' else age}


def save_selection(session_id: str, facility_level: str, age_group: str) -> None:
    try:
        from app.agent.state_manager import DataAnalysisStateManager
        manager = DataAnalysisStateManager(session_id)
        manager.save_tpr_selection('facility_level', facility_level)
        manager.save_tpr_selection('age_group', age_group)
    except Exception as exc:
        logger.warning("Could not save selection for %s: %s", session_id, exc)


def describe(selection: Dict[str, Optional[str]]) -> str:
    """'primary facilities, under 5s'."""
    facility = FACILITY_LABELS.get(selection.get('facility_level') or '', selection.get('facility_level') or 'all facilities')
    age = AGE_LABELS.get(selection.get('age_group') or '', selection.get('age_group') or 'all ages')
    return f"{facility}, {age}"


def record_risk_selection(session_id: str) -> None:
    """Remember which selection the risk analysis (and anything built on it) used."""
    try:
        with open(_status_path(session_id), 'w') as handle:
            json.dump({'risk_analysis_for': current_selection(session_id)}, handle)
    except Exception as exc:
        logger.debug("Could not record risk selection: %s", exc)


def stale_note(session_id: str) -> str:
    """A warning when the risk results were made for a different selection than the current one."""
    try:
        with open(_status_path(session_id)) as handle:
            used = json.load(handle).get('risk_analysis_for') or {}
    except Exception:
        return ''
    current = current_selection(session_id)
    if not used or not current.get('facility_level') or used == current:
        return ''
    return (f"**Note:** the risk ranking and any bed-net plan were made for {describe(used)}; the current "
            f"selection is {describe(current)}. Type **run malaria risk analysis** to update them.\n\n")
