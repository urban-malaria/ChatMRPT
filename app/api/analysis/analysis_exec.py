"""Execution endpoints for standard risk analysis."""

from __future__ import annotations

import logging
from typing import Optional

from flask import jsonify, request, session

from app.auth.decorators import require_auth
from app.analysis.engine import AnalysisEngine
from app.utils.decorators import handle_errors, log_execution_time, validate_session
from app.utils.exceptions import ValidationError
from app.services.standard_workflow import SessionDataMissing, get_data_handler

from . import analysis_bp, logger
from .utils import resync_session_flags



def _extract_ranked_wards(data_handler, result) -> tuple[list[str], list[str], list[str]]:
    high: list[str] = []
    medium: list[str] = []
    low: list[str] = []

    rankings = getattr(data_handler, 'vulnerability_rankings', None)
    if rankings is not None and 'vulnerability_category' in rankings.columns and 'WardName' in rankings.columns:
        high = rankings[rankings['vulnerability_category'] == 'High']['WardName'].tolist()
        medium = rankings[rankings['vulnerability_category'] == 'Medium']['WardName'].tolist()
        low = rankings[rankings['vulnerability_category'] == 'Low']['WardName'].tolist()

    return (
        high or result.get('high_risk_wards', []),
        medium or result.get('medium_risk_wards', []),
        low or result.get('low_risk_wards', []),
    )


@analysis_bp.route('/run_analysis', methods=['POST'])
@require_auth
@validate_session
@handle_errors
@log_execution_time
def run_analysis():
    """Execute the standard composite analysis for the current session."""
    session_id = session.get('session_id', 'default')
    logger.info("[DEBUG] run_analysis: session_id=%s", session_id)

    try:
        data_handler = get_data_handler(session_id)
        logger.info("[DEBUG] run_analysis: data handler loaded successfully")
    except SessionDataMissing as exc:
        raise ValidationError(str(exc))

    # Rescan upload folder to mirror actual file state into session flags
    try:
        resync_session_flags(session_id)
    except Exception as exc:  # pragma: no cover - best effort
        logger.warning("[DEBUG] run_analysis: rescan flags failed: %s", exc)

    if not session.get('csv_loaded', False) or not session.get('shapefile_loaded', False):
        raise ValidationError('Please upload both CSV and shapefile data before running analysis')

    payload = request.json or {}
    selected_variables: Optional[list[str]] = payload.get('selected_variables')

    engine = AnalysisEngine(data_handler)
    if selected_variables:
        result = engine.run_custom_analysis(
            data_handler=data_handler,
            selected_variables=selected_variables,
            session_id=session_id,
        )
    else:
        result = engine.run_standard_analysis(
            data_handler=data_handler,
            session_id=session_id,
        )

    if result.get('status') != 'success':
        return jsonify({
            'status': 'error',
            'message': result.get('message', 'Error running analysis'),
        }), 400

    session['analysis_complete'] = True
    session['variables_used'] = result.get('variables_used', [])
    session.modified = True

    high, medium, low = _extract_ranked_wards(data_handler, result)

    return jsonify({
        'status': 'success',
        'message': 'Analysis completed successfully',
        'variables_used': result.get('variables_used', []),
        'high_risk_wards': high[:10],
        'medium_risk_wards': medium[:10],
        'low_risk_wards': low[:10],
    })


@analysis_bp.route('/explain_variable_selection', methods=['GET'])
@require_auth
@validate_session
@handle_errors
@log_execution_time
def explain_variable_selection():
    """Explain why particular variables were chosen for the analysis."""
    session_id = session.get('session_id')
    if not session_id:
        raise ValidationError('No active session found')

    try:
        data_handler = get_data_handler(session_id)
    except SessionDataMissing as exc:
        raise ValidationError(str(exc))

    if not session.get('analysis_complete', False) or not session.get('variables_used'):
        raise ValidationError('Analysis not yet performed')

    variables = session.get('variables_used', [])
    if not variables:
        raise ValidationError('No variables found from analysis')

    engine = AnalysisEngine(data_handler)
    result = engine.explain_variable_selection(variables=variables, data_handler=data_handler)

    if result.get('status') != 'success':
        return jsonify({
            'status': 'error',
            'message': result.get('message', 'Error generating explanation'),
        }), 400

    return jsonify({
        'status': 'success',
        'message': 'Generated variable selection explanation',
        'explanation': result.get('explanations', {}),
        'variables': variables,
    })
