"""Debug endpoint to check session state."""
from flask import Blueprint, jsonify, session
import logging

debug_bp = Blueprint('debug', __name__, url_prefix='/debug')
logger = logging.getLogger(__name__)

@debug_bp.route('/session-state', methods=['GET'])
def get_session_state():
    """Return current session state for debugging."""
    return jsonify({
        'session_id': session.get('session_id'),
        'tpr_workflow_active': session.get('tpr_workflow_active'),
        'tpr_session_id': session.get('tpr_session_id'),
        'should_ask_analysis_permission': session.get('should_ask_analysis_permission'),
        'data_loaded': session.get('data_loaded'),
        'csv_loaded': session.get('csv_loaded'),
        'shapefile_loaded': session.get('shapefile_loaded'),
        'risk_workflow_active': session.get('risk_workflow_active'),
        'tpr_transition_complete': session.get('tpr_transition_complete')
    })
