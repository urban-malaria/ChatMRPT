# app/web/routes/debug_routes.py
"""
Debug routes for session state checking.
"""

from flask import Blueprint, jsonify, session
import os
import json
import logging

logger = logging.getLogger(__name__)

debug_bp = Blueprint('debug', __name__)

@debug_bp.route('/debug/session_state', methods=['GET'])
def get_session_state():
    """Get current session state for debugging and report generation."""
    try:
        session_id = session.get('session_id')
        
        # Basic session state
        session_state = {
            'session_id': session_id,
            'csv_loaded': session.get('csv_loaded', False),
            'shapefile_loaded': session.get('shapefile_loaded', False),
            'analysis_complete': session.get('analysis_complete', False),
            'variables_used': session.get('variables_used', []),
            'data_loaded': session.get('data_loaded', False),
            'tpr_complete': session.get('tpr_complete', False),
            'itn_planning_complete': False
        }
        
        # CRITICAL FIX: Check if analysis files exist as fallback
        if session_id and not session_state['analysis_complete']:
            analysis_files = [
                f"instance/uploads/{session_id}/analysis_composite_scores.csv",
                f"instance/uploads/{session_id}/analysis_pca_scores.csv",
                f"instance/uploads/{session_id}/analysis_vulnerability_rankings.csv"
            ]
            # If any analysis result files exist, mark analysis as complete
            if any(os.path.exists(f) for f in analysis_files):
                session_state['analysis_complete'] = True
                logger.info(f"Session {session_id}: Analysis files detected, marking analysis_complete=True")
        
        # Check for ITN results
        if session_id:
            itn_results_path = f"instance/uploads/{session_id}/itn_distribution_results.json"
            if os.path.exists(itn_results_path):
                session_state['itn_planning_complete'] = True
                try:
                    with open(itn_results_path, 'r') as f:
                        itn_data = json.load(f)
                        session_state['itn_summary'] = {
                            'total_nets': itn_data.get('summary', {}).get('total_nets_allocated', 0),
                            'wards_covered': itn_data.get('summary', {}).get('wards_with_full_coverage', 0),
                            'population_covered': itn_data.get('summary', {}).get('population_covered', 0)
                        }
                except Exception as e:
                    logger.error(f"Error reading ITN results: {e}")
        
        return jsonify({
            'status': 'success',
            'session_state': session_state
        })
        
    except Exception as e:
        logger.error(f"Error getting session state: {e}")
        return jsonify({
            'status': 'error',
            'message': str(e)
        }), 500