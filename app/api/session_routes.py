# app/web/routes/session_routes.py
"""
Session Routes module for session state verification.

This module provides endpoints for verifying session state
in multi-worker environments.
"""

import logging
from flask import Blueprint, session, jsonify, g
from app.utils.decorators import handle_errors
from app.utils.session_scope import get_effective_session_id

logger = logging.getLogger(__name__)

# Create the session routes blueprint
session_bp = Blueprint('session', __name__)


def _apply_session_no_cache_headers(response):
    response.headers['Cache-Control'] = 'no-store, no-cache, must-revalidate, max-age=0'
    response.headers['Pragma'] = 'no-cache'
    response.headers['Expires'] = '0'
    return response


@session_bp.after_request
def apply_no_cache(response):
    return _apply_session_no_cache_headers(response)


@session_bp.route('/api/session/verify-tpr', methods=['GET'])
@handle_errors
def verify_tpr_session():
    """
    Verify TPR workflow session state.
    
    This endpoint is used by the frontend to check if TPR workflow
    is active in the backend session. Critical for multi-worker
    environments where session state may not be properly shared.
    
    Returns:
        JSON response with TPR workflow status
    """
    # Get TPR workflow state from session
    tpr_workflow_active = session.get('tpr_workflow_active', False)
    tpr_session_id = session.get('tpr_session_id')
    
    # Log the verification request
    logger.debug(f"TPR session verification: active={tpr_workflow_active}, session_id={tpr_session_id}")
    
    # Additional checks for TPR state
    tpr_loaded = session.get('tprLoaded', False)
    upload_type = session.get('upload_type')
    
    # Consider TPR workflow active if any TPR-related flags are set
    is_tpr_active = (
        tpr_workflow_active or 
        tpr_loaded or 
        upload_type in ['tpr_excel', 'tpr_shapefile']
    )
    
    return jsonify({
        'tpr_workflow_active': is_tpr_active,
        'tpr_session_id': tpr_session_id,
        'session_flags': {
            'tpr_workflow_active': tpr_workflow_active,
            'tprLoaded': tpr_loaded,
            'upload_type': upload_type
        }
    })


@session_bp.route('/api/session/status', methods=['GET'])
@handle_errors
def get_session_status():
    """
    Get general session status.
    
    Returns:
        JSON response with session status information
    """
    session_id = get_effective_session_id()
    return jsonify({
        'session_id': session_id,
        'base_session_id': session.get('base_session_id'),
        'conversation_id': getattr(g, 'conversation_id', None),
        'data_loaded': session.get('data_loaded', False),
        'csv_loaded': session.get('csv_loaded', False),
        'shapefile_loaded': session.get('shapefile_loaded', False),
        'analysis_complete': session.get('analysis_complete', False),
        'upload_type': session.get('upload_type'),
        'tpr_workflow_active': session.get('tpr_workflow_active', False)
    })


@session_bp.route('/api/session/redis-status', methods=['GET'])
@handle_errors
def get_redis_status():
    """
    Get Redis session store status.
    
    Returns:
        JSON response with Redis connection information
    """
    from flask import current_app
    from ...config.redis_config import RedisConfig
    
    redis_info = RedisConfig.get_redis_info(current_app)
    
    # Add session type information
    session_type = current_app.config.get('SESSION_TYPE', 'filesystem')
    redis_info['session_type'] = session_type
    redis_info['session_prefix'] = current_app.config.get('SESSION_KEY_PREFIX', 'session:')
    
    return jsonify(redis_info)
