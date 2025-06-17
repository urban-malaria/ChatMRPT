# app/web/routes/core_routes.py
"""
Core Routes module for basic application functionality.

This module contains the core routes for the ChatMRPT web application:
- Main index page
- Session management  
- Application status
- Session clearing and data reloading
"""

import os
import uuid
import logging
from flask import Blueprint, render_template, session, request, current_app, jsonify
from datetime import datetime

from ...core.decorators import handle_errors, log_execution_time, validate_session
from ...core.exceptions import SessionError, ValidationError
from ...core.utils import convert_to_json_serializable

logger = logging.getLogger(__name__)

# Create the core routes blueprint
core_bp = Blueprint('core', __name__)


@core_bp.before_request
def log_session_activity():
    """Log session activity for non-static requests."""
    if request.endpoint and not request.endpoint.startswith('static'):
        session_id = session.get('session_id')
        if session_id and hasattr(current_app, 'services'):
            # Get browser and IP info
            browser_info = request.user_agent.string
            ip_address = request.remote_addr
            
            # Log session activity
            interaction_logger = current_app.services.interaction_logger
            if interaction_logger:
                try:
                    interaction_logger.log_session_start(session_id, browser_info, ip_address)
                except Exception as e:
                    logger.warning(f"Failed to log session activity: {e}")


@core_bp.route('/')
@handle_errors
@log_execution_time
def index():
    """
    Render the main application interface.
    
    Initializes a new session if one doesn't exist and handles
    UI switching between Bootstrap and Tailwind versions.
    """
    # Initialize session data if needed
    if 'session_id' not in session:
        session['session_id'] = str(uuid.uuid4())
        session['conversation_history'] = []
        session['data_loaded'] = False
        session['analysis_complete'] = False
        session['csv_loaded'] = False
        session['shapefile_loaded'] = False
        session['current_language'] = 'en'
        
        # Initialize dialogue state tracking
        session['pending_action'] = None
        session['pending_variables'] = None
        session['last_visualization'] = None
        session['dialogue_context'] = {}
        
        # Log new session
        if hasattr(current_app, 'services'):
            interaction_logger = current_app.services.interaction_logger
            if interaction_logger:
                try:
                    interaction_logger.log_session_start(
                        session['session_id'], 
                        request.user_agent.string, 
                        request.remote_addr
                    )
                except Exception as e:
                    logger.warning(f"Failed to log new session: {e}")
    
    # Check for UI preference
    use_tailwind = request.args.get('use_tailwind', 'false').lower() == 'true'
    
    # Select template based on preference
    if use_tailwind:
        template_name = 'index_tailwind.html'
    else:
        template_name = 'index.html'
    
    logger.info(f"Rendering {template_name} for session {session['session_id']}")
    
    return render_template(template_name)


@core_bp.route('/session_info')
@handle_errors
@validate_session
def session_info():
    """
    Return session information.
    
    Provides information about the current session state including
    loaded data files and analysis status.
    """
    session_id = session.get('session_id', 'unknown')
    
    # Get data service for additional information
    data_service = current_app.services.data_service if hasattr(current_app, 'services') else None
    
    # Prepare session information
    info = {
        'session_id': session_id,
        'csv_loaded': session.get('csv_loaded', False),
        'shapefile_loaded': session.get('shapefile_loaded', False),
        'analysis_complete': session.get('analysis_complete', False),
        'data_loaded': session.get('data_loaded', False),
        'csv_filename': session.get('csv_filename', None),
        'shapefile_filename': session.get('shapefile_filename', None),
        'csv_rows': session.get('csv_rows', 0),
        'csv_columns': session.get('csv_columns', 0),
        'shapefile_features': session.get('shapefile_features', 0),
        'conversation_history_length': len(session.get('conversation_history', [])),
        'current_language': session.get('current_language', 'en'),
        'pending_action': session.get('pending_action'),
        'last_visualization': session.get('last_visualization')
    }
    
    # Add available variables if data is loaded
    if data_service and session.get('csv_loaded', False):
        try:
            available_variables = data_service.get_available_variables(session_id)
            info['available_variables'] = available_variables
        except Exception as e:
            logger.error(f"Error getting available variables: {e}")
            info['available_variables'] = []
    
    # Make sure the response is JSON serializable
    info = convert_to_json_serializable(info)
    
    return jsonify(info)


@core_bp.route('/clear_session', methods=['POST'])
@handle_errors
@validate_session
def clear_session():
    """
    Clear the current session data.
    
    Resets all session variables and clears conversation history
    while maintaining the session ID.
    """
    session_id = session.get('session_id')
    
    try:
        # Clear session data but keep session_id
        session['conversation_history'] = []
        session['data_loaded'] = False
        session['analysis_complete'] = False
        session['csv_loaded'] = False
        session['shapefile_loaded'] = False
        session['pending_action'] = None
        session['pending_variables'] = None
        session['last_visualization'] = None
        session['dialogue_context'] = {}
        
        # Clear filenames and metadata
        session['csv_filename'] = None
        session['shapefile_filename'] = None
        session['csv_rows'] = 0
        session['csv_columns'] = 0
        session['shapefile_features'] = 0
        session['available_variables'] = []
        
        # Clear data from data service if available
        data_service = current_app.services.data_service if hasattr(current_app, 'services') else None
        if data_service:
            try:
                data_service.clear_session_data(session_id)
            except Exception as e:
                logger.warning(f"Error clearing data service session: {e}")
        
        # Log session clearing
        if hasattr(current_app, 'services'):
            interaction_logger = current_app.services.interaction_logger
            if interaction_logger:
                try:
                    interaction_logger.log_message(session_id, 'system', 'Session cleared')
                except Exception as e:
                    logger.warning(f"Failed to log session clearing: {e}")
        
        logger.info(f"Session {session_id} cleared successfully")
        
        return jsonify({
            'status': 'success',
            'message': 'Session cleared successfully'
        })
        
    except Exception as e:
        logger.error(f"Error clearing session {session_id}: {str(e)}", exc_info=True)
        return jsonify({
            'status': 'error',
            'message': f'Error clearing session: {str(e)}'
        }), 500


@core_bp.route('/app_status')
@handle_errors
def app_status():
    """
    Return application status information.
    
    Provides information about the application health, services status,
    and system information for monitoring and debugging.
    """
    try:
        status_info = {
            'status': 'healthy',
            'timestamp': datetime.utcnow().isoformat() + 'Z',
            'version': getattr(current_app, 'version', 'unknown'),
            'services': {},
            'system': {
                'python_version': None,
                'memory_usage': None
            }
        }
        
        # Check services availability if services are configured
        if hasattr(current_app, 'services'):
            services = current_app.services
            
            # Check data service
            if hasattr(services, 'data_service') and services.data_service:
                try:
                    # Simple health check for data service
                    status_info['services']['data_service'] = 'available'
                except Exception as e:
                    status_info['services']['data_service'] = f'error: {str(e)}'
            else:
                status_info['services']['data_service'] = 'not_configured'
            
            # Check interaction logger
            if hasattr(services, 'interaction_logger') and services.interaction_logger:
                try:
                    # Simple health check for interaction logger
                    status_info['services']['interaction_logger'] = 'available'
                except Exception as e:
                    status_info['services']['interaction_logger'] = f'error: {str(e)}'
            else:
                status_info['services']['interaction_logger'] = 'not_configured'
            
            # Check analysis service
            if hasattr(services, 'analysis_service') and services.analysis_service:
                status_info['services']['analysis_service'] = 'available'
            else:
                status_info['services']['analysis_service'] = 'not_configured'
            
            # Check ConversationalEpidemiologist status (NEW SYSTEM)
            if hasattr(services, 'conversational_epidemiologist') and services.conversational_epidemiologist:
                try:
                    # ConversationalEpidemiologist is available
                    status_info['services']['conversational_epidemiologist'] = 'available'
                except Exception as e:
                    status_info['services']['conversational_epidemiologist'] = f'error: {str(e)}'
            else:
                status_info['services']['conversational_epidemiologist'] = 'not_configured'
        
        else:
            status_info['services']['note'] = 'Services not configured'
        
        # Add system information
        try:
            import sys
            import psutil
            
            status_info['system'] = {
                'python_version': sys.version,
                'memory_usage': psutil.virtual_memory().percent,
                'disk_usage': psutil.disk_usage('/').percent if os.path.exists('/') else None
            }
        except ImportError:
            status_info['system']['note'] = 'System monitoring not available'
        except Exception as e:
            status_info['system']['error'] = str(e)
        
        # Check overall health
        service_errors = [v for v in status_info['services'].values() if isinstance(v, str) and v.startswith('error:')]
        if service_errors:
            status_info['status'] = 'degraded'
            status_info['issues'] = service_errors
        
        return jsonify(status_info)
        
    except Exception as e:
        logger.error(f"Error getting app status: {str(e)}", exc_info=True)
        return jsonify({
            'status': 'error',
            'message': f'Error getting status: {str(e)}',
            'timestamp': datetime.utcnow().isoformat() + 'Z'
        }), 500


@core_bp.route('/reload_session_data', methods=['POST'])
@handle_errors
@validate_session
def reload_session_data():
    """
    Reload session data from the data service.
    
    Useful for recovering session state after application restart
    or when session data gets out of sync.
    """
    session_id = session.get('session_id')
    
    try:
        # Get data service
        data_service = current_app.services.data_service if hasattr(current_app, 'services') else None
        if not data_service:
            return jsonify({
                'status': 'error',
                'message': 'Data service not available'
            }), 500
        
        # Try to reload data from service
        reload_result = data_service.get_session_status(session_id)
        
        if reload_result:
            # Update session with current data state
            session['csv_loaded'] = reload_result.get('csv_loaded', False)
            session['shapefile_loaded'] = reload_result.get('shapefile_loaded', False)
            session['analysis_complete'] = reload_result.get('analysis_complete', False)
            session['data_loaded'] = session['csv_loaded'] and session['shapefile_loaded']
            
            # Update metadata if available
            if 'csv_info' in reload_result:
                csv_info = reload_result['csv_info']
                session['csv_filename'] = csv_info.get('filename')
                session['csv_rows'] = csv_info.get('rows', 0)
                session['csv_columns'] = csv_info.get('columns', 0)
            
            if 'shapefile_info' in reload_result:
                shp_info = reload_result['shapefile_info']
                session['shapefile_filename'] = shp_info.get('filename')
                session['shapefile_features'] = shp_info.get('features', 0)
            
            # Get available variables if CSV is loaded
            if session['csv_loaded']:
                try:
                    available_variables = data_service.get_available_variables(session_id)
                    session['available_variables'] = available_variables
                except Exception as e:
                    logger.warning(f"Could not reload available variables: {e}")
            
            logger.info(f"Session {session_id} data reloaded successfully")
            
            return jsonify({
                'status': 'success',
                'message': 'Session data reloaded successfully',
                'session_state': {
                    'csv_loaded': session['csv_loaded'],
                    'shapefile_loaded': session['shapefile_loaded'],
                    'analysis_complete': session['analysis_complete'],
                    'data_loaded': session['data_loaded']
                }
            })
        
        else:
            return jsonify({
                'status': 'warning',
                'message': 'No session data found to reload'
            })
            
    except Exception as e:
        logger.error(f"Error reloading session data for {session_id}: {str(e)}", exc_info=True)
        return jsonify({
            'status': 'error',
            'message': f'Error reloading session data: {str(e)}'
        }), 500 