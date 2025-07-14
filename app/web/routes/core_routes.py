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
import time
import traceback
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
    """Log session activity and start response timing for non-static requests."""
    if request.endpoint and not request.endpoint.startswith('static'):
        # 🎯 START RESPONSE TIME TRACKING - CRITICAL FOR DEMO ANALYTICS
        request.start_time = time.time()
        
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
                    
                    # 🎯 LOG SESSION ACTIVITY ERRORS - DEMO MONITORING
                    if hasattr(current_app, 'services') and current_app.services.interaction_logger:
                        current_app.services.interaction_logger.log_error(
                            session_id=session_id,
                            error_type=f'SessionActivityError:{type(e).__name__}',
                            error_message=str(e),
                            stack_trace=traceback.format_exc()
                        )


@core_bp.after_request
def log_response_timing(response):
    """Log comprehensive response timing for demo analytics."""
    if hasattr(request, 'start_time') and request.endpoint and not request.endpoint.startswith('static'):
        response_time = time.time() - request.start_time
        session_id = session.get('session_id')
        
        # 🎯 LOG RESPONSE TIMING - CRITICAL FOR DEMO ANALYTICS
        if (hasattr(current_app, 'services') and current_app.services.interaction_logger and 
            response_time > 0.05):  # Only log requests > 50ms to avoid noise
            
            interaction_logger = current_app.services.interaction_logger
            
            try:
                # Categorize performance
                if response_time < 0.5:
                    performance_category = 'excellent'
                elif response_time < 2.0:
                    performance_category = 'good'
                elif response_time < 5.0:
                    performance_category = 'acceptable'
                else:
                    performance_category = 'slow'
                
                # Log comprehensive timing event
                interaction_logger.log_analysis_event(
                    session_id=session_id,
                    event_type='endpoint_response_timing',
                    details={
                        'endpoint': request.endpoint,
                        'method': request.method,
                        'response_time_seconds': response_time,
                        'response_time_ms': round(response_time * 1000, 1),
                        'status_code': response.status_code,
                        'performance_category': performance_category,
                        'url_path': request.path,
                        'is_success': response.status_code < 400,
                        'content_length': response.content_length or 0,
                        'timestamp': time.time()
                    },
                    success=response.status_code < 400
                )
                
            except Exception as e:
                logger.warning(f"Failed to log response timing: {e}")
    
    return response


@core_bp.route('/')
@handle_errors
@log_execution_time
def index():
    """
    Root endpoint for the API.
    
    Returns a simple JSON response indicating the API is running.
    React frontend is served separately.
    """
    return jsonify({
        'status': 'ok',
        'message': 'ChatMRPT API is running. Please use the React frontend at http://localhost:3000',
        'version': '3.0',
        'endpoints': {
            'session': '/session_info',
            'chat': '/send_message',
            'upload': '/upload_both_files',
            'analysis': '/run_analysis',
            'clear': '/clear_session'
        }
    })


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
            
            # 🎯 LOG VARIABLE RETRIEVAL ERRORS - DEMO MONITORING
            if hasattr(current_app, 'services') and current_app.services.interaction_logger:
                current_app.services.interaction_logger.log_error(
                    session_id=session_id,
                    error_type=f'VariableRetrievalError:{type(e).__name__}',
                    error_message=str(e),
                    stack_trace=traceback.format_exc()
                )
    
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
                
                # 🎯 LOG SESSION CLEARING ERRORS - DEMO MONITORING
                if hasattr(current_app, 'services') and current_app.services.interaction_logger:
                    current_app.services.interaction_logger.log_error(
                        session_id=session_id,
                        error_type=f'SessionClearingError:{type(e).__name__}',
                        error_message=str(e),
                        stack_trace=traceback.format_exc()
                    )
        
        # Log session clearing
        if hasattr(current_app, 'services'):
            interaction_logger = current_app.services.interaction_logger
            if interaction_logger:
                try:
                    interaction_logger.log_message(session_id, 'system', 'Session cleared')
                except Exception as e:
                    logger.warning(f"Failed to log session clearing: {e}")
                    
                    # 🎯 LOG SESSION CLEARING LOG ERRORS - DEMO MONITORING
                    if hasattr(current_app, 'services') and current_app.services.interaction_logger:
                        current_app.services.interaction_logger.log_error(
                            session_id=session_id,
                            error_type=f'SessionClearingLogError:{type(e).__name__}',
                            error_message=str(e),
                            stack_trace=traceback.format_exc()
                        )
        
        logger.info(f"Session {session_id} cleared successfully")
        
        return jsonify({
            'status': 'success',
            'message': 'Session cleared successfully'
        })
        
    except Exception as e:
        logger.error(f"Error clearing session {session_id}: {str(e)}", exc_info=True)
        
        # 🎯 LOG SESSION CLEARING CRITICAL ERRORS - DEMO MONITORING
        if hasattr(current_app, 'services') and current_app.services.interaction_logger:
            current_app.services.interaction_logger.log_error(
                session_id=session_id,
                error_type=f'CriticalSessionClearingError:{type(e).__name__}',
                error_message=str(e),
                stack_trace=traceback.format_exc()
            )
            
            # Log as analysis event for demo monitoring
            current_app.services.interaction_logger.log_analysis_event(
                session_id=session_id,
                event_type='session_clearing_failed',
                details={
                    'error_type': type(e).__name__,
                    'error_message': str(e),
                    'endpoint': '/clear_session',
                    'severity': 'critical'
                },
                success=False
            )
        
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
                    
                    # 🎯 LOG DATA SERVICE HEALTH CHECK ERRORS - DEMO MONITORING
                    if hasattr(current_app, 'services') and current_app.services.interaction_logger:
                        current_app.services.interaction_logger.log_error(
                            session_id=session.get('session_id'),
                            error_type=f'DataServiceHealthCheckError:{type(e).__name__}',
                            error_message=str(e),
                            stack_trace=traceback.format_exc()
                        )
            else:
                status_info['services']['data_service'] = 'not_configured'
            
            # Check interaction logger
            if hasattr(services, 'interaction_logger') and services.interaction_logger:
                try:
                    # Simple health check for interaction logger
                    status_info['services']['interaction_logger'] = 'available'
                except Exception as e:
                    status_info['services']['interaction_logger'] = f'error: {str(e)}'
                    
                    # 🎯 LOG INTERACTION LOGGER HEALTH CHECK ERRORS - DEMO MONITORING
                    logger.warning(f"Interaction logger health check failed: {e}")
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
                    
                    # 🎯 LOG CONVERSATIONAL EPIDEMIOLOGIST HEALTH CHECK ERRORS - DEMO MONITORING
                    if hasattr(current_app, 'services') and current_app.services.interaction_logger:
                        current_app.services.interaction_logger.log_error(
                            session_id=session.get('session_id'),
                            error_type=f'ConversationalEpidemiologistHealthCheckError:{type(e).__name__}',
                            error_message=str(e),
                            stack_trace=traceback.format_exc()
                        )
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
            
            # 🎯 LOG SYSTEM MONITORING ERRORS - DEMO MONITORING
            if hasattr(current_app, 'services') and current_app.services.interaction_logger:
                current_app.services.interaction_logger.log_error(
                    session_id=session.get('session_id'),
                    error_type=f'SystemMonitoringError:{type(e).__name__}',
                    error_message=str(e),
                    stack_trace=traceback.format_exc()
                )
        
        # Check overall health
        service_errors = [v for v in status_info['services'].values() if isinstance(v, str) and v.startswith('error:')]
        if service_errors:
            status_info['status'] = 'degraded'
            status_info['issues'] = service_errors
        
        return jsonify(status_info)
        
    except Exception as e:
        logger.error(f"Error getting app status: {str(e)}", exc_info=True)
        
        # 🎯 LOG APP STATUS CRITICAL ERRORS - DEMO MONITORING
        if hasattr(current_app, 'services') and current_app.services.interaction_logger:
            current_app.services.interaction_logger.log_error(
                session_id=session.get('session_id'),
                error_type=f'CriticalAppStatusError:{type(e).__name__}',
                error_message=str(e),
                stack_trace=traceback.format_exc()
            )
            
            # Log as analysis event for demo monitoring
            current_app.services.interaction_logger.log_analysis_event(
                session_id=session.get('session_id'),
                event_type='app_status_failed',
                details={
                    'error_type': type(e).__name__,
                    'error_message': str(e),
                    'endpoint': '/app_status',
                    'severity': 'critical'
                },
                success=False
            )
        
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
                    
                    # 🎯 LOG VARIABLE RELOAD ERRORS - DEMO MONITORING
                    if hasattr(current_app, 'services') and current_app.services.interaction_logger:
                        current_app.services.interaction_logger.log_error(
                            session_id=session_id,
                            error_type=f'VariableReloadError:{type(e).__name__}',
                            error_message=str(e),
                            stack_trace=traceback.format_exc()
                        )
            
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
        
        # 🎯 LOG SESSION RELOAD CRITICAL ERRORS - DEMO MONITORING
        if hasattr(current_app, 'services') and current_app.services.interaction_logger:
            current_app.services.interaction_logger.log_error(
                session_id=session_id,
                error_type=f'CriticalSessionReloadError:{type(e).__name__}',
                error_message=str(e),
                stack_trace=traceback.format_exc()
            )
            
            # Log as analysis event for demo monitoring
            current_app.services.interaction_logger.log_analysis_event(
                session_id=session_id,
                event_type='session_reload_failed',
                details={
                    'error_type': type(e).__name__,
                    'error_message': str(e),
                    'endpoint': '/reload_session_data',
                    'severity': 'critical'
                },
                success=False
            )
        
        return jsonify({
            'status': 'error',
            'message': f'Error reloading session data: {str(e)}'
        }), 500 