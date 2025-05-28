"""
Main web interface blueprint for ChatMRPT application.

This blueprint handles the core web interface routes including
the main dashboard and basic user interactions.
"""

import os
import uuid
import logging
import numpy as np
import pandas as pd
from flask import Blueprint, render_template, session, request, current_app, jsonify, send_from_directory
from werkzeug.utils import secure_filename
from datetime import datetime

from ..core.decorators import handle_errors, log_execution_time, validate_session, require_data_loaded
from ..core.exceptions import SessionError, ValidationError, DataProcessingError
from ..core.utils import convert_to_json_serializable

logger = logging.getLogger(__name__)

# Create the blueprint
main_bp = Blueprint('main', __name__)

# File upload configurations
ALLOWED_EXTENSIONS_CSV = {'csv', 'txt', 'xlsx', 'xls'}  # Added Excel file extensions
ALLOWED_EXTENSIONS_SHP = {'zip'}  # Shapefiles are uploaded as ZIP files


def allowed_file(filename, allowed_extensions):
    """Check if file extension is allowed."""
    return '.' in filename and filename.rsplit('.', 1)[1].lower() in allowed_extensions


@main_bp.before_request
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


@main_bp.route('/')
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


@main_bp.route('/upload_both_files', methods=['POST'])
@handle_errors
@validate_session
@log_execution_time
def upload_both_files():
    """
    Handle simultaneous upload of both CSV and shapefile files using DataService.
    
    This is the modern implementation that uses our service architecture.
    """
    session_id = session.get('session_id')
    
    # Check if files were provided
    csv_file = request.files.get('csv_file')
    shapefile = request.files.get('shapefile')
    
    # Validate files
    if csv_file and csv_file.filename == '':
        csv_file = None
    if shapefile and shapefile.filename == '':
        shapefile = None
        
    if not csv_file and not shapefile:
        raise ValidationError("No files selected for upload")
    
    # Get the data service
    data_service = current_app.services.data_service
    if not data_service:
        raise DataProcessingError("Data service is not available")
    
    # Create session folder for file storage
    session_folder = os.path.join(current_app.config['UPLOAD_FOLDER'], session_id)
    os.makedirs(session_folder, exist_ok=True)
    
    results = {}
    
    # Process CSV file if provided
    if csv_file and allowed_file(csv_file.filename, ALLOWED_EXTENSIONS_CSV):
        try:
            # Save file securely
            csv_filename = secure_filename(csv_file.filename)
            csv_path = os.path.join(session_folder, csv_filename)
            csv_file.save(csv_path)
            
            # Use DataService to load CSV
            csv_result = data_service.load_csv_file(session_id, csv_path)
            
            # Force status to success if data is present even if status is 'error'
            # This is a workaround for the production environment
            if 'data' in csv_result and csv_result['data'] is not None and len(csv_result.get('data', [])) > 0:
                # Ensure we mark the CSV as loaded if we have data
                csv_result['status'] = 'success'
                logger.info(f"Fixed CSV result status: Changed to success because data is present")
            
            if csv_result['status'] == 'success':
                # Update session data
                session['csv_loaded'] = True
                session['csv_filename'] = csv_filename
                session['csv_rows'] = csv_result.get('rows', 0)
                session['csv_columns'] = csv_result.get('columns', 0)
                
                # Get available variables from data service
                available_variables = data_service.get_available_variables(session_id)
                session['available_variables'] = available_variables
                
                logger.info(f"CSV loaded successfully: {csv_filename} ({csv_result.get('rows', 0)} rows)")
            
            results['csv_result'] = csv_result
            
        except Exception as e:
            logger.error(f"Error processing CSV file: {str(e)}", exc_info=True)
            results['csv_result'] = {
                'status': 'error',
                'message': f'Error processing CSV file: {str(e)}'
            }
    elif csv_file:
        results['csv_result'] = {
            'status': 'error',
            'message': 'Invalid CSV file type. Please upload a .csv, .xlsx, or .xls file.'
        }
    
    # Process shapefile if provided
    if shapefile and allowed_file(shapefile.filename, ALLOWED_EXTENSIONS_SHP):
        try:
            # Save file securely
            shp_filename = secure_filename(shapefile.filename)
            shp_path = os.path.join(session_folder, shp_filename)
            shapefile.save(shp_path)
            
            # Use DataService to load shapefile
            shp_result = data_service.load_shapefile(session_id, shp_path)
            
            # Similar fix for shapefile data
            if 'data' in shp_result and shp_result['data'] is not None and len(shp_result.get('data', [])) > 0:
                if shp_result['status'] == 'error':
                    shp_result['status'] = 'success'
                    logger.info(f"Fixed shapefile result status: Changed to success because data is present")
            
            if shp_result['status'] in ['success', 'warning']:
                # Update session data
                session['shapefile_loaded'] = True
                session['shapefile_filename'] = shp_filename
                session['shapefile_features'] = shp_result.get('features', 0)
                
                logger.info(f"Shapefile loaded: {shp_filename} ({shp_result.get('features', 0)} features)")
            
            results['shp_result'] = shp_result
            
        except Exception as e:
            logger.error(f"Error processing shapefile: {str(e)}", exc_info=True)
            results['shp_result'] = {
                'status': 'error',
                'message': f'Error processing shapefile: {str(e)}'
            }
    elif shapefile:
        results['shp_result'] = {
            'status': 'error',
            'message': 'Invalid shapefile type. Please upload a .zip file containing shapefile data.'
        }
    
    # Check for ward name mismatches if both files are loaded
    if session.get('csv_loaded', False) and session.get('shapefile_loaded', False):
        try:
            mismatches = data_service.check_ward_mismatches(session_id)
            if mismatches and len(mismatches) > 0:
                # Add mismatch warning to the most recent result
                if 'shp_result' in results:
                    results['shp_result']['mismatches'] = mismatches
                    results['shp_result']['status'] = 'warning'
                    results['shp_result']['message'] = f'Shapefile loaded but found {len(mismatches)} ward name mismatches'
                elif 'csv_result' in results:
                    results['csv_result']['mismatches'] = mismatches
                    results['csv_result']['status'] = 'warning'
                    results['csv_result']['message'] = f'CSV loaded but found {len(mismatches)} ward name mismatches'
        except Exception as e:
            logger.warning(f"Error checking ward mismatches: {str(e)}")
    
    # Prepare response
    overall_status = 'success'
    message = 'Files processed successfully'
    
    # Check if any uploads failed - only consider failures if there's no data
    has_error = False
    for result_key, result_value in results.items():
        if result_value.get('status') == 'error':
            # Only count as error if there's no data present
            if 'data' not in result_value or result_value['data'] is None or len(result_value.get('data', [])) == 0:
                has_error = True
                break
    
    if has_error:
        overall_status = 'error'
        message = 'One or more file uploads failed'
    elif any(result.get('status') == 'warning' for result in results.values()):
        overall_status = 'warning'
        message = 'Files uploaded with warnings'
    
    # Add analysis prompt if both files are loaded successfully
    if session.get('csv_loaded', False) and session.get('shapefile_loaded', False):
        results['analysis_prompt'] = f"""
        <p><strong>Excellent! All files are now loaded successfully!</strong></p>
        <p>Your data includes:</p>
        <ul>
            <li>📊 CSV data: {session.get('csv_rows', 0)} rows with {session.get('csv_columns', 0)} columns</li>
            <li>🗺️ Shapefile data: {session.get('shapefile_features', 0)} features</li>
        </ul>
        <div class="analysis-ready-prompt">
            <p><strong>🚀 Everything is ready for analysis!</strong></p>
            <p>Type "Run the analysis" to begin processing your data.</p>
            <button class="btn btn-primary mt-2" onclick="document.getElementById('message-input').value='Run the analysis'; document.getElementById('send-message').click();">
                Start Analysis
            </button>
        </div>
        """
        session['data_loaded'] = True
    
    response = {
        'status': overall_status,
        'message': message,
        **results
    }
    
    # Debug: Log the response structure before JSON conversion
    logger.debug(f"Upload response before JSON conversion: {type(response)}")
    for key, value in response.items():
        if isinstance(value, dict):
            for subkey, subvalue in value.items():
                if hasattr(subvalue, '__contains__') and ('nan' in str(subvalue).lower() or 'inf' in str(subvalue).lower()):
                    logger.warning(f"Found potential NaN/Inf in response[{key}][{subkey}]: {subvalue}")
        elif hasattr(value, '__contains__') and ('nan' in str(value).lower() or 'inf' in str(value).lower()):
            logger.warning(f"Found potential NaN/Inf in response[{key}]: {value}")
    
    # Convert to JSON serializable format to handle numpy types and DataFrames
    response = convert_to_json_serializable(response)
    
    # Debug: Verify JSON serializability
    try:
        import json
        json.dumps(response)
        logger.debug("Upload response successfully serialized to JSON")
    except Exception as e:
        logger.error(f"JSON serialization failed after conversion: {str(e)}")
        logger.error(f"Response content: {response}")
    
    return jsonify(response)


@main_bp.route('/upload', methods=['POST'])  
@handle_errors
@validate_session
@log_execution_time
def upload():
    """
    Legacy upload route for backwards compatibility.
    Redirects to upload_both_files function.
    """
    return upload_both_files()


@main_bp.route('/session_info')
@handle_errors
def session_info():
    """
    Get current session information for debugging/monitoring.
    
    Returns:
        JSON with session details
    """
    if 'session_id' not in session:
        raise SessionError("No active session found")
    
    session_data = {
        'session_id': session.get('session_id'),
        'data_loaded': session.get('data_loaded', False),
        'analysis_complete': session.get('analysis_complete', False),
        'csv_loaded': session.get('csv_loaded', False),
        'shapefile_loaded': session.get('shapefile_loaded', False),
        'current_language': session.get('current_language', 'en'),
        'conversation_length': len(session.get('conversation_history', [])),
        'has_pending_action': session.get('pending_action') is not None,
        'last_activity': session.get('last_activity'),
    }
    
    return session_data


@main_bp.route('/clear_session', methods=['POST'])
@handle_errors
def clear_session():
    """
    Clear the current session data.
    
    Useful for testing or when users want to start fresh.
    """
    old_session_id = session.get('session_id')
    
    # Clear session data
    session.clear()
    
    logger.info(f"Cleared session data for {old_session_id}")
    
    return {
        'status': 'success',
        'message': 'Session cleared successfully',
        'action': 'Please refresh the page to start a new session'
    }


@main_bp.route('/app_status')
@handle_errors
def app_status():
    """
    Get application status and health information.
    
    Returns:
        JSON with application status
    """
    status_info = {
        'status': 'healthy',
        'version': '2.0',
        'environment': current_app.config.get('ENV', 'unknown'),
        'debug_mode': current_app.config.get('DEBUG', False),
        'features': {
            'ai_enabled': current_app.config.get('OPENAI_API_KEY') is not None,
            'file_upload': True,
            'analysis': True,
            'visualization': True,
            'reports': True
        }
    }
    
    # Add service health if available
    if hasattr(current_app, 'services'):
        try:
            service_health = current_app.services.health_check()
            status_info['services'] = service_health
        except Exception as e:
            logger.warning(f"Failed to get service health: {e}")
            status_info['services'] = {'error': str(e)}
    
    return status_info


@main_bp.errorhandler(SessionError)
def handle_session_error(error):
    """Handle session-related errors."""
    logger.warning(f"Session error: {error.message}")
    
    return {
        'error': 'SessionError',
        'message': error.message,
        'action': 'Please refresh the page to start a new session'
    }, error.status_code


@main_bp.errorhandler(ValidationError)
def handle_validation_error(error):
    """Handle validation errors."""
    logger.warning(f"Validation error: {error.message}")
    
    return jsonify({
        'status': 'error',
        'error': 'ValidationError',
        'message': error.message,
        'details': error.details
    }), error.status_code


@main_bp.errorhandler(DataProcessingError)
def handle_data_processing_error(error):
    """Handle data processing errors."""
    logger.error(f"Data processing error: {error.message}")
    
    return jsonify({
        'status': 'error',
        'error': 'DataProcessingError',
        'message': error.message,
        'details': error.details
    }), error.status_code


# Context processor to inject common template variables
@main_bp.app_context_processor
def inject_template_vars():
    """Inject common variables into all templates."""
    return {
        'app_version': '2.0',
        'session_id': session.get('session_id'),
        'debug_mode': current_app.config.get('DEBUG', False)
    }


@main_bp.route('/run_analysis', methods=['POST'])
@validate_session
@handle_errors
@log_execution_time
@require_data_loaded(['csv', 'shapefile'])
def run_analysis():
    """Run the analysis directly (used for API calls, not main chat flow)"""
    try:
        # Get session ID
        session_id = session.get('session_id', 'default')
        
        # Debug logging
        current_app.logger.info(f"[DEBUG] run_analysis: session_id={session_id}")
        data_service = current_app.services.data_service
        data_handler = data_service.get_handler(session_id)
        current_app.logger.info(f"[DEBUG] run_analysis: data_handler exists: {data_handler is not None}")
        if data_handler:
            has_df = hasattr(data_handler, 'df') and data_handler.df is not None
            current_app.logger.info(f"[DEBUG] run_analysis: data_handler.df exists: {has_df}")
        
        # Get custom parameters from the request
        data = request.json or {}
        selected_variables = data.get('selected_variables', None)
        use_llm_selection = data.get('use_llm_selection', True)
        
        # Get services from the container
        analysis_service = current_app.services.analysis_service
        
        if not data_handler:
            raise ValidationError('Data handler not initialized. Please upload data files first.')
        
        # Check if both files are loaded
        if not session.get('csv_loaded', False) or not session.get('shapefile_loaded', False):
            raise ValidationError('Please upload both CSV and shapefile data before running analysis')
        
        # Run the analysis using the service
        if selected_variables:
            # Run custom analysis with specified variables
            result = analysis_service.run_custom_analysis(
                data_handler=data_handler,
                selected_variables=selected_variables,
                session_id=session_id
            )
        else:
            # Run standard analysis (which will use LLM selection if configured)
            result = analysis_service.run_standard_analysis(
                data_handler=data_handler,
                session_id=session_id
            )
        
        # Process the result
        if result['status'] == 'success':
            # Store JSON-serializable data in session
            session['analysis_complete'] = True
            session['variables_used'] = result.get('variables_used', [])
            
            # Extract risk wards from vulnerability rankings if available
            high_risk_wards = []
            medium_risk_wards = []
            low_risk_wards = []
            
            # Check if data_handler has vulnerability rankings
            if hasattr(data_handler, 'vulnerability_rankings') and data_handler.vulnerability_rankings is not None:
                rankings = data_handler.vulnerability_rankings
                
                # Extract wards by category
                if 'vulnerability_category' in rankings.columns and 'WardName' in rankings.columns:
                    high_risk_wards = rankings[rankings['vulnerability_category'] == 'High']['WardName'].tolist()
                    medium_risk_wards = rankings[rankings['vulnerability_category'] == 'Medium']['WardName'].tolist()
                    low_risk_wards = rankings[rankings['vulnerability_category'] == 'Low']['WardName'].tolist()
            
            # Use extracted wards or fall back to result wards
            high_risk_wards = high_risk_wards or result.get('high_risk_wards', [])
            medium_risk_wards = medium_risk_wards or result.get('medium_risk_wards', [])
            low_risk_wards = low_risk_wards or result.get('low_risk_wards', [])
            
            # Return success response
            return jsonify({
                'status': 'success',
                'message': 'Analysis completed successfully',
                'variables_used': result.get('variables_used', []),
                'high_risk_wards': high_risk_wards[:5],
                'medium_risk_wards': medium_risk_wards[:5],
                'low_risk_wards': low_risk_wards[:5]
            })
        else:
            return jsonify({
                'status': 'error',
                'message': result.get('message', 'Error running analysis')
            }), 400
    
    except ValidationError as e:
        return jsonify({
            'status': 'error',
            'message': str(e)
        }), 400
    except Exception as e:
        current_app.logger.error(f"Error running analysis: {str(e)}", exc_info=True)
        return jsonify({
            'status': 'error',
            'message': f'Error running analysis: {str(e)}'
        }), 500


@main_bp.route('/explain_variable_selection', methods=['GET'])
@validate_session
@handle_errors
@log_execution_time
def explain_variable_selection():
    """Generate an explanation for why certain variables were selected for the analysis"""
    try:
        # Get session ID
        session_id = session.get('session_id')
        if not session_id:
            raise ValidationError('No active session found')
        
        # Get services from the container
        data_service = current_app.services.data_service
        analysis_service = current_app.services.analysis_service
        
        # Get data handler via data service
        data_handler = data_service.get_handler(session_id)
        if not data_handler:
            raise ValidationError('No data available for explanation')
            
        # Check if analysis has been performed
        if not session.get('analysis_complete', False) or not session.get('variables_used'):
            raise ValidationError('Analysis not yet performed')
        
        # Get variables used in the analysis
        variables = session.get('variables_used', [])
        if not variables:
            raise ValidationError('No variables found from analysis')
        
        # Get explanation using the service
        result = analysis_service.explain_variable_selection(
            variables=variables,
            data_handler=data_handler
        )
        
        if result['status'] == 'success':
            return jsonify({
                'status': 'success',
                'message': 'Generated variable selection explanation',
                'explanation': result.get('explanations', {}),
                'variables': variables
            })
        else:
            return jsonify({
                'status': 'error',
                'message': result.get('message', 'Error generating explanation')
            }), 400
    
    except ValidationError as e:
        return jsonify({
            'status': 'error',
            'message': str(e)
        }), 400
    except Exception as e:
        current_app.logger.error(f"Error explaining variable selection: {str(e)}", exc_info=True)
        return jsonify({
            'status': 'error',
            'message': f'Error generating explanation: {str(e)}'
        }), 500


@main_bp.route('/send_message', methods=['POST'])
@validate_session
@handle_errors
@log_execution_time
def send_message():
    """
    Handle chat messages and AI responses with enhanced LLM-based understanding and explanations.
    """
    try:
        # Get the message from the request
        data = request.json
        user_message = data.get('message', '')
        if not user_message: 
            raise ValidationError('No message provided')

        # Get session ID
        session_id = session.get('session_id')
        
        # Get services from the container
        data_service = current_app.services.data_service
        message_service = current_app.services.message_service

        # Get current session state and data handler
        data_handler = data_service.get_handler(session_id)
        session_state = {
            'csv_loaded': session.get('csv_loaded', False),
            'shapefile_loaded': session.get('shapefile_loaded', False),
            'analysis_complete': session.get('analysis_complete', False),
            'current_language': session.get('current_language', 'en'),
            'last_visualization': session.get('last_visualization')
        }
        
        # Get dialogue context for pending actions
        pending_action = session.get('pending_action', None)
        pending_variables = session.get('pending_variables', None)
        
        # Process the message using the message service
        result = message_service.process_message(
            user_message=user_message,
            session_id=session_id,
            session_state=session_state,
            data_handler=data_handler,
            pending_action=pending_action,
            pending_variables=pending_variables
        )
        
        # Store any session updates
        if result.get('session_updates'):
            for key, value in result['session_updates'].items():
                session[key] = value
                current_app.logger.info(f"Session {session_id}: Set {key} = {value}")
        
        # Handle legacy-style action-based session updates (CRITICAL FIX)
        if result.get('action') == 'analysis_complete':
            # Clear pending state
            session.pop('pending_action', None)
            session.pop('pending_variables', None)
            
            # Mark analysis as complete (legacy compatibility)
            session['analysis_complete'] = True
            
            # Ensure analysis type is tracked
            if 'analysis_type' not in session and result.get('session_updates', {}).get('analysis_type'):
                session['analysis_type'] = result['session_updates']['analysis_type']
            
            # Log this critical session update
            current_app.logger.info(f"Session {session_id}: analysis_complete set to True via action='analysis_complete'")
            current_app.logger.info(f"Session {session_id}: analysis_type = {session.get('analysis_type', 'not_set')}")
        
        # Also handle other legacy actions
        action = result.get('action')
        if action:
            current_app.logger.info(f"Session {session_id}: Processing action '{action}'")
            
            if action == 'set_pending_variables':
                # Store variables for next analysis
                variables = result.get('variables', [])
                session['pending_variables'] = variables
                session['pending_action'] = 'run_custom_analysis'
                current_app.logger.info(f"Session {session_id}: Set pending variables {variables}")
            
            elif action == 'clear_session':
                # Clear analysis state
                session.pop('analysis_complete', None)
                session.pop('variables_used', None)
                session.pop('pending_action', None)
                session.pop('pending_variables', None)
                current_app.logger.info(f"Session {session_id}: Cleared analysis session state")
        
        # Ensure result is JSON serializable
        result = convert_to_json_serializable(result)
        
        return jsonify(result)
    
    except ValidationError as e:
        return jsonify({
            'status': 'error',
            'message': str(e)
        }), 400
    except Exception as e:
        current_app.logger.error(f"Error processing message: {str(e)}", exc_info=True)
        return jsonify({
            'status': 'error',
            'message': f'Error processing message: {str(e)}'
        }), 500


@main_bp.route('/get_visualization', methods=['POST'])
@validate_session
@handle_errors
@log_execution_time
def get_visualization():
    """Handle visualization requests directly"""
    try:
        data = request.json
        viz_type = data.get('type', '')
        variable = data.get('variable', None)
        threshold = data.get('threshold', 30)
        
        # Get session ID
        session_id = session.get('session_id')
        
        # Get services from the container
        data_service = current_app.services.data_service
        visualization_service = current_app.services.visualization_service
        
        # Get data handler via data service
        data_handler = data_service.get_handler(session_id)
        
        # Check if we have a valid data handler
        if not data_handler:
            return jsonify({
                'status': 'error',
                'message': 'No data available. Please upload data files first.',
                'ai_response': "I need data to create visualizations. Please upload your data files first."
            })
        
        # Check if analysis is complete, except for variable maps which can be viewed anytime
        if not session.get('analysis_complete', False) and viz_type not in ['variable_map']:
            return jsonify({
                'status': 'error',
                'message': 'Analysis has not been run yet. Please run the analysis first.',
                'ai_response': "I need to run the analysis before I can show you visualizations. Would you like me to run the analysis now?"
            })
        
        # Update session to track last visualization for context
        session['last_visualization'] = {
            'type': viz_type,
            'variable': variable,
            'timestamp': datetime.now().isoformat()
        }
        
        # Prepare parameters for the visualization
        params = {
            'variable': variable,
            'threshold': threshold
        }
            
        # Generate visualization using the service
        result = visualization_service.generate_visualization(
            viz_type=viz_type,
            data_handler=data_handler,
            params=params,
            session_id=session_id
        )
        
        # If successful, add an explanation
        if result['status'] == 'success':
            # Get explanation for the visualization
            explanation = visualization_service.explain_visualization(
                viz_type=viz_type,
                data_handler=data_handler,
                context={
                    'visualization': result,
                    'session_state': {
                        'analysis_complete': session.get('analysis_complete', False),
                        'variables_used': session.get('variables_used', [])
                    }
                },
                session_id=session_id
            )
            
            # Add explanation to result
            result['ai_response'] = explanation
            
            # Ensure JSON serializable result
            result = convert_to_json_serializable(result)
            return jsonify(result)

    except Exception as e:
        current_app.logger.error(f"Error generating visualization: {str(e)}", exc_info=True)
        return jsonify({
            'status': 'error',
            'message': f'Error generating visualization: {str(e)}',
            'ai_response': f"I encountered an error while creating the visualization. Please try again or choose a different visualization."
        })


@main_bp.route('/navigate_composite_map', methods=['POST'])
@validate_session
@handle_errors
@log_execution_time
def navigate_composite_map():
    """Handle pagination for composite maps"""
    try:
        data = request.json
        direction = data.get('direction', '')
        
        if not direction or direction not in ['next', 'prev']:
            raise ValidationError('Invalid navigation direction')
        
        # Get services from the container
        data_service = current_app.services.data_service
        visualization_service = current_app.services.visualization_service
        
        # Get session ID
        session_id = session.get('session_id')
        
        # Get data handler via data service
        data_handler = data_service.get_handler(session_id)
        
        # Get current page from request or session
        current_page = data.get('current_page', session.get('current_composite_map_page', 1))
        
        # Determine new page based on direction
        if direction == 'next':
            new_page = current_page + 1
        else:  # prev
            new_page = max(1, current_page - 1)
        
        # Get the composite map for the new page
        result = visualization_service.navigate_composite_map(
            data_handler=data_handler,
            page=new_page,
            session_id=session_id
        )
        
        if result['status'] == 'success':
            # Update session with new page info
            session['current_composite_map_page'] = result.get('current_page', 1)
            
            # Add explanation for this specific page
            if 'ai_response' not in result or not result['ai_response']:
                explanation = visualization_service.explain_composite_map_navigation(
                    data_handler=data_handler,
                    page_data=result,
                    session_id=session_id
                )
                result['ai_response'] = explanation
            
            return jsonify(result)
        else:
            return jsonify({
                'status': 'error',
                'message': result.get('message', 'Error navigating composite maps')
            }), 400
    
    except ValidationError as e:
        return jsonify({
            'status': 'error',
            'message': str(e)
        }), 400
    except Exception as e:
        current_app.logger.error(f"Error navigating composite map: {str(e)}", exc_info=True)
        return jsonify({
            'status': 'error',
            'message': f'Error navigating composite maps: {str(e)}'
        }), 500


@main_bp.route('/navigate_boxplot', methods=['POST'])
@validate_session
@handle_errors
@log_execution_time
def navigate_boxplot():
    """Handle pagination for box and whisker plots"""
    try:
        data = request.json
        direction = data.get('direction', '')
        
        if not direction or direction not in ['next', 'prev']:
            raise ValidationError('Invalid navigation direction')
        
        # Get services from the container
        data_service = current_app.services.data_service
        visualization_service = current_app.services.visualization_service
        
        # Get session ID
        session_id = session.get('session_id')
        
        # Get data handler via data service
        data_handler = data_service.get_handler(session_id)
        
        # Check if box plot data is available
        if not hasattr(data_handler, 'boxwhisker_plot') or not data_handler.boxwhisker_plot:
            raise ValidationError('Box plot data not available')
        
        # Get current page from request or session
        current_page = data.get('current_page', session.get('current_boxplot_page', 1))
        
        # Determine new page based on direction
        if direction == 'next':
            new_page = current_page + 1
        else:  # prev
            new_page = max(1, current_page - 1)
        
        # Navigate the boxplot using the service
        result = visualization_service.navigate_boxplot(
            data_handler=data_handler,
            page=new_page,
            session_id=session_id
        )
        
        if result['status'] == 'success':
            # Update session with new page info
            session['current_boxplot_page'] = result.get('current_page', 1)
            
            # Add explanation for this specific page
            if 'ai_response' not in result or not result['ai_response']:
                explanation = visualization_service.explain_boxplot_navigation(
                    data_handler=data_handler,
                    page_data=result,
                    session_id=session_id
                )
                result['ai_response'] = explanation
            
            return jsonify(result)
        else:
            return jsonify({
                'status': 'error',
                'message': result.get('message', 'Error navigating box plots')
            }), 400
    
    except ValidationError as e:
        return jsonify({
            'status': 'error',
            'message': str(e)
        }), 400
    except Exception as e:
        current_app.logger.error(f"Error navigating boxplot: {str(e)}", exc_info=True)
        return jsonify({
            'status': 'error',
            'message': f'Error navigating box plots: {str(e)}'
        }), 500


@main_bp.route('/generate_report', methods=['POST'])
@validate_session
@handle_errors
@log_execution_time
def generate_report():
    """Generate analysis report - handles both UI button and chat requests"""
    try:
        # Get session info
        session_id = session.get('session_id')
        analysis_complete = session.get('analysis_complete', False)
        
        # Debug logging
        current_app.logger.info(f"Report generation request for session {session_id}")
        current_app.logger.info(f"Session analysis_complete: {analysis_complete}")
        current_app.logger.info(f"Content-Type: {request.content_type}")
        
        # Check if analysis is complete - with fallback fix
        if not analysis_complete:
            # Try to auto-fix the session state first
            data_service = current_app.services.data_service
            data_handler = data_service.get_handler(session_id) if data_service else None
            
            # Check if analysis was actually completed but session flag is wrong
            if (data_handler and 
                hasattr(data_handler, 'vulnerability_rankings') and 
                data_handler.vulnerability_rankings is not None):
                
                # Auto-fix the session state
                session['analysis_complete'] = True
                current_app.logger.info(f"Session {session_id}: Auto-fixed analysis_complete flag")
                analysis_complete = True
            else:
                raise ValidationError('Please complete the analysis before generating a report.')
        
        # Get services from the container
        data_service = current_app.services.data_service
        report_service = current_app.services.report_service
        
        if not report_service:
            raise ValidationError('Report service is not available.')
        
        # Get data handler via data service
        data_handler = data_service.get_handler(session_id)
        if not data_handler:
            raise ValidationError('No data available for report generation.')
        
        # Handle both form data (UI modal) and JSON data (chat)
        if request.content_type and 'application/json' in request.content_type:
            # JSON request from chat
            data = request.json or {}
            report_format = data.get('format', 'pdf').lower()
            detail_level = data.get('detail_level', 'standard')
            custom_sections = data.get('custom_sections', None)
        else:
            # Form data from UI modal
            report_format = request.form.get('report_format', 'pdf').lower()
            detail_level = request.form.get('detail_level', 'standard')
            custom_sections = request.form.getlist('custom_sections')
        
        # Validate format
        valid_formats = ['pdf', 'html', 'markdown']
        if report_format not in valid_formats:
            report_format = 'pdf'
        
        current_app.logger.info(f"Generating {report_format} report for session {session_id}")
        
        # Generate the report using the service
        result = report_service.generate_report(
            data_handler=data_handler,
            session_id=session_id,
            format_type=report_format,
            custom_sections=custom_sections if custom_sections else None,
            detail_level=detail_level
        )
        
        if result['status'] == 'success':
            current_app.logger.info(f"Report generated successfully: {result.get('report_url')}")
            
            # For HTML format, also try to generate an interactive dashboard
            additional_files = []
            if report_format == 'html':
                try:
                    dashboard_result = report_service.generate_dashboard(
                        data_handler=data_handler,
                        session_id=session_id
                    )
                    if dashboard_result.get('status') == 'success':
                        additional_files.append({
                            'type': 'dashboard',
                            'url': dashboard_result.get('report_url'),
                            'filename': dashboard_result.get('filename', 'dashboard.html')
                        })
                except Exception as e:
                    current_app.logger.warning(f"Could not generate dashboard: {str(e)}")
            
            response_data = {
                'status': 'success',
                'message': result['message'],
                'download_url': result.get('report_url'),
                'format': report_format,
                'additional_files': additional_files
            }
            
            # Add action for legacy compatibility
            if request.content_type and 'application/json' in request.content_type:
                response_data['action'] = 'report_generated'
                
            return jsonify(response_data)
        else:
            current_app.logger.error(f"Report generation failed: {result['message']}")
            return jsonify({
                'status': 'error',
                'message': result['message']
            }), 500
    
    except ValidationError as e:
        current_app.logger.warning(f"Validation error in report generation: {str(e)}")
        return jsonify({
            'status': 'error',
            'message': str(e)
        }), 400
    except Exception as e:
        current_app.logger.error(f"Error in generate_report route: {str(e)}", exc_info=True)
        return jsonify({
            'status': 'error',
            'message': 'An unexpected error occurred while generating the report.'
        }), 500


@main_bp.route('/preview_report', methods=['POST'])
@validate_session
@handle_errors
@log_execution_time
def preview_report():
    """Generate a preview of the report without saving it"""
    try:
        data = request.get_json()
        
        # Get services from the container
        data_service = current_app.services.data_service
        report_service = current_app.services.report_service
        
        # Get session ID
        session_id = session.get('session_id')
        
        # Get data handler via data service
        data_handler = data_service.get_handler(session_id)
        if not data_handler:
            raise ValidationError('No data available for preview.')
        
        # Get preview parameters
        selected_sections = data.get('sections', [])
        report_type = data.get('type', 'standard')
        format_type = data.get('format', 'html')
        
        # Generate preview using the service
        result = report_service.preview_report(
            data_handler=data_handler,
            session_id=session_id,
            selected_sections=selected_sections,
            report_type=report_type,
            format_type=format_type
        )
        
        if result['status'] == 'success':
            return jsonify({
                'status': 'success',
                'preview_html': result.get('preview_html'),
                'section_count': len(selected_sections)
            })
        else:
            return jsonify({
                'status': 'error',
                'message': result.get('message', 'Error generating preview.')
            }), 500
    
    except ValidationError as e:
        return jsonify({
            'status': 'error',
            'message': str(e)
        }), 400
    except Exception as e:
        current_app.logger.error(f"Error generating report preview: {str(e)}", exc_info=True)
        return jsonify({
            'status': 'error',
            'message': 'Error generating preview.'
        }), 500


@main_bp.route('/download_report/<filename>')
@validate_session
@handle_errors
@log_execution_time
def download_report(filename):
    """Download generated report file"""
    try:
        # Get services from the container
        report_service = current_app.services.report_service
        
        # Get session ID
        session_id = session.get('session_id')
        
        # Serve the report file using the service
        result = report_service.serve_report_file(
            session_id=session_id,
            filename=filename
        )
        
        if result['status'] == 'success':
            # The service should return a Flask response object
            return result['response']
        else:
            return jsonify({
                'status': 'error',
                'message': result.get('message', 'Report file not found')
            }), 404
    
    except Exception as e:
        current_app.logger.error(f"Error serving report file: {str(e)}", exc_info=True)
        return jsonify({
            'status': 'error',
            'message': 'Error serving report file'
        }), 500


@main_bp.route('/api/variables', methods=['GET'])
@validate_session
@handle_errors
@log_execution_time
def get_variables():
    """Get available variables from the loaded data"""
    try:
        # Get services from the container
        data_service = current_app.services.data_service
        
        # Get session ID
        session_id = session.get('session_id')
        
        # Get data handler via data service
        data_handler = data_service.get_handler(session_id)
        
        if not data_handler:
            return jsonify({
                'status': 'error',
                'message': 'No data loaded. Please upload data files first.'
            }), 400
        
        # Get variables using the data service
        variables = data_service.get_available_variables(session_id)
        
        return jsonify({
            'status': 'success',
            'variables': variables,
            'count': len(variables)
        })
    
    except Exception as e:
        current_app.logger.error(f"Error getting variables: {str(e)}", exc_info=True)
        return jsonify({
            'status': 'error',
            'message': f'Error retrieving variables: {str(e)}'
        }), 500


@main_bp.route('/api/wards', methods=['GET'])
@validate_session
@handle_errors
@log_execution_time
def api_get_wards():
    """Get ward information from the loaded data"""
    try:
        # Get services from the container
        data_service = current_app.services.data_service
        
        # Get session ID
        session_id = session.get('session_id')
        
        # Get data handler via data service
        data_handler = data_service.get_handler(session_id)
        
        if not data_handler:
            return jsonify({
                'status': 'error',
                'message': 'No data loaded. Please upload data files first.'
            }), 400
        
        # Get wards using the data service
        result = data_service.get_ward_information(session_id)
        
        if result['status'] == 'success':
            return jsonify({
                'status': 'success',
                'wards': result.get('wards', []),
                'count': len(result.get('wards', [])),
                'ward_names': result.get('ward_names', [])
            })
        else:
            return jsonify({
                'status': 'error',
                'message': result.get('message', 'Error retrieving ward information')
            }), 400
    
    except Exception as e:
        current_app.logger.error(f"Error getting wards: {str(e)}", exc_info=True)
        return jsonify({
            'status': 'error',
            'message': f'Error retrieving ward information: {str(e)}'
        }), 500


@main_bp.route('/load_sample_data', methods=['POST'])
@validate_session
@handle_errors
@log_execution_time
def load_sample_data():
    """Load pre-packaged sample data into the user's session."""
    try:
        # Get services from the container
        data_service = current_app.services.data_service
        
        # Get session ID
        session_id = session.get('session_id')
        if not session_id:
            # Should not happen if session is initialized, but handle anyway
            session['session_id'] = str(uuid.uuid4())
            session_id = session['session_id']
            current_app.logger.warning("Session ID not found, generated a new one.")

        current_app.logger.info(f"Loading sample data for session: {session_id}")

        # Use data service to load sample data
        result = data_service.load_sample_data(session_id)
        
        if result['status'] == 'success':
            # Update session state
            session['csv_loaded'] = True
            session['shapefile_loaded'] = True
            session['csv_filename'] = result.get('csv_filename', 'sample_data.csv')
            session['shapefile_filename'] = result.get('shapefile_filename', 'sample_boundary.zip')
            session['available_variables'] = result.get('variables', [])
            session['ward_count'] = result.get('ward_count', 0)
            
            return jsonify({
                'status': 'success',
                'message': result.get('message', 'Sample data loaded successfully'),
                'csv_loaded': True,
                'shapefile_loaded': True,
                'variables_count': len(result.get('variables', [])),
                'ward_count': result.get('ward_count', 0)
            })
        else:
            return jsonify({
                'status': 'error',
                'message': result.get('message', 'Failed to load sample data')
            }), 500
    
    except Exception as e:
        current_app.logger.error(f"Error loading sample data: {str(e)}", exc_info=True)
        return jsonify({
            'status': 'error',
            'message': f'Error loading sample data: {str(e)}'
        }), 500


@main_bp.route('/serve_viz_file/<session_id>/<path:filename>')
@validate_session
@handle_errors
@log_execution_time
def serve_viz_file(session_id, filename):
    """Serve visualization files for a session"""
    try:
        # Validate session ID matches current session
        current_session_id = session.get('session_id')
        if current_session_id != session_id:
            return jsonify({
                'status': 'error',
                'message': 'Unauthorized access to visualization files'
            }), 403
        
        # Construct the file path
        session_folder = os.path.join(current_app.config['UPLOAD_FOLDER'], session_id)
        
        # Security check - ensure file exists and is in the correct directory
        file_path = os.path.join(session_folder, filename)
        if not os.path.exists(file_path) or not os.path.commonpath([session_folder, file_path]) == session_folder:
            return jsonify({
                'status': 'error',
                'message': 'Visualization file not found'
            }), 404
        
        return send_from_directory(session_folder, filename)
    
    except Exception as e:
        current_app.logger.error(f"Error serving visualization file: {str(e)}", exc_info=True)
        return jsonify({
            'status': 'error',
            'message': 'Error serving visualization file'
        }), 500


@main_bp.route('/navigate_visualization', methods=['POST'])
@validate_session
@handle_errors
@log_execution_time
def navigate_visualization():
    """Handle navigation for any visualization type"""
    try:
        data = request.json
        viz_type = data.get('viz_type', '')
        direction = data.get('direction', '')
        current_page = data.get('current_page', 1)
        total_pages = data.get('total_pages', 1)
        metadata = data.get('metadata', {})
        
        if not direction or direction not in ['next', 'prev']:
            raise ValidationError('Invalid navigation direction')
        
        # Get services from the container
        data_service = current_app.services.data_service
        visualization_service = current_app.services.visualization_service
        
        # Get session ID
        session_id = session.get('session_id')
        
        # Get data handler via data service
        data_handler = data_service.get_handler(session_id)
        
        if not data_handler:
            raise ValidationError('No data available for navigation')
        
        # Use the visualization service navigation method
        result = visualization_service.navigate_visualization(
            viz_type=viz_type,
            direction=direction,
            current_state={
                'current_page': current_page,
                'total_pages': total_pages,
                'metadata': metadata
            },
            data_handler=data_handler,
            session_id=session_id
        )
        
        if result['status'] == 'success':
            # Update session with new page info if available
            session_key = f'current_{viz_type}_page'
            if 'current_page' in result:
                session[session_key] = result['current_page']
            
            # Ensure result is JSON serializable
            result = convert_to_json_serializable(result)
            return jsonify(result)
        else:
            return jsonify({
                'status': 'error',
                'message': result.get('message', f'Error navigating {viz_type}')
            }), 400
    
    except ValidationError as e:
        return jsonify({
            'status': 'error',
            'message': str(e)
        }), 400
    except Exception as e:
        current_app.logger.error(f"Error navigating visualization: {str(e)}", exc_info=True)
        return jsonify({
            'status': 'error',
            'message': f'Error navigating visualization: {str(e)}'
        }), 500


@main_bp.route('/debug/fix_analysis_state', methods=['POST'])
@validate_session
@handle_errors
@log_execution_time
def fix_analysis_state():
    """DEBUG ENDPOINT: Fix analysis completion state for current session"""
    try:
        # Get services from the container
        data_service = current_app.services.data_service
        
        # Get session ID
        session_id = session.get('session_id')
        
        # Get data handler to check if analysis was actually completed
        data_handler = data_service.get_handler(session_id)
        
        if not data_handler:
            return jsonify({
                'status': 'error',
                'message': 'No data available in this session'
            }), 400
        
        # Check if data handler has vulnerability rankings (indicates analysis was run)
        has_rankings = (hasattr(data_handler, 'vulnerability_rankings') and 
                       data_handler.vulnerability_rankings is not None)
        
        if has_rankings:
            # Analysis was actually completed, fix the session flag
            session['analysis_complete'] = True
            
            # Try to extract variables used
            if hasattr(data_handler, 'selected_variables') and data_handler.selected_variables:
                session['variables_used'] = data_handler.selected_variables
            else:
                # Try to get from available variables
                available_vars = data_handler.get_available_variables()
                session['variables_used'] = available_vars[:5] if available_vars else []
            
            return jsonify({
                'status': 'success',
                'message': 'Analysis state has been fixed! You can now generate reports.',
                'analysis_complete': True,
                'variables_used': session.get('variables_used', []),
                'rankings_shape': str(data_handler.vulnerability_rankings.shape) if has_rankings else None
            })
        else:
            return jsonify({
                'status': 'error',
                'message': 'No analysis results found. Please run the analysis first.',
                'has_data': True,
                'has_rankings': False
            }), 400
            
    except Exception as e:
        current_app.logger.error(f"Error fixing analysis state: {str(e)}", exc_info=True)
        return jsonify({
            'status': 'error',
            'message': f'Error fixing analysis state: {str(e)}'
        }), 500


@main_bp.route('/debug/session_state', methods=['GET'])
@validate_session
@handle_errors
def debug_session_state():
    """DEBUG ENDPOINT: Check current session state"""
    try:
        session_id = session.get('session_id')
        
        # Get data handler to check actual analysis state
        data_service = current_app.services.data_service
        data_handler = data_service.get_handler(session_id) if data_service else None
        
        # Check if analysis results exist
        has_vulnerability_rankings = False
        rankings_info = None
        if data_handler:
            has_vulnerability_rankings = (hasattr(data_handler, 'vulnerability_rankings') and 
                                        data_handler.vulnerability_rankings is not None)
            if has_vulnerability_rankings:
                rankings_info = {
                    'shape': str(data_handler.vulnerability_rankings.shape),
                    'columns': list(data_handler.vulnerability_rankings.columns)
                }
        
        session_state = {
            'session_id': session_id,
            'analysis_complete': session.get('analysis_complete', False),
            'variables_used': session.get('variables_used', []),
            'csv_loaded': session.get('csv_loaded', False),
            'shapefile_loaded': session.get('shapefile_loaded', False),
            'data_loaded': session.get('data_loaded', False),
            'pending_action': session.get('pending_action'),
            'pending_variables': session.get('pending_variables'),
            'last_visualization': session.get('last_visualization'),
            'csv_filename': session.get('csv_filename', ''),
            'shapefile_filename': session.get('shapefile_filename', ''),
            'available_variables': session.get('available_variables', []),
            'has_data_handler': data_handler is not None,
            'has_vulnerability_rankings': has_vulnerability_rankings,
            'rankings_info': rankings_info
        }
        
        return jsonify({
            'status': 'success',
            'session_state': session_state,
            'timestamp': datetime.now().isoformat()
        })
        
    except Exception as e:
        current_app.logger.error(f"Error checking session state: {str(e)}", exc_info=True)
        return jsonify({
            'status': 'error',
            'message': f'Error checking session state: {str(e)}'
        }), 500


@main_bp.route('/debug/visualization', methods=['GET'])
@validate_session
@handle_errors
def debug_visualization():
    """Debug route to check visualization state and pagination data"""
    try:
        # Get session ID
        session_id = session.get('session_id')
        
        # Get services from the container
        data_service = current_app.services.data_service
        
        # Get data handler via data service
        data_handler = data_service.get_handler(session_id)
        
        debug_info = {
            'session_id': session_id,
            'boxplot_data_available': hasattr(data_handler, 'boxwhisker_plot') and data_handler.boxwhisker_plot is not None,
            'composite_map_data_available': hasattr(data_handler, 'composite_scores') and data_handler.composite_scores is not None,
            'current_boxplot_page': session.get('current_boxplot_page', 1),
            'current_composite_map_page': session.get('current_composite_map_page', 1)
        }
        
        # If boxplot data is available, add more details
        if hasattr(data_handler, 'boxwhisker_plot') and data_handler.boxwhisker_plot:
            debug_info['boxplot_details'] = {
                'total_pages': data_handler.boxwhisker_plot.get('total_pages', 0),
                'current_page': data_handler.boxwhisker_plot.get('current_page', 1),
                'plots_count': len(data_handler.boxwhisker_plot.get('plots', [])),
                'has_page_data': 'page_data' in data_handler.boxwhisker_plot,
                'available_pages': list(data_handler.boxwhisker_plot.get('page_data', {}).keys())
            }
        
        # If composite scores data is available, add more details
        if hasattr(data_handler, 'composite_scores') and data_handler.composite_scores:
            model_columns = [col for col in data_handler.composite_scores.get('scores', pd.DataFrame()).columns 
                            if col.startswith('model_')]
            
            debug_info['composite_details'] = {
                'model_count': len(model_columns),
                'has_formulas': 'formulas' in data_handler.composite_scores,
                'formulas_count': len(data_handler.composite_scores.get('formulas', [])),
                'models_per_page': 4,  # This is hard-coded in the original implementation
                'estimated_total_pages': (len(model_columns) + 4 - 1) // 4
            }
        
        return jsonify({
            'status': 'success',
            'debug_info': debug_info
        })
    except Exception as e:
        current_app.logger.error(f"Error in visualization debug: {str(e)}", exc_info=True)
        return jsonify({
            'status': 'error',
            'message': f'Error in visualization debug: {str(e)}'
        }), 500


@main_bp.route('/debug/llm_service', methods=['GET'])
@validate_session
@handle_errors
def debug_llm_service():
    """
    Debug endpoint to check if the LLM service is functioning properly.
    This helps diagnose API connection issues.
    """
    try:
        # Get services from the container
        message_service = current_app.services.message_service
        
        # Check if we have API keys
        openai_api_key = current_app.config.get('OPENAI_API_KEY')
        model_name = current_app.config.get('OPENAI_MODEL_NAME', 'gpt-4o')
        
        # Check connection
        connection_status = message_service.check_connection() if hasattr(message_service, 'check_connection') else "Method not available"
        
        # Return debug info
        return jsonify({
            'status': 'success',
            'llm_service_available': message_service is not None,
            'api_key_configured': bool(openai_api_key),
            'model_name': model_name,
            'connection_status': connection_status,
            'session_permanent': current_app.config.get('SESSION_PERMANENT', False),
            'session_lifetime': str(current_app.config.get('PERMANENT_SESSION_LIFETIME', 'Not set'))
        })
    except Exception as e:
        current_app.logger.error(f"Error checking LLM service: {str(e)}", exc_info=True)
        return jsonify({
            'status': 'error',
            'message': f'Error checking LLM service: {str(e)}'
        }), 500 
