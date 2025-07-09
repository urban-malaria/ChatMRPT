# app/web/routes/analysis_routes.py
"""
Analysis Routes module for data analysis operations.

This module contains the analysis-related routes for the ChatMRPT web application:
- Main analysis execution (run_analysis)
- Variable selection explanation  
- AI chat message processing (send_message)
- Analysis state management
"""

import logging
import time
import traceback
import json
from datetime import datetime
from flask import Blueprint, session, request, current_app, jsonify, Response

from ...core.decorators import handle_errors, log_execution_time, validate_session
from ...core.exceptions import ValidationError
from ...core.utils import convert_to_json_serializable

logger = logging.getLogger(__name__)

# Create the analysis routes blueprint
analysis_bp = Blueprint('analysis', __name__)


@analysis_bp.route('/run_analysis', methods=['POST'])
@validate_session
@handle_errors
@log_execution_time
def run_analysis():
    """Run the analysis directly (used for API calls, not main chat flow)"""
    try:
        # Get session ID
        session_id = session.get('session_id', 'default')
        
        # Debug logging
        logger.info(f"[DEBUG] run_analysis: session_id={session_id}")
        data_service = current_app.services.data_service
        data_handler = data_service.get_handler(session_id)
        logger.info(f"[DEBUG] run_analysis: data_handler exists: {data_handler is not None}")
        if data_handler:
            has_df = hasattr(data_handler, 'df') and data_handler.df is not None
            logger.info(f"[DEBUG] run_analysis: data_handler.df exists: {has_df}")
        
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
                'high_risk_wards': high_risk_wards[:10],
                'medium_risk_wards': medium_risk_wards[:10],
                'low_risk_wards': low_risk_wards[:10]
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
        logger.error(f"Error running analysis: {str(e)}", exc_info=True)
        return jsonify({
            'status': 'error',
            'message': f'Error running analysis: {str(e)}'
        }), 500


@analysis_bp.route('/explain_variable_selection', methods=['GET'])
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
        logger.error(f"Error explaining variable selection: {str(e)}", exc_info=True)
        return jsonify({
            'status': 'error',
            'message': f'Error generating explanation: {str(e)}'
        }), 500


@analysis_bp.route('/send_message', methods=['POST'])
@validate_session
@handle_errors
@log_execution_time
def send_message():
    """
    Handle chat messages with Request Interpreter (NEW TOOL-BASED SYSTEM).
    """
    print("🔥 MESSAGE RECEIVED - USING NEW REQUEST INTERPRETER SYSTEM!")
    import sys
    sys.stdout.flush()
    
    try:
        # Get the message from the request
        data = request.json
        user_message = data.get('message', '')
        if not user_message: 
            raise ValidationError('No message provided')

        # Get session ID
        session_id = session.get('session_id')
        
        # 🎯 COMPREHENSIVE RESPONSE TIME TRACKING - CRITICAL FOR DEMO ANALYTICS
        request_start_time = time.time()
        message_start_time = time.time()
        if hasattr(current_app, 'services') and current_app.services.interaction_logger:
            interaction_logger = current_app.services.interaction_logger
            
            # Log incoming user message with metadata
            interaction_logger.log_message(
                session_id=session_id,
                sender='user',
                content=user_message,
                intent=None,  # Will be filled after request interpretation
                entities={
                    'message_length': len(user_message),
                    'timestamp': message_start_time,
                    'session_message_count': session.get('message_count', 0) + 1,
                    'request_endpoint': '/send_message'
                }
            )
            
            # Track user journey milestone
            interaction_logger.log_analysis_event(
                session_id=session_id,
                event_type='user_interaction',
                details={
                    'action': 'message_sent',
                    'message_type': 'chat_request',
                    'session_duration': time.time() - session.get('session_start_time', time.time()),
                    'is_follow_up': session.get('message_count', 0) > 0
                },
                success=True
            )
            
            # Update session message counter
            session['message_count'] = session.get('message_count', 0) + 1
        
        # Get Request Interpreter service
        try:
            request_interpreter = current_app.services.request_interpreter
            if request_interpreter is None:
                logger.error("Request Interpreter not available")
                return jsonify({
                    'status': 'error',
                    'message': 'Request processing system not available'
                }), 500
        except Exception as e:
            logger.error(f"Error getting Request Interpreter: {str(e)}")
            return jsonify({
                'status': 'error',
                'message': 'Error accessing request processing system'
            }), 500
        
        # Process message with Request Interpreter
        logger.info(f"Processing message with Request Interpreter: '{user_message[:100]}...'")
        processing_start_time = time.time()
        response = request_interpreter.process_message(user_message, session_id)
        processing_duration = time.time() - processing_start_time
        
        # 🎯 LOG AI RESPONSE - CRITICAL FOR DEMO ANALYTICS
        if hasattr(current_app, 'services') and current_app.services.interaction_logger:
            interaction_logger = current_app.services.interaction_logger
            
            # Calculate comprehensive response timing
            total_response_time = time.time() - request_start_time
            overhead_time = total_response_time - processing_duration
            
            # Log AI response with comprehensive timing metadata
            ai_response_content = response.get('response', 'Request processed successfully')
            interaction_logger.log_message(
                session_id=session_id,
                sender='assistant',
                content=ai_response_content,
                intent=response.get('intent_type'),
                entities={
                    'response_length': len(ai_response_content),
                    'processing_time_seconds': processing_duration,
                    'total_response_time_seconds': total_response_time,
                    'overhead_time_seconds': overhead_time,
                    'response_efficiency': round(processing_duration / total_response_time * 100, 1) if total_response_time > 0 else 100,
                    'tools_used': response.get('tools_used', []),
                    'tools_count': len(response.get('tools_used', [])),
                    'visualizations_created': len(response.get('visualizations', [])),
                    'status': response.get('status', 'success'),
                    'timestamp': time.time(),
                    'performance_category': 'fast' if total_response_time < 5 else 'medium' if total_response_time < 15 else 'slow'
                }
            )
            
            # Log comprehensive response timing for demo analytics
            detailed_timing = response.get('timing_breakdown', {})
            performance_metrics = response.get('performance_metrics', {})
            
            interaction_logger.log_analysis_event(
                session_id=session_id,
                event_type='response_timing',
                details={
                    'total_response_time_seconds': total_response_time,
                    'processing_time_seconds': processing_duration,
                    'overhead_time_seconds': overhead_time,
                    'response_efficiency_percent': round(processing_duration / total_response_time * 100, 1) if total_response_time > 0 else 100,
                    'performance_category': 'fast' if total_response_time < 5 else 'medium' if total_response_time < 15 else 'slow',
                    'message_length': len(user_message),
                    'response_length': len(ai_response_content),
                    'complexity_score': len(response.get('tools_used', [])) * 2 + len(response.get('visualizations', [])),
                    'endpoint': '/send_message',
                    # Enhanced timing breakdown
                    'timing_breakdown': {
                        'context_retrieval_ms': detailed_timing.get('context_retrieval', 0) * 1000,
                        'prompt_building_ms': detailed_timing.get('prompt_building', 0) * 1000,
                        'llm_processing_ms': detailed_timing.get('llm_processing', 0) * 1000,
                        'tool_execution_ms': detailed_timing.get('tool_execution', 0) * 1000,
                        'response_formatting_ms': detailed_timing.get('response_formatting', 0) * 1000,
                        'total_duration_ms': detailed_timing.get('total_duration', 0) * 1000
                    },
                    'performance_metrics': {
                        'llm_percentage': performance_metrics.get('llm_percentage', 0),
                        'tool_percentage': performance_metrics.get('tool_percentage', 0),
                        'context_percentage': performance_metrics.get('context_percentage', 0),
                        'bottleneck': performance_metrics.get('bottleneck', 'unknown')
                    }
                },
                success=True
            )
            
            # Log tools usage for analytics
            tools_used = response.get('tools_used', [])
            if tools_used:
                interaction_logger.log_analysis_event(
                    session_id=session_id,
                    event_type='tools_execution',
                    details={
                        'tools_used': tools_used,
                        'execution_time_seconds': processing_duration,
                        'total_response_time_seconds': total_response_time,
                        'request_type': response.get('intent_type', 'unknown'),
                        'success_rate': 1.0 if response.get('status') == 'success' else 0.0,
                        'user_message_trigger': user_message[:100] + '...' if len(user_message) > 100 else user_message,
                        'performance_impact': round((processing_duration / total_response_time) * 100, 1) if total_response_time > 0 else 0
                    },
                    success=response.get('status') == 'success'
                )
        
        # Format response for frontend
        formatted_response = {
            'status': response.get('status', 'success'),
            'message': response.get('response', 'Request processed successfully'),
            'response': response.get('response', 'Request processed successfully'),  # Frontend expects this field
            'explanations': response.get('explanations', []),
            'data_summary': response.get('data_summary'),
            'tools_used': response.get('tools_used', []),
            'intent_type': response.get('intent_type'),
            'processing_time': f"{processing_duration:.2f}s",
            'total_response_time': f"{total_response_time:.2f}s",
            'response_efficiency': f"{round(processing_duration / total_response_time * 100, 1) if total_response_time > 0 else 100}%"
        }
        
        # Only include visualizations if they actually exist and have valid content
        visualizations = response.get('visualizations', [])
        if visualizations and len(visualizations) > 0:
            # Filter out empty or invalid visualizations
            valid_visualizations = [
                viz for viz in visualizations 
                if isinstance(viz, dict) and (viz.get('url') or viz.get('path') or viz.get('html'))
            ]
            if valid_visualizations:
                formatted_response['visualizations'] = valid_visualizations
        
        # Update session state based on tools used
        tools_used = response.get('tools_used', [])
        
        if any(tool in tools_used for tool in ['run_composite_analysis', 'run_pca_analysis', 'runcompleteanalysis']):
            session['analysis_complete'] = True
            if 'runcompleteanalysis' in tools_used:
                session['analysis_type'] = 'dual_method'
            elif 'run_composite_analysis' in tools_used:
                session['analysis_type'] = 'composite'
            else:
                session['analysis_type'] = 'pca'
            logger.info(f"Session {session_id}: Analysis completed via Request Interpreter ({session['analysis_type']})")
        
        # Clear any pending actions if analysis was run
        if any(tool in tools_used for tool in ['run_composite_analysis', 'run_pca_analysis', 'runcompleteanalysis']):
                session.pop('pending_action', None)
                session.pop('pending_variables', None)
        
        # Ensure response is JSON serializable
        formatted_response = convert_to_json_serializable(formatted_response)
        
        logger.info(f"Request Interpreter response sent: status={formatted_response.get('status')}, tools={len(tools_used)}")
        return jsonify(formatted_response)
    
    except ValidationError as e:
        # 🎯 LOG VALIDATION ERRORS - DEMO INSIGHTS
        if hasattr(current_app, 'services') and current_app.services.interaction_logger:
            interaction_logger = current_app.services.interaction_logger
            interaction_logger.log_error(
                session_id=session.get('session_id'),
                error_type='ValidationError',
                error_message=str(e),
                stack_trace=traceback.format_exc()
            )
        
        return jsonify({
            'status': 'error',
            'message': str(e)
        }), 400
    except Exception as e:
        # 🎯 LOG SYSTEM ERRORS - CRITICAL FOR DEMO MONITORING
        session_id = session.get('session_id')
        error_details = {
            'error_type': type(e).__name__,
            'error_message': str(e),
            'endpoint': '/send_message',
            'user_message': locals().get('user_message', 'Unknown')[:100],
            'processing_stage': 'request_interpreter_processing',
            'timestamp': time.time()
        }
        
        if hasattr(current_app, 'services') and current_app.services.interaction_logger:
            interaction_logger = current_app.services.interaction_logger
            interaction_logger.log_error(
                session_id=session_id,
                error_type=type(e).__name__,
                error_message=str(e),
                stack_trace=traceback.format_exc()
            )
            
            # Log error as analysis event for demo monitoring
            interaction_logger.log_analysis_event(
                session_id=session_id,
                event_type='system_error',
                details=error_details,
                success=False
            )
        
        logger.error(f"Error processing message with Request Interpreter: {str(e)}", exc_info=True)
        return jsonify({
            'status': 'error',
            'message': f'Error processing message: {str(e)}'
        }), 500


@analysis_bp.route('/send_message_streaming', methods=['POST'])
@validate_session
@handle_errors
@log_execution_time
def send_message_streaming():
    """
    Handle chat messages with streaming response for better UX.
    """
    print("🔥 STREAMING MESSAGE RECEIVED - USING NEW STREAMING SYSTEM!")
    import sys
    sys.stdout.flush()
    
    try:
        # Get the message from the request
        data = request.json
        user_message = data.get('message', '')
        if not user_message: 
            raise ValidationError('No message provided')

        # Get session ID
        session_id = session.get('session_id')
        
        # Get Request Interpreter service
        try:
            request_interpreter = current_app.services.request_interpreter
            if request_interpreter is None:
                logger.error("Request Interpreter not available")
                return jsonify({
                    'status': 'error',
                    'message': 'Request processing system not available'
                }), 500
        except Exception as e:
            logger.error(f"Error getting Request Interpreter: {str(e)}")
            return jsonify({
                'status': 'error',
                'message': 'Error accessing request processing system'
            }), 500
        
        # Capture Flask context for use in generator
        app = current_app._get_current_object()
        flask_session = dict(session)  # Copy session data
        
        # Make session available in streaming context
        def setup_streaming_context():
            # Set up Flask session context for streaming
            for key, value in flask_session.items():
                session[key] = value
        
        # Use streaming response
        def generate():
            try:
                with app.app_context():
                    # Set up session context for streaming
                    setup_streaming_context()
                    
                    logger.info(f"Processing streaming message: '{user_message[:100]}...'")
                    
                    # Use the full conversational data access system for streaming
                    # This ensures users get the same capabilities in streaming mode
                    logger.info("Using full conversational data access system for streaming")
                    result = request_interpreter.process_message(user_message, session_id)
                    
                    # Simulate streaming by breaking response into chunks
                    response_text = result.get('response', '')
                    words = response_text.split()
                    chunk_size = 5  # words per chunk
                    
                    for i in range(0, len(words), chunk_size):
                        chunk_words = words[i:i + chunk_size]
                        chunk_text = ' '.join(chunk_words)
                        
                        chunk = {
                            'content': chunk_text + (' ' if i + chunk_size < len(words) else ''),
                            'status': 'streaming'
                        }
                        chunk_json = json.dumps(chunk)
                        logger.debug(f"Sending chunk: {chunk_json}")
                        yield f"data: {chunk_json}\n\n"
                        
                        # Small delay for better UX
                        import time
                        time.sleep(0.1)
                    
                    # Send final chunk with complete data
                    final_chunk = {
                        'content': '',  # No additional content
                        'status': result.get('status', 'success'),
                        'visualizations': result.get('visualizations', []),
                        'tools_used': result.get('tools_used', []),
                        'done': True
                    }
                    
                    # Send final chunk
                    final_json = json.dumps(final_chunk)
                    logger.debug(f"Sending final chunk: {final_json}")
                    yield f"data: {final_json}\n\n"
                    
                    # Log completion
                    tools_used = result.get('tools_used', [])
                    if any(tool in tools_used for tool in ['run_composite_analysis', 'run_pca_analysis', 'runcompleteanalysis']):
                        if 'runcompleteanalysis' in tools_used:
                            analysis_type = 'dual_method'
                        elif 'run_composite_analysis' in tools_used:
                            analysis_type = 'composite'
                        else:
                            analysis_type = 'pca'
                        logger.info(f"Session {session_id}: Analysis completed via streaming ({analysis_type})")
                    
                    # Log completion with enhanced timing
                    if hasattr(app, 'services') and app.services.interaction_logger:
                        interaction_logger = app.services.interaction_logger
                        detailed_timing = result.get('timing_breakdown', {})
                        performance_metrics = result.get('performance_metrics', {})
                        
                        interaction_logger.log_message(
                            session_id=session_id,
                            sender='assistant',
                            content=response_text,
                            intent=result.get('intent_type'),
                            entities={
                                'streaming': True,
                                'tools_used': tools_used,
                                'status': result.get('status', 'success'),
                                'timing_breakdown': {
                                    'context_retrieval_ms': detailed_timing.get('context_retrieval', 0) * 1000,
                                    'prompt_building_ms': detailed_timing.get('prompt_building', 0) * 1000,
                                    'llm_processing_ms': detailed_timing.get('llm_processing', 0) * 1000,
                                    'tool_execution_ms': detailed_timing.get('tool_execution', 0) * 1000,
                                    'response_formatting_ms': detailed_timing.get('response_formatting', 0) * 1000,
                                    'total_duration_ms': detailed_timing.get('total_duration', 0) * 1000
                                },
                                'performance_metrics': {
                                    'llm_percentage': performance_metrics.get('llm_percentage', 0),
                                    'tool_percentage': performance_metrics.get('tool_percentage', 0),
                                    'context_percentage': performance_metrics.get('context_percentage', 0),
                                    'bottleneck': performance_metrics.get('bottleneck', 'unknown')
                                }
                            }
                        )
                            
            except Exception as e:
                logger.error(f"Error in streaming processing: {e}")
                error_json = json.dumps({'content': f'Error: {str(e)}', 'status': 'error', 'done': True})
                yield f"data: {error_json}\n\n"
        
        # Return streaming response with proper headers
        response = Response(generate(), mimetype='text/event-stream')
        response.headers['Cache-Control'] = 'no-cache'
        response.headers['Connection'] = 'keep-alive'
        response.headers['Access-Control-Allow-Origin'] = '*'
        return response
    
    except ValidationError as e:
        return jsonify({
            'status': 'error',
            'message': str(e)
        }), 400
    except Exception as e:
        logger.error(f"Error in streaming endpoint: {str(e)}", exc_info=True)
        return jsonify({
            'status': 'error',
            'message': f'Error processing streaming message: {str(e)}'
        }), 500


# ========================================================================
# ANALYSIS UTILITY FUNCTIONS
# ========================================================================

def validate_analysis_requirements(session_state, data_handler=None):
    """
    Validate that analysis requirements are met.
    
    Args:
        session_state: Current session state
        data_handler: Data handler instance
        
    Returns:
        tuple: (is_valid, error_message)
    """
    if not session_state.get('csv_loaded', False):
        return False, 'CSV data must be loaded before running analysis'
    
    if not session_state.get('shapefile_loaded', False):
        return False, 'Shapefile data must be loaded before running analysis'
    
    if not data_handler:
        return False, 'Data handler not available'
    
    # Check if data handler has required data
    if not hasattr(data_handler, 'df') or data_handler.df is None:
        return False, 'No CSV data found in data handler'
    
    if not hasattr(data_handler, 'shapefile_data') or data_handler.shapefile_data is None:
        return False, 'No shapefile data found in data handler'
    
    return True, ''


def get_analysis_status(session_state, data_handler=None):
    """
    Get current analysis status and progress.
    
    Args:
        session_state: Current session state
        data_handler: Data handler instance
        
    Returns:
        dict: Analysis status information
    """
    status = {
        'csv_loaded': session_state.get('csv_loaded', False),
        'shapefile_loaded': session_state.get('shapefile_loaded', False),
        'analysis_complete': session_state.get('analysis_complete', False),
        'variables_used': session_state.get('variables_used', []),
        'analysis_type': session_state.get('analysis_type', 'none'),
        'can_run_analysis': False,
        'can_view_results': False
    }
    
    # Check if analysis can be run
    if status['csv_loaded'] and status['shapefile_loaded']:
        status['can_run_analysis'] = True
    
    # Check if results can be viewed
    if status['analysis_complete'] and data_handler:
        if hasattr(data_handler, 'vulnerability_rankings') and data_handler.vulnerability_rankings is not None:
            status['can_view_results'] = True
    
    return status


def extract_risk_wards(data_handler, limit=10):
    """
    Extract high, medium, and low risk wards from analysis results.
    
    Args:
        data_handler: Data handler with analysis results
        limit: Maximum number of wards to return per category
        
    Returns:
        dict: Dictionary with high_risk, medium_risk, and low_risk ward lists
    """
    risk_wards = {
        'high_risk': [],
        'medium_risk': [],
        'low_risk': []
    }
    
    if not data_handler or not hasattr(data_handler, 'vulnerability_rankings'):
        return risk_wards
    
    rankings = data_handler.vulnerability_rankings
    if rankings is None or 'vulnerability_category' not in rankings.columns:
        return risk_wards
    
    try:
        if 'WardName' in rankings.columns:
            risk_wards['high_risk'] = rankings[
                rankings['vulnerability_category'] == 'High'
            ]['WardName'].tolist()[:limit]
            
            risk_wards['medium_risk'] = rankings[
                rankings['vulnerability_category'] == 'Medium'
            ]['WardName'].tolist()[:limit]
            
            risk_wards['low_risk'] = rankings[
                rankings['vulnerability_category'] == 'Low'
            ]['WardName'].tolist()[:limit]
            
    except Exception as e:
        logger.error(f"Error extracting risk wards: {e}")
    
    return risk_wards


def update_analysis_session_state(session, analysis_result):
    """
    Update session state based on analysis results.
    
    Args:
        session: Flask session object
        analysis_result: Analysis result dictionary
    """
    if analysis_result.get('status') == 'success':
        session['analysis_complete'] = True
        session['variables_used'] = analysis_result.get('variables_used', [])
        
        # Set analysis type if provided
        if 'analysis_type' in analysis_result:
            session['analysis_type'] = analysis_result['analysis_type']
        
        # Clear any pending actions
        session.pop('pending_action', None)
        session.pop('pending_variables', None)
        
        # Update timestamp
        session['analysis_completion_time'] = datetime.utcnow().isoformat()
        
        logger.info(f"Session {session.get('session_id')}: Analysis completed with {len(session['variables_used'])} variables")


def clear_analysis_session_state(session):
    """
    Clear analysis-related session state.
    
    Args:
        session: Flask session object
    """
    # Clear analysis flags
    session.pop('analysis_complete', None)
    session.pop('variables_used', None)
    session.pop('analysis_type', None)
    session.pop('analysis_completion_time', None)
    
    # Clear pending actions
    session.pop('pending_action', None)
    session.pop('pending_variables', None)
    
    # Clear visualization state
    session.pop('last_visualization', None)
    
    logger.info(f"Session {session.get('session_id')}: Analysis state cleared") 