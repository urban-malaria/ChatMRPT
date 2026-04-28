# app/web/routes/reports_api_routes.py
"""
Reports & API Routes module for reports generation and API endpoints.

This module contains the reports and API-related routes for the ChatMRPT web application:
- Report generation and preview
- Report file downloads
- API endpoints for variables and wards
- Data export functionality
"""

import os
import uuid
import logging
from flask import Blueprint, session, request, current_app, jsonify

from app.utils.decorators import handle_errors, log_execution_time, validate_session
from app.utils.exceptions import ValidationError
from app.utils.core_utils import convert_to_json_serializable

logger = logging.getLogger(__name__)

# Create the reports and API routes blueprint
reports_bp = Blueprint('reports_api', __name__)


@reports_bp.route('/generate_report', methods=['POST'])
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
        logger.info(f"Report generation request for session {session_id}")
        logger.info(f"Session analysis_complete: {analysis_complete}")
        logger.info(f"Content-Type: {request.content_type}")
        
        # Check if analysis is complete - with fallback fix
        if not analysis_complete:
            # Try to auto-fix the session state first
            data_service = current_app.services.data_service
            data_handler = data_service.get_handler(session_id) if data_service else None
            
            # Check for ITN results file
            itn_results_path = f"instance/uploads/{session_id}/itn_distribution_results.json"
            has_itn_results = os.path.exists(itn_results_path)
            
            # Check if analysis was actually completed but session flag is wrong
            # OR if ITN distribution has been run
            if (data_handler and 
                ((hasattr(data_handler, 'vulnerability_rankings') and 
                  data_handler.vulnerability_rankings is not None) or
                 has_itn_results)):
                
                # Auto-fix the session state
                session['analysis_complete'] = True
                logger.info(f"Session {session_id}: Auto-fixed analysis_complete flag (has_itn_results: {has_itn_results})")
                analysis_complete = True
            else:
                raise ValidationError('Please run an analysis or ITN distribution before generating a report.')
        
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
        valid_formats = ['pdf', 'html', 'markdown', 'export']
        if report_format not in valid_formats:
            report_format = 'export'  # Default to export package
        
        logger.info(f"Generating {report_format} report for session {session_id}")
        
        # Generate the report using the service
        result = report_service.generate_report(
            data_handler=data_handler,
            session_id=session_id,
            format_type=report_format,
            custom_sections=custom_sections if custom_sections else None,
            detail_level=detail_level
        )
        
        if result['status'] == 'success':
            logger.info(f"Report generated successfully: {result.get('report_url')}")
            
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
                    logger.warning(f"Could not generate dashboard: {str(e)}")
            
            response_data = {
                'status': 'success',
                'message': result['message'],
                'download_url': result.get('web_path') or result.get('report_url'),
                'format': report_format,
                'additional_files': additional_files
            }
            
            # Add action for legacy compatibility
            if request.content_type and 'application/json' in request.content_type:
                response_data['action'] = 'report_generated'
                
            return jsonify(response_data)
        else:
            logger.error(f"Report generation failed: {result['message']}")
            return jsonify({
                'status': 'error',
                'message': result['message']
            }), 500
    
    except ValidationError as e:
        logger.warning(f"Validation error in report generation: {str(e)}")
        return jsonify({
            'status': 'error',
            'message': str(e)
        }), 400
    except Exception as e:
        logger.error(f"Error in generate_report route: {str(e)}", exc_info=True)
        return jsonify({
            'status': 'error',
            'message': 'An unexpected error occurred while generating the report.'
        }), 500


@reports_bp.route('/preview_report', methods=['POST'])
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
        logger.error(f"Error generating report preview: {str(e)}", exc_info=True)
        return jsonify({
            'status': 'error',
            'message': 'Error generating preview.'
        }), 500


@reports_bp.route('/download_report/<filename>')
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
        logger.error(f"Error serving report file: {str(e)}", exc_info=True)
        return jsonify({
            'status': 'error',
            'message': 'Error serving report file'
        }), 500


@reports_bp.route('/api/variables', methods=['GET'])
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
        logger.error(f"Error getting variables: {str(e)}", exc_info=True)
        return jsonify({
            'status': 'error',
            'message': f'Error retrieving variables: {str(e)}'
        }), 500


@reports_bp.route('/api/variable_metadata', methods=['GET'])
@validate_session  
@handle_errors
@log_execution_time
def get_variable_metadata():
    """Get intelligent metadata for variables using LLM-powered analysis"""
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
        
        # Get available variables
        variables = data_service.get_available_variables(session_id)
        
        # Get specific variable if requested
        var_code = request.args.get('variable')
        
        if var_code:
            # Return simple metadata for single variable
            metadata = {
                'name': var_code,
                'display_name': var_code.replace('_', ' ').title(),
                'type': 'numeric' if var_code in ['TPR', 'Composite_Score'] else 'categorical'
            }
            
            return jsonify({
                'status': 'success',
                'variable': var_code,
                'metadata': metadata
            })
        else:
            # Return simple metadata for all variables  
            all_metadata = {}
            for var in variables:
                all_metadata[var] = {
                    'name': var,
                    'display_name': var.replace('_', ' ').title(),
                    'type': 'numeric' if var in ['TPR', 'Composite_Score'] else 'categorical'
                }
            
            return jsonify({
                'status': 'success',
                'variables': all_metadata,
                'count': len(variables)
            })
    
    except Exception as e:
        logger.error(f"Error getting variable metadata: {str(e)}", exc_info=True)
        return jsonify({
            'status': 'error',
            'message': f'Error retrieving variable metadata: {str(e)}'
        }), 500


@reports_bp.route('/api/wards', methods=['GET'])
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
        logger.error(f"Error getting wards: {str(e)}", exc_info=True)
        return jsonify({
            'status': 'error',
            'message': f'Error retrieving ward information: {str(e)}'
        }), 500


# ========================================================================
# REPORT & API UTILITY FUNCTIONS
# ========================================================================

def validate_report_requirements(session_state, data_handler=None):
    """
    Validate that report generation requirements are met.
    
    Args:
        session_state: Current session state
        data_handler: Data handler instance
        
    Returns:
        tuple: (is_valid, error_message)
    """
    if not session_state.get('analysis_complete', False):
        # Check if analysis was actually completed but flag is wrong
        if (data_handler and 
            hasattr(data_handler, 'vulnerability_rankings') and 
            data_handler.vulnerability_rankings is not None):
            return True, ''  # Analysis is actually complete
        return False, 'Analysis must be completed before generating reports'
    
    if not data_handler:
        return False, 'No data available for report generation'
    
    return True, ''


def get_report_status(session_state, data_handler=None):
    """
    Get current report generation status.
    
    Args:
        session_state: Current session state
        data_handler: Data handler instance
        
    Returns:
        dict: Report availability information
    """
    status = {
        'can_generate_report': False,
        'analysis_complete': session_state.get('analysis_complete', False),
        'available_formats': [],
        'has_vulnerability_data': False,
        'has_visualization_data': False
    }
    
    # Check if analysis is actually complete (even if flag is wrong)
    analysis_actually_complete = (
        session_state.get('analysis_complete', False) or
        (data_handler and 
         hasattr(data_handler, 'vulnerability_rankings') and 
         data_handler.vulnerability_rankings is not None)
    )
    
    if analysis_actually_complete and data_handler:
        status['can_generate_report'] = True
        status['available_formats'] = ['pdf', 'html', 'markdown']
        
        # Check data availability
        if hasattr(data_handler, 'vulnerability_rankings'):
            status['has_vulnerability_data'] = data_handler.vulnerability_rankings is not None
        
        if hasattr(data_handler, 'composite_map_path'):
            status['has_visualization_data'] = True
    
    return status


def prepare_report_context(session_state, data_handler=None):
    """
    Prepare context information for report generation.
    
    Args:
        session_state: Current session state
        data_handler: Data handler instance
        
    Returns:
        dict: Context for report generation
    """
    context = {
        'session_id': session_state.get('session_id'),
        'analysis_complete': session_state.get('analysis_complete', False),
        'variables_used': session_state.get('variables_used', []),
        'analysis_type': session_state.get('analysis_type', 'standard'),
        'csv_filename': session_state.get('csv_filename'),
        'shapefile_filename': session_state.get('shapefile_filename'),
        'csv_rows': session_state.get('csv_rows', 0),
        'shapefile_features': session_state.get('shapefile_features', 0)
    }
    
    # Add data handler information
    if data_handler:
        context['has_vulnerability_rankings'] = (
            hasattr(data_handler, 'vulnerability_rankings') and 
            data_handler.vulnerability_rankings is not None
        )
        context['has_composite_map'] = hasattr(data_handler, 'composite_map_path')
        context['has_boxplot_data'] = (
            hasattr(data_handler, 'boxwhisker_plot') and 
            data_handler.boxwhisker_plot is not None
        )
        
        # Get data shape information
        if hasattr(data_handler, 'df') and data_handler.df is not None:
            context['data_shape'] = data_handler.df.shape
            context['data_columns'] = list(data_handler.df.columns)
        
    return context


def validate_api_request(request_data, required_fields=None):
    """
    Validate API request data.
    
    Args:
        request_data: Request data to validate
        required_fields: List of required fields
        
    Returns:
        tuple: (is_valid, error_message)
    """
    if not request_data:
        return False, 'No request data provided'
    
    if required_fields:
        missing_fields = [field for field in required_fields if field not in request_data]
        if missing_fields:
            return False, f"Missing required fields: {', '.join(missing_fields)}"
    
    return True, ''


def format_api_response(data, status='success', message=None):
    """
    Format standardized API response.
    
    Args:
        data: Response data
        status: Response status
        message: Optional message
        
    Returns:
        dict: Formatted API response
    """
    response = {
        'status': status,
        'data': data
    }
    
    if message:
        response['message'] = message
    
    # Ensure JSON serializable
    response = convert_to_json_serializable(response)
    
    return response


def get_available_api_endpoints():
    """
    Get list of available API endpoints.
    
    Returns:
        dict: Available API endpoints with descriptions
    """
    endpoints = {
        'GET /api/variables': 'Get available variables from loaded data',
        'GET /api/wards': 'Get ward information from loaded data',
        'POST /generate_report': 'Generate analysis report',
        'POST /preview_report': 'Preview report without saving',
        'GET /download_report/<filename>': 'Download generated report file'
    }
    
    return endpoints 