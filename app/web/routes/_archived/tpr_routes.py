"""
TPR Routes for ChatMRPT.

This module provides API endpoints for the TPR (Test Positivity Rate) workflow,
handling conversational interactions and analysis execution.
"""

import os
import logging
from flask import Blueprint, session, request, jsonify, send_file, current_app
from werkzeug.exceptions import BadRequest

from ...core.decorators import handle_errors, validate_session, log_execution_time
from ...core.exceptions import ValidationError, DataProcessingError

# TPR Module imports
try:
    from ...tpr_module.integration.tpr_handler import get_tpr_handler, cleanup_tpr_handler
    TPR_MODULE_AVAILABLE = True
except ImportError:
    TPR_MODULE_AVAILABLE = False

logger = logging.getLogger(__name__)

# Create TPR routes blueprint
tpr_bp = Blueprint('tpr', __name__, url_prefix='/api/tpr')


@tpr_bp.route('/status', methods=['GET'])
@validate_session
@handle_errors
def get_tpr_status():
    """Get current TPR workflow status."""
    session_id = session.get('session_id')
    
    if not session.get('tpr_workflow_active'):
        return jsonify({
            'status': 'success',
            'active': False,
            'message': 'No active TPR workflow'
        })
    
    if not TPR_MODULE_AVAILABLE:
        return jsonify({
            'status': 'error',
            'message': 'TPR module not available'
        }), 503
    
    try:
        tpr_handler = get_tpr_handler(session_id)
        status = tpr_handler.get_tpr_status()
        
        return jsonify({
            'status': 'success',
            **status
        })
        
    except Exception as e:
        logger.error(f"Error getting TPR status: {e}")
        return jsonify({
            'status': 'error',
            'message': f'Failed to get TPR status: {str(e)}'
        }), 500


@tpr_bp.route('/process', methods=['POST'])
@validate_session
@handle_errors
@log_execution_time
def process_tpr_message():
    """Process a message in the TPR workflow."""
    session_id = session.get('session_id')
    
    if not session.get('tpr_workflow_active'):
        return jsonify({
            'status': 'error',
            'message': 'No active TPR workflow. Please upload a TPR file first.'
        }), 400
    
    if not TPR_MODULE_AVAILABLE:
        return jsonify({
            'status': 'error',
            'message': 'TPR module not available'
        }), 503
    
    # Get user message
    data = request.get_json()
    if not data or 'message' not in data:
        raise BadRequest('No message provided')
    
    user_message = data['message']
    
    try:
        tpr_handler = get_tpr_handler(session_id)
        result = tpr_handler.process_tpr_message(user_message)
        
        # Check if analysis is complete
        if result.get('stage') == 'completed':
            # Clear TPR workflow flag
            session.pop('tpr_workflow_active', None)
            cleanup_tpr_handler(session_id)
        
        return jsonify(result)
        
    except Exception as e:
        logger.error(f"Error processing TPR message: {e}")
        return jsonify({
            'status': 'error',
            'response': f'Failed to process message: {str(e)}'
        }), 500


@tpr_bp.route('/cancel', methods=['POST'])
@validate_session
@handle_errors
def cancel_tpr_workflow():
    """Cancel the current TPR workflow."""
    session_id = session.get('session_id')
    
    if not session.get('tpr_workflow_active'):
        return jsonify({
            'status': 'success',
            'message': 'No active TPR workflow to cancel'
        })
    
    if not TPR_MODULE_AVAILABLE:
        return jsonify({
            'status': 'error',
            'message': 'TPR module not available'
        }), 503
    
    try:
        tpr_handler = get_tpr_handler(session_id)
        result = tpr_handler.cancel_tpr_workflow()
        
        # Clean up
        cleanup_tpr_handler(session_id)
        
        return jsonify(result)
        
    except Exception as e:
        logger.error(f"Error cancelling TPR workflow: {e}")
        return jsonify({
            'status': 'error',
            'message': f'Failed to cancel workflow: {str(e)}'
        }), 500


@tpr_bp.route('/download-links', methods=['GET'])
@validate_session
@handle_errors
def get_tpr_download_links():
    """Get TPR download links for current session."""
    session_id = session.get('session_id')
    
    # Get download links from session
    download_links = session.get('tpr_download_links', [])
    
    # If download_links is a list (new format), return as is
    # If it's a dict (old format), convert to list
    if isinstance(download_links, dict):
        # Convert old format to new format
        links_list = []
        for key, path in download_links.items():
            type_map = {
                'tpr_analysis': 'TPR Analysis Data',
                'main_analysis': 'Complete Analysis',
                'shapefile': 'Shapefile',
                'summary': 'Summary Report',
                'html_report': 'TPR Analysis Report'
            }
            links_list.append({
                'type': type_map.get(key, key),
                'path': path,
                'name': os.path.basename(path) if path else ''
            })
        download_links = links_list
    
    return jsonify({
        'status': 'success',
        'download_links': download_links,
        'session_id': session_id
    })


@tpr_bp.route('/download/<file_type>', methods=['GET'])
@validate_session
@handle_errors
def download_tpr_output(file_type):
    """Download TPR output files."""
    session_id = session.get('session_id')
    session_folder = os.path.join(current_app.config['UPLOAD_FOLDER'], session_id)
    
    # Map file types to actual filenames
    file_mapping = {
        'tpr_analysis': None,
        'main_analysis': None,
        'shapefile': None,
        'summary': None,
        'html_report': None
    }
    
    # Find the appropriate file
    if file_type not in file_mapping:
        return jsonify({
            'status': 'error',
            'message': f'Invalid file type: {file_type}'
        }), 400
    
    # Look for files with the appropriate pattern
    try:
        import glob
        
        patterns = {
            'tpr_analysis': '*_TPR_Analysis_*.csv',
            'main_analysis': '*_plus.csv',
            'shapefile': '*_state.zip',
            'summary': '*_Summary_Report.md',
            'html_report': '*.html'
        }
        
        pattern = patterns.get(file_type)
        if not pattern:
            raise ValueError(f"No pattern for file type: {file_type}")
        
        files = glob.glob(os.path.join(session_folder, pattern))
        
        if not files:
            return jsonify({
                'status': 'error',
                'message': f'No {file_type} file found. Please complete the TPR analysis first.'
            }), 404
        
        # Get the most recent file
        latest_file = max(files, key=os.path.getctime)
        
        # Determine MIME type
        mime_types = {
            'tpr_analysis': 'text/csv',
            'main_analysis': 'text/csv',
            'shapefile': 'application/zip',
            'summary': 'text/markdown',
            'html_report': 'text/html'
        }
        
        return send_file(
            latest_file,
            as_attachment=True,
            download_name=os.path.basename(latest_file),
            mimetype=mime_types.get(file_type, 'application/octet-stream')
        )
        
    except Exception as e:
        logger.error(f"Error downloading TPR file: {e}")
        return jsonify({
            'status': 'error',
            'message': f'Failed to download file: {str(e)}'
        }), 500


@tpr_bp.route('/states', methods=['GET'])
@validate_session
@handle_errors
def get_available_states():
    """Get list of available states for TPR analysis."""
    session_id = session.get('session_id')
    
    if not session.get('tpr_workflow_active'):
        return jsonify({
            'status': 'error',
            'message': 'No active TPR workflow'
        }), 400
    
    if not TPR_MODULE_AVAILABLE:
        return jsonify({
            'status': 'error',
            'message': 'TPR module not available'
        }), 503
    
    try:
        tpr_handler = get_tpr_handler(session_id)
        status = tpr_handler.get_tpr_status()
        
        return jsonify({
            'status': 'success',
            'states': status.get('available_states', [])
        })
        
    except Exception as e:
        logger.error(f"Error getting available states: {e}")
        return jsonify({
            'status': 'error',
            'message': f'Failed to get states: {str(e)}'
        }), 500


@tpr_bp.route('/validate-state', methods=['POST'])
@validate_session
@handle_errors
def validate_state_selection():
    """Validate a state selection for TPR analysis."""
    if not TPR_MODULE_AVAILABLE:
        return jsonify({
            'status': 'error',
            'message': 'TPR module not available'
        }), 503
    
    data = request.get_json()
    if not data or 'state' not in data:
        raise BadRequest('No state provided')
    
    state_name = data['state']
    
    try:
        from ...tpr_module.services.state_selector import StateSelector
        
        selector = StateSelector()
        matched_state, confidence = selector.match_state(state_name)
        
        if matched_state and confidence > 0.6:
            return jsonify({
                'status': 'success',
                'valid': True,
                'matched_state': matched_state,
                'confidence': confidence,
                'suggestions': [] if confidence > 0.9 else selector.suggest_states(state_name, 3)
            })
        else:
            return jsonify({
                'status': 'success',
                'valid': False,
                'suggestions': selector.suggest_states(state_name, 5),
                'message': f'State "{state_name}" not found. Did you mean one of these?'
            })
            
    except Exception as e:
        logger.error(f"Error validating state: {e}")
        return jsonify({
            'status': 'error',
            'message': f'Failed to validate state: {str(e)}'
        }), 500


# Error handlers specific to TPR routes
@tpr_bp.errorhandler(ValidationError)
def handle_validation_error(e):
    """Handle validation errors in TPR routes."""
    return jsonify({
        'status': 'error',
        'message': str(e)
    }), 400


@tpr_bp.errorhandler(DataProcessingError)
def handle_processing_error(e):
    """Handle data processing errors in TPR routes."""
    return jsonify({
        'status': 'error',
        'message': str(e)
    }), 500


