# app/web/routes/upload_routes.py
"""
Upload Routes module for file upload operations.

This module contains the file upload routes for the ChatMRPT web application:
- Modern dual file upload (CSV + shapefile)
- Legacy single file upload 
- Sample data loading
- File validation and processing logic
"""

import os
import uuid
import logging
from flask import Blueprint, session, request, current_app, jsonify
from werkzeug.utils import secure_filename

from ...core.decorators import handle_errors, log_execution_time, validate_session
from ...core.exceptions import ValidationError, DataProcessingError
from ...core.utils import convert_to_json_serializable
from ...core.responses import ResponseBuilder

logger = logging.getLogger(__name__)

# Create the upload routes blueprint
upload_bp = Blueprint('upload', __name__)

# File upload configurations
ALLOWED_EXTENSIONS_CSV = {'csv', 'txt', 'xlsx', 'xls'}  # Added Excel file extensions
ALLOWED_EXTENSIONS_SHP = {'zip'}  # Shapefiles are uploaded as ZIP files


def allowed_file(filename, allowed_extensions):
    """Check if file extension is allowed."""
    return '.' in filename and filename.rsplit('.', 1)[1].lower() in allowed_extensions


@upload_bp.route('/upload_both_files', methods=['POST'])
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
    
        # Create session folder for file storage
    session_folder = os.path.join(current_app.config['UPLOAD_FOLDER'], session_id)
    os.makedirs(session_folder, exist_ok=True)
    
    # Create session-specific DataHandler (don't use global data_service)
    from app.data import DataHandler
    interaction_logger = current_app.services.interaction_logger
    data_service = DataHandler(session_folder, interaction_logger)
    
    results = {}
    
    # Process CSV file if provided
    if csv_file and allowed_file(csv_file.filename, ALLOWED_EXTENSIONS_CSV):
        try:
            # Save file securely
            csv_filename = secure_filename(csv_file.filename)
            csv_path = os.path.join(session_folder, csv_filename)
            csv_file.save(csv_path)
            
            # Use DataService to load CSV
            csv_result = data_service.load_csv_file(csv_path)
            
            # Only mark as loaded if we have valid data and a success status
            if csv_result['status'] == 'success' and 'data' in csv_result and csv_result['data'] is not None and len(csv_result.get('data', [])) > 0:
                # Update session data
                session['csv_loaded'] = True
                session['csv_filename'] = csv_filename
                session['csv_rows'] = csv_result.get('rows', 0)
                session['csv_columns'] = csv_result.get('columns', 0)
                
                # Get available variables from data service
                available_variables = data_service.get_available_variables()
                session['available_variables'] = available_variables
                
                logger.info(f"CSV loaded successfully: {csv_filename} ({csv_result.get('rows', 0)} rows)")
            else:
                # Ensure we mark the CSV as not loaded if there was an error
                session['csv_loaded'] = False
                logger.warning(f"CSV not loaded properly: status={csv_result['status']}")
            
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
            try:
                shp_result = data_service.load_shapefile(shp_path)
                
                # Only mark as loaded if status is success and data is present
                if shp_result['status'] == 'success' and 'data' in shp_result and shp_result['data'] is not None:
                    # Update session data
                    session['shapefile_loaded'] = True
                    session['shapefile_filename'] = shp_filename
                    session['shapefile_features'] = shp_result.get('features', 0)
                    
                    logger.info(f"Shapefile loaded: {shp_filename} ({shp_result.get('features', 0)} features)")
                    
                    # Remove the data from the result to avoid JSON serialization issues
                    # Keep only metadata for the response
                    shp_result_safe = {
                        'status': shp_result['status'],
                        'message': shp_result['message'],
                        'features': shp_result.get('features', 0),
                        'crs': shp_result.get('crs', 'Unknown'),
                        'file_path': shp_result.get('file_path')
                    }
                    results['shp_result'] = shp_result_safe
                else:
                    # Ensure we mark the shapefile as not loaded if there was an error
                    session['shapefile_loaded'] = False
                    logger.warning(f"Shapefile not loaded properly: status={shp_result['status']}")
                    
                    # Return error result without data
                    results['shp_result'] = {
                        'status': shp_result['status'],
                        'message': shp_result.get('message', 'Shapefile loading failed'),
                        'error_type': shp_result.get('error_type', 'unknown')
                    }
                    
            except Exception as load_error:
                logger.error(f"Exception during shapefile loading: {str(load_error)}", exc_info=True)
                session['shapefile_loaded'] = False
                results['shp_result'] = {
                    'status': 'error',
                    'message': f'Error loading shapefile: {str(load_error)}',
                    'error_type': 'loading_exception'
                }
            
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
            mismatches = data_service.check_wardname_mismatches()
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
    
    # Trigger automatic data description workflow if both files loaded successfully
    if session.get('csv_loaded', False) and session.get('shapefile_loaded', False) and overall_status in ['success', 'warning']:
        try:
            # Get data summary for automatic description
            data_summary = data_service.get_data_summary()
            
            # Store the data summary for the conversation system
            session['data_summary'] = data_summary
            session['should_describe_data'] = True
            session['should_ask_analysis_permission'] = True
            
            logger.info("Data upload complete - automatic data description workflow triggered")
            
        except Exception as e:
            logger.warning(f"Failed to generate data summary for automatic workflow: {e}")
    
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


@upload_bp.route('/upload', methods=['POST'])  
@handle_errors
@validate_session
@log_execution_time
def upload():
    """
    Legacy upload route for backwards compatibility.
    Redirects to upload_both_files function.
    """
    return upload_both_files()


@upload_bp.route('/load_sample_data', methods=['POST'])
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
            logger.warning("Session ID not found, generated a new one.")

        logger.info(f"Loading sample data for session: {session_id}")

        # Use data service to load sample data
        result = data_service.load_sample_data(session_id)
        
        if result['status'] == 'success':
            # Update session state with complete information
            session['csv_loaded'] = True
            session['shapefile_loaded'] = True
            session['csv_filename'] = result.get('csv_filename', 'sample_data.csv')
            session['shapefile_filename'] = result.get('shapefile_filename', 'sample_boundary.zip')
            session['available_variables'] = result.get('variables', [])
            session['ward_count'] = result.get('ward_count', 0)
            session['csv_rows'] = result.get('rows', 10)  # Sample data has 10 wards
            session['csv_columns'] = len(result.get('variables', [])) + 1  # +1 for WardName
            session['shapefile_features'] = result.get('features', 10)  # 10 ward features
            session['data_loaded'] = True  # Important flag for analysis availability
            session['analysis_complete'] = False
            session['variables_used'] = []
            
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
        logger.error(f"Error loading sample data: {str(e)}", exc_info=True)
        return jsonify({
            'status': 'error',
            'message': f'Error loading sample data: {str(e)}'
        }), 500


# ========================================================================
# UTILITY FUNCTIONS - Updated to use unified ResponseBuilder
# ========================================================================

def validate_csv_file(file_obj):
    """
    Validate CSV file before processing.
    
    Args:
        file_obj: Flask file object
        
    Returns:
        dict: Validation result with status and message
    """
    if not file_obj:
        return ResponseBuilder.validation_error('No file provided')
    
    if file_obj.filename == '':
        return ResponseBuilder.validation_error('No file selected')
    
    if not allowed_file(file_obj.filename, ALLOWED_EXTENSIONS_CSV):
        return ResponseBuilder.validation_error(
            f'Invalid file type. Allowed types: {", ".join(ALLOWED_EXTENSIONS_CSV)}'
        )
    
    # Check file size (optional - add size limits if needed)
    # file_obj.seek(0, 2)  # Seek to end
    # file_size = file_obj.tell()
    # file_obj.seek(0)  # Reset to beginning
    # if file_size > MAX_FILE_SIZE:
    #     return ResponseBuilder.validation_error('File too large')
    
    return ResponseBuilder.success('File validation passed')


def validate_shapefile(file_obj):
    """
    Validate shapefile (zip) before processing.
    
    Args:
        file_obj: Flask file object
        
    Returns:
        dict: Validation result with status and message
    """
    if not file_obj:
        return ResponseBuilder.validation_error('No shapefile provided')
    
    if file_obj.filename == '':
        return ResponseBuilder.validation_error('No shapefile selected')
    
    if not allowed_file(file_obj.filename, ALLOWED_EXTENSIONS_SHP):
        return ResponseBuilder.validation_error(
            'Invalid shapefile type. Must be a ZIP file containing shapefile data.'
        )
    
    return ResponseBuilder.success('Shapefile validation passed')


def create_session_upload_folder(session_id):
    """
    Create upload folder for session if it doesn't exist.
    
    Args:
        session_id: Session identifier
        
    Returns:
        str: Path to session upload folder
    """
    session_folder = os.path.join(current_app.config['UPLOAD_FOLDER'], session_id)
    os.makedirs(session_folder, exist_ok=True)
    return session_folder


def cleanup_session_files(session_id, file_types=None):
    """
    Clean up uploaded files for a session.
    
    Args:
        session_id: Session identifier
        file_types: List of file types to clean up (optional)
        
    Returns:
        dict: Cleanup result with status and message
    """
    try:
        session_folder = os.path.join(current_app.config['UPLOAD_FOLDER'], session_id)
        
        if not os.path.exists(session_folder):
            return ResponseBuilder.success('No files to clean up')
        
        files_removed = 0
        
        if file_types:
            # Remove specific file types
            for filename in os.listdir(session_folder):
                file_ext = filename.rsplit('.', 1)[-1].lower() if '.' in filename else ''
                if file_ext in file_types:
                    file_path = os.path.join(session_folder, filename)
                    os.remove(file_path)
                    files_removed += 1
                    logger.info(f"Removed file: {filename}")
        else:
            # Remove all files in session folder
            for filename in os.listdir(session_folder):
                file_path = os.path.join(session_folder, filename)
                if os.path.isfile(file_path):
                    os.remove(file_path)
                    files_removed += 1
                    logger.info(f"Removed file: {filename}")
            
            # Remove empty directory
            if not os.listdir(session_folder):
                os.rmdir(session_folder)
                logger.info(f"Removed empty session folder: {session_folder}")
        
        return ResponseBuilder.success(f'Cleaned up {files_removed} files')
        
    except Exception as e:
        logger.error(f"Error cleaning up files for session {session_id}: {e}")
        return ResponseBuilder.from_exception(e, 'File cleanup')
