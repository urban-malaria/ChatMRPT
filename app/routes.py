# app/routes.py
import numpy as np
import json
from datetime import datetime
import shutil
import os
import uuid
import sqlite3
import logging
import pandas as pd
import re
from flask import Blueprint, render_template, request, jsonify, current_app, session, send_from_directory, send_file
from werkzeug.utils import secure_filename
import geopandas as gpd

from .models.data_handler import DataHandler
import app.models.visualization as viz
import app.models.report_generator as report_gen
from .kb import get_knowledge
from .ai_utils import get_llm_manager, classify_question, LLMManager, select_optimal_variables_with_llm, convert_markdown_to_html

# Set up logging
#logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Create blueprint
main = Blueprint('main', __name__)

# Helper function to get the interaction logger
def get_interaction_logger():
    """Get the interaction logger from app config"""
    return current_app.config.get('INTERACTION_LOGGER')

# Helper function to get the LLM manager
def get_llm_manager_instance():
    """Get an instance of the LLM manager with the interaction logger"""
    interaction_logger = get_interaction_logger()
    return get_llm_manager(interaction_logger)

# Allowed file extensions
ALLOWED_EXTENSIONS_CSV = {'csv', 'xlsx', 'xls'}
ALLOWED_EXTENSIONS_SHP = {'zip'}

def allowed_file(filename, allowed_extensions):
    return '.' in filename and filename.rsplit('.', 1)[1].lower() in allowed_extensions

@main.before_request
def log_session_start():
    """Log session start for new sessions"""
    if request.endpoint and not request.endpoint.startswith('static'):
        session_id = session.get('session_id')
        if session_id:
            # Get browser and IP info
            browser_info = request.user_agent.string
            ip_address = request.remote_addr
            
            # Log session start/activity
            logger = get_interaction_logger()
            if logger:
                logger.log_session_start(session_id, browser_info, ip_address)

@main.route('/')
def index():
    """Render the main page"""
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
        logger = get_interaction_logger()
        if logger:
            logger.log_session_start(
                session['session_id'], 
                request.user_agent.string, 
                request.remote_addr
            )
    
    # Check for ?use_tailwind query parameter to switch between old and new UI
    use_tailwind = request.args.get('use_tailwind', 'false').lower() == 'true'
    
    if use_tailwind:
        return render_template('index_tailwind.html')
    else:
        return render_template('index.html')


@main.route('/upload_both_files', methods=['POST'])
def upload_both_files():
    """Handle simultaneous upload of both CSV and shapefile files"""
    response = {'status': 'error', 'message': 'No files received'}
    
    # Check if files were provided
    csv_file = None
    shapefile = None
    
    if 'csv_file' in request.files:
        csv_file = request.files['csv_file']
        if csv_file.filename == '':
            csv_file = None
    
    if 'shapefile' in request.files:
        shapefile = request.files['shapefile']
        if shapefile.filename == '':
            shapefile = None
    
    if not csv_file and not shapefile:
        return jsonify({'status': 'error', 'message': 'No files selected'}), 400
    
    # Create session folder
    session_folder = os.path.join(current_app.config['UPLOAD_FOLDER'], session.get('session_id', 'default'))
    os.makedirs(session_folder, exist_ok=True)
    
    # Process CSV file if provided
    csv_result = None
    if csv_file and allowed_file(csv_file.filename, ALLOWED_EXTENSIONS_CSV):
        csv_filename = secure_filename(csv_file.filename)
        csv_path = os.path.join(session_folder, csv_filename)
        csv_file.save(csv_path)
        
        # Process the CSV file
        data_handler = DataHandler(session_folder)
        csv_result = data_handler.load_csv(csv_path)
        
        if csv_result['status'] == 'success':
            session['csv_loaded'] = True
            session['csv_filename'] = csv_filename
            session['csv_rows'] = csv_result.get('rows', 0)
            session['csv_columns'] = csv_result.get('columns', 0)
            
            # Store available variables in session
            session['available_variables'] = data_handler.get_available_variables()
            
            # Log the file upload
            logger = get_interaction_logger()
            if logger and session.get('session_id'):
                metadata = {
                    'rows': csv_result.get('rows', 0),
                    'columns': csv_result.get('columns', 0),
                    'missing_values': len(csv_result.get('missing_columns', [])),
                    'available_variables': session['available_variables'][:10]  # Just log first 10
                }
                logger.log_file_upload(
                    session.get('session_id'),
                    'csv',
                    csv_filename,
                    os.path.getsize(csv_path),
                    metadata
                )
        else:
            # Log the error
            logger = get_interaction_logger()
            if logger and session.get('session_id'):
                logger.log_error(
                    session.get('session_id'),
                    'csv_upload_error',
                    csv_result.get('message', 'Unknown error processing CSV file')
                )
    elif csv_file:
        csv_result = {'status': 'error', 'message': 'Invalid CSV file type'}
        
    # Process shapefile if provided
    shp_result = None
    if shapefile and allowed_file(shapefile.filename, ALLOWED_EXTENSIONS_SHP):
        shp_filename = secure_filename(shapefile.filename)
        shp_path = os.path.join(session_folder, shp_filename)
        shapefile.save(shp_path)
        
        # Process the shapefile
        data_handler = DataHandler(session_folder)
        shp_result = data_handler.load_shapefile(shp_path)
        
        if shp_result['status'] == 'success':
            session['shapefile_loaded'] = True
            session['shapefile_filename'] = shp_filename
            session['shapefile_features'] = shp_result.get('features', 0)
            
            # Log the file upload
            logger = get_interaction_logger()
            if logger and session.get('session_id'):
                metadata = {
                    'features': shp_result.get('features', 0),
                    'crs': shp_result.get('crs', ''),
                    'has_mismatches': shp_result.get('mismatches') is not None
                }
                logger.log_file_upload(
                    session.get('session_id'),
                    'shapefile',
                    shp_filename,
                    os.path.getsize(shp_path),
                    metadata
                )
        else:
            # Log the error
            logger = get_interaction_logger()
            if logger and session.get('session_id'):
                logger.log_error(
                    session.get('session_id'),
                    'shapefile_upload_error',
                    shp_result.get('message', 'Unknown error processing shapefile')
                )
    elif shapefile:
        shp_result = {'status': 'error', 'message': 'Invalid shapefile type'}
        
    # If both files are uploaded, check for ward name mismatches
    if session.get('csv_loaded', False) and session.get('shapefile_loaded', False):
        # We need to use either the data_handler from CSV or shapefile processing
        # Prioritize using the one that was just processed
        mismatches = None
        if csv_result and shp_result:
            # If both were uploaded at once, recreate a fresh data handler
            data_handler = DataHandler(session_folder)
            # Load both files again to ensure consistency
            data_handler.load_csv(os.path.join(session_folder, session['csv_filename']))
            data_handler.load_shapefile(os.path.join(session_folder, session['shapefile_filename']))
            mismatches = data_handler.check_wardname_mismatches()
        elif csv_result:
            # CSV was just uploaded, load the shapefile data into the handler
            data_handler.load_shapefile(os.path.join(session_folder, session['shapefile_filename']))
            mismatches = data_handler.check_wardname_mismatches()
        elif shp_result:
            # Shapefile was just uploaded, load the CSV data into the handler
            data_handler.load_csv(os.path.join(session_folder, session['csv_filename']))
            mismatches = data_handler.check_wardname_mismatches()
        
        if mismatches and len(mismatches) > 0:
            if shp_result:
                shp_result['mismatches'] = mismatches
                shp_result['status'] = 'warning'
                shp_result['message'] = f'Shapefile loaded but found {len(mismatches)} ward name mismatches'
            elif csv_result:
                csv_result['mismatches'] = mismatches
                csv_result['status'] = 'warning'
                csv_result['message'] = f'CSV loaded but found {len(mismatches)} ward name mismatches'
        
        # Create analysis prompt
        analysis_prompt = f"""
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
    
    # Prepare final response based on which files were processed
    if csv_result and shp_result:
        # Both files were uploaded
        if csv_result['status'] == 'success' and shp_result['status'] in ['success', 'warning']:
            response = {
                'status': 'success',
                'message': 'Both files uploaded successfully',
                'csv_result': csv_result,
                'shp_result': shp_result
            }
            if session.get('csv_loaded', False) and session.get('shapefile_loaded', False):
                response['analysis_prompt'] = analysis_prompt
        else:
            # At least one upload failed
            response = {
                'status': 'error',
                'message': 'One or more file uploads failed',
                'csv_result': csv_result,
                'shp_result': shp_result
            }
    elif csv_result:
        # Only CSV was uploaded
        response = {
            'status': csv_result['status'],
            'message': csv_result['message'],
            'csv_result': csv_result
        }
        if csv_result['status'] == 'success' and not session.get('shapefile_loaded', False):
            response['note'] = 'CSV loaded successfully. Please upload a shapefile.'
    elif shp_result:
        # Only shapefile was uploaded
        response = {
            'status': shp_result['status'],
            'message': shp_result['message'],
            'shp_result': shp_result
        }
        if shp_result['status'] in ['success', 'warning'] and not session.get('csv_loaded', False):
            response['note'] = 'Shapefile loaded successfully. Please upload a CSV file.'
    
    return jsonify(response)


@main.route('/upload_csv', methods=['POST'])
def upload_csv():
    """Handle CSV file upload"""
    if 'file' not in request.files:
        return jsonify({'status': 'error', 'message': 'No file part'}), 400
    
    file = request.files['file']
    
    if file.filename == '':
        return jsonify({'status': 'error', 'message': 'No file selected'}), 400
    
    if file and allowed_file(file.filename, ALLOWED_EXTENSIONS_CSV):
        filename = secure_filename(file.filename)
        session_folder = os.path.join(current_app.config['UPLOAD_FOLDER'], session.get('session_id', 'default'))
        os.makedirs(session_folder, exist_ok=True)
        
        file_path = os.path.join(session_folder, filename)
        file.save(file_path)
        
        # Process the CSV file
        data_handler = DataHandler(session_folder)
        result = data_handler.load_csv(file_path)
        
        if result['status'] == 'success':
            session['csv_loaded'] = True
            session['csv_filename'] = filename
            session['csv_rows'] = result.get('rows', 0)
            session['csv_columns'] = result.get('columns', 0)
            
            # Store available variables in session
            session['available_variables'] = data_handler.get_available_variables()
            
            # Log the file upload
            logger = get_interaction_logger()
            if logger and session.get('session_id'):
                metadata = {
                    'rows': result.get('rows', 0),
                    'columns': result.get('columns', 0),
                    'missing_values': len(result.get('missing_columns', [])),
                    'available_variables': session['available_variables'][:10]  # Just log first 10
                }
                logger.log_file_upload(
                    session.get('session_id'),
                    'csv',
                    filename,
                    os.path.getsize(file_path),
                    metadata
                )
            
            return jsonify({
                'status': 'success', 
                'message': f'CSV file {filename} uploaded successfully',
                'rows': result.get('rows', 0),
                'columns': result.get('columns', 0),
                'missing_values': result.get('missing_values', 0),
                'available_variables': session['available_variables']
            })
        else:
            # Log the error
            logger = get_interaction_logger()
            if logger and session.get('session_id'):
                logger.log_error(
                    session.get('session_id'),
                    'csv_upload_error',
                    result.get('message', 'Unknown error processing CSV file')
                )
            return jsonify({'status': 'error', 'message': result.get('message', 'Failed to process CSV file')}), 400
    
    return jsonify({'status': 'error', 'message': 'Invalid file type'}), 400

@main.route('/upload_shapefile', methods=['POST'])
def upload_shapefile():
    """Handle shapefile (ZIP) upload"""
    if 'file' not in request.files:
        return jsonify({'status': 'error', 'message': 'No file part'}), 400
    
    file = request.files['file']
    
    if file.filename == '':
        return jsonify({'status': 'error', 'message': 'No file selected'}), 400
    
    if file and allowed_file(file.filename, ALLOWED_EXTENSIONS_SHP):
        filename = secure_filename(file.filename)
        session_folder = os.path.join(current_app.config['UPLOAD_FOLDER'], session.get('session_id', 'default'))
        os.makedirs(session_folder, exist_ok=True)
        
        file_path = os.path.join(session_folder, filename)
        file.save(file_path)
        
        # Process the shapefile
        data_handler = DataHandler(session_folder)
        result = data_handler.load_shapefile(file_path)
        
        if result['status'] == 'success':
            session['shapefile_loaded'] = True
            session['shapefile_filename'] = filename
            session['shapefile_features'] = result.get('features', 0)
            
            # Log the file upload
            logger = get_interaction_logger()
            if logger and session.get('session_id'):
                metadata = {
                    'features': result.get('features', 0),
                    'crs': result.get('crs', ''),
                    'has_mismatches': result.get('mismatches') is not None
                }
                logger.log_file_upload(
                    session.get('session_id'),
                    'shapefile',
                    filename,
                    os.path.getsize(file_path),
                    metadata
                )
            
            # Check for ward name mismatches if CSV is already loaded
            if session.get('csv_loaded', False):
                mismatches = data_handler.check_wardname_mismatches()
                if mismatches and len(mismatches) > 0:
                    return jsonify({
                        'status': 'warning', 
                        'message': f'Shapefile loaded but found {len(mismatches)} ward name mismatches',
                        'features': result.get('features', 0),
                        'mismatches': mismatches
                    })
            
            # Check if both files are loaded
            if session.get('csv_loaded', False) and session.get('shapefile_loaded', False):
                analysis_prompt = f"""
                <p><strong>Excellent! All files are now loaded successfully!</strong></p>
                <p>Your data includes:</p>
                <ul>
                    <li>📊 CSV data: {session.get('csv_rows', 0)} rows with {session.get('csv_columns', 0)} columns</li>
                    <li>🗺️ Shapefile data: {result.get('features', 0)} features</li>
                </ul>
                <div class="analysis-ready-prompt">
                    <p><strong>🚀 Everything is ready for analysis!</strong></p>
                    <p>Type "Run the analysis" to begin processing your data.</p>
                    <button class="btn btn-primary mt-2" onclick="document.getElementById('message-input').value='Run the analysis'; document.getElementById('send-message').click();">
                        Start Analysis
                    </button>
                </div>
                """
                
                return jsonify({
                    'status': 'success', 
                    'message': f'Shapefile {filename} uploaded successfully',
                    'features': result.get('features', 0),
                    'analysis_prompt': analysis_prompt
                })
            else:
                return jsonify({
                    'status': 'success', 
                    'message': f'Shapefile {filename} uploaded successfully',
                    'features': result.get('features', 0),
                    'note': 'Waiting for CSV file...'
                })
        else:
            # Log the error
            logger = get_interaction_logger()
            if logger and session.get('session_id'):
                logger.log_error(
                    session.get('session_id'),
                    'shapefile_upload_error',
                    result.get('message', 'Unknown error processing shapefile')
                )
            return jsonify({'status': 'error', 'message': result.get('message', 'Failed to process shapefile')}), 400
    
    return jsonify({'status': 'error', 'message': 'Invalid file type'}), 400

@main.route('/run_analysis', methods=['POST'])
def run_analysis():
    """Run the analysis directly (used for API calls, not main chat flow)"""
    from flask import current_app
    
    try:
        # Get session ID
        session_id = session.get('session_id', 'default')
        
        # Get custom parameters from the request
        data = request.json or {}
        selected_variables = data.get('selected_variables', None)
        use_llm_selection = data.get('use_llm_selection', True)  # Default to using LLM if no variables specified
        
        # Get data handler
        data_handler = get_data_handler()
        
        # Check if data handler is available
        if not data_handler:
            return jsonify({
                'status': 'error',
                'message': 'Data handler not initialized. Please upload data files first.'
            }), 400
        
        # Check if both files are loaded
        if not session.get('csv_loaded', False) or not session.get('shapefile_loaded', False):
            return jsonify({
                'status': 'error',
                'message': 'Please upload both CSV and shapefile data before running analysis'
            }), 400
        
        # Get the analysis service from the service container
        analysis_service = current_app.services.analysis_service
        
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
    
    except Exception as e:
        logger.error(f"Error running analysis: {str(e)}", exc_info=True)
        return jsonify({
            'status': 'error',
            'message': f'Error running analysis: {str(e)}'
        }), 500
    
@main.route('/explain_variable_selection', methods=['GET'])
def explain_variable_selection():
    """Generate an explanation for why certain variables were selected for the analysis"""
    from flask import current_app
    
    try:
        # Get session ID
        session_id = session.get('session_id')
        if not session_id:
            return jsonify({'status': 'error', 'message': 'No active session found'}), 400
        
        # Get data handler
        data_handler = get_data_handler()
        if not data_handler:
            return jsonify({'status': 'error', 'message': 'No data available for explanation'}), 400
            
        # Check if analysis has been performed
        if not session.get('analysis_complete', False) or not session.get('variables_used'):
            return jsonify({'status': 'error', 'message': 'Analysis not yet performed'}), 400
        
        # Get variables used in the analysis
        variables = session.get('variables_used', [])
        if not variables:
            return jsonify({'status': 'error', 'message': 'No variables found from analysis'}), 400
        
        # Get the analysis service from the service container
        analysis_service = current_app.services.analysis_service
        
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
    
    except Exception as e:
        logger.error(f"Error explaining variable selection: {str(e)}", exc_info=True)
        return jsonify({
            'status': 'error',
            'message': f'Error generating explanation: {str(e)}'
        }), 500

@main.route('/load_sample_data', methods=['POST'])
def load_sample_data():
    """Load pre-packaged sample data into the user's session."""
    try:
        session_id = session.get('session_id')
        if not session_id:
            # Should not happen if session is initialized, but handle anyway
            session['session_id'] = str(uuid.uuid4())
            session_id = session['session_id']
            logger.warning("Session ID not found, generated a new one.")

        logger.info(f"Loading sample data for session: {session_id}")

        # Define paths
        sample_data_dir = os.path.join(current_app.root_path, 'sample_data')
        source_csv_path = os.path.join(sample_data_dir, 'sample_data_template.csv')
        source_zip_path = os.path.join(sample_data_dir, 'sample_boundary_template.zip')

        session_folder = os.path.join(current_app.config['UPLOAD_FOLDER'], session_id)
        os.makedirs(session_folder, exist_ok=True) # Ensure folder exists

        target_csv_path = os.path.join(session_folder, 'sample_data.csv')
        target_zip_path = os.path.join(session_folder, 'sample_boundary.zip')

        # --- Check if sample files exist ---
        if not os.path.exists(source_csv_path) or not os.path.exists(source_zip_path):
             logger.error("Sample data template files not found in app/sample_data/")
             return jsonify({'status': 'error', 'message': 'Sample data files are missing on the server.'}), 500

        # --- Copy sample files to session folder ---
        shutil.copy(source_csv_path, target_csv_path)
        shutil.copy(source_zip_path, target_zip_path)
        logger.info("Sample files copied to session folder.")

        # --- Process copied files using DataHandler ---
        # NOTE: We create a temporary handler just for loading metadata into the session.
        # The main get_data_handler() will be used for actual analysis later.
        temp_data_handler = DataHandler(session_folder)

        # Load CSV and update session
        csv_result = temp_data_handler.load_csv(target_csv_path)
        if csv_result['status'] != 'success':
            logger.error(f"Failed to process sample CSV: {csv_result.get('message')}")
            return jsonify({'status': 'error', 'message': f"Failed to process sample CSV: {csv_result.get('message')}"}), 500

        session['csv_loaded'] = True
        session['csv_filename'] = 'sample_data.csv'
        session['csv_rows'] = csv_result.get('rows', 0)
        session['csv_columns'] = csv_result.get('columns', 0)
        
        # Store available variables in session
        session['available_variables'] = temp_data_handler.get_available_variables()
        
        logger.info("Sample CSV processed and session updated.")

        # Load Shapefile and update session
        shp_result = temp_data_handler.load_shapefile(target_zip_path)
        if shp_result['status'] != 'success':
             logger.error(f"Failed to process sample Shapefile: {shp_result.get('message')}")
             return jsonify({'status': 'error', 'message': f"Failed to process sample Shapefile: {shp_result.get('message')}"}), 500

        session['shapefile_loaded'] = True
        session['shapefile_filename'] = 'sample_boundary.zip'
        session['shapefile_features'] = shp_result.get('features', 0)
        logger.info("Sample Shapefile processed and session updated.")
        
        # Log sample data loading
        logger = get_interaction_logger()
        if logger:
            # Log CSV sample
            logger.log_file_upload(
                session_id,
                'sample_csv',
                'sample_data.csv',
                os.path.getsize(target_csv_path),
                {'rows': csv_result.get('rows', 0), 'columns': csv_result.get('columns', 0)}
            )
            
            # Log shapefile sample
            logger.log_file_upload(
                session_id,
                'sample_shapefile',
                'sample_boundary.zip',
                os.path.getsize(target_zip_path),
                {'features': shp_result.get('features', 0)}
            )
            
            # Log the event
            logger.log_analysis_event(
                session_id,
                'load_sample_data',
                {'success': True},
                True
            )

        # --- Generate the 'analysis ready' prompt ---
        analysis_prompt = f"""
        <p><strong>Sample data loaded successfully!</strong></p>
        <p>The sample dataset includes:</p>
        <ul>
            <li>📊 CSV data: {session.get('csv_rows', 0)} rows with {session.get('csv_columns', 0)} columns</li>
            <li>🗺️ Shapefile data: {session.get('shapefile_features', 0)} features</li>
        </ul>
        <div class="analysis-ready-prompt">
            <p><strong>🚀 Everything is ready for analysis!</strong></p>
            <p>Type "Run the analysis" or click the button below to begin processing the sample data.</p>
            <button class="btn btn-primary mt-2" onclick="document.getElementById('message-input').value='Run the analysis'; document.getElementById('send-message').click();">
                Start Analysis on Sample Data
            </button>
        </div>
        """

        return jsonify({
            'status': 'success',
            'message': 'Sample data loaded successfully.',
            'rows': session.get('csv_rows', 0),
            'columns': session.get('csv_columns', 0),
            'features': session.get('shapefile_features', 0),
            'analysis_prompt': analysis_prompt
        })

    except Exception as e:
        logger.error(f"Error loading sample data: {str(e)}", exc_info=True)
        # Log the error
        logger = get_interaction_logger()
        if logger and session.get('session_id'):
            import traceback
            logger.log_error(
                session.get('session_id'),
                'sample_data_error',
                str(e),
                traceback.format_exc()
            )
        return jsonify({'status': 'error', 'message': f'An internal error occurred while loading sample data: {str(e)}'}), 500

@main.route('/serve_viz_file/<session_id>/<path:filename>')
def serve_viz_file(session_id, filename):
    """Serve visualization files (HTML) from the session's upload folder in the instance path."""
    # UPLOAD_FOLDER now points to instance_path/uploads
    directory = os.path.join(current_app.config['UPLOAD_FOLDER'], session_id)
    
    # Security check: ensure the filename is safe and doesn't try to escape the directory
    safe_path = os.path.abspath(os.path.join(directory, filename))
    if not safe_path.startswith(os.path.abspath(directory)):
        logger.error(f"Attempt to access unsafe path: {filename}")
        return jsonify({'status': 'error', 'message': 'Invalid file path.'}), 400
    
    if not os.path.exists(safe_path):
        logger.error(f"Visualization file not found: {safe_path}")
        return jsonify({'status': 'error', 'message': 'Visualization file not found.'}), 404
    try:
        return send_from_directory(directory, filename)
    except Exception as e:
        logger.error(f"Error serving viz file {filename} for session {session_id}: {e}")
        return jsonify({'status': 'error', 'message': 'Could not serve visualization file.'}), 500

@main.route('/get_visualization', methods=['POST'])
def get_visualization():
    """Handle visualization requests directly"""
    from flask import current_app
    
    data = request.json
    viz_type = data.get('type', '')
    variable = data.get('variable', None)
    threshold = data.get('threshold', 30)
    
    # Get session ID
    session_id = session.get('session_id')
    
    # Get data handler from session
    data_handler = get_data_handler()
    
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
    
    # Get the visualization service from the service container
    visualization_service = current_app.services.visualization_service
    
    # Prepare parameters for the visualization
    params = {
            'variable': variable,
            'threshold': threshold
        }
        
    try:
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
            
            # Convert to HTML if needed
            if explanation and not explanation.startswith('<'):
                from .ai_utils import convert_markdown_to_html
                explanation = convert_markdown_to_html(explanation)
            
            # Add explanation to result
            result['ai_response'] = explanation
        
        # Ensure the result is JSON serializable
        result = convert_to_json_serializable(result)
        
        return jsonify(result)

    except Exception as e:
        logger.error(f"Error generating visualization: {str(e)}", exc_info=True)
        return jsonify({
            'status': 'error',
            'message': f'Error generating visualization: {str(e)}',
            'ai_response': f"I encountered an error while creating the visualization. Please try again or choose a different visualization."
        })

@main.route('/navigate_composite_map', methods=['POST'])
def navigate_composite_map():
   """Handle pagination for composite maps"""
   data = request.json
   direction = data.get('direction', '')
   
   if not direction or direction not in ['next', 'prev']:
       return jsonify({'status': 'error', 'message': 'Invalid navigation direction'}), 400
   
   # Get data handler
   data_handler = get_data_handler()
   
   # Get current page from request or session
   current_page = data.get('current_page', session.get('current_composite_map_page', 1))
   
   # Determine new page based on direction
   if direction == 'next':
       new_page = current_page + 1
   else:  # prev
       new_page = max(1, current_page - 1)
   
   # Get the composite map for the new page
   result = viz.create_composite_map(data_handler, new_page)
   
   if result['status'] == 'success':
       # Update session with new page info
       session['current_composite_map_page'] = result.get('current_page', 1)
       
       # Log the navigation event
       logger = get_interaction_logger()
       if logger and session.get('session_id'):
           details = {
               'map_type': 'composite_map',
               'direction': direction,
               'new_page': new_page
           }
           logger.log_analysis_event(
               session.get('session_id'),
               'map_navigation',
               details,
               True
           )
       
       # Add LLM-generated explanation for this specific page
       if 'ai_response' not in result or not result['ai_response']:
           llm_manager = get_llm_manager_instance()
           
           # Create context for this specific page
           viz_metadata = {
               'type': 'composite_map',
               'data_summary': result.get('data_summary', {}),
               'visual_elements': result.get('visual_elements', {}),
               'current_page': new_page,
               'total_pages': result.get('total_pages', 1),
               'model_details': result.get('model_details', {})
           }
           
           ai_response = llm_manager.explain_visualization(
               session_id=session.get('session_id'),
               viz_type='composite_map',
               context=viz_metadata
           )
           
           # Convert markdown to HTML
           ai_response_html = convert_markdown_to_html(ai_response)
           
           result['ai_response'] = ai_response_html
       
       # Ensure all values in the result dictionary are JSON serializable
       result = convert_to_json_serializable(result)
       
       return jsonify(result)
   else:
       # Log the error
       logger = get_interaction_logger()
       if logger and session.get('session_id'):
           logger.log_error(
               session.get('session_id'),
               'map_navigation_error',
               result.get('message', 'Error navigating composite maps')
           )
       return jsonify({
           'status': 'error',
           'message': result.get('message', 'Error navigating composite maps')
       }), 400

@main.route('/navigate_boxplot', methods=['POST'])
def navigate_boxplot():
   """Handle pagination for box and whisker plots"""
   data = request.json
   direction = data.get('direction', '')
   
   if not direction or direction not in ['next', 'prev']:
       return jsonify({'status': 'error', 'message': 'Invalid navigation direction'}), 400
   
   # Get data handler
   data_handler = get_data_handler()
   
   # Check if box plot data is available
   if not hasattr(data_handler, 'boxwhisker_plot') or not data_handler.boxwhisker_plot:
       return jsonify({
           'status': 'error',
           'message': 'Box plot data not available'
       }), 400
   
   # Get current page from request or session
   current_page = data.get('current_page', session.get('current_boxplot_page', 1))
   total_pages = len(data_handler.boxwhisker_plot['plots'])
   
   # Determine new page based on direction
   if direction == 'next':
       new_page = min(current_page + 1, total_pages)
   else:  # prev
       new_page = max(1, current_page - 1)
   
   # Get the plot for the new page
   if 1 <= new_page <= total_pages:
       plot_fig = data_handler.boxwhisker_plot['plots'][new_page - 1]
   else:
       return jsonify({
           'status': 'error',
           'message': f'Invalid page number: {new_page}. Valid range is 1-{total_pages}'
       }), 400
   
   # Save as HTML
   html_path = viz.create_plotly_html(plot_fig, f"vulnerability_plot_page{new_page}.html")
   
   # Update session
   session['current_boxplot_page'] = new_page
   
   # Log the navigation event
   logger = get_interaction_logger()
   if logger and session.get('session_id'):
       details = {
           'plot_type': 'vulnerability_plot',
           'direction': direction,
           'new_page': new_page,
           'total_pages': total_pages
       }
       logger.log_analysis_event(
           session.get('session_id'),
           'plot_navigation',
           details,
           True
       )
   
   # Prepare result with LLM explanation
   result = {
       'status': 'success',
       'message': f'Successfully navigated to box plot page {new_page}',
       'image_path': html_path,
       'current_page': int(new_page),
       'total_pages': int(total_pages),
       'viz_type': 'vulnerability_plot'
   }
   
   # Get explanation from LLM for this page
   llm_manager = get_llm_manager_instance()
   
   # Gather information about the wards on this page
   wards_info = data_handler.boxwhisker_plot.get('page_data', {}).get(str(new_page), [])
   
   viz_metadata = {
       'type': 'vulnerability_plot',
       'data_summary': {
           'current_page': new_page,
           'total_pages': total_pages,
           'wards_shown': [ward['ward_name'] for ward in wards_info[:5]]  # Show first 5 for brevity
       },
       'visual_elements': {
           'plot_type': 'Box and whisker',
           'color_scheme': 'By vulnerability category'
       },
       'ward_details': wards_info[:5]  # Limit to first 5 for context size
   }
   
   ai_response = llm_manager.explain_visualization(
       session_id=session.get('session_id'),
       viz_type='vulnerability_plot',
       context=viz_metadata
   )
   
   # Convert markdown to HTML
   ai_response_html = convert_markdown_to_html(ai_response)
   
   result['ai_response'] = ai_response_html
   
   # Ensure all values in the result dictionary are JSON serializable
   result = convert_to_json_serializable(result)
   
   return jsonify(result)

@main.route('/update_boxplot_pagination', methods=['POST'])
def update_boxplot_pagination():
   """Update box plot pagination with new wards per page"""
   data = request.json
   wards_per_page = data.get('wards_per_page', 20)
   
   # Get data handler
   data_handler = get_data_handler()
   
   # Check if composite scores are available
   if not hasattr(data_handler, 'composite_scores') or not data_handler.composite_scores:
       return jsonify({
           'status': 'error',
           'message': 'Composite scores not available'
       }), 400
   
   # Generate new box plot with updated wards per page
   box_plot_result = viz.box_plot_function(data_handler.composite_scores['scores'], wards_per_page)
   
   if box_plot_result['status'] == 'success':
       # Store the box plot data for pagination
       data_handler.boxwhisker_plot = box_plot_result
       # Get the first plot
       plot_fig = box_plot_result['plots'][0]
       # Save as HTML
       html_path = viz.create_plotly_html(plot_fig, "vulnerability_plot.html")
       
       # Update session
       session['current_boxplot_page'] = 1
       
       # Log the pagination update
       logger = get_interaction_logger()
       if logger and session.get('session_id'):
           details = {
               'plot_type': 'vulnerability_plot',
               'wards_per_page': wards_per_page,
               'total_pages': box_plot_result['total_pages']
           }
           logger.log_analysis_event(
               session.get('session_id'),
               'update_boxplot_pagination',
               details,
               True
           )
       
       # Prepare result
       result = {
           'status': 'success',
           'message': 'Successfully updated box plot pagination',
           'image_path': html_path,
           'current_page': 1,
           'total_pages': box_plot_result['total_pages'],
           'viz_type': 'vulnerability_plot'
       }
       
       # Get explanation from LLM
       llm_manager = get_llm_manager_instance()
       
       # Gather information about the wards on the first page
       wards_info = box_plot_result.get('page_data', {}).get('1', [])
       
       viz_metadata = {
           'type': 'vulnerability_plot',
           'data_summary': {
               'current_page': 1,
               'total_pages': box_plot_result['total_pages'],
               'wards_per_page': wards_per_page,
               'wards_shown': [ward['ward_name'] for ward in wards_info[:5]]  # Show first 5 for brevity
           },
           'visual_elements': {
               'plot_type': 'Box and whisker',
               'color_scheme': 'By vulnerability category'
           }
       }
       
       ai_response = llm_manager.explain_visualization(
           session_id=session.get('session_id'),
           viz_type='vulnerability_plot',
           context=viz_metadata
       )
       
       # Convert markdown to HTML
       ai_response_html = convert_markdown_to_html(ai_response)
       
       result['ai_response'] = ai_response_html
       
       # Ensure all values in the result dictionary are JSON serializable
       result = convert_to_json_serializable(result)
       
       return jsonify(result)
   else:
       # Log the error
       logger = get_interaction_logger()
       if logger and session.get('session_id'):
           logger.log_error(
               session.get('session_id'),
               'update_boxplot_pagination_error',
               box_plot_result.get('message', 'Error updating box plot pagination')
           )
       return jsonify({
           'status': 'error',
           'message': box_plot_result.get('message', 'Error updating box plot pagination')
       }), 400

@main.route('/download_report/<filename>')
def download_report(filename):
   """Handle report downloads"""
   # REPORTS_FOLDER now points to instance_path/reports
   session_folder = os.path.join(current_app.config['REPORTS_FOLDER'], session.get('session_id', 'default'))
   
   # Security check
   safe_path = os.path.abspath(os.path.join(session_folder, filename))
   if not safe_path.startswith(os.path.abspath(session_folder)):
       logger.error(f"Attempt to access unsafe report path: {filename}")
       return jsonify({'status': 'error', 'message': 'Invalid file path.'}), 400

   if not os.path.exists(safe_path):
       logger.error(f"Report file not found: {safe_path}")
       return jsonify({'status': 'error', 'message': 'Report file not found.'}), 404
   try:
       # Log the report download
       logger = get_interaction_logger()
       if logger and session.get('session_id'):
           details = {
               'report_file': filename,
               'file_size': os.path.getsize(safe_path)
           }
           logger.log_analysis_event(
               session.get('session_id'),
               'report_download',
               details,
               True
           )
       return send_from_directory(session_folder, filename, as_attachment=True)
   except Exception as e:
       logger.error(f"Error serving report file {filename} for session {session.get('session_id', 'default')}: {e}")
       # Log the error
       if logger and session.get('session_id'):
           logger.log_error(
               session.get('session_id'),
               'report_download_error',
               str(e)
           )
       return jsonify({'status': 'error', 'message': 'Could not serve report file.'}), 500

@main.route('/send_message', methods=['POST'])
def send_message():
    """
    Handle chat messages and AI responses with enhanced LLM-based understanding and explanations.
    """
    from flask import current_app
    
    # Get the message from the request
    data = request.json
    user_message = data.get('message', '')
    if not user_message: 
        return jsonify({'status': 'error', 'message': 'No message provided'}), 400

    # Get session ID
    session_id = session.get('session_id')
    
    # Get the message service from the service container
    message_service = current_app.services.message_service

    # Get current session state and data handler
    data_handler = get_data_handler()
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
    
    # Check if we need to update session state based on the response
    if result.get('status') == 'success':
        # Handle pending action if present
        if result.get('action') == 'confirm_custom_analysis':
            session['pending_action'] = 'confirm_custom_analysis'
            session['pending_variables'] = result.get('pending_variables')
        elif result.get('action') == 'analysis_complete':
            # Clear pending state
            if 'pending_action' in session:
                session.pop('pending_action', None)
            if 'pending_variables' in session:
                session.pop('pending_variables', None)
            
            # Mark analysis as complete
            session['analysis_complete'] = True
    
    # Return the result to the client
    return jsonify(result)

# --- Helper functions ---

def get_data_handler():
    """Get the data handler for the current session"""
    session_id = session.get('session_id', 'default')
    session_data = current_app.config.get('SESSION_DATA', {})
    
    if session_id in session_data:
        return session_data[session_id]['data_handler']
    
    # Create new data handler if not found
    session_folder = os.path.join(current_app.config['UPLOAD_FOLDER'], session_id)
    data_handler = DataHandler(session_folder)
    
    # Load files if they exist in session
    csv_filename = session.get('csv_filename', '')
    shapefile_filename = session.get('shapefile_filename', '')
    
    if csv_filename:
        csv_path = os.path.join(session_folder, csv_filename)
        if os.path.exists(csv_path):
            data_handler.load_csv(csv_path)
    
    if shapefile_filename:
        shp_path = os.path.join(session_folder, shapefile_filename)
        if os.path.exists(shp_path):
            data_handler.load_shapefile(shp_path)
    
    # Store in session data
    current_app.config.setdefault('SESSION_DATA', {})
    current_app.config['SESSION_DATA'][session_id] = {
        'data_handler': data_handler,
        'timestamp': datetime.now()
    }
    
    return data_handler

def convert_to_json_serializable(obj):
    """
    Recursively convert objects to JSON serializable types.
    Specifically handles NumPy types which are not JSON serializable by default.
    """
    if isinstance(obj, dict):
        return {k: convert_to_json_serializable(v) for k, v in obj.items()}
    elif isinstance(obj, list):
        return [convert_to_json_serializable(item) for item in obj]
    
    # Integer types
    elif hasattr(np, 'integer') and isinstance(obj, np.integer):
        return int(obj)
    # Floating point types
    elif hasattr(np, 'floating') and isinstance(obj, np.floating):
        return float(obj)
    # Boolean types
    elif hasattr(np, 'bool_') and isinstance(obj, np.bool_):
        return bool(obj)
    # NumPy arrays
    elif isinstance(obj, np.ndarray):
        return convert_to_json_serializable(obj.tolist())
        
    # Other Python types
    elif obj is None or isinstance(obj, (str, int, float, bool)):
        return obj
    # For other types, try string conversion
    else:
        try:
            return str(obj)
        except:
            return f"Unserializable object of type: {type(obj).__name__}"

# --- Admin routes for interaction logging ---

@main.route('/admin/logs', methods=['GET'])
def admin_logs():
    """Admin interface to view interaction logs"""
    # Simple password protection (replace with proper authentication)
    if request.args.get('key') != current_app.config.get('ADMIN_KEY', 'admin'):
        return jsonify({'status': 'error', 'message': 'Unauthorized'}), 401
    
    # Get interaction logger
    logger = get_interaction_logger()
    if not logger:
        return jsonify({'status': 'error', 'message': 'Interaction logger not initialized'}), 500
    
    # Connect to database
    try:
        conn = sqlite3.connect(logger.db_path)
        conn.row_factory = sqlite3.Row  # Return rows as dictionaries
        cursor = conn.cursor()
        
        # Get sessions
        cursor.execute('''
        SELECT * FROM sessions ORDER BY last_activity DESC LIMIT 100
        ''')
        sessions = []
        for row in cursor.fetchall():
            session_dict = dict(row)
            # Ensure last_activity and start_time are strings
            for time_field in ['last_activity', 'start_time']:
                if time_field in session_dict and session_dict[time_field] is not None:
                    session_dict[time_field] = str(session_dict[time_field])
                else:
                    session_dict[time_field] = ""
            sessions.append(session_dict)
        
        # Get message counts by session
        cursor.execute('''
        SELECT session_id, COUNT(*) as message_count FROM messages
        GROUP BY session_id
        ''')
        message_counts = {row['session_id']: row['message_count'] for row in cursor.fetchall()}
        
        # Get error counts by session
        cursor.execute('''
        SELECT session_id, COUNT(*) as error_count FROM errors
        GROUP BY session_id
        ''')
        error_counts = {row['session_id']: row['error_count'] for row in cursor.fetchall()}
        
        # Add counts to sessions
        for session in sessions:
            session_id = session['session_id']
            session['message_count'] = message_counts.get(session_id, 0)
            session['error_count'] = error_counts.get(session_id, 0)
        
        conn.close()
        
        # Add today's date for filtering
        today_date = datetime.now().strftime('%Y-%m-%d')
        
        return render_template('admin_logs.html', sessions=sessions, today_date=today_date)
        
    except Exception as e:
        current_app.logger.error(f"Error retrieving logs: {str(e)}", exc_info=True)
        return jsonify({'status': 'error', 'message': f'Error retrieving logs: {str(e)}'}), 500

@main.route('/admin/session/<session_id>', methods=['GET'])
def admin_session_detail(session_id):
    """View detailed logs for a specific session"""
    # Simple password protection (replace with proper authentication)
    if request.args.get('key') != current_app.config.get('ADMIN_KEY', 'admin'):
        return jsonify({'status': 'error', 'message': 'Unauthorized'}), 401
    
    # Get interaction logger
    logger = get_interaction_logger()
    if not logger:
        return jsonify({'status': 'error', 'message': 'Interaction logger not initialized'}), 500
    
    # Connect to database
    try:
        conn = sqlite3.connect(logger.db_path)
        conn.row_factory = sqlite3.Row  # Return rows as dictionaries
        cursor = conn.cursor()
        
        # Get session info
        cursor.execute('SELECT * FROM sessions WHERE session_id = ?', (session_id,))
        session_info = dict(cursor.fetchone() or {})
        
        if not session_info:
            return jsonify({'status': 'error', 'message': 'Session not found'}), 404
        
        # Get messages
        cursor.execute('''
        SELECT * FROM messages WHERE session_id = ? ORDER BY timestamp
        ''', (session_id,))
        messages = [dict(row) for row in cursor.fetchall()]
        
        # Get file uploads
        cursor.execute('''
        SELECT * FROM file_uploads WHERE session_id = ? ORDER BY timestamp
        ''', (session_id,))
        uploads = [dict(row) for row in cursor.fetchall()]
        
        # Get analysis events
        cursor.execute('''
        SELECT * FROM analysis_events WHERE session_id = ? ORDER BY timestamp
        ''', (session_id,))
        events = [dict(row) for row in cursor.fetchall()]
        
        # Get errors
        cursor.execute('''
        SELECT * FROM errors WHERE session_id = ? ORDER BY timestamp
        ''', (session_id,))
        errors = [dict(row) for row in cursor.fetchall()]
        
        # Get analysis steps
        cursor.execute('''
        SELECT * FROM analysis_steps WHERE session_id = ? ORDER BY timestamp
        ''', (session_id,))
        analysis_steps = [dict(row) for row in cursor.fetchall()]
        
        # Get visualization metadata
        cursor.execute('''
        SELECT * FROM visualization_metadata WHERE session_id = ? ORDER BY timestamp
        ''', (session_id,))
        visualizations = [dict(row) for row in cursor.fetchall()]
        
        # Get LLM interactions
        cursor.execute('''
        SELECT * FROM llm_interactions WHERE session_id = ? ORDER BY timestamp
        ''', (session_id,))
        llm_interactions = [dict(row) for row in cursor.fetchall()]
        
        conn.close()
        
        return render_template(
            'admin_session_detail.html', 
            session_info=session_info,
            messages=messages,
            uploads=uploads,
            events=events,
            errors=errors,
            analysis_steps=analysis_steps,
            visualizations=visualizations,
            llm_interactions=llm_interactions
        )
        
    except Exception as e:
        current_app.logger.error(f"Error retrieving session details: {str(e)}", exc_info=True)
        return jsonify({'status': 'error', 'message': f'Error retrieving session details: {str(e)}'}), 500

@main.route('/admin/export', methods=['GET'])
def admin_export_logs():
    """Export logs as JSON"""
    # Simple password protection (replace with proper authentication)
    if request.args.get('key') != current_app.config.get('ADMIN_KEY', 'admin'):
        return jsonify({'status': 'error', 'message': 'Unauthorized'}), 401
    
    # Get interaction logger
    logger = get_interaction_logger()
    if not logger:
        return jsonify({'status': 'error', 'message': 'Interaction logger not initialized'}), 500
    
    # Get export format
    format_type = request.args.get('format', 'json')
    
    if format_type == 'csv':
        # Use the export_to_csv method
        export_dir = os.path.join(current_app.instance_path, 'exports')
        os.makedirs(export_dir, exist_ok=True)
        
        result = logger.export_to_csv(output_dir=export_dir)
        
        if result['status'] == 'success':
            # Create a zip file with all CSV files
            import zipfile
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            zip_filename = f'mrpt_logs_{timestamp}.zip'
            zip_path = os.path.join(export_dir, zip_filename)
            
            with zipfile.ZipFile(zip_path, 'w') as zip_file:
                for table, file_path in result['files'].items():
                    zip_file.write(file_path, os.path.basename(file_path))
            
            return send_from_directory(export_dir, zip_filename, as_attachment=True)
        else:
            return jsonify({'status': 'error', 'message': result['message']}), 500
    else:
        # Use the export_to_json method
        try:
            # Generate unique filename with timestamp
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            output_file = os.path.join(current_app.instance_path, 'exports', f'mrpt_logs_{timestamp}.json')
            os.makedirs(os.path.dirname(output_file), exist_ok=True)
            
            # Use the export method with requested parameters
            result = logger.export_to_json(
                session_id=request.args.get('session_id'),
                include_llm_data=request.args.get('include_llm', 'true').lower() == 'true',
                compact=request.args.get('compact', 'false').lower() == 'true',
                output_file=output_file
            )
            
            if result['status'] == 'success':
                return send_from_directory(os.path.dirname(output_file), os.path.basename(output_file), as_attachment=True)
            else:
                return jsonify({'status': 'error', 'message': result['message']}), 500
        except Exception as e:
            current_app.logger.error(f"Error exporting logs: {str(e)}", exc_info=True)
            return jsonify({'status': 'error', 'message': f'Error exporting logs: {str(e)}'}), 500

@main.route('/admin/training_data', methods=['GET'])
def admin_export_training_data():
    """Export conversation training data for fine-tuning"""
    # Simple password protection (replace with proper authentication)
    if request.args.get('key') != current_app.config.get('ADMIN_KEY', 'admin'):
        return jsonify({'status': 'error', 'message': 'Unauthorized'}), 401
    
    # Get interaction logger
    logger = get_interaction_logger()
    if not logger:
        return jsonify({'status': 'error', 'message': 'Interaction logger not initialized'}), 500
    
    try:
        # Get training data with optional filters
        training_data = logger.get_conversation_training_data(
            session_id=request.args.get('session_id'),
            min_quality=request.args.get('min_quality'),
            start_date=request.args.get('start_date'),
            end_date=request.args.get('end_date')
        )
        
        if not training_data:
            return jsonify({'status': 'error', 'message': 'No training data found matching criteria'}), 404
        
        # Generate unique filename with timestamp
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        output_dir = os.path.join(current_app.instance_path, 'exports')
        os.makedirs(output_dir, exist_ok=True)
        output_file = os.path.join(output_dir, f'training_data_{timestamp}.json')
        
        # Write to file
        with open(output_file, 'w') as f:
            json.dump(training_data, f, indent=2)
        
        return send_from_directory(output_dir, os.path.basename(output_file), as_attachment=True)
        
    except Exception as e:
        current_app.logger.error(f"Error exporting training data: {str(e)}", exc_info=True)
        return jsonify({'status': 'error', 'message': f'Error exporting training data: {str(e)}'}), 500

@main.route('/api/explain', methods=['POST'])
def api_explain():
    """API endpoint for generating explanations"""
    # Get request data
    data = request.json
    
    if not data:
        return jsonify({'status': 'error', 'message': 'No request data provided'}), 400
    
    entity_type = data.get('entity_type')
    entity_name = data.get('entity_name')
    question = data.get('question')
    session_id = data.get('session_id', session.get('session_id'))
    
    if not session_id:
        return jsonify({'status': 'error', 'message': 'Session ID required'}), 400
    
    if not entity_type or not entity_name:
        return jsonify({'status': 'error', 'message': 'Entity type and name required'}), 400
    
    # Initialize LLM manager
    llm_manager = get_llm_manager_instance()
    
    try:
        # Generate appropriate explanation based on entity type
        if entity_type == 'ward':
            explanation = llm_manager.explain_ward(session_id, entity_name, question)
        elif entity_type == 'variable':
            explanation = llm_manager.explain_variable(session_id, entity_name, question)
        elif entity_type == 'methodology':
            explanation = llm_manager.explain_methodology(session_id, entity_name, question)
        elif entity_type == 'visualization':
            explanation = llm_manager.explain_visualization(session_id, entity_name, question)
        else:
            return jsonify({'status': 'error', 'message': f'Unknown entity type: {entity_type}'}), 400
        
        # Log the explanation if interaction logger is available
        interaction_logger = get_interaction_logger()
        if interaction_logger:
            interaction_logger.log_explanation(
                session_id=session_id,
                entity_type=entity_type,
                entity_name=entity_name,
                question_type='api_request',
                question=question,
                explanation=explanation
            )
        
        return jsonify({
            'status': 'success',
            'explanation': explanation,
            'entity_type': entity_type,
            'entity_name': entity_name
        })
        
    except Exception as e:
        current_app.logger.error(f"Error generating explanation: {str(e)}", exc_info=True)
        return jsonify({'status': 'error', 'message': f'Error generating explanation: {str(e)}'}), 500

@main.route('/api/variables', methods=['GET'])
def get_variables():
    """API endpoint to get available variables for auto-complete functionality"""
    try:
        # Get data handler from session
        data_handler = get_data_handler()
        
        # Check if data is loaded
        if not data_handler or not hasattr(data_handler, 'df') or data_handler.df is None:
            return jsonify({
                'status': 'error',
                'message': 'No data loaded'
            })
        
        # Get available variables
        variables = []
        
        # Get column names from dataframe
        if hasattr(data_handler, 'df') and data_handler.df is not None:
            # Get column names excluding non-variable columns like ID, Name, etc.
            exclude_columns = ['ward_id', 'ward_name', 'geometry', 'the_geom', 'id', 'name', 'index', 'lat', 'lon']
            variables = [col for col in data_handler.df.columns if col.lower() not in [x.lower() for x in exclude_columns]]
            
            # Sort by name for easier browsing
            variables.sort()
        
        # If additional variables exist (e.g., derived variables)
        if hasattr(data_handler, 'derived_variables') and data_handler.derived_variables:
            for var in data_handler.derived_variables:
                if var not in variables:
                    variables.append(var)
        
        # Return the variable list
        return jsonify({
            'status': 'success',
            'variables': variables
        })
    except Exception as e:
        logger.error(f"Error getting variables: {str(e)}", exc_info=True)
        return jsonify({
            'status': 'error',
            'message': f'Error getting variables: {str(e)}'
        })

@main.route('/api/wards', methods=['GET'])
def api_get_wards():
    """API endpoint to get ward information"""
    session_id = request.args.get('session_id', session.get('session_id'))
    
    if not session_id:
        return jsonify({'status': 'error', 'message': 'Session ID required'}), 400
    
    try:
        # Get data handler for the session
        data_handler = get_data_handler()
        
        if not data_handler:
            return jsonify({'status': 'error', 'message': 'Data handler not found for session'}), 404
        
        # Check if analysis is complete
        if not hasattr(data_handler, 'vulnerability_rankings') or not data_handler.vulnerability_rankings is not None:
            return jsonify({'status': 'error', 'message': 'Vulnerability rankings not available'}), 400
        
        # Get vulnerable wards
        wards = data_handler.vulnerability_rankings.to_dict('records')
        
        return jsonify({
            'status': 'success',
            'wards': wards
        })
        
    except Exception as e:
        current_app.logger.error(f"Error retrieving wards: {str(e)}", exc_info=True)
        return jsonify({'status': 'error', 'message': f'Error retrieving wards: {str(e)}'}), 500

def run_custom_analysis(data_handler, selected_variables=None, question=None):
    """
    Run custom analysis with selected variables
    
    Args:
        data_handler: DataHandler instance
        selected_variables: List of selected variables
        question: User's analysis question
        
    Returns:
        dict: Analysis results
    """
    from flask import current_app
    
    try:
        # Check if data handler is valid
        if data_handler is None:
            return {
                'status': 'error',
                'message': 'No data handler provided. Please load data first.'
            }
            
        # Get the analysis service from the service container
        analysis_service = current_app.services.analysis_service
        
        # Check if we have any variables to validate
        if not selected_variables:
            return {
                'status': 'error',
                'message': 'No variables provided for analysis'
            }
        
        # Validate the selected variables using the new flexible method
        validation_result = data_handler.validate_variables(selected_variables)
        
        if not validation_result['is_valid']:
            # Return a helpful error message with suggestions
            invalid_vars = validation_result.get('invalid_variables', [])
            invalid_msg = f"Could not recognize: {', '.join(invalid_vars)}" if invalid_vars else ""
            
            # Get some available variable examples for the error message
            available_vars = data_handler.get_available_variables()[:5]  # Just show 5 examples
            vars_msg = f"\n\nAvailable variables include: {', '.join(available_vars)}" if available_vars else ""
            
            return {
                'status': 'error',
                'message': f"Not enough valid variables for analysis. {invalid_msg}{vars_msg}"
            }
        
        # Use the validated variables
        valid_variables = validation_result['valid_variables']
        
        # Get session ID for logging (if available)
        session_id = None
        if hasattr(current_app, 'session') and 'session_id' in current_app.session:
            session_id = current_app.session.get('session_id')
        
        # Check if any fuzzy matching happened
        match_details = validation_result.get('match_details', {})
        if match_details:
            logger.info(f"Variables fuzzy matched: {match_details}")
        
        # Run custom analysis using the analysis service
        result = analysis_service.run_custom_analysis(
            data_handler=data_handler,
            selected_variables=valid_variables,
            question=question,
            session_id=session_id
        )
        
        # If successful, add the matching information
        if result['status'] == 'success' and match_details:
            result['variable_matching'] = match_details
        
        # Return the result
        return result
            
    except Exception as e:
        logger.error(f"Error in custom analysis: {str(e)}", exc_info=True)
        return {
            'status': 'error',
            'message': f'Error running custom analysis: {str(e)}'
        }

system_message = f"""
You are an expert in malaria epidemiology. Explain this visualization for a non-technical audience.
- Use bullet points.
- Limit each section to 1-2 sentences.
- Only include the most important details for understanding and decision-making.
- Do not repeat information across sections.
- Be concise and clear.
Sections:
1. Overview: What is this visualization and why does it matter?
2. How to Read: What do the colors/symbols mean?
3. Key Insights: What are the main takeaways?
4. Action: What should the user do or consider next?
"""

# Add routes for the remaining HTMX endpoints
@main.route('/upload_form', methods=['GET'])
def upload_form():
    """Return the upload form modal content"""
    # Send a simple message for upload form
    return """
    <div class="p-4">
        <h3 class="text-xl font-semibold mb-4">Upload Data Files</h3>
        <form id="upload-form" enctype="multipart/form-data" hx-post="/upload_both_files" hx-target="#chat-messages" hx-swap="beforeend" hx-indicator="#upload-indicator">
            <div class="mb-4">
                <label for="csv-file" class="block text-sm font-medium text-gray-700 dark:text-gray-300 mb-2">
                    CSV/Excel File (Required):
                </label>
                <input type="file" id="csv-file" name="csv_file" accept=".csv,.xlsx,.xls" class="block w-full text-sm text-gray-500 file:mr-4 file:py-2 file:px-4 file:rounded file:border-0 file:text-sm file:font-semibold file:bg-primary-50 file:text-primary-700 hover:file:bg-primary-100 dark:file:bg-primary-900 dark:file:text-primary-200">
                <p class="text-xs text-gray-500 dark:text-gray-400 mt-1">Contains your malaria and risk factor data</p>
            </div>
            
            <div class="mb-4">
                <label for="shapefile" class="block text-sm font-medium text-gray-700 dark:text-gray-300 mb-2">
                    Shapefile (Optional):
                </label>
                <input type="file" id="shapefile" name="shapefile" accept=".zip" class="block w-full text-sm text-gray-500 file:mr-4 file:py-2 file:px-4 file:rounded file:border-0 file:text-sm file:font-semibold file:bg-primary-50 file:text-primary-700 hover:file:bg-primary-100 dark:file:bg-primary-900 dark:file:text-primary-200">
                <p class="text-xs text-gray-500 dark:text-gray-400 mt-1">Zipped shapefile for geographic visualization (.zip)</p>
            </div>
            
            <div class="mb-4 bg-gray-50 dark:bg-gray-900 p-3 rounded-md">
                <div class="flex items-center text-xs text-gray-600 dark:text-gray-400">
                    <i class="fas fa-info-circle mr-2 text-primary-600 dark:text-primary-400"></i>
                    <span>Your data should contain administrative units with malaria cases and relevant variables.</span>
                </div>
            </div>
            
            <div>
                <button type="submit" class="w-full flex justify-center items-center py-2 px-4 border border-transparent rounded-md shadow-sm text-sm font-medium text-white bg-primary-600 hover:bg-primary-700 focus:outline-none focus:ring-2 focus:ring-offset-2 focus:ring-primary-500">
                    <i class="fas fa-upload mr-2"></i>
                    Upload Files
                </button>
                
                <div id="upload-indicator" class="htmx-indicator flex justify-center mt-2">
                    <div class="inline-flex items-center px-4 py-2 font-semibold leading-6 text-sm rounded-md text-primary-800 bg-primary-100 dark:text-primary-200 dark:bg-primary-900">
                        <svg class="animate-spin -ml-1 mr-3 h-5 w-5 text-primary-600 dark:text-primary-400" xmlns="http://www.w3.org/2000/svg" fill="none" viewBox="0 0 24 24">
                            <circle class="opacity-25" cx="12" cy="12" r="10" stroke="currentColor" stroke-width="4"></circle>
                            <path class="opacity-75" fill="currentColor" d="M4 12a8 8 0 018-8V0C5.373 0 0 5.373 0 12h4zm2 5.291A7.962 7.962 0 014 12H0c0 3.042 1.135 5.824 3 7.938l3-2.647z"></path>
                        </svg>
                        Uploading files...
                    </div>
                </div>
            </div>
        </form>
    </div>
    """

@main.route('/report_form', methods=['GET'])
def report_form():
    """Return the report generation form modal content"""
    # Send a simple message for report form
    return """
    <div class="p-4">
        <h3 class="text-xl font-semibold mb-4">Generate Analysis Report</h3>
        <form id="report-form" hx-post="/generate_report" hx-target="#report-status" hx-swap="innerHTML" hx-indicator="#report-indicator">
            <div class="mb-4">
                <label for="report-format" class="block text-sm font-medium text-gray-700 dark:text-gray-300 mb-2">
                    Select Report Format:
                </label>
                <select id="report-format" name="report_format" class="block w-full rounded-md border-gray-300 dark:border-gray-700 shadow-sm focus:border-primary-500 focus:ring-primary-500 dark:bg-gray-700 dark:text-white py-2 px-3">
                    <option value="pdf" selected>PDF Document</option>
                    <option value="html">HTML Web Page</option>
                    <option value="markdown">Markdown (.md)</option>
                </select>
            </div>
            
            <div class="mb-4 bg-gray-50 dark:bg-gray-900 p-4 rounded-md">
                <h4 class="text-sm font-medium text-gray-700 dark:text-gray-300 mb-2">Report Contents:</h4>
                <ul class="text-xs text-gray-500 dark:text-gray-400 ml-5 list-disc">
                    <li>Data Overview & Methodology Summary</li>
                    <li>Missing Value Handling Details</li>
                    <li>Variable Relationship Analysis</li>
                    <li>Composite Score Calculation Insights</li>
                    <li>Ward Vulnerability Rankings & Prioritization</li>
                    <li>Urban Extent Analysis Findings</li>
                </ul>
            </div>
            
            <div>
                <button type="submit" class="w-full flex justify-center items-center py-2 px-4 border border-transparent rounded-md shadow-sm text-sm font-medium text-white bg-primary-600 hover:bg-primary-700 focus:outline-none focus:ring-2 focus:ring-offset-2 focus:ring-primary-500">
                    <i class="fas fa-cogs mr-2"></i>
                    Generate Report
                </button>
                
                <div id="report-indicator" class="htmx-indicator flex justify-center mt-2">
                    <div class="inline-flex items-center px-4 py-2 font-semibold leading-6 text-sm rounded-md text-primary-800 bg-primary-100 dark:text-primary-200 dark:bg-primary-900">
                        <svg class="animate-spin -ml-1 mr-3 h-5 w-5 text-primary-600 dark:text-primary-400" xmlns="http://www.w3.org/2000/svg" fill="none" viewBox="0 0 24 24">
                            <circle class="opacity-25" cx="12" cy="12" r="10" stroke="currentColor" stroke-width="4"></circle>
                            <path class="opacity-75" fill="currentColor" d="M4 12a8 8 0 018-8V0C5.373 0 0 5.373 0 12h4zm2 5.291A7.962 7.962 0 014 12H0c0 3.042 1.135 5.824 3 7.938l3-2.647z"></path>
                        </svg>
                        Generating report...
                    </div>
                </div>
                
                <div id="report-status" class="mt-2">
                    <!-- Report generation status messages will appear here -->
                </div>
            </div>
        </form>
    </div>
    """