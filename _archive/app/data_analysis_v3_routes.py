"""
Data Analysis V3 Routes
Handles file uploads and queries for the Data Analysis tab
"""

import os
import logging
from datetime import datetime
from flask import Blueprint, request, jsonify, session, current_app
from werkzeug.utils import secure_filename
from app.data_analysis_v3.core.metadata_cache import MetadataCache

logger = logging.getLogger(__name__)

# Create blueprint
data_analysis_v3_bp = Blueprint('data_analysis_v3', __name__)

ALLOWED_EXTENSIONS = {'csv', 'xlsx', 'xls', 'json', 'txt'}

def allowed_file(filename):
    """Check if file extension is allowed."""
    return '.' in filename and filename.rsplit('.', 1)[1].lower() in ALLOWED_EXTENSIONS


@data_analysis_v3_bp.route('/api/data-analysis/upload', methods=['POST'])
def upload_for_analysis():
    """
    Handle file uploads from the Data Analysis tab.
    This is specifically for general data exploration, not malaria risk analysis.
    """
    try:
        # Get session ID
        session_id = session.get('session_id')
        if not session_id:
            session_id = request.form.get('session_id', f'session_{os.urandom(8).hex()}')
            session['session_id'] = session_id
        
        # Check for file
        if 'file' not in request.files:
            return jsonify({
                'status': 'error',
                'message': 'No file provided'
            }), 400
        
        file = request.files['file']
        
        if file.filename == '':
            return jsonify({
                'status': 'error',
                'message': 'No file selected'
            }), 400
        
        if not allowed_file(file.filename):
            return jsonify({
                'status': 'error',
                'message': f'Invalid file type. Allowed types: {", ".join(ALLOWED_EXTENSIONS)}'
            }), 400
        
        # Create upload directory
        upload_dir = os.path.join(current_app.config.get('UPLOAD_FOLDER', 'instance/uploads'), session_id)
        os.makedirs(upload_dir, exist_ok=True)
        
        # Save file
        filename = secure_filename(file.filename)
        filepath = os.path.join(upload_dir, filename)
        file.save(filepath)
        
        # Also save as a standard name for easy access
        if filename.endswith('.csv'):
            standard_path = os.path.join(upload_dir, 'data_analysis.csv')
        elif filename.endswith(('.xlsx', '.xls')):
            standard_path = os.path.join(upload_dir, 'data_analysis.xlsx')
        elif filename.endswith('.json'):
            standard_path = os.path.join(upload_dir, 'data_analysis.json')
        else:
            standard_path = os.path.join(upload_dir, 'data_analysis.txt')
        
        # Copy to standard name
        import shutil
        shutil.copy2(filepath, standard_path)
        
        # Extract and cache metadata for quick access
        logger.info(f"📊 Extracting metadata for {filename}...")
        metadata = MetadataCache.update_file_metadata(session_id, filepath, filename)
        
        # Also cache metadata for the standard file
        if standard_path != filepath:
            standard_filename = os.path.basename(standard_path)
            MetadataCache.update_file_metadata(session_id, standard_path, standard_filename)
        
        logger.info(f"📊 Data Analysis file uploaded: {filename} for session {session_id}")
        if metadata.get('is_sampled'):
            logger.info(f"📊 Large file detected ({metadata.get('file_size_mb')}MB), using sampling for metadata")
        
        # Store in session with workflow context
        session['has_data_analysis_file'] = True
        session['data_analysis_filename'] = filename
        session['workflow_source'] = 'data_analysis_v3'  # Track where workflow originated
        session['workflow_stage'] = 'uploaded'  # Track current stage
        session.modified = True
        
        # Also create a flag file for cross-worker detection
        flag_file = os.path.join(upload_dir, '.data_analysis_mode')
        with open(flag_file, 'w') as f:
            f.write(f'{filename}\n{datetime.now().isoformat()}')
        
        # Clear any leftover workflow transition state from previous sessions
        from app.core.workflow_state_manager import WorkflowStateManager, WorkflowSource, WorkflowStage
        workflow_manager = WorkflowStateManager(session_id)
        
        # Transition to Data Analysis V3 workflow
        workflow_manager.transition_workflow(
            from_source=WorkflowSource.STANDARD,
            to_source=WorkflowSource.DATA_ANALYSIS_V3,
            new_stage=WorkflowStage.UPLOADED,
            clear_markers=['.analysis_complete']  # Clear stale markers
        )
        logger.info(f"Transitioned to Data Analysis V3 workflow for session {session_id}")
        
        # CRITICAL: Also clear the DataAnalysisStateManager flags
        # The chat endpoint checks DataAnalysisStateManager, not WorkflowStateManager
        from app.data_analysis_v3.core.state_manager import DataAnalysisStateManager
        da_state_manager = DataAnalysisStateManager(session_id)
        da_state_manager.update_state({
            'workflow_transitioned': False,
            'tpr_completed': False
        })
        logger.info(f"✅ Cleared DataAnalysisStateManager workflow flags for session {session_id}")
        
        # Sync to other instances for multi-instance support
        try:
            from app.core.instance_sync import sync_session_after_upload
            sync_session_after_upload(session_id)
            logger.info(f"🔄 Initiated sync for session {session_id}")
        except Exception as e:
            logger.warning(f"Could not sync to other instances: {e}")
        
        # Prepare response with metadata info
        response_data = {
            'status': 'success',
            'message': f'File "{filename}" uploaded successfully',
            'session_id': session_id,
            'filename': filename,
            'filepath': standard_path,
            'file_size': os.path.getsize(filepath),
            'metadata': {
                'rows': metadata.get('rows', 'Unknown'),
                'columns': metadata.get('columns', 'Unknown'),
                'is_sampled': metadata.get('is_sampled', False),
                'file_size_mb': metadata.get('file_size_mb', 0)
            }
        }
        
        # Add estimation note if applicable
        if metadata.get('rows_estimated'):
            response_data['metadata']['rows_note'] = 'Estimated from file size'
        
        # Add instance identifier for debugging
        import socket
        response_data['instance'] = socket.gethostname()
        
        response = jsonify(response_data)
        
        # Set cookie for session affinity
        response.set_cookie(
            'ChatMRPT-Session',
            value=session_id,
            max_age=3600,  # 1 hour
            httponly=True,
            samesite='Lax',
            path='/'
        )
        
        return response
        
    except Exception as e:
        logger.error(f"Error in data analysis upload: {str(e)}")
        return jsonify({
            'status': 'error',
            'message': f'Upload failed: {str(e)}'
        }), 500


@data_analysis_v3_bp.route('/api/data-analysis/status', methods=['GET'])
def get_upload_status():
    """Check if data has been uploaded for analysis."""
    session_id = session.get('session_id')
    has_file = session.get('has_data_analysis_file', False)
    filename = session.get('data_analysis_filename', None)
    
    return jsonify({
        'status': 'success',
        'has_file': has_file,
        'filename': filename,
        'session_id': session_id
    })


@data_analysis_v3_bp.route('/api/data-analysis/clear-mode', methods=['POST'])
def clear_data_analysis_mode():
    """Clear Data Analysis V3 mode when switching tabs."""
    session['has_data_analysis_file'] = False
    session['use_data_analysis_v3'] = False
    session['active_tab'] = 'standard-upload'
    session.modified = True
    
    logger.info(f"📊 Data Analysis V3 mode cleared for session {session.get('session_id')}")
    
    return jsonify({
        'status': 'success',
        'message': 'Data Analysis mode cleared'
    })


@data_analysis_v3_bp.route('/api/data-analysis/activate-mode', methods=['POST'])
def activate_data_analysis_mode():
    """Activate Data Analysis V3 mode when tab is selected with data."""
    if session.get('has_data_analysis_file', False):
        session['use_data_analysis_v3'] = True
        session['active_tab'] = 'data-analysis'
        session.modified = True
        
        logger.info(f"📊 Data Analysis V3 mode activated for session {session.get('session_id')}")
        
        return jsonify({
            'status': 'success',
            'message': 'Data Analysis mode activated'
        })
    else:
        return jsonify({
            'status': 'info',
            'message': 'No data file uploaded yet'
        })


@data_analysis_v3_bp.route('/images/plotly_figures/pickle/<path:filename>')
def serve_pickle_visualization(filename):
    """
    Serve pickle visualization files or convert to HTML on-the-fly.
    This fixes the 502 error when frontend requests pickle files.
    """
    import pickle
    import os
    import uuid

    try:
        # Get session ID from session or cookie
        session_id = session.get('session_id')
        if not session_id:
            # Try to extract from filename pattern if it includes session ID
            # Otherwise return 404
            logger.warning(f"No session ID for pickle request: {filename}")
            return jsonify({'error': 'Session not found'}), 404

        # Try multiple possible locations for pickle files
        possible_paths = [
            f"instance/uploads/{session_id}/visualizations/{filename}",
            f"instance/uploads/{session_id}/{filename}",
            f"app/static/visualizations/{filename}",
        ]

        pickle_path = None
        for path in possible_paths:
            if os.path.exists(path):
                pickle_path = path
                break

        if not pickle_path:
            logger.error(f"Pickle file not found: {filename} for session {session_id}")
            return jsonify({'error': 'Visualization not found'}), 404

        # Load the pickle file
        with open(pickle_path, 'rb') as f:
            fig = pickle.load(f)

        # Convert to HTML
        html_content = fig.to_html(include_plotlyjs='cdn')

        # Return HTML directly
        from flask import Response
        return Response(html_content, mimetype='text/html')

    except Exception as e:
        logger.error(f"Error serving pickle visualization {filename}: {str(e)}")
        return jsonify({'error': str(e)}), 500


@data_analysis_v3_bp.route('/static/visualizations/<path:filename>')
def serve_static_visualization(filename):
    """
    Serve HTML visualization files from the static directory.
    This ensures proper serving of Data Analysis V3 generated visualizations.
    """
    try:
        from flask import send_from_directory
        import os

        # Serve from the static visualizations directory
        viz_dir = os.path.join(current_app.root_path, 'static', 'visualizations')

        if not os.path.exists(os.path.join(viz_dir, filename)):
            logger.warning(f"Visualization file not found: {filename}")
            return jsonify({'error': 'Visualization not found'}), 404

        # Serve the HTML file
        return send_from_directory(viz_dir, filename, mimetype='text/html')

    except Exception as e:
        logger.error(f"Error serving static visualization {filename}: {str(e)}")
        return jsonify({'error': str(e)}), 500


@data_analysis_v3_bp.route('/api/v1/data-analysis/chat', methods=['POST'])
def data_analysis_chat():
    """
    Handle chat messages for Data Analysis V3.
    This endpoint analyzes uploaded data and provides comprehensive insights.
    """
    try:
        import asyncio
        
        data = request.get_json()
        message = data.get('message', '')
        session_id = data.get('session_id') or session.get('session_id')
        
        if not session_id:
            return jsonify({
                'success': False,
                'error': 'No session ID provided'
            }), 400
        
        logger.info(f"Data Analysis V3 chat request for session {session_id}")
        
        # Check if workflow has transitioned out of Data Analysis mode
        from app.data_analysis_v3.core.state_manager import DataAnalysisStateManager
        state_manager = DataAnalysisStateManager(session_id)
        current_state = state_manager.get_state()
        
        # If workflow has transitioned, signal frontend to exit Data Analysis mode
        if current_state.get('workflow_transitioned'):
            logger.info(f"Workflow has transitioned for session {session_id}, exiting Data Analysis mode")
            
            # CRITICAL FIX: Actually trigger risk analysis to set Flask session flags
            # This ensures the Flask session is properly configured before exiting V3
            from app.data_analysis_v3.core.tpr_workflow_handler import TPRWorkflowHandler
            try:
                tpr_handler = TPRWorkflowHandler(session_id, state_manager, None)
                risk_result = tpr_handler.trigger_risk_analysis()
                logger.info(f"✅ Triggered risk analysis for session {session_id}: {risk_result.get('success')}")
            except Exception as e:
                logger.error(f"Failed to trigger risk analysis: {e}")
            
            return jsonify({
                'success': True,
                'exit_data_analysis_mode': True,
                'message': "Data has been prepared. Switching to main ChatMRPT workflow.",
                'redirect_message': message,  # Pass the original message to be sent to main workflow
                'session_id': session_id
            })
        
        # Import and use the Data Analysis V3 agent
        from app.data_analysis_v3.core.agent import DataAnalysisAgent
        
        # Create agent instance for this session
        agent = DataAnalysisAgent(session_id)
        
        # Run the async analyze method in a sync context
        async def run_analysis():
            # Analyze the uploaded data
            if 'analyze' in message.lower() or 'uploaded' in message.lower():
                # Use simpler instruction like original
                return await agent.analyze("Show me a summary of the uploaded data")
            else:
                # Handle regular chat message
                return await agent.analyze(message)
        
        # Run the async function
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        try:
            result = loop.run_until_complete(run_analysis())
            logger.info(f"Analysis completed for session {session_id}")

            # Add warning for first message after upload
            try:
                # Check if this is the first message after upload
                if 'uploaded' in message.lower() or 'analyze' in message.lower():
                    already_shown = state_manager.get_field('overview_warning_shown', False)
                    if not already_shown and isinstance(result, dict):
                        message_content = result.get('message') or ''
                        # ALWAYS put warning FIRST
                        warning = (
                            "💡 Important: Your data is loaded and ready to explore. When you're ready to calculate TPR and rank wards for intervention, "
                            "just say 'TPR' or 'run TPR analysis'.\n\n"
                        )
                        # Ensure warning is at the start
                        if not message_content.startswith('💡'):
                            result['message'] = warning + message_content

                        # Mark as shown
                        state_manager.update_state({'overview_warning_shown': True})
            except Exception as e:
                logger.debug(f"Could not add warning: {e}")

            # REMOVED ALL SANITIZATION - just return the actual result!
            return jsonify(result)
        finally:
            loop.close()

    except Exception as e:
        logger.error(f"Error in data analysis chat: {str(e)}", exc_info=True)
        import traceback
        return jsonify({
            'success': False,
            'error': str(e),
            'traceback': traceback.format_exc(),
            'debug': {
                'session_id': session_id,
                'message': message[:100] if 'message' in locals() else 'Unknown',
                'error_location': 'data_analysis_chat'
            }
        }), 500
