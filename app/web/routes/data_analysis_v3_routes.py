"""
Data Analysis V3 Routes
Handles file uploads and queries for the Data Analysis tab
"""
from __future__ import annotations

import os
import logging
import time
from datetime import datetime
from pathlib import Path
from flask import Blueprint, request, jsonify, session, current_app
from werkzeug.utils import secure_filename
from app.data_analysis_v3.core.metadata_cache import MetadataCache
from app.data_analysis_v3.core.tpr_language_interface import TPRLanguageInterface
from app.auth.decorators import require_auth
from app.interaction.core import InteractionCore
from app.runtime.upload_service import UploadService

logger = logging.getLogger(__name__)

# Initialize interaction logger for capturing all user/assistant messages
interaction_core = InteractionCore()

# Create blueprint
data_analysis_v3_bp = Blueprint('data_analysis_v3', __name__)

ALLOWED_EXTENSIONS = {'csv', 'xlsx', 'xls', 'json', 'txt'}

def allowed_file(filename):
    """Check if file extension is allowed."""
    return '.' in filename and filename.rsplit('.', 1)[1].lower() in ALLOWED_EXTENSIONS


def _select_metadata_entry(cache: dict) -> tuple[dict, str] | tuple[None, None]:
    """Pick the most relevant metadata entry from the cache."""
    files = (cache or {}).get('files', {})
    if not files:
        return None, None

    priority = [
        'unified_dataset.csv',
        'data_analysis.csv',
        'raw_data.csv',
        'uploaded_data.csv',
    ]

    for name in priority:
        if name in files:
            return files[name], name

    # Fallback to first available entry
    name, meta = next(iter(files.items()))
    return meta, name


def _build_general_workflow_context(session_id: str) -> dict:
    """Construct workflow context for general (non-TPR) data analysis."""
    context: dict = {
        'workflow': 'data_analysis_v3',
        'stage': 'no_data',
        'valid_options': [],
        'data_loaded': False,
        'session_id': session_id,
    }

    columns: list[str] = []
    rows: int | None = None
    dataset_name: str | None = None

    try:
        cache = MetadataCache.load_cache(session_id) or {}
        metadata, dataset_name = _select_metadata_entry(cache)

        if metadata:
            columns = metadata.get('column_names') or []
            rows = metadata.get('rows') if isinstance(metadata.get('rows'), (int, float)) else None
            profile = metadata.get('profile', {}) or {}
            metrics = profile.get('metrics', {}) or {}

            if not columns:
                columns = metrics.get('column_examples', [])

            dtype_summary = metrics.get('dtype_summary', {})

            context.update({
                'data_loaded': True,
                'data_columns': columns,
                'columns_total': len(columns),
                'data_shape': {
                    'rows': rows,
                    'cols': len(columns),
                },
                'data_types': dtype_summary,
                'dataset_name': dataset_name,
            })

            if metrics.get('numeric_columns'):
                context['numeric_samples'] = metrics['numeric_columns']
            if metrics.get('categorical_columns'):
                context['categorical_samples'] = metrics['categorical_columns']
    except Exception as exc:  # pragma: no cover - defensive
        logger.warning(f"[WORKFLOW CONTEXT] Failed to load metadata cache for {session_id}: {exc}")

    session_path = Path('instance/uploads') / session_id

    if not columns:
        # Fallback: load a tiny sample directly
        for candidate in ['unified_dataset.csv', 'data_analysis.csv', 'raw_data.csv', 'uploaded_data.csv']:
            candidate_path = session_path / candidate
            if candidate_path.exists():
                try:
                    from app.data_analysis_v3.core.encoding_handler import EncodingHandler

                    sample = EncodingHandler.read_csv_with_encoding(candidate_path, nrows=5)
                    columns = sample.columns.tolist()
                    rows = rows or sample.shape[0]
                    context.update({
                        'data_loaded': True,
                        'data_columns': columns,
                        'columns_total': len(columns),
                        'data_shape': {
                            'rows': rows,
                            'cols': len(columns),
                        },
                        'dataset_name': dataset_name or candidate,
                    })
                    break
                except Exception as exc:  # pragma: no cover - defensive
                    logger.warning(f"[WORKFLOW CONTEXT] Failed to sample {candidate_path}: {exc}")

    if session_path.joinpath('unified_dataset.csv').exists():
        context['stage'] = 'post_analysis'
    elif context['data_loaded']:
        context['stage'] = 'data_exploring'

    # Limit columns to avoid overwhelming the prompt
    if context.get('data_columns') and len(context['data_columns']) > 120:
        context['data_columns_preview'] = context['data_columns'][:120]
    
    return context


@data_analysis_v3_bp.route('/api/data-analysis/upload', methods=['POST'])
@require_auth
def upload_for_analysis():
    """
    Handle file uploads from the Data Analysis tab.
    This is specifically for general data exploration, not malaria risk analysis.
    """
    try:
        previous_session_id = session.get('session_id')
        base_session_id = session.get('base_session_id') or previous_session_id

        # ALWAYS generate a new session ID for each upload to prevent session reuse
        # This fixes the concurrent user data bleed issue
        import uuid
        session_id = str(uuid.uuid4())
        session['session_id'] = session_id
        if base_session_id:
            session['base_session_id'] = base_session_id
        else:
            base_session_id = session_id
            session['base_session_id'] = base_session_id

        # Log for debugging
        logger.info(f"📊 Generated new session ID for upload: {session_id}")
        
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

        # CRITICAL: Set flags for routing logic to recognize Data Analysis V3 mode
        session['use_data_analysis_v3'] = True
        session['csv_loaded'] = True
        session['data_analysis_active'] = True
        session['active_tab'] = 'data-analysis'

        session.modified = True
        logger.info(f"✓ Data Analysis V3 mode activated for session {session_id}")
        
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

        if base_session_id and base_session_id != session_id:
            try:
                upload_service = UploadService(current_app.config.get('UPLOAD_FOLDER', 'instance/uploads'))
                upload_service.mirror_artifacts(
                    source_session_id=session_id,
                    target_session_id=base_session_id,
                )
                logger.info(
                    f"🗂️ Mirrored upload artefacts from {session_id} to base session {base_session_id}"
                )
            except Exception as mirror_exc:
                logger.warning(
                    f"Could not mirror artefacts to base session {base_session_id}: {mirror_exc}"
                )

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
@require_auth
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
@require_auth
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
@require_auth
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
@require_auth
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
        html_content = fig.to_html(include_plotlyjs=True)

        # Return HTML directly
        from flask import Response
        return Response(html_content, mimetype='text/html')

    except Exception as e:
        logger.error(f"Error serving pickle visualization {filename}: {str(e)}")
        return jsonify({'error': str(e)}), 500


@data_analysis_v3_bp.route('/static/visualizations/<path:filename>')
@require_auth
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
@require_auth
def data_analysis_chat():
    """
    Handle chat messages for Data Analysis V3.
    This endpoint mediates between the structured TPR workflow and the open data-analysis agent.
    """
    try:
        import asyncio
        import glob
        import os

        data = request.get_json() or {}
        message = data.get('message', '')
        session_id = data.get('session_id') or session.get('session_id')

        # Ensure the server-side session matches the client-provided identifier.
        # In multi-instance deployments the Flask session cookie may map to a
        # different worker than the one that generated the upload, so we treat
        # the client's session_id as the source of truth to keep state files in sync.
        existing_session_id = session.get('session_id')
        if session_id and existing_session_id and existing_session_id != session_id:
            logger.info(
                "[DATA-ANALYSIS] Realigning session context from %s to client session %s",
                existing_session_id,
                session_id,
            )
            session['session_id'] = session_id
            session['base_session_id'] = session.get('base_session_id') or session_id
            session.modified = True
        elif session_id and not existing_session_id:
            session['session_id'] = session_id
            session['base_session_id'] = session.get('base_session_id') or session_id
            session.modified = True

        if not session_id:
            return jsonify({
                'success': False,
                'error': 'No session ID provided'
            }), 400

        logger.info(f"[DEBUG] Data Analysis V3 chat request for session {session_id}")
        logger.info(f"[DEBUG] Message received: {message}")

        # 🎯 LOG INCOMING USER MESSAGE - CRITICAL FOR COMPLETE INTERACTION CAPTURE
        request_start_time = time.time()
        interaction_core.log_message(
            session_id=session_id,
            sender='user',
            content=message,
            intent=None,  # Will be determined during processing
            entities={
                'message_length': len(message),
                'timestamp': request_start_time,
                'endpoint': '/api/v1/data-analysis/chat',
                'workflow': 'data_analysis_v3'
            }
        )
        logger.info(f"✅ Logged user message for session {session_id}")

        from app.data_analysis_v3.core.state_manager import DataAnalysisStateManager, ConversationStage
        state_manager = DataAnalysisStateManager(session_id)
        current_state = state_manager.get_state() or {}

        tpr_language = TPRLanguageInterface(session_id)
        try:
            tpr_language.update_from_metadata(current_state)
        except Exception:
            pass

        if current_state.get('workflow_transitioned'):
            logger.info(f"Workflow has transitioned for session {session_id}, exiting Data Analysis mode")

            # 🎯 LOG TRANSITION MESSAGE
            transition_message = 'Data has been prepared. Switching to main ChatMRPT workflow.'
            interaction_core.log_message(
                session_id=session_id,
                sender='assistant',
                content=transition_message,
                intent='workflow_transition',
                entities={
                    'response_time_seconds': time.time() - request_start_time,
                    'endpoint': '/api/v1/data-analysis/chat',
                    'workflow': 'transition'
                }
            )

            return jsonify({
                'success': True,
                'exit_data_analysis_mode': True,
                'message': transition_message,
                'redirect_message': message,
                'session_id': session_id
            })

        from app.data_analysis_v3.tpr.workflow_manager import TPRWorkflowHandler
        from app.data_analysis_v3.tpr.data_analyzer import TPRDataAnalyzer
        from app.data_analysis_v3.core.agent import DataAnalysisAgent
        from app.data_analysis_v3.core.encoding_handler import EncodingHandler

        agent = DataAnalysisAgent(session_id)

        async def run_agent(query: str, workflow_context=None):
            return await agent.analyze(query, workflow_context=workflow_context)

        def run_agent_sync(query: str, workflow_context=None):
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
            try:
                return loop.run_until_complete(run_agent(query, workflow_context))
            finally:
                loop.close()

        lower_message = (message or '').lower().strip()

        is_tpr_active = state_manager.is_tpr_workflow_active()

        if is_tpr_active:
            logger.info(f"[2-ROUTE] TPR workflow active for session {session_id}")
            tpr_analyzer = TPRDataAnalyzer()
            tpr_handler = TPRWorkflowHandler(session_id, state_manager, tpr_analyzer)

            # Load data
            df = None
            try:
                data_dir = os.path.join('instance', 'uploads', session_id)
                data_files = glob.glob(os.path.join(data_dir, '*.csv'))
                if data_files:
                    latest = max(data_files, key=os.path.getctime)
                    df = EncodingHandler.read_csv_with_encoding(latest)
                    tpr_handler.set_data(df)
            except Exception as load_err:
                logger.error(f"[2-ROUTE] Failed to load dataset for TPR workflow: {load_err}")

            tpr_handler.load_state_from_manager()
            current_stage = state_manager.get_workflow_stage()

            # ============================================================
            # PRIORITY 1: CHECK FOR EXACT VISUALIZATION PHRASES ONLY
            # ============================================================
            # ONLY exact phrases that request stored TPR visualizations
            # Must be checked BEFORE everything else (confirmation, commands, agent)
            # Using EXACT matching (not substring) to avoid false positives like "plot distribution"
            viz_exact_phrases = [
                "show facility charts",
                "show age charts"
            ]

            # Explanation phrases - conceptual help for decision making
            explanation_phrases = [
                "explain the differences",
                "explain differences",
                "what's the difference",
                "what is the difference",
                "compare them"
            ]

            message_normalized = message.lower().strip()

            # Check for explanation requests first
            if message_normalized in explanation_phrases:
                logger.info(f"[EXPLANATION] Detected explanation request: '{message}'")

                # Get conceptual explanation based on current workflow step
                # Use existing tpr_handler which is already initialized
                explanation_result = tpr_handler.handle_explanation_request()

                return jsonify(explanation_result)

            if message_normalized in viz_exact_phrases:
                logger.info(f"[VISUALIZATION] Detected viz request: '{message}'")

                # Get pending visualizations from state
                pending_viz = current_state.get('pending_visualizations', {})

                if not pending_viz:
                    # No visualizations stored yet
                    return jsonify({
                        "success": True,
                        "message": "No visualizations are currently available. Please make a selection first to generate charts.",
                        "session_id": session_id
                    })

                # Determine which viz to return based on current stage
                if current_stage == ConversationStage.TPR_FACILITY_LEVEL and 'facility_level' in pending_viz:
                    viz_list = pending_viz['facility_level']
                    msg = "Here are the facility-level visualizations to help inform your decision:\n\n"
                    msg += "**Chart 1:** Facility mix by level\n"
                    msg += "**Chart 2:** Test availability (RDT vs Microscopy)\n\n"
                    msg += "Based on this data, which facility level would you like to analyze?\n"
                    msg += "(**primary**, **secondary**, **tertiary**, or **all**)"

                    return jsonify({
                        'success': True,
                        'message': msg,
                        'session_id': session_id,
                        'workflow': 'tpr',
                        'stage': 'facility_selection',
                        'visualizations': viz_list
                    })

                elif current_stage == ConversationStage.TPR_AGE_GROUP and 'age_group' in pending_viz:
                    viz_list = pending_viz['age_group']
                    msg = "Here are the age group visualizations:\n\n"
                    msg += "**Chart 1:** Test volume breakdown by age group\n"
                    msg += "**Chart 2:** Positivity rate comparisons across ages\n\n"
                    msg += "This data helps identify which age groups have the highest burden.\n\n"
                    msg += "Which age group would you like to analyze?\n"
                    msg += "(**u5**, **o5**, **pw**, or **all**)"

                    return jsonify({
                        'success': True,
                        'message': msg,
                        'session_id': session_id,
                        'workflow': 'tpr',
                        'stage': 'age_selection',
                        'visualizations': viz_list
                    })
                else:
                    # No visualizations for current stage
                    return jsonify({
                        "success": True,
                        "message": "Visualizations aren't available for the current stage.",
                        "session_id": session_id
                    })

            # ============================================================
            # PRIORITY 2: CHECK FOR CONFIRMATION
            # ============================================================
            # This handles natural language like "sure thing", "yES Let's go", etc.
            if current_state.get('tpr_awaiting_confirmation'):
                logger.info(f"[CONFIRMATION] TPR workflow awaiting confirmation")
                confirmation_keywords = ['yes', 'y', 'continue', 'proceed', 'start', 'begin', 'ok', 'okay', 'sure', 'ready']
                message_clean = message.lower().strip()

                # Check if message contains any confirmation keyword
                if message_clean in confirmation_keywords or any(kw in message_clean.split() for kw in confirmation_keywords):
                    logger.info(f"[CONFIRMATION] Detected confirmation keyword in: '{message}'")
                    response = tpr_handler.execute_confirmation()
                    return jsonify(response)

                logger.info(f"[CONFIRMATION] No confirmation keyword detected, proceeding with 2-route logic")

            # ============================================================
            # PRIORITY 3: INTENT-FIRST CLASSIFICATION (New Architecture)
            # ============================================================
            # Single-pass intent classification determines routing
            # Intent "selection" → extract command → execute
            # Intent "question" → route to agent

            # Determine valid options for current stage
            valid_options = []
            if current_stage == ConversationStage.TPR_STATE_SELECTION:
                # Get available states from data
                if df is not None:
                    try:
                        state_analysis = tpr_analyzer.analyze_states(df)
                        valid_options = list(state_analysis.get('states', {}).keys())
                    except:
                        pass
                valid_options.extend(['yes', 'continue', 'back', 'exit', 'status'])
            elif current_stage == ConversationStage.TPR_FACILITY_LEVEL:
                valid_options = ['primary', 'secondary', 'tertiary', 'all', 'back', 'exit', 'status']
            elif current_stage == ConversationStage.TPR_AGE_GROUP:
                valid_options = ['u5', 'o5', 'pw', 'all', 'back', 'exit', 'status']
            else:
                valid_options = ['yes', 'continue', 'start', 'back', 'exit']

            # STEP 1: Classify intent first
            intent_result = tpr_language.classify_intent(
                message=message,
                stage=current_stage.name if current_stage else 'unknown',
                valid_options=valid_options
            )

            # STEP 2: Route based on intent
            if intent_result['intent'] == 'selection' and intent_result['confidence'] >= 0.7:
                # User is making a selection → Extract and execute
                logger.info(f"[INTENT] Selection detected (confidence={intent_result['confidence']:.2f})")
                logger.info(f"   Rationale: {intent_result.get('rationale', 'N/A')}")

                command = tpr_language.extract_command(
                    message=message,
                    stage=current_stage.name if current_stage else 'unknown',
                    valid_options=valid_options,
                    context={'session_id': session_id, 'stage': current_stage}
                )

                if command:
                    # Command extracted successfully → execute
                    logger.info(f"[INTENT→COMMAND] Extracted: '{message}' → '{command}'")
                    response = tpr_handler.execute_command(command, current_stage)
                else:
                    # Intent says selection but can't extract → ask for clarification
                    logger.warning(f"[INTENT→COMMAND] Intent is 'selection' but extraction failed")
                    response = {
                        "success": True,
                        "message": f"I understood you're making a selection, but couldn't determine which option. Please choose from: {', '.join(valid_options)}",
                        "session_id": session_id,
                        "workflow": "tpr",
                        "stage": current_stage.name if current_stage else None
                    }
            else:
                # User is asking a question → Route to agent
                logger.info(f"[INTENT] Question detected (confidence={intent_result.get('confidence', 0):.2f})")
                logger.info(f"   Rationale: {intent_result.get('rationale', 'N/A')}")
                logger.info(f"   Routing to agent with workflow context")

                # Build workflow context for agent
                workflow_context = {
                    'workflow': 'tpr',
                    'stage': current_stage.name if current_stage else None,
                    'valid_options': valid_options,
                    'selections': state_manager.get_tpr_selections() or {},
                    'data_loaded': df is not None,
                    'session_id': session_id
                }

                response = run_agent_sync(message, workflow_context=workflow_context)

            # 🎯 LOG ASSISTANT RESPONSE - CRITICAL FOR COMPLETE INTERACTION CAPTURE
            response_time = time.time() - request_start_time
            assistant_message = response.get('message', '') if isinstance(response, dict) else str(response)
            interaction_core.log_message(
                session_id=session_id,
                sender='assistant',
                content=assistant_message,
                intent=response.get('workflow') if isinstance(response, dict) else None,
                entities={
                    'response_length': len(assistant_message),
                    'response_time_seconds': response_time,
                    'endpoint': '/api/v1/data-analysis/chat',
                    'workflow': 'tpr' if isinstance(response, dict) and response.get('workflow') == 'tpr' else 'data_analysis_v3',
                    'stage': response.get('stage') if isinstance(response, dict) else None,
                    'visualizations_count': len(response.get('visualizations') or []) if isinstance(response, dict) else 0,
                    'status': response.get('success', True) if isinstance(response, dict) else True
                }
            )
            logger.info(f"✅ Logged assistant response for session {session_id} (response_time={response_time:.2f}s)")

            # CRITICAL FIX: Check if TPR workflow completed and add exit flag
            if isinstance(response, dict):
                stage = (response.get('stage') or '').upper()
                workflow = (response.get('workflow') or '').lower()

                # If TPR is complete, ensure exit_data_analysis_mode is set
                if stage == 'COMPLETE' and workflow in ['tpr', 'data_upload']:
                    if not response.get('exit_data_analysis_mode'):
                        logger.info(f"🚦 TPR COMPLETE detected - adding exit_data_analysis_mode flag")
                        response['exit_data_analysis_mode'] = True
                        # Also change workflow to data_upload to indicate transition
                        if workflow == 'tpr':
                            response['workflow'] = 'data_upload'
                            logger.info(f"🚦 Changed workflow from 'tpr' to 'data_upload'")

            return jsonify(response)

        # TPR workflow start triggers - removed standalone 'tpr' to prevent false positives (e.g., "map tpr distribution")
        start_triggers = ['start tpr', 'tpr workflow', 'test positivity', 'test positivity rate', 'run tpr']
        if any(trigger in lower_message for trigger in start_triggers):
            logger.info(f"[BRIDGE] Detected TPR start request: '{message}'")
            data_dir = os.path.join('instance', 'uploads', session_id)
            data_files = glob.glob(os.path.join(data_dir, '*.csv'))
            if not data_files:
                return jsonify({
                    'success': False,
                    'message': 'No data found. Please upload your dataset before starting the TPR workflow.',
                    'session_id': session_id
                })

            latest = max(data_files, key=os.path.getctime)
            df = EncodingHandler.read_csv_with_encoding(latest)

            tpr_analyzer = TPRDataAnalyzer()
            tpr_handler = TPRWorkflowHandler(session_id, state_manager, tpr_analyzer)
            tpr_handler.set_data(df)
            try:
                tpr_language.update_from_dataframe(df)
            except Exception:
                pass

            state_manager.mark_tpr_workflow_active()
            state_manager.update_workflow_stage(ConversationStage.TPR_STATE_SELECTION)

            return jsonify(tpr_handler.start_workflow())

        workflow_context = _build_general_workflow_context(session_id)
        logger.info(
            "[AGENT CONTEXT] Session %s → stage=%s columns=%d",
            session_id,
            workflow_context.get('stage'),
            len(workflow_context.get('data_columns') or [])
        )

        result = run_agent_sync(message, workflow_context=workflow_context)

        # 🎯 LOG ASSISTANT RESPONSE (agent path) - CRITICAL FOR COMPLETE INTERACTION CAPTURE
        response_time = time.time() - request_start_time
        assistant_message = result.get('message', '') if isinstance(result, dict) else str(result)
        interaction_core.log_message(
            session_id=session_id,
            sender='assistant',
            content=assistant_message,
            intent='agent_query',
            entities={
                'response_length': len(assistant_message),
                'response_time_seconds': response_time,
                'endpoint': '/api/v1/data-analysis/chat',
                'workflow': 'data_analysis_v3_agent',
                'visualizations_count': len(result.get('visualizations') or []) if isinstance(result, dict) else 0,
                'status': result.get('success', True) if isinstance(result, dict) else True
            }
        )
        logger.info(f"✅ Logged agent response for session {session_id} (response_time={response_time:.2f}s)")

        return jsonify(result)

    except Exception as e:
        logger.error(f"Error in data analysis chat: {str(e)}", exc_info=True)
        import traceback

        # 🎯 LOG ERROR - CRITICAL FOR DEBUGGING
        error_message = f"Error: {str(e)}"
        try:
            interaction_core.log_message(
                session_id=session_id if 'session_id' in locals() else 'unknown',
                sender='assistant',
                content=error_message,
                intent='error',
                entities={
                    'error_type': type(e).__name__,
                    'endpoint': '/api/v1/data-analysis/chat',
                    'workflow': 'error',
                    'traceback': traceback.format_exc()
                }
            )
        except Exception as log_err:
            logger.error(f"Failed to log error message: {log_err}")

        return jsonify({
            'success': False,
            'error': str(e),
            'traceback': traceback.format_exc()
        }), 500
