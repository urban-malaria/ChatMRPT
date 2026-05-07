"""
Data Analysis V3 Routes
Handles file uploads and queries for the Data Analysis tab
"""
from __future__ import annotations

import os
import logging
import time
from flask import Blueprint, request, jsonify, session, current_app
from app.auth.decorators import require_auth
from app.services.interaction_core import InteractionCore
from app.services.analysis_chat_service import run_analysis_message, stream_analysis_events
from app.services.analysis_upload_service import process_analysis_upload
from app.upload.upload_service import UploadService

logger = logging.getLogger(__name__)

# Initialize interaction logger for capturing all user/assistant messages
interaction_core = InteractionCore()

# Create blueprint
data_analysis_v3_bp = Blueprint('data_analysis_v3', __name__)

ALLOWED_EXTENSIONS = {'csv', 'xlsx', 'xls', 'json', 'txt'}


def _save_and_respond(response_dict, session_id, status_code=200):
    """Save assistant message to SessionMemory, then return JSON response."""
    try:
        from app.services.session_memory import SessionMemory, MessageType
        # Always write to base_session_id so messages stay with the original conversation
        mem_sid = session.get('base_session_id') or session_id
        msg = response_dict.get('message', '') if isinstance(response_dict, dict) else str(response_dict)
        if msg:
            viz = (response_dict.get('visualizations') or []) if isinstance(response_dict, dict) else []
            meta = {'visualizations': viz} if viz else {}
            SessionMemory(mem_sid).add_message(MessageType.ASSISTANT, msg, metadata=meta)
    except Exception as err:
        logger.debug("SessionMemory save (v3 helper) failed: %s", err)
    if status_code != 200:
        return jsonify(response_dict), status_code
    return jsonify(response_dict)


def allowed_file(filename):
    """Check if file extension is allowed."""
    return '.' in filename and filename.rsplit('.', 1)[1].lower() in ALLOWED_EXTENSIONS


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

        # Store the upload session_id in the base session's SessionMemory
        # (via key_entities which survives _save_memory rewrites) so resume
        # can find the uploaded files in the child session folder.
        if base_session_id and base_session_id != session_id:
            try:
                from app.services.session_memory import SessionMemory as _SM
                _base_mem = _SM(base_session_id)
                _base_mem.key_entities['upload_session_id'] = session_id
                _base_mem._save_memory()
                logger.info(f"📊 Stored upload_session_id={session_id} in base session {base_session_id}")
            except Exception as _link_err:
                logger.debug(f"Failed to link upload session to base: {_link_err}")

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
        
        upload_root = current_app.config.get('UPLOAD_FOLDER', 'instance/uploads')
        upload_result = process_analysis_upload(
            session_id=session_id,
            file_obj=file,
            original_filename=file.filename,
            upload_root=upload_root,
        )

        filename = upload_result.original_filename
        standard_path = str(upload_result.standard_path)
        metadata = upload_result.metadata or {}

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
        
        # Sync to other instances for multi-instance support
        try:
            from app.services.instance_sync import sync_session_after_upload
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
            'file_size': upload_result.file_size,
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
    Routes to: TPR active workflow → TPR start → general agent.
    """
    try:
        data = request.get_json() or {}
        message = data.get('message', '')
        session_id = data.get('session_id') or session.get('session_id')

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

        logger.info("[CHAT] Session %s: %s", session_id, message[:80])

        request_start_time = time.time()
        interaction_core.log_message(
            session_id=session_id, sender='user', content=message, intent=None,
            entities={
                'message_length': len(message),
                'timestamp': request_start_time,
                'endpoint': '/api/v1/data-analysis/chat',
                'workflow': 'data_analysis_v3',
            }
        )

        mem_sid = session.get('base_session_id') or session_id
        try:
            from app.services.session_memory import SessionMemory, MessageType
            SessionMemory(mem_sid).add_message(MessageType.USER, message)
        except Exception:
            pass

        result = run_analysis_message(session_id, message)

        response_time = time.time() - request_start_time
        assistant_message = result.get('message', '') if isinstance(result, dict) else str(result)
        workflow = result.get('workflow') if isinstance(result, dict) else None
        workflow = workflow or 'data_analysis_v3_agent'
        interaction_core.log_message(
            session_id=session_id, sender='assistant', content=assistant_message,
            intent=workflow if workflow == 'tpr' else 'agent_query',
            entities={
                'response_length': len(assistant_message),
                'response_time_seconds': response_time,
                'endpoint': '/api/v1/data-analysis/chat',
                'workflow': workflow,
                'stage': result.get('stage') if isinstance(result, dict) else None,
                'visualizations_count': len(result.get('visualizations') or []) if isinstance(result, dict) else 0,
                'status': result.get('success', True) if isinstance(result, dict) else True,
            }
        )
        logger.info("Agent response logged for session %s (%.2fs)", session_id, response_time)
        return _save_and_respond(result, session_id)

    except Exception as e:
        logger.error("Error in data analysis chat: %s", str(e), exc_info=True)
        import traceback
        error_message = f"Error: {str(e)}"
        try:
            interaction_core.log_message(
                session_id=session_id if 'session_id' in locals() else 'unknown',
                sender='assistant', content=error_message, intent='error',
                entities={
                    'error_type': type(e).__name__,
                    'endpoint': '/api/v1/data-analysis/chat',
                    'workflow': 'error',
                    'traceback': traceback.format_exc(),
                }
            )
        except Exception as log_err:
            logger.error("Failed to log error: %s", log_err)
        return jsonify({
            'success': False,
            'error': str(e),
            'traceback': traceback.format_exc(),
        }), 500


@data_analysis_v3_bp.route('/api/v1/data-analysis/chat/stream', methods=['POST'])
@require_auth
def data_analysis_chat_stream():
    """
    SSE streaming endpoint for data analysis chat.
    The shared service emits orchestration events; this route formats them as SSE.
    """
    import json as _json
    from flask import Response, stream_with_context

    data = request.get_json() or {}
    message = data.get('message', '')
    session_id = data.get('session_id') or session.get('session_id')

    if not session_id:
        def _no_session():
            yield f"data: {_json.dumps({'type': 'error', 'error': 'No session ID provided'})}\n\n"
            yield "data: [DONE]\n\n"
        return Response(stream_with_context(_no_session()), mimetype='text/event-stream')

    existing_session_id = session.get('session_id')
    if session_id and existing_session_id and existing_session_id != session_id:
        session['session_id'] = session_id
        session['base_session_id'] = session.get('base_session_id') or session_id
        session.modified = True
    elif session_id and not existing_session_id:
        session['session_id'] = session_id
        session['base_session_id'] = session.get('base_session_id') or session_id
        session.modified = True

    interaction_core.log_message(
        session_id=session_id, sender='user', content=message, intent=None,
        entities={
            'message_length': len(message),
            'timestamp': time.time(),
            'endpoint': '/api/v1/data-analysis/chat/stream',
            'workflow': 'data_analysis_v3',
        }
    )

    mem_sid = session.get('base_session_id') or session_id
    try:
        from app.services.session_memory import SessionMemory, MessageType
        SessionMemory(mem_sid).add_message(MessageType.USER, message)
    except Exception:
        pass

    app_obj = current_app._get_current_object()
    captured_session_id = session_id
    captured_mem_sid = mem_sid

    def generate():
        with app_obj.app_context():
            request_start = time.time()
            try:
                from app.services.session_memory import SessionMemory, MessageType

                for event in stream_analysis_events(captured_session_id, message):
                    yield f"data: {_json.dumps(event)}\n\n"
                    if event.get('type') == 'result':
                        response_time = time.time() - request_start
                        event_data = event.get('data') or {}
                        if not isinstance(event_data, dict):
                            event_data = {}
                        assistant_text = event_data.get('message', '')
                        workflow = event_data.get('workflow')
                        workflow = workflow or 'data_analysis_v3_agent'
                        try:
                            SessionMemory(captured_mem_sid).add_message(MessageType.ASSISTANT, assistant_text)
                        except Exception:
                            pass
                        try:
                            interaction_core.log_message(
                                session_id=captured_session_id, sender='assistant',
                                content=assistant_text,
                                intent=workflow if workflow == 'tpr' else 'agent_query',
                                entities={
                                    'response_time_seconds': response_time,
                                    'endpoint': '/api/v1/data-analysis/chat/stream',
                                    'workflow': workflow,
                                    'stage': event_data.get('stage'),
                                    'visualizations_count': len(
                                        event_data.get('visualizations') or []
                                    ),
                                    'status': event_data.get('success', True),
                                }
                            )
                        except Exception:
                            pass
            except Exception as e:
                logger.exception("[STREAM] Agent error for session %s: %s", captured_session_id, e)
                try:
                    interaction_core.log_message(
                        session_id=captured_session_id, sender='assistant',
                        content=f'Error: {e}', intent='error',
                        entities={
                            'error_type': type(e).__name__,
                            'endpoint': '/api/v1/data-analysis/chat/stream',
                        }
                    )
                except Exception:
                    pass
                yield f"data: {_json.dumps({'type': 'error', 'error': str(e)})}\n\n"
            yield "data: [DONE]\n\n"

    resp = Response(stream_with_context(generate()), mimetype='text/event-stream')
    resp.headers['Cache-Control'] = 'no-cache'
    resp.headers['Connection'] = 'keep-alive'
    resp.headers['X-Accel-Buffering'] = 'no'
    return resp
