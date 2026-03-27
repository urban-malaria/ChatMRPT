"""Streaming chat handler for analysis workflow."""

from __future__ import annotations

import json
import os
import shutil

from typing import Any, Dict

from flask import Response, current_app, jsonify, request, session

from app.core.exceptions import ValidationError
from app.data_analysis_v3.tpr.workflow_manager import reset_tpr_handler_cache

from . import logger
from .chat_routing import route_with_mistral
from .utils import resync_session_flags, run_async

__all__ = ["handle_send_message_streaming"]


def handle_send_message_streaming() -> Response:
    data = request.json or {}
    user_message = data.get('message', '')
    payload_session_id = data.get('session_id')

    logger.info("=" * 60)
    logger.info("🔧 BACKEND: /send_message_streaming endpoint hit")
    logger.info("  📝 User Message: %s...", user_message[:100] if user_message else 'EMPTY')
    logger.info("  🆔 Session ID: %s", session.get('session_id', 'NO SESSION'))
    logger.info("  📂 Session Keys: %s", list(session.keys()))
    logger.info("  🎯 Analysis Complete: %s", session.get('analysis_complete', False))
    logger.info("  📊 Data Loaded: %s", session.get('data_loaded', False))
    logger.info("  🔄 TPR Complete: %s", session.get('tpr_workflow_complete', False))
    logger.info("=" * 60)

    logger.warning(
        "[TPR ROUTE STREAM] payload=%s scoped=%s",
        payload_session_id,
        session.get('session_id'),
    )

    try:
        _sync_tpr_outputs_for_stream(payload_session_id)
    except Exception as sync_exc:
        logger.error("[TPR SYNC] Streaming mirror failed: %s", sync_exc)

    if not user_message:
        def generate_error():
            yield json.dumps({
                'content': 'Please provide a message to continue.',
                'status': 'success',
                'done': True,
            })

        response = Response((f"data: {chunk}\n\n" for chunk in generate_error()), mimetype='text/event-stream')
        response.headers['Cache-Control'] = 'no-cache'
        response.headers['Connection'] = 'keep-alive'
        response.headers['Access-Control-Allow-Origin'] = '*'
        return response

    tab_context = data.get('tab_context', 'standard-upload')
    is_data_analysis = data.get('is_data_analysis', False)

    if is_data_analysis:
        session['active_tab'] = 'data-analysis'
        session['use_data_analysis_v3'] = True
        logger.info("📊 Data Analysis tab active - setting V3 mode")

    session_id = session.get('session_id')
    # For SessionMemory, always write to the base (original) session so all
    # messages stay in one file even after upload creates a child session.
    memory_session_id = session.get('base_session_id') or session_id

    # ── Conversation history: title on first msg, activity on every msg ──
    try:
        from app.services.conversation_history import (
            ConversationHistoryService, get_user_id, generate_title,
        )
        redis_client = current_app.config.get('SESSION_REDIS')
        _conv_svc = ConversationHistoryService(redis_client=redis_client)
        _uid = get_user_id()
        _msg_count = session.get('message_count', 0)
        session['message_count'] = _msg_count + 1
        session.modified = True
        if _msg_count == 0:
            _conv_svc.set_title(_uid, memory_session_id, generate_title(user_message))
        has_files = any(session.get(f, False) for f in ('csv_loaded', 'shapefile_loaded'))
        _conv_svc.update_activity(_uid, memory_session_id, preview=user_message[:80], has_files=has_files)
    except Exception as _conv_err:
        logger.debug("Conversation history update failed: %s", _conv_err)

    # ── Persist user message to SessionMemory so resume can load it ──
    try:
        from app.services.session_memory import SessionMemory, MessageType
        _mem = SessionMemory(memory_session_id)
        _mem.add_message(MessageType.USER, user_message)
    except Exception as _mem_err:
        logger.debug("SessionMemory save (user) failed: %s", _mem_err)

    if session.get('data_analysis_active', False):
        logger.info(
            "Data analysis workflow active for session %s, clearing legacy flag",
            session_id,
        )
        session.pop('data_analysis_active', None)
        session.modified = True
        logger.warning("Old data analysis flag detected, clearing and falling through to main chat")

    elif session.get('tpr_workflow_active', False):
        logger.info("TPR workflow active for session %s", session_id)
        try:
            from ...data_analysis_v3.core.tpr_workflow_handler import TPRWorkflowHandler
            from ...data_analysis_v3.core.state_manager import DataAnalysisStateManager
            from ...data_analysis_v3.tpr.data_analyzer import TPRDataAnalyzer

            state_manager = DataAnalysisStateManager(session_id)
            tpr_analyzer = TPRDataAnalyzer()
            handler = TPRWorkflowHandler(session_id, state_manager, tpr_analyzer)
            tpr_result = handler.handle_workflow(user_message)

            if tpr_result.get('response') == '__DATA_UPLOADED__' or tpr_result.get('status') == 'tpr_to_main_transition':
                logger.info("TPR router requesting transition to main interpreter for __DATA_UPLOADED__")
                session.pop('tpr_workflow_active', None)
                session.pop('tpr_session_id', None)
                session['csv_loaded'] = True
                session['has_uploaded_files'] = True
                session['analysis_complete'] = True
                session.modified = True
                user_message = '__DATA_UPLOADED__'
            elif tpr_result.get('stage') == 'complete':
                # Workflow finished — clear the flag so subsequent messages
                # go to the main request interpreter (where switch/compare tools live)
                session.pop('tpr_workflow_active', None)
                session.modified = True
                logger.info("TPR workflow complete, cleared tpr_workflow_active flag")
                return _response_from_tpr_result(tpr_result)
            else:
                return _response_from_tpr_result(tpr_result)
        except Exception as exc:
            logger.error("Error routing to TPR handler: %s", exc)

    if user_message == '__DATA_UPLOADED__':
        try:
            reset_tpr_handler_cache(session_id)
        except Exception:
            logger.debug("No TPR handler cache to reset for session %s", session_id)

    try:
        resync_session_flags(session_id)
    except Exception as exc:  # pragma: no cover - defensive
        logger.warning("[DEBUG] streaming: resync session flags failed: %s", exc)

    has_uploaded_files = any(
        session.get(flag, False)
        for flag in ('data_loaded', 'analysis_complete', 'csv_loaded')
    )

    session_context = {
        'has_uploaded_files': has_uploaded_files,
        'session_id': session_id,
        'csv_loaded': session.get('csv_loaded', False),
        'shapefile_loaded': session.get('shapefile_loaded', False),
        'analysis_complete': session.get('analysis_complete', False),
        'use_data_analysis_v3': session.get('use_data_analysis_v3', False),
        'data_analysis_active': session.get('data_analysis_active', False),
    }

    if session.get('pending_clarification'):
        original_context = session['pending_clarification']
        combined_message = f"{original_context['original_message']} {user_message}"
        session.pop('pending_clarification', None)
        session.modified = True
        routing_decision = run_async(route_with_mistral(combined_message, session_context))
        logger.info("Clarification response routing: %s", routing_decision)
        user_message = original_context['original_message']
    else:
        routing_decision = run_async(route_with_mistral(user_message, session_context))
        if routing_decision == 'needs_clarification':
            if len(user_message.strip().split()) <= 3:
                routing_decision = 'can_answer'
            else:
                clarification = {
                    'needs_clarification': True,
                    'clarification_type': 'intent',
                    'message': "I need more information to help you. Are you looking to:",
                    'options': [
                        {
                            'id': 'analyze_data',
                            'label': 'Analyze your uploaded data',
                            'icon': '📊',
                            'value': 'tools',
                        },
                        {
                            'id': 'general_info',
                            'label': 'Get general information',
                            'icon': '📚',
                            'value': 'arena',
                        },
                    ],
                    'original_message': user_message,
                    'session_context': session_context,
                }
                session['pending_clarification'] = {
                    'original_message': user_message,
                    'context': session_context,
                }
                session.modified = True
                return _clarification_response(clarification)

    # Check if this should trigger Arena mode
    # Arena triggers for general knowledge questions (can_answer) that are substantive
    if routing_decision == 'can_answer':
        from .arena_helpers import is_arena_eligible_message, start_arena_battle, format_arena_response, ArenaSetupError
        from app.config.arena import is_arena_available

        if is_arena_available() and is_arena_eligible_message(user_message):
            logger.info("Arena mode triggered for general knowledge question: '%s...'", user_message[:50])
            try:
                battle_result = start_arena_battle(user_message, session_id)
                arena_response = format_arena_response(battle_result, user_message)

                # Store battle ID in session
                session['current_battle_id'] = battle_result.get('battle_id')
                session.modified = True

                return _arena_response(arena_response)
            except ArenaSetupError as e:
                logger.warning("Arena failed, falling back to normal response: %s", e)
                # Fall through to request interpreter
            except Exception as e:
                logger.error("Unexpected arena error, falling back: %s", e)
                # Fall through to request interpreter

    # Main chat streaming uses request interpreter for tool-based responses
    return _stream_request_interpreter(
        user_message=user_message,
        session_id=session_id,
        memory_session_id=memory_session_id,
        tab_context=tab_context,
        is_data_analysis=is_data_analysis,
    )


def _clarification_response(payload: Dict[str, Any]) -> Response:
    # Save clarification to SessionMemory
    try:
        from app.services.session_memory import SessionMemory, MessageType
        _sid = session.get('base_session_id') or session.get('session_id')
        if _sid:
            SessionMemory(_sid).add_message(
                MessageType.ASSISTANT, payload.get('message', 'Please clarify your request.'))
    except Exception:
        pass

    def generate():
        yield json.dumps(payload)

    response = Response((f"data: {chunk}\n\n" for chunk in generate()), mimetype='text/event-stream')
    response.headers['Cache-Control'] = 'no-cache'
    response.headers['Connection'] = 'keep-alive'
    response.headers['Access-Control-Allow-Origin'] = '*'
    return response


def _arena_response(arena_data: Dict[str, Any]) -> Response:
    """Return arena battle data as a streaming response."""
    # Save arena result to SessionMemory so it appears on resume
    try:
        from app.services.session_memory import SessionMemory, MessageType
        _sid = session.get('base_session_id') or session.get('session_id')
        if _sid:
            # Store the winning-side text (response_a) as the assistant message,
            # with full arena data in metadata for richer resume if needed later.
            content = arena_data.get('response_a', '') or arena_data.get('response_b', '')
            _meta = {
                'arena': True,
                'battle_id': arena_data.get('battle_id'),
                'model_a': arena_data.get('model_a'),
                'model_b': arena_data.get('model_b'),
            }
            SessionMemory(_sid).add_message(MessageType.ASSISTANT, content, metadata=_meta)
    except Exception:
        pass

    def generate():
        yield json.dumps(arena_data)

    response = Response((f"data: {chunk}\n\n" for chunk in generate()), mimetype='text/event-stream')
    response.headers['Cache-Control'] = 'no-cache'
    response.headers['Connection'] = 'keep-alive'
    response.headers['Access-Control-Allow-Origin'] = '*'
    return response


def _stream_request_interpreter(
    *,
    user_message: str,
    session_id: str,
    memory_session_id: str,
    tab_context: str,
    is_data_analysis: bool,
) -> Response:
    request_interpreter = getattr(current_app.services, 'request_interpreter', None)
    if request_interpreter is None:
        logger.error("Request Interpreter not available")
        return jsonify({
            'status': 'error',
            'message': 'Error accessing request processing system',
        }), 500

    app = current_app._get_current_object()
    session_data = dict(session)

    def generate():
        def format_response(text: str) -> str:
            if not text:
                return ''
            import re

            s = text.replace('\r\n', '\n').replace('\r', '\n')
            s = re.sub(r'^(\s*)\*\s+', r'\1- ', s, flags=re.MULTILINE)
            s = re.sub(r'^(\s*)[\u2022•]\s+', r'\1- ', s, flags=re.MULTILINE)
            s = re.sub(r'^•\s*', '- ', s, flags=re.MULTILINE)
            s = re.sub(r'^-\s+', '- ', s, flags=re.MULTILINE)
            s = re.sub(r'\n{3,}', '\n\n', s)
            return s

        try:
            with app.app_context():
                logger.info("Processing streaming message: '%s...'", user_message[:100])
                final_chunk = None
                response_content = ''
                tools_used: list[str] = []

                for chunk in request_interpreter.process_message_streaming(
                    user_message,
                    session_id,
                    session_data,
                    is_data_analysis=is_data_analysis,
                    tab_context=tab_context,
                ):
                    if chunk.get('content'):
                        chunk['content'] = format_response(chunk['content'])
                        response_content += chunk.get('content', '')
                    if chunk.get('tools_used'):
                        tools_used.extend(chunk.get('tools_used', []))
                    if chunk.get('done'):
                        final_chunk = chunk
                        # Save assistant response BEFORE yielding final chunk.
                        # If we save after the yield, the generator may be closed
                        # by gunicorn before the save executes (client disconnects
                        # after reading done:true), losing the assistant message.
                        _log_stream_completion(app, final_chunk, response_content, memory_session_id or session_id)
                    chunk_json = json.dumps(chunk)
                    logger.debug("Sending streaming chunk: %s", chunk_json)
                    yield f"data: {chunk_json}\n\n"

                from flask import session as flask_session
                if tools_used:
                    if any(tool in tools_used for tool in ['run_composite_analysis', 'run_pca_analysis', 'runcompleteanalysis']):
                        flask_session['analysis_complete'] = True
                        if 'runcompleteanalysis' in tools_used:
                            flask_session['analysis_type'] = 'dual_method'
                        elif 'run_composite_analysis' in tools_used:
                            flask_session['analysis_type'] = 'composite'
                        else:
                            flask_session['analysis_type'] = 'pca'
                        flask_session.modified = True
                        logger.info(
                            "Session %s: Analysis completed via streaming, session updated",
                            session_id,
                        )
                    if any(tool in tools_used for tool in ['run_composite_analysis', 'run_pca_analysis', 'runcompleteanalysis']):
                        flask_session.pop('pending_action', None)
                        flask_session.pop('pending_variables', None)
                        flask_session.modified = True
        except Exception as exc:
            logger.error("Error in streaming processing: %s", exc)
            error_json = json.dumps({'content': f'Error: {str(exc)}', 'status': 'error', 'done': True})
            yield f"data: {error_json}\n\n"

    response = Response(generate(), mimetype='text/event-stream')
    response.headers['Cache-Control'] = 'no-cache'
    response.headers['Connection'] = 'keep-alive'
    response.headers['Access-Control-Allow-Origin'] = '*'
    response.headers['X-Accel-Buffering'] = 'no'
    return response


def _log_stream_completion(app, final_chunk: Dict[str, Any], response_content: str, session_id: str) -> None:
    tools_used = final_chunk.get('tools_used', [])
    if any(tool in tools_used for tool in ['run_composite_analysis', 'run_pca_analysis', 'runcompleteanalysis']):
        if 'runcompleteanalysis' in tools_used:
            analysis_type = 'dual_method'
        elif 'run_composite_analysis' in tools_used:
            analysis_type = 'composite'
        else:
            analysis_type = 'pca'
        logger.info(
            "Session %s: Analysis completed via streaming (%s)",
            session_id,
            analysis_type,
        )

    interaction_logger = getattr(app.services, 'interaction_logger', None)
    if interaction_logger:
        interaction_logger.log_message(
            session_id=session_id,
            sender='assistant',
            content=response_content,
            intent=final_chunk.get('intent_type', 'streaming'),
            entities={
                'streaming': True,
                'tools_used': tools_used,
                'status': final_chunk.get('status', 'success'),
            },
        )

    # Persist assistant message to SessionMemory for conversation resume
    try:
        from app.services.session_memory import SessionMemory, MessageType
        _mem = SessionMemory(session_id)
        # Include visualizations in metadata so they can be restored on resume
        _viz = final_chunk.get('visualizations') or []
        _metadata = {'visualizations': _viz} if _viz else {}
        _mem.add_message(MessageType.ASSISTANT, response_content, metadata=_metadata)
    except Exception as _mem_err:
        logger.debug("SessionMemory save (assistant) failed: %s", _mem_err)


def _response_from_tpr_result(tpr_result: Dict[str, Any]) -> Response:
    # Save TPR assistant response to SessionMemory for conversation resume
    try:
        from app.services.session_memory import SessionMemory, MessageType
        _sid = session.get('base_session_id') or session.get('session_id')
        if _sid:
            _mem = SessionMemory(_sid)
            _viz = tpr_result.get('visualizations') or []
            _metadata = {'visualizations': _viz} if _viz else {}
            _mem.add_message(MessageType.ASSISTANT, tpr_result.get('response', ''), metadata=_metadata)
    except Exception as _err:
        logger.debug("SessionMemory save (TPR assistant) failed: %s", _err)

    def generate():
        yield json.dumps(
            {
                'content': tpr_result.get('response', ''),
                'status': tpr_result.get('status', 'success'),
                'visualizations': tpr_result.get('visualizations', []),
                'tools_used': tpr_result.get('tools_used', []),
                'workflow': tpr_result.get('workflow', 'tpr'),
                'stage': tpr_result.get('stage'),
                'download_links': tpr_result.get('download_links', []),
                'trigger_data_uploaded': tpr_result.get('trigger_data_uploaded', False),
                'done': True,
            }
        )

    response = Response((f"data: {chunk}\n\n" for chunk in generate()), mimetype='text/event-stream')
    response.headers['Cache-Control'] = 'no-cache'
    response.headers['Connection'] = 'keep-alive'
    response.headers['Access-Control-Allow-Origin'] = '*'
    return response
def _sync_tpr_outputs_for_stream(payload_session_id: str | None) -> None:
    scoped_session_id = session.get('session_id')

    base_session_id = session.get('base_session_id')
    if not base_session_id:
        base_session_id = payload_session_id
        if not base_session_id and scoped_session_id and '__' in scoped_session_id:
            base_session_id = scoped_session_id.split('__', 1)[0]
        elif not base_session_id:
            base_session_id = scoped_session_id

    if not base_session_id or not scoped_session_id or base_session_id == scoped_session_id:
        return

    upload_root = current_app.config.get('UPLOAD_FOLDER', 'instance/uploads')
    source_folder = os.path.join(upload_root, base_session_id)
    dest_folder = os.path.join(upload_root, scoped_session_id)

    if not os.path.isdir(source_folder):
        return

    os.makedirs(dest_folder, exist_ok=True)

    critical_files = [
        'raw_data.csv',
        'raw_shapefile.zip',
        'tpr_results.csv',
        'tpr_distribution_map.html',
        '.agent_state.json',
        '.risk_ready',
        '.analysis_complete',
        '.tpr_waiting_confirmation',
        'tpr_debug.json',
    ]

    for filename in critical_files:
        source_path = os.path.join(source_folder, filename)
        if os.path.exists(source_path):
            dest_path = os.path.join(dest_folder, filename)
            shutil.copy2(source_path, dest_path)
            logger.warning(
                "[TPR SYNC] streaming copied=%s destination=%s scoped=%s",
                filename,
                dest_path,
                scoped_session_id,
            )

    source_viz = os.path.join(source_folder, 'visualizations')
    if os.path.isdir(source_viz):
        dest_viz = os.path.join(dest_folder, 'visualizations')
        shutil.copytree(source_viz, dest_viz, dirs_exist_ok=True)
        logger.warning("[TPR SYNC] streaming copied visualizations to scoped=%s", scoped_session_id)
