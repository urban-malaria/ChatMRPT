"""Synchronous chat handler for analysis workflow."""

from __future__ import annotations

from typing import Any, Dict

import os
import time
import traceback

from flask import jsonify, current_app, session, request

from app.core.exceptions import ValidationError
from app.core.utils import convert_to_json_serializable
from . import logger
from .arena_helpers import ArenaSetupError, is_arena_eligible_message, start_arena_battle, format_arena_response
from .chat_routing import route_with_mistral
from .utils import resync_session_flags, run_async

__all__ = ["handle_send_message"]


def handle_send_message():
    """Handle chat messages with the request interpreter."""
    interaction_logger = getattr(getattr(current_app, 'services', None), 'interaction_logger', None)
    try:
        data = request.json or {}
        user_message = data.get('message', '')
        if not user_message:
            raise ValidationError('No message provided')

        tab_context = data.get('tab_context', 'standard-upload')
        is_data_analysis = data.get('is_data_analysis', False)

        if is_data_analysis:
            session['active_tab'] = 'data-analysis'
            session['use_data_analysis_v3'] = True
            logger.info("📊 Data Analysis tab active - setting V3 mode")

        session_id = session.get('session_id')

        request_start_time = time.time()
        message_start_time = time.time()

        if interaction_logger:
            interaction_logger.log_message(
                session_id=session_id,
                sender='user',
                content=user_message,
                intent=None,
                entities={
                    'message_length': len(user_message),
                    'timestamp': message_start_time,
                    'session_message_count': session.get('message_count', 0) + 1,
                    'request_endpoint': '/send_message',
                },
            )
            interaction_logger.log_analysis_event(
                session_id=session_id,
                event_type='user_interaction',
                details={
                    'action': 'message_sent',
                    'message_type': 'chat_request',
                    'session_duration': time.time() - session.get('session_start_time', time.time()),
                    'is_follow_up': session.get('message_count', 0) > 0,
                },
                success=True,
            )
            session['message_count'] = session.get('message_count', 0) + 1

        try:
            from app.core.instance_sync import ensure_session_available
            ensure_session_available(session_id)
        except Exception:  # pragma: no cover - defensive
            pass

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
                    return jsonify(clarification)

        # Check if this should trigger Arena mode
        from app.config.arena import is_arena_available
        use_arena = (
            routing_decision == 'can_answer'
            and is_arena_available()
            and is_arena_eligible_message(user_message)
        )
        use_tools = routing_decision == 'needs_tools'

        logger.info("Final routing: use_arena=%s, use_tools=%s", use_arena, use_tools)

        processing_start_time = time.time()
        response = None

        if use_arena:
            try:
                logger.info("Arena mode triggered for general knowledge question: '%s...'", user_message[:50])
                battle_result = start_arena_battle(user_message, session_id)
                response = format_arena_response(battle_result, user_message)

                # Store battle ID in session
                session['current_battle_id'] = battle_result.get('battle_id')
                session.modified = True
            except ArenaSetupError as exc:
                logger.warning("Arena failed, falling back to normal response: %s", exc)
                use_arena = False
            except Exception as exc:
                logger.error("Unexpected arena error, falling back: %s", exc)
                use_arena = False

        if use_arena and response:
            processing_duration = time.time() - processing_start_time
            total_response_time = time.time() - request_start_time
            if interaction_logger and response:
                interaction_logger.log_message(
                    session_id=session_id,
                    sender='assistant',
                    content=response.get('response', ''),
                    intent='arena',
                    entities={
                        'response_length': len(response.get('response', '')),
                        'processing_time_seconds': processing_duration,
                        'total_response_time_seconds': total_response_time,
                        'tools_used': [],
                        'status': response.get('status', 'success'),
                    },
                )
                interaction_logger.log_analysis_event(
                    session_id=session_id,
                    event_type='arena_preview',
                    details={
                        'battle_id': response.get('battle_id'),
                        'models': [response.get('model_a'), response.get('model_b')],
                    },
                    success=True,
                )
            return jsonify(response)

        if use_tools and (
            session.get('use_data_analysis_v3', False) or session.get('data_analysis_active', False)
        ):
            try:
                from app.data_analysis_v3.core.agent import DataAnalysisAgent
                agent = DataAnalysisAgent(session_id=session_id)
                result = run_async(agent.analyze(user_message))
                response = {
                    'status': 'success',
                    'response': result.get('message', ''),
                    'message': result.get('message', ''),
                    'visualizations': result.get('visualizations', []),
                    'insights': result.get('insights', []),
                    'success': result.get('success', True),
                    'debug': result.get('debug'),
                }
            except Exception as exc:
                logger.error("Error with Data Analysis V3 agent: %s", exc, exc_info=True)
                response = {
                    'status': 'error',
                    'response': f"I encountered an issue: {str(exc)}",
                    'message': f"I encountered an issue: {str(exc)}",
                }
        else:
            request_interpreter = getattr(current_app.services, 'request_interpreter', None)
            if request_interpreter is None:
                logger.error("Request Interpreter not available")
                return jsonify({
                    'status': 'error',
                    'message': 'Request processing system not available',
                }), 500
            logger.info("Processing message with Request Interpreter: '%s...'", user_message[:100])
            response = request_interpreter.process_message(
                user_message,
                session_id,
                is_data_analysis=is_data_analysis,
                tab_context=tab_context,
            )

        processing_duration = time.time() - processing_start_time

        if interaction_logger:
            total_response_time = time.time() - request_start_time
            overhead_time = total_response_time - processing_duration
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
                    'performance_category': 'fast' if total_response_time < 5 else 'medium' if total_response_time < 15 else 'slow',
                },
            )
            interaction_logger.log_analysis_event(
                session_id=session_id,
                event_type='response_timing',
                details={
                    'total_response_time_seconds': total_response_time,
                    'processing_time_seconds': processing_duration,
                    'overhead_time_seconds': overhead_time,
                    'tools_used': response.get('tools_used', []),
                    'intent_type': response.get('intent_type'),
                },
                success=response.get('status') == 'success',
            )

        formatted_response = convert_to_json_serializable(
            _format_response_payload(response, processing_duration, request_start_time)
        )

        tools_used = response.get('tools_used', [])
        if any(tool in tools_used for tool in ['run_composite_analysis', 'run_pca_analysis', 'runcompleteanalysis']):
            session['analysis_complete'] = True
            if 'runcompleteanalysis' in tools_used:
                session['analysis_type'] = 'dual_method'
            elif 'run_composite_analysis' in tools_used:
                session['analysis_type'] = 'composite'
            else:
                session['analysis_type'] = 'pca'
            session.modified = True
            logger.info(
                "Session %s: Analysis completed via Request Interpreter (%s)",
                session_id,
                session['analysis_type'],
            )
            session.pop('pending_action', None)
            session.pop('pending_variables', None)
            session.modified = True

        logger.info(
            "Request Interpreter response sent: status=%s, tools=%d",
            formatted_response.get('status'),
            len(tools_used),
        )
        return jsonify(formatted_response)

    except ValidationError as exc:
        if interaction_logger:
            interaction_logger.log_error(
                session_id=session.get('session_id'),
                error_type='ValidationError',
                error_message=str(exc),
                stack_trace=traceback.format_exc(),
            )
        return jsonify({'status': 'error', 'message': str(exc)}), 400
    except Exception as exc:
        session_id = session.get('session_id')
        error_details = {
            'error_type': type(exc).__name__,
            'error_message': str(exc),
            'endpoint': '/send_message',
            'user_message': locals().get('user_message', 'Unknown')[:100],
            'processing_stage': 'request_interpreter_processing',
            'timestamp': time.time(),
        }
        interaction_logger = getattr(getattr(current_app, 'services', None), 'interaction_logger', None)
        if interaction_logger:
            interaction_logger.log_error(
                session_id=session_id,
                error_type=type(exc).__name__,
                error_message=str(exc),
                stack_trace=traceback.format_exc(),
            )
            interaction_logger.log_analysis_event(
                session_id=session_id,
                event_type='system_error',
                details=error_details,
                success=False,
            )
        logger.error("Error processing message with Request Interpreter: %s", exc, exc_info=True)
        return jsonify({'status': 'error', 'message': f'Error processing message: {str(exc)}'}), 500

def _format_response_payload(response: Dict[str, Any], processing_duration: float, request_start_time: float) -> Dict[str, Any]:
    total_response_time = time.time() - request_start_time
    if response.get('arena_mode'):
        return {
            'status': response.get('status', 'success'),
            'message': response.get('response', 'Arena comparison ready'),
            'response': response.get('response', 'Arena comparison ready'),
            'arena_mode': True,
            'battle_id': response.get('battle_id'),
            'response_a': response.get('response_a'),
            'response_b': response.get('response_b'),
            'latency_a': response.get('latency_a'),
            'latency_b': response.get('latency_b'),
            'view_index': response.get('view_index'),
            'model_a': response.get('model_a'),
            'model_b': response.get('model_b'),
            'processing_time': f"{processing_duration:.2f}s",
            'total_response_time': f"{total_response_time:.2f}s",
            'response_efficiency': f"{round(processing_duration / total_response_time * 100, 1) if total_response_time > 0 else 100}%",
        }
    payload = {
        'status': response.get('status', 'success'),
        'message': response.get('response', 'Request processed successfully'),
        'response': response.get('response', 'Request processed successfully'),
        'explanations': response.get('explanations', []),
        'data_summary': response.get('data_summary'),
        'tools_used': response.get('tools_used', []),
        'intent_type': response.get('intent_type'),
        'processing_time': f"{processing_duration:.2f}s",
        'total_response_time': f"{total_response_time:.2f}s",
        'response_efficiency': f"{round(processing_duration / total_response_time * 100, 1) if total_response_time > 0 else 100}%",
    }
    visualizations = response.get('visualizations', [])
    if visualizations:
        valid_visualizations = [
            viz
            for viz in visualizations
            if isinstance(viz, dict) and (viz.get('url') or viz.get('path') or viz.get('html'))
        ]
        if valid_visualizations:
            payload['visualizations'] = valid_visualizations
    return payload
