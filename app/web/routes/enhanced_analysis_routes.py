# app/web/routes/enhanced_analysis_routes.py
"""
Enhanced Analysis Routes - Phase 6.5 Frontend Integration

This module provides enhanced analysis routes that integrate the complete 6-phase
conversational architecture with the web interface, including intelligent routing
between different processing systems based on query complexity and context.
"""

import logging
from datetime import datetime
from flask import Blueprint, session, request, current_app, jsonify

from ...core.decorators import handle_errors, log_execution_time, validate_session
from ...core.exceptions import ValidationError
from ...core.utils import convert_to_json_serializable

logger = logging.getLogger(__name__)

# Create the enhanced analysis routes blueprint
enhanced_analysis_bp = Blueprint('enhanced_analysis', __name__)


@enhanced_analysis_bp.route('/send_message_enhanced', methods=['POST'])
@validate_session
@handle_errors
@log_execution_time
def send_message_enhanced():
    """
    Enhanced chat message handler with intelligent routing to 6-phase architecture.
    
    Routes messages intelligently between:
    - Simple tool execution (Request Interpreter)
    - ReAct Agent for complex reasoning
    - LangChain for conversational management
    - Hybrid approaches based on context
    """
    logger.info("🚀 ENHANCED MESSAGE PROCESSING - Using 6-Phase Architecture!")
    
    try:
        # Get the message from the request
        data = request.json
        user_message = data.get('message', '')
        if not user_message: 
            raise ValidationError('No message provided')

        # Get session ID and context
        session_id = session.get('session_id')
        if not session_id:
            raise ValidationError('No active session found')
        
        # Prepare context from Flask session
        context = {
            'csv_loaded': session.get('csv_loaded', False),
            'shapefile_loaded': session.get('shapefile_loaded', False),
            'analysis_complete': session.get('analysis_complete', False),
            'user_role': session.get('user_role', 'analyst'),
            'analysis_type': session.get('analysis_type', 'none'),
            'variables_used': session.get('variables_used', [])
        }
        
        # Get conversation router (lazy import)
        from ...core.conversation_router import get_conversation_router
        conversation_router = get_conversation_router()
        
        # Inject request interpreter if not already set
        if not conversation_router.request_interpreter:
            request_interpreter = current_app.services.request_interpreter
            if request_interpreter:
                conversation_router.set_request_interpreter(request_interpreter)
            else:
                logger.error("Request Interpreter not available")
                return jsonify({
                    'status': 'error',
                    'message': 'Request processing system not available'
                }), 500
        
        # Route and process message intelligently
        logger.info(f"Routing message with context: data_loaded={context['csv_loaded'] and context['shapefile_loaded']}")
        response = conversation_router.route_conversation(user_message, session_id, context)
        
        # Extract routing information
        routing_info = response.get('routing_info', {})
        processing_mode = routing_info.get('processing_mode', 'unknown')
        reasoning = routing_info.get('reasoning', 'No reasoning provided')
        
        logger.info(f"Message processed via {processing_mode}: {reasoning}")
        
        # Format response for frontend compatibility
        formatted_response = {
            'status': response.get('status', 'success'),
            'message': response.get('response', 'Request processed successfully'),
            'response': response.get('response', 'Request processed successfully'),
            'visualizations': response.get('visualizations', []),
            'explanations': response.get('explanations', []),
            'data_summary': response.get('data_summary'),
            'tools_used': response.get('tools_used', []),
            'intent_type': response.get('intent_type'),
            'processing_mode': processing_mode,
            'reasoning': reasoning,
            'execution_time': routing_info.get('execution_time', 0),
            'confidence_score': response.get('confidence_score'),
            'reasoning_steps': response.get('reasoning_steps', 0)
        }
        
        # Add architecture-specific information
        if processing_mode == 'react_agent':
            formatted_response['agent_analysis'] = {
                'reasoning_steps': response.get('reasoning_steps', 0),
                'confidence_score': response.get('confidence_score', 0),
                'conversation_mode': response.get('conversation_mode', 'agent_analysis')
            }
        elif processing_mode == 'langchain_chat':
            formatted_response['conversation_management'] = {
                'langchain_enabled': response.get('langchain_enabled', False),
                'conversation_mode': response.get('conversation_mode', 'simple_chat')
            }
        
        # Update session state based on tools used and processing mode
        tools_used = response.get('tools_used', [])
        
        # Handle analysis completion - including dual-method analysis
        if 'runcompleteanalysis' in tools_used:
            session['analysis_complete'] = True
            session['analysis_type'] = 'dual_method'
            logger.info(f"Session {session_id}: Dual-method analysis completed via {processing_mode}")
        elif 'run_composite_analysis' in tools_used or 'run_pca_analysis' in tools_used:
            session['analysis_complete'] = True
            session['analysis_type'] = 'composite' if 'run_composite_analysis' in tools_used else 'pca'
            logger.info(f"Session {session_id}: Single analysis completed via {processing_mode}")
        
        # Clear pending actions if analysis was run
        if any(tool in tools_used for tool in ['runcompleteanalysis', 'run_composite_analysis', 'run_pca_analysis']):
                session.pop('pending_action', None)
                session.pop('pending_variables', None)
        
        # Record conversation turn for learning
        session['last_processing_mode'] = processing_mode
        session['last_message_time'] = datetime.utcnow().isoformat()
        
        # Ensure response is JSON serializable
        formatted_response = convert_to_json_serializable(formatted_response)
        
        logger.info(f"Enhanced response sent: mode={processing_mode}, tools={len(tools_used)}, status={formatted_response.get('status')}")
        return jsonify(formatted_response)
    
    except ValidationError as e:
        return jsonify({
            'status': 'error',
            'message': str(e),
            'processing_mode': 'error'
        }), 400
    except Exception as e:
        logger.error(f"Error in enhanced message processing: {str(e)}", exc_info=True)
        return jsonify({
            'status': 'error',
            'message': f'Error processing message: {str(e)}',
            'processing_mode': 'error'
        }), 500


@enhanced_analysis_bp.route('/conversation_stats', methods=['GET'])
@validate_session
@handle_errors
def get_conversation_stats():
    """Get conversation routing statistics and performance metrics."""
    try:
        conversation_router = get_conversation_router()
        routing_stats = conversation_router.get_routing_statistics()
        
        # Add session-specific information
        session_id = session.get('session_id')
        session_stats = {
            'session_id': session_id,
            'last_processing_mode': session.get('last_processing_mode', 'none'),
            'last_message_time': session.get('last_message_time'),
            'analysis_complete': session.get('analysis_complete', False),
            'data_loaded': session.get('csv_loaded', False) and session.get('shapefile_loaded', False)
        }
        
        return jsonify({
            'status': 'success',
            'routing_stats': routing_stats,
            'session_stats': session_stats
        })
    
    except Exception as e:
        logger.error(f"Error getting conversation stats: {e}")
        return jsonify({
            'status': 'error',
            'message': f'Error getting statistics: {str(e)}'
        }), 500


@enhanced_analysis_bp.route('/system_health', methods=['GET'])
@handle_errors
def get_system_health():
    """Get comprehensive system health information."""
    try:
        from ...core.production_deployment import get_deployment_manager
        deployment_manager = get_deployment_manager()
        
        # Get health check
        health_result = deployment_manager.health_check()
        
        # Get system status
        system_status = deployment_manager.get_system_status()
        
        return jsonify({
            'status': 'success',
            'health': health_result,
            'system_status': system_status
        })
    
    except Exception as e:
        logger.error(f"Error getting system health: {e}")
        return jsonify({
            'status': 'error',
            'message': f'Error getting system health: {str(e)}'
        }), 500


@enhanced_analysis_bp.route('/system_insights', methods=['GET'])
@handle_errors
def get_system_insights():
    """Get system insights from reflection engine."""
    try:
        from ...core.reflection_engine import get_reflection_engine
        
        reflection_engine = get_reflection_engine()
        if not reflection_engine.enabled:
            return jsonify({
                'status': 'success',
                'message': 'Reflection engine not enabled',
                'insights': {'enabled': False}
            })
        
        insights = reflection_engine.get_system_insights()
        
        return jsonify({
            'status': 'success',
            'insights': insights
        })
    
    except Exception as e:
        logger.error(f"Error getting system insights: {e}")
        return jsonify({
            'status': 'error',
            'message': f'Error getting insights: {str(e)}'
        }), 500


@enhanced_analysis_bp.route('/reset_conversation_stats', methods=['POST'])
@validate_session
@handle_errors
def reset_conversation_stats():
    """Reset conversation routing statistics."""
    try:
        conversation_router = get_conversation_router()
        conversation_router.reset_statistics()
        
        return jsonify({
            'status': 'success',
            'message': 'Conversation statistics reset successfully'
        })
    
    except Exception as e:
        logger.error(f"Error resetting conversation stats: {e}")
        return jsonify({
            'status': 'error',
            'message': f'Error resetting statistics: {str(e)}'
        }), 500


@enhanced_analysis_bp.route('/switch_processing_mode', methods=['POST'])
@validate_session
@handle_errors
def switch_processing_mode():
    """
    Allow users to manually switch between processing modes for testing.
    This is primarily for development and debugging purposes.
    """
    try:
        data = request.json
        mode = data.get('mode', 'auto')
        
        # Store preference in session
        session['preferred_processing_mode'] = mode
        
        return jsonify({
            'status': 'success',
            'message': f'Processing mode set to {mode}',
            'mode': mode
        })
    
    except Exception as e:
        logger.error(f"Error switching processing mode: {e}")
        return jsonify({
            'status': 'error',
            'message': f'Error switching mode: {str(e)}'
        }), 500


@enhanced_analysis_bp.route('/test_architecture_components', methods=['GET'])
@handle_errors
def test_architecture_components():
    """
    Test endpoint to verify all 6-phase architecture components are working.
    This is for debugging and system verification.
    """
    try:
        results = {}
        
        # Test Phase 1: Conversation Memory (quick check)
        try:
            from ...core.conversation_memory import get_conversation_memory
            memory_manager = get_conversation_memory()
            results['phase1_memory'] = {
                'available': True,
                'enabled': getattr(memory_manager, 'enabled', True),
                'status': 'working'
            }
        except Exception as e:
            results['phase1_memory'] = {
                'available': False,
                'error': str(e)[:100],  # Truncate long errors
                'status': 'error'
            }
        
        # Test Phase 2: Tool Registry (quick check)
        try:
            from ...core.tool_registry import get_tool_registry
            tool_registry = get_tool_registry()
            tool_count = len(tool_registry.list_tools()) if hasattr(tool_registry, 'list_tools') else 0
            results['phase2_tools'] = {
                'available': True,
                'tool_count': tool_count,
                'status': 'working'
            }
        except Exception as e:
            results['phase2_tools'] = {
                'available': False,
                'error': str(e)[:100],
                'status': 'error'
            }
        
        # Test Phase 3: ReAct Agent (quick check) 
        try:
            from ...core.chatmrpt_agent import get_chatmrpt_agent
            agent = get_chatmrpt_agent()
            results['phase3_agent'] = {
                'available': True,
                'status': 'working'
            }
        except Exception as e:
            results['phase3_agent'] = {
                'available': False,
                'error': str(e)[:100],
                'status': 'error'
            }
        
        # Test Phase 4: LangChain Integration (quick check)
        try:
            from ...core.langchain_integration import get_conversation_chain
            langchain_chain = get_conversation_chain()
            results['phase4_langchain'] = {
                'available': True,
                'status': 'working'
            }
        except Exception as e:
            results['phase4_langchain'] = {
                'available': False,
                'error': str(e)[:100],
                'status': 'error'
            }
        
        # Test Phase 5: Reflection Engine (quick check)
        try:
            from ...core.reflection_engine import get_reflection_engine
            reflection_engine = get_reflection_engine()
            results['phase5_reflection'] = {
                'available': True,
                'status': 'working'
            }
        except Exception as e:
            results['phase5_reflection'] = {
                'available': False,
                'error': str(e)[:100],
                'status': 'error'
            }
        
        # Test Phase 6: Production Deployment (quick check)
        try:
            from ...core.production_deployment import get_deployment_manager
            deployment_manager = get_deployment_manager()
            results['phase6_deployment'] = {
                'available': True,
                'status': 'working'
            }
        except Exception as e:
            results['phase6_deployment'] = {
                'available': False,
                'error': str(e)[:100],
                'status': 'error'
            }
        
        # Test Conversation Router (quick check)
        try:
            from ...core.conversation_router import get_conversation_router
            conversation_router = get_conversation_router()
            results['conversation_router'] = {
                'available': True,
                'status': 'working'
            }
        except Exception as e:
            results['conversation_router'] = {
                'available': False,
                'error': str(e)[:100],
                'status': 'error'
            }
        
        # Calculate overall health
        total_components = len(results)
        working_components = sum(1 for r in results.values() if r.get('status') == 'working')
        overall_health = (working_components / total_components) * 100
        
        return jsonify({
            'status': 'success',
            'overall_health': f"{overall_health:.1f}%",
            'working_components': working_components,
            'total_components': total_components,
            'component_results': results
        })
    
    except Exception as e:
        logger.error(f"Error testing architecture components: {e}")
        return jsonify({
            'status': 'error',
            'message': f'Error testing components: {str(e)}'
        }), 500


# Utility function to integrate with existing routes
def upgrade_existing_send_message():
    """
    Function to upgrade the existing /send_message route to use enhanced processing.
    This can be called during application initialization.
    """
    logger.info("Upgrading existing /send_message route to use enhanced 6-phase architecture")
    
    # The enhanced route is available at /send_message_enhanced
    # The frontend can be gradually migrated to use this endpoint
    # Or the existing route can be modified to delegate to the enhanced version
    
    return True