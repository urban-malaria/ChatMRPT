"""
Compatibility Layer for ConversationalEpidemiologist Integration

This module provides conversion functions to bridge the new LLM-first
conversational system with existing web route expectations.
"""

from typing import Dict, List, Any, Optional
import logging
import re

logger = logging.getLogger(__name__)

def convert_epidemiologist_response_to_legacy(epidemiologist_response: Dict[str, Any]) -> Dict[str, Any]:
    """
    Convert ConversationalEpidemiologist response to legacy MessageService format.
    
    Args:
        epidemiologist_response: Response from ConversationalEpidemiologist.process_message()
        
    Returns:
        Dict compatible with existing route expectations
    """
    try:
        # Extract tool calls for action determination
        tool_calls = epidemiologist_response.get('tool_calls', [])
        
        # Determine legacy action based on tool calls
        action = extract_action_from_tool_calls(tool_calls)
        
        # Extract session updates from context and tool results
        session_updates = extract_session_updates(epidemiologist_response)
        
        # CRITICAL FIX: Extract visualization data from tool calls
        visualization_data = extract_visualization_data(tool_calls)
        
        # Get the response text
        response_text = epidemiologist_response.get('response', '')
        
        # Clean the response text to remove any technical details
        response_text = clean_response_text(response_text)
        
        # Build legacy response - BOTH 'message' and 'response' for compatibility
        legacy_response = {
            'status': epidemiologist_response.get('status', 'success'),
            'message': response_text,  # Frontend expects this field
            'response': response_text,  # Also include this for chat manager
            'action': action,
            'session_updates': session_updates,
            'context': epidemiologist_response.get('context', {}),
            
            # Additional fields for compatibility
            'tool_calls': tool_calls,  # Keep for debugging
            'analysis_type': session_updates.get('analysis_type'),
            'variables': session_updates.get('variables', []),
            
            # CRITICAL FIX: Add visualization data for frontend
            'visualization': visualization_data.get('file_path'),
            'visualizations': visualization_data.get('visualizations', []),
            'inline_visualizations': visualization_data.get('inline_visualizations', []),
            'viz_type': visualization_data.get('viz_type'),
            'variable': visualization_data.get('variable')
        }
        
        # CRITICAL FIX: If we have visualization data, set appropriate action
        if visualization_data.get('has_visualization'):
            if not action:  # Only set if no action already determined
                legacy_response['action'] = 'show_visualization'
        
        # Debug logging
        logger.info(f"Converted epidemiologist response: action={action}, updates={len(session_updates)}")
        logger.info(f"Response text length: {len(response_text)} chars")
        logger.info(f"Response preview: '{response_text[:100]}...'")
        
        # CRITICAL FIX: Log visualization extraction
        if visualization_data.get('has_visualization'):
            viz_count = len(visualization_data.get('visualizations', []))
            inline_count = len(visualization_data.get('inline_visualizations', []))
            logger.info(f"Extracted visualizations: {viz_count} files, {inline_count} inline")
        
        return legacy_response
        
    except Exception as e:
        logger.error(f"Error converting epidemiologist response: {str(e)}", exc_info=True)
        
        # Fallback response
        return {
            'status': 'error',
            'message': f'Error processing request: {str(e)}',
            'action': None,
            'session_updates': {},
            'context': {}
        }

def extract_action_from_tool_calls(tool_calls: List[Dict[str, Any]]) -> Optional[str]:
    """
    Extract legacy action from tool calls.
    
    Args:
        tool_calls: List of tool call results
        
    Returns:
        Legacy action string or None
    """
    if not tool_calls:
        return None
    
    # Map tool functions to legacy actions
    for tool_call in tool_calls:
        function_name = tool_call.get('function', '')
        result = tool_call.get('result', {})
        
        # Analysis completion actions
        if function_name == 'run_composite_analysis':
            if result.get('status') == 'success':
                return 'analysis_complete'
            
        elif function_name == 'run_pca_analysis':
            if result.get('status') == 'success':
                return 'analysis_complete'
                
        # Variable selection actions  
        elif function_name == 'get_session_status':
            # Check if this was part of variable selection workflow
            status_data = result.get('data', {})
            if status_data.get('pending_variables'):
                return 'set_pending_variables'
                
        # Session management actions
        elif function_name == 'navigate_visualization':
            return 'navigate_visualization'
            
        elif function_name == 'create_urban_extent_map':
            if result.get('status') == 'success':
                return 'visualization_created'
    
    # Default: no specific action required
    return None

def extract_session_updates(epidemiologist_response: Dict[str, Any]) -> Dict[str, Any]:
    """
    Extract session updates from epidemiologist response.
    
    Args:
        epidemiologist_response: Full response from ConversationalEpidemiologist
        
    Returns:
        Dictionary of session updates
    """
    updates = {}
    
    # Extract from context
    context = epidemiologist_response.get('context', {})
    if context.get('state'):
        updates['state_name'] = context['state']
    
    # Extract from tool call results
    tool_calls = epidemiologist_response.get('tool_calls', [])
    
    for tool_call in tool_calls:
        function_name = tool_call.get('function', '')
        result = tool_call.get('result', {})
        
        if result.get('status') != 'success':
            continue
            
        data = result.get('data', {})
        
        # Analysis completion updates
        if function_name in ['run_composite_analysis', 'run_pca_analysis']:
            updates['analysis_complete'] = True
            updates['analysis_type'] = 'composite' if 'composite' in function_name else 'pca'
            
            # Extract variables used
            if 'variables_used' in data:
                updates['variables_used'] = data['variables_used']
                
            # Extract visualization info
            if 'created_visualizations' in data:
                updates['last_visualization'] = data['created_visualizations']
                
        # Session status updates
        elif function_name == 'get_session_status':
            if data.get('has_csv'):
                updates['csv_loaded'] = True
            if data.get('has_shapefile'):
                updates['shapefile_loaded'] = True
            if data.get('analysis_ready'):
                updates['data_loaded'] = True
                
        # Ward information updates
        elif function_name == 'get_ward_info':
            if 'ward_column' in data:
                updates['ward_column'] = data['ward_column']
    
    return updates

def handle_legacy_session_state(session_state: Dict[str, Any], session_updates: Dict[str, Any]) -> None:
    """
    Handle legacy session state management.
    
    Args:
        session_state: Current Flask session state
        session_updates: Updates to apply
    """
    # Apply updates to Flask session
    for key, value in session_updates.items():
        if key in ['analysis_complete', 'csv_loaded', 'shapefile_loaded', 'data_loaded']:
            session_state[key] = value
            logger.info(f"Session update: {key} = {value}")
        elif key == 'analysis_type':
            session_state['analysis_type'] = value
        elif key == 'variables_used':
            session_state['variables_used'] = value
        elif key == 'last_visualization':
            session_state['last_visualization'] = value

def create_error_response(error_message: str) -> Dict[str, Any]:
    """
    Create standardized error response.
    
    Args:
        error_message: Error description
        
    Returns:
        Standardized error response dict
    """
    return {
        'status': 'error',
        'message': error_message,
        'action': None,
        'session_updates': {},
        'context': {}
    }

def clean_response_text(response_text: str) -> str:
    """
    Clean response text to remove technical details and ensure user-friendly format.
    
    Args:
        response_text: Raw response text from LLM
        
    Returns:
        Cleaned response text suitable for user display
    """
    if not response_text or not isinstance(response_text, str):
        return response_text
    
    # Remove any JSON-like structures that might have leaked through
    response_text = re.sub(r'\{[^{}]*"status"[^{}]*\}', '', response_text)
    response_text = re.sub(r'\{[^{}]*"data"[^{}]*\}', '', response_text)
    response_text = re.sub(r'\{[^{}]*"message"[^{}]*\}', '', response_text)
    
    # Remove technical function names
    technical_terms = [
        'get_session_status', 'run_composite_analysis', 'run_pca_analysis',
        'create_urban_extent_map', 'navigate_visualization', 'get_ward_info',
        'tool_calls', 'function_name', 'session_id', 'data_handler'
    ]
    
    for term in technical_terms:
        response_text = re.sub(f'\\b{term}\\b', '', response_text, flags=re.IGNORECASE)
    
    # Remove multiple spaces and clean up
    response_text = re.sub(r'\s+', ' ', response_text)
    response_text = response_text.strip()
    
    # Remove any remaining brackets or technical artifacts
    response_text = re.sub(r'\[\s*\]', '', response_text)
    response_text = re.sub(r'\(\s*\)', '', response_text)
    
    # Fallback if response becomes empty after cleaning
    if not response_text or len(response_text.strip()) < 10:
        response_text = "I'm processing your request. Please let me know if you need any clarification or assistance."
    
    return response_text

def extract_visualization_data(tool_calls: List[Dict[str, Any]]) -> Dict[str, Any]:
    """
    Extract visualization data from tool calls for frontend display.
    
    Args:
        tool_calls: List of tool call results
        
    Returns:
        Dictionary with visualization data for frontend
    """
    viz_data = {
        'has_visualization': False,
        'file_path': None,
        'visualizations': [],
        'inline_visualizations': [],
        'viz_type': None,
        'variable': None
    }
    
    if not tool_calls:
        return viz_data
    
    # Look for visualization-related tool calls
    for tool_call in tool_calls:
        function_name = tool_call.get('function', '')
        result = tool_call.get('result', {})
        
        if result.get('status') != 'success':
            continue
        
        # Check for visualization functions (agent orchestrator responses)
        if hasattr(result.get('data'), 'get'):
            agent_data = result.get('data', {})
            
            # Extract from agent responses that contain visualization results
            if 'visualizations' in agent_data:
                viz_data['has_visualization'] = True
                agent_vizs = agent_data['visualizations']
                
                if isinstance(agent_vizs, list):
                    for viz in agent_vizs:
                        if isinstance(viz, dict):
                            # Handle inline visualizations
                            if viz.get('type') == 'inline' and 'data' in viz:
                                viz_data['inline_visualizations'].append(viz['data'])
                            # Handle file-based visualizations
                            elif viz.get('type') == 'file' and 'path' in viz:
                                viz_data['visualizations'].append({
                                    'path': viz['path'],
                                    'title': viz.get('title', 'Visualization')
                                })
                                if not viz_data['file_path']:
                                    viz_data['file_path'] = viz['path']
            
            # Extract visualization type and variable if available
            if 'primary_results' in agent_data:
                primary = agent_data['primary_results']
                if isinstance(primary, dict):
                    viz_data['viz_type'] = primary.get('visualization_type')
                    if 'parameters' in primary:
                        params = primary['parameters']
                        if isinstance(params, dict):
                            viz_data['variable'] = params.get('variable_name')
        
        # Direct visualization tool calls (legacy support)
        elif function_name in ['create_composite_score_maps', 'create_vulnerability_map', 
                              'create_box_plot_ranking', 'create_urban_extent_map', 
                              'create_decision_tree', 'create_pca_vulnerability_map']:
            
            viz_data['has_visualization'] = True
            viz_data['viz_type'] = function_name.replace('create_', '').replace('_', ' ')
            
            # Extract file paths
            if 'file_path' in result:
                viz_data['file_path'] = result['file_path']
                viz_data['visualizations'].append({
                    'path': result['file_path'],
                    'title': viz_data['viz_type'].title()
                })
            
            if 'web_path' in result:
                viz_data['visualizations'].append({
                    'path': result['web_path'],
                    'title': viz_data['viz_type'].title()
                })
            
            # Extract inline visualizations
            if 'plotly_json' in result and result['plotly_json']:
                inline_viz = {
                    'type': 'plotly',
                    'data': result['plotly_json'],
                    'title': viz_data['viz_type'].title(),
                    'interactive': True
                }
                viz_data['inline_visualizations'].append(inline_viz)
    
    return viz_data 