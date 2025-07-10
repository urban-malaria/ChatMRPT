# app/web/routes/visualization_routes.py
"""
Visualization Routes module for visualization and media operations.

This module contains the visualization-related routes for the ChatMRPT web application:
- Visualization generation (get_visualization)
- Navigation for composite maps and boxplots
- File serving for visualization assets
- Generic visualization navigation
"""

import os
import logging
import time
import traceback
from datetime import datetime
from flask import Blueprint, session, request, current_app, jsonify, send_from_directory

from ...core.decorators import handle_errors, log_execution_time, validate_session
from ...core.exceptions import ValidationError
from ...core.utils import convert_to_json_serializable

logger = logging.getLogger(__name__)

# Create the visualization routes blueprint
viz_bp = Blueprint('visualization', __name__)


@viz_bp.route('/get_visualization', methods=['POST'])
@validate_session
@handle_errors
@log_execution_time
def get_visualization():
    """Handle visualization requests directly"""
    try:
        data = request.json
        viz_type = data.get('type', '')
        variable = data.get('variable', None)
        threshold = data.get('threshold', 30)
        
        # Get session ID
        session_id = session.get('session_id')
        
        # Get services from the container
        data_service = current_app.services.data_service
        visualization_service = current_app.services.visualization_service
        
        # Get data handler via data service
        data_handler = data_service.get_handler(session_id)
        
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
        
        # Prepare parameters for the visualization
        params = {
            'variable': variable,
            'threshold': threshold
        }
            
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
            
            # Add explanation to result
            result['ai_response'] = explanation
            
            # Ensure JSON serializable result
            result = convert_to_json_serializable(result)
            return jsonify(result)

    except Exception as e:
        logger.error(f"Error generating visualization: {str(e)}", exc_info=True)
        return jsonify({
            'status': 'error',
            'message': f'Error generating visualization: {str(e)}',
            'ai_response': f"I encountered an error while creating the visualization. Please try again or choose a different visualization."
        })


@viz_bp.route('/navigate_composite_map', methods=['POST'])
@validate_session
@handle_errors
@log_execution_time
def navigate_composite_map():
    """Handle pagination for composite maps"""
    try:
        data = request.json
        direction = data.get('direction', '')
        
        if not direction or direction not in ['next', 'prev']:
            raise ValidationError('Invalid navigation direction')
        
        # Get services from the container
        data_service = current_app.services.data_service
        visualization_service = current_app.services.visualization_service
        
        # Get session ID
        session_id = session.get('session_id')
        
        # Get data handler via data service
        data_handler = data_service.get_handler(session_id)
        
        # Get current page from request or session
        current_page = data.get('current_page', session.get('current_composite_map_page', 1))
        
        # Determine new page based on direction
        if direction == 'next':
            new_page = current_page + 1
        else:  # prev
            new_page = max(1, current_page - 1)
        
        # Get the composite map for the new page
        result = visualization_service.navigate_composite_map(
            data_handler=data_handler,
            page=new_page,
            session_id=session_id
        )
        
        if result['status'] == 'success':
            # Update session with new page info
            session['current_composite_map_page'] = result.get('current_page', 1)
            
            # Add explanation for this specific page
            if 'ai_response' not in result or not result['ai_response']:
                explanation = visualization_service.explain_composite_map_navigation(
                    data_handler=data_handler,
                    page_data=result,
                    session_id=session_id
                )
                result['ai_response'] = explanation
            
            return jsonify(result)
        else:
            return jsonify({
                'status': 'error',
                'message': result.get('message', 'Error navigating composite maps')
            }), 400
    
    except ValidationError as e:
        return jsonify({
            'status': 'error',
            'message': str(e)
        }), 400
    except Exception as e:
        logger.error(f"Error navigating composite map: {str(e)}", exc_info=True)
        return jsonify({
            'status': 'error',
            'message': f'Error navigating composite maps: {str(e)}'
        }), 500


@viz_bp.route('/navigate_boxplot', methods=['POST'])
@validate_session
@handle_errors
@log_execution_time
def navigate_boxplot():
    """Handle pagination for box and whisker plots"""
    try:
        data = request.json
        direction = data.get('direction', '')
        
        if not direction or direction not in ['next', 'prev']:
            raise ValidationError('Invalid navigation direction')
        
        # Get services from the container
        data_service = current_app.services.data_service
        visualization_service = current_app.services.visualization_service
        
        # Get session ID
        session_id = session.get('session_id')
        
        # Get data handler via data service
        data_handler = data_service.get_handler(session_id)
        
        # Check if box plot data is available
        if not hasattr(data_handler, 'boxwhisker_plot') or not data_handler.boxwhisker_plot:
            raise ValidationError('Box plot data not available')
        
        # Get current page from request or session
        current_page = data.get('current_page', session.get('current_boxplot_page', 1))
        
        # Determine new page based on direction
        if direction == 'next':
            new_page = current_page + 1
        else:  # prev
            new_page = max(1, current_page - 1)
        
        # Navigate the boxplot using the service
        result = visualization_service.navigate_boxplot(
            data_handler=data_handler,
            page=new_page,
            session_id=session_id
        )
        
        if result['status'] == 'success':
            # Update session with new page info
            session['current_boxplot_page'] = result.get('current_page', 1)
            
            # Add explanation for this specific page
            if 'ai_response' not in result or not result['ai_response']:
                explanation = visualization_service.explain_boxplot_navigation(
                    data_handler=data_handler,
                    page_data=result,
                    session_id=session_id
                )
                result['ai_response'] = explanation
            
            return jsonify(result)
        else:
            return jsonify({
                'status': 'error',
                'message': result.get('message', 'Error navigating box plots')
            }), 400
    
    except ValidationError as e:
        return jsonify({
            'status': 'error',
            'message': str(e)
        }), 400
    except Exception as e:
        logger.error(f"Error navigating boxplot: {str(e)}", exc_info=True)
        return jsonify({
            'status': 'error',
            'message': f'Error navigating box plots: {str(e)}'
        }), 500


@viz_bp.route('/serve_viz_file/<session_id>/<path:filename>')
@validate_session
@handle_errors
@log_execution_time
def serve_viz_file(session_id, filename):
    """Serve visualization files for a session"""
    try:
        # Validate session ID matches current session
        current_session_id = session.get('session_id')
        if current_session_id != session_id:
            return jsonify({
                'status': 'error',
                'message': 'Unauthorized access to visualization files'
            }), 403
        
        # Construct the file path
        session_folder = os.path.join(current_app.config['UPLOAD_FOLDER'], session_id)
        
        # Security check - ensure file exists and is in the correct directory
        file_path = os.path.join(session_folder, filename)
        if not os.path.exists(file_path) or not os.path.commonpath([session_folder, file_path]) == session_folder:
            return jsonify({
                'status': 'error',
                'message': 'Visualization file not found'
            }), 404
        
        return send_from_directory(session_folder, filename)
    
    except Exception as e:
        logger.error(f"Error serving visualization file: {str(e)}", exc_info=True)
        return jsonify({
            'status': 'error',
            'message': 'Error serving visualization file'
        }), 500


@viz_bp.route('/navigate_visualization', methods=['POST'])
@validate_session
@handle_errors
@log_execution_time
def navigate_visualization():
    """Handle navigation for any visualization type"""
    try:
        data = request.json
        viz_type = data.get('viz_type', '')
        direction = data.get('direction', '')
        current_page = data.get('current_page', 1)
        total_pages = data.get('total_pages', 1)
        metadata = data.get('metadata', {})
        
        if not direction or direction not in ['next', 'prev']:
            raise ValidationError('Invalid navigation direction')
        
        # Get services from the container
        data_service = current_app.services.data_service
        visualization_service = current_app.services.visualization_service
        
        # Get session ID
        session_id = session.get('session_id')
        
        # Get data handler via data service
        data_handler = data_service.get_handler(session_id)
        
        if not data_handler:
            raise ValidationError('No data available for navigation')
        
        # Use the visualization service navigation method
        result = visualization_service.navigate_visualization(
            viz_type=viz_type,
            direction=direction,
            current_state={
                'current_page': current_page,
                'total_pages': total_pages,
                'metadata': metadata
            },
            data_handler=data_handler,
            session_id=session_id
        )
        
        if result['status'] == 'success':
            # Update session with new page info if available
            session_key = f'current_{viz_type}_page'
            if 'current_page' in result:
                session[session_key] = result['current_page']
            
            # Ensure result is JSON serializable
            result = convert_to_json_serializable(result)
            return jsonify(result)
        else:
            return jsonify({
                'status': 'error',
                'message': result.get('message', f'Error navigating {viz_type}')
            }), 400
    
    except ValidationError as e:
        return jsonify({
            'status': 'error',
            'message': str(e)
        }), 400
    except Exception as e:
        logger.error(f"Error navigating visualization: {str(e)}", exc_info=True)
        return jsonify({
            'status': 'error',
            'message': f'Error navigating visualization: {str(e)}'
        }), 500


@viz_bp.route('/explain_visualization', methods=['POST'])
@validate_session
@handle_errors
@log_execution_time
def explain_visualization():
    """Handle visualization explanation requests using AI vision"""
    try:
        data = request.json
        visualization_path = data.get('visualization_path')
        base64_data = data.get('base64_data')
        title = data.get('title', 'Visualization')
        viz_type = data.get('viz_type', 'unknown')
        
        # Get session ID
        session_id = session.get('session_id')
        
        # Get the universal visualization explainer service
        from app.services.universal_viz_explainer import get_universal_viz_explainer
        
        # Get LLM manager from services container
        llm_manager = current_app.services.llm_manager
        
        # Create explainer instance
        explainer = get_universal_viz_explainer(llm_manager=llm_manager)
        
        # Generate AI-powered explanation
        if base64_data:
            # For base64 images, we need to save temporarily and process
            import tempfile
            import base64
            
            # Decode base64 to image file
            img_data = base64.b64decode(base64_data)
            
            # Save to temporary file
            with tempfile.NamedTemporaryFile(mode='wb', suffix='.png', delete=False) as tmp_file:
                tmp_file.write(img_data)
                tmp_path = tmp_file.name
            
            try:
                # Get AI explanation
                explanation = explainer.explain_visualization(
                    viz_path=tmp_path,
                    viz_type=viz_type,
                    session_id=session_id
                )
            finally:
                # Clean up temp file
                import os
                if os.path.exists(tmp_path):
                    os.unlink(tmp_path)
                    
        elif visualization_path:
            # For file-based visualizations, construct full path
            import os
            
            # Handle relative paths - construct full path from session folder
            if not os.path.isabs(visualization_path):
                session_folder = os.path.join(current_app.config['UPLOAD_FOLDER'], session_id)
                full_path = os.path.join(session_folder, visualization_path)
            else:
                full_path = visualization_path
            
            # Get AI explanation
            explanation = explainer.explain_visualization(
                viz_path=full_path,
                viz_type=viz_type,
                session_id=session_id
            )
        else:
            return jsonify({
                'status': 'error',
                'message': 'Either visualization_path or base64_data must be provided'
            }), 400
        
        return jsonify({
            'status': 'success',
            'explanation': explanation,
            'title': title,
            'viz_type': viz_type
        })
        
    except Exception as e:
        logger.error(f"Error explaining visualization: {str(e)}", exc_info=True)
        return jsonify({
            'status': 'error',
            'message': f'Error generating explanation: {str(e)}'
        }), 500


# ========================================================================
# VISUALIZATION UTILITY FUNCTIONS
# ========================================================================

def validate_visualization_requirements(session_state, data_handler=None, viz_type=None):
    """
    Validate that visualization requirements are met.
    
    Args:
        session_state: Current session state
        data_handler: Data handler instance
        viz_type: Type of visualization to validate
        
    Returns:
        tuple: (is_valid, error_message)
    """
    if not data_handler:
        return False, 'No data available for visualization'
    
    # Variable maps can be shown without analysis
    if viz_type == 'variable_map':
        if not session_state.get('csv_loaded', False):
            return False, 'CSV data must be loaded to view variable maps'
        return True, ''
    
    # Other visualizations require analysis
    if not session_state.get('analysis_complete', False):
        return False, 'Analysis must be completed before generating visualizations'
    
    # Check specific requirements for different visualization types
    if viz_type == 'composite_map':
        if not hasattr(data_handler, 'vulnerability_rankings') or data_handler.vulnerability_rankings is None:
            return False, 'Vulnerability rankings not available for composite maps'
    
    elif viz_type == 'boxplot':
        if not hasattr(data_handler, 'boxwhisker_plot') or not data_handler.boxwhisker_plot:
            return False, 'Box plot data not available'
    
    return True, ''


def get_visualization_status(session_state, data_handler=None):
    """
    Get current visualization availability status.
    
    Args:
        session_state: Current session state
        data_handler: Data handler instance
        
    Returns:
        dict: Visualization availability information
    """
    status = {
        'can_view_variable_maps': False,
        'can_view_composite_maps': False,
        'can_view_boxplots': False,
        'can_view_charts': False,
        'analysis_complete': session_state.get('analysis_complete', False),
        'available_visualizations': []
    }
    
    # Variable maps available if CSV loaded
    if session_state.get('csv_loaded', False):
        status['can_view_variable_maps'] = True
        status['available_visualizations'].append('variable_map')
    
    # Other visualizations require analysis
    if status['analysis_complete'] and data_handler:
        # Composite maps
        if hasattr(data_handler, 'vulnerability_rankings') and data_handler.vulnerability_rankings is not None:
            status['can_view_composite_maps'] = True
            status['available_visualizations'].append('composite_map')
        
        # Box plots
        if hasattr(data_handler, 'boxwhisker_plot') and data_handler.boxwhisker_plot:
            status['can_view_boxplots'] = True
            status['available_visualizations'].append('boxplot')
        
        # General charts
        status['can_view_charts'] = True
        status['available_visualizations'].extend(['bar_chart', 'scatter_plot', 'histogram'])
    
    return status


def prepare_visualization_context(session_state, data_handler=None):
    """
    Prepare context information for visualization generation.
    
    Args:
        session_state: Current session state
        data_handler: Data handler instance
        
    Returns:
        dict: Context for visualization generation
    """
    context = {
        'session_id': session_state.get('session_id'),
        'analysis_complete': session_state.get('analysis_complete', False),
        'variables_used': session_state.get('variables_used', []),
        'last_visualization': session_state.get('last_visualization'),
        'available_variables': session_state.get('available_variables', [])
    }
    
    # Add data handler information
    if data_handler:
        context['has_vulnerability_rankings'] = (
            hasattr(data_handler, 'vulnerability_rankings') and 
            data_handler.vulnerability_rankings is not None
        )
        context['has_boxplot_data'] = (
            hasattr(data_handler, 'boxwhisker_plot') and 
            data_handler.boxwhisker_plot is not None
        )
        context['data_shape'] = getattr(data_handler.df, 'shape', (0, 0)) if hasattr(data_handler, 'df') else (0, 0)
    
    return context


def update_visualization_session_state(session, viz_type, result):
    """
    Update session state based on visualization results.
    
    Args:
        session: Flask session object
        viz_type: Type of visualization
        result: Visualization generation result
    """
    if result.get('status') == 'success':
        # Update last visualization
        session['last_visualization'] = {
            'type': viz_type,
            'timestamp': datetime.utcnow().isoformat(),
            'success': True
        }
        
        # Update specific visualization page states
        if 'current_page' in result:
            page_key = f'current_{viz_type}_page'
            session[page_key] = result['current_page']
        
        # Track total pages if available
        if 'total_pages' in result:
            total_key = f'total_{viz_type}_pages'
            session[total_key] = result['total_pages']
        
        logger.info(f"Session {session.get('session_id')}: {viz_type} visualization updated")


def clear_visualization_session_state(session):
    """
    Clear visualization-related session state.
    
    Args:
        session: Flask session object
    """
    # Clear general visualization state
    session.pop('last_visualization', None)
    
    # Clear page states for different visualization types
    viz_types = ['composite_map', 'boxplot', 'variable_map']
    for viz_type in viz_types:
        session.pop(f'current_{viz_type}_page', None)
        session.pop(f'total_{viz_type}_pages', None)
    
    logger.info(f"Session {session.get('session_id')}: Visualization state cleared") 