# app/web/routes/debug_routes.py
"""
Debug Routes module for debugging and admin interface.

This module contains the debug-related routes for the ChatMRPT web application:
- Analysis state debugging and fixing
- Session state inspection
- Visualization debugging
- LLM service diagnostics
- Admin interface routes
"""

import logging
from datetime import datetime
from flask import Blueprint, session, request, current_app, jsonify

from ...core.decorators import handle_errors, log_execution_time, validate_session
from ...core.exceptions import ValidationError
from ...core.utils import convert_to_json_serializable

logger = logging.getLogger(__name__)

# Create the debug routes blueprint
debug_bp = Blueprint('debug', __name__)


@debug_bp.route('/test_architecture_components', methods=['GET'])
@handle_errors
@log_execution_time
def test_architecture_components():
    """Test endpoint for architecture components health check."""
    try:
        # Check core components
        components = {
            'data_service': False,
            'analysis_service': False,
            'visualization_service': False,
            'request_interpreter': False,
            'production_deployment_manager': False,
            'conversation_router': False,
            'chatmrpt_agent': False,
            'langchain_chain': False,
            'reflection_engine': False
        }
        
        working_components = 0
        total_components = len(components)
        
        # Test each service
        services = current_app.services
        
        for component_name in components:
            try:
                component = getattr(services, component_name, None)
                if component is not None:
                    components[component_name] = True
                    working_components += 1
            except Exception as e:
                logger.warning(f"Component {component_name} failed: {e}")
                components[component_name] = False
        
        # Calculate health percentage
        health_percentage = (working_components / total_components) * 100
        
        return jsonify({
            'status': 'success',
            'overall_health': f'{health_percentage:.1f}%',
            'working_components': working_components,
            'total_components': total_components,
            'component_status': components,
            'timestamp': datetime.utcnow().isoformat()
        })
        
    except Exception as e:
        logger.error(f"Architecture components test failed: {e}")
        return jsonify({
            'status': 'error',
            'message': f'Architecture test failed: {str(e)}',
            'overall_health': '0%',
            'working_components': 0,
            'total_components': 9
        }), 500


@debug_bp.route('/debug/fix_analysis_state', methods=['POST'])
@validate_session
@handle_errors
@log_execution_time
def fix_analysis_state():
    """DEBUG ENDPOINT: Fix analysis completion state for current session"""
    try:
        # Get services from the container
        data_service = current_app.services.data_service
        
        # Get session ID
        session_id = session.get('session_id')
        
        # Get data handler to check if analysis was actually completed
        data_handler = data_service.get_handler(session_id)
        
        if not data_handler:
            return jsonify({
                'status': 'error',
                'message': 'No data available in this session'
            }), 400
        
        # Check if data handler has vulnerability rankings (indicates analysis was run)
        has_rankings = (hasattr(data_handler, 'vulnerability_rankings') and 
                       data_handler.vulnerability_rankings is not None)
        
        if has_rankings:
            # Analysis was actually completed, fix the session flag
            session['analysis_complete'] = True
            
            # Try to extract variables used
            if hasattr(data_handler, 'selected_variables') and data_handler.selected_variables:
                session['variables_used'] = data_handler.selected_variables
            else:
                # Try to get from available variables
                available_vars = data_handler.get_available_variables()
                session['variables_used'] = available_vars[:5] if available_vars else []
            
            return jsonify({
                'status': 'success',
                'message': 'Analysis state has been fixed! You can now generate reports.',
                'analysis_complete': True,
                'variables_used': session.get('variables_used', []),
                'rankings_shape': str(data_handler.vulnerability_rankings.shape) if has_rankings else None
            })
        else:
            return jsonify({
                'status': 'error',
                'message': 'No analysis results found. Please run the analysis first.',
                'has_data': True,
                'has_rankings': False
            }), 400
            
    except Exception as e:
        logger.error(f"Error fixing analysis state: {str(e)}", exc_info=True)
        return jsonify({
            'status': 'error',
            'message': f'Error fixing analysis state: {str(e)}'
        }), 500


@debug_bp.route('/debug/session_state', methods=['GET'])
@validate_session
@handle_errors
def debug_session_state():
    """DEBUG ENDPOINT: Check current session state"""
    try:
        session_id = session.get('session_id')
        
        # Get data handler to check actual analysis state
        data_service = current_app.services.data_service
        data_handler = data_service.get_handler(session_id) if data_service else None
        
        # Check if analysis results exist
        has_vulnerability_rankings = False
        rankings_info = None
        if data_handler:
            has_vulnerability_rankings = (hasattr(data_handler, 'vulnerability_rankings') and 
                                        data_handler.vulnerability_rankings is not None)
            if has_vulnerability_rankings:
                rankings_info = {
                    'shape': str(data_handler.vulnerability_rankings.shape),
                    'columns': list(data_handler.vulnerability_rankings.columns)
                }
        
        session_state = {
            'session_id': session_id,
            'analysis_complete': session.get('analysis_complete', False),
            'variables_used': session.get('variables_used', []),
            'csv_loaded': session.get('csv_loaded', False),
            'shapefile_loaded': session.get('shapefile_loaded', False),
            'data_loaded': session.get('data_loaded', False),
            'pending_action': session.get('pending_action'),
            'pending_variables': session.get('pending_variables'),
            'last_visualization': session.get('last_visualization'),
            'csv_filename': session.get('csv_filename', ''),
            'shapefile_filename': session.get('shapefile_filename', ''),
            'available_variables': session.get('available_variables', []),
            'has_data_handler': data_handler is not None,
            'has_vulnerability_rankings': has_vulnerability_rankings,
            'rankings_info': rankings_info
        }
        
        return jsonify({
            'status': 'success',
            'session_state': session_state,
            'timestamp': datetime.now().isoformat()
        })
        
    except Exception as e:
        logger.error(f"Error checking session state: {str(e)}", exc_info=True)
        return jsonify({
            'status': 'error',
            'message': f'Error checking session state: {str(e)}'
        }), 500


@debug_bp.route('/debug/visualization', methods=['GET'])
@validate_session
@handle_errors
def debug_visualization():
    """Debug route to check visualization state and pagination data"""
    try:
        # Get session ID
        session_id = session.get('session_id')
        
        # Get services from the container
        data_service = current_app.services.data_service
        
        # Get data handler via data service
        data_handler = data_service.get_handler(session_id)
        
        debug_info = {
            'session_id': session_id,
            'boxplot_data_available': hasattr(data_handler, 'boxwhisker_plot') and data_handler.boxwhisker_plot is not None,
            'composite_map_data_available': hasattr(data_handler, 'composite_scores') and data_handler.composite_scores is not None,
            'current_boxplot_page': session.get('current_boxplot_page', 1),
            'current_composite_map_page': session.get('current_composite_map_page', 1)
        }
        
        # If boxplot data is available, add more details
        if hasattr(data_handler, 'boxwhisker_plot') and data_handler.boxwhisker_plot:
            debug_info['boxplot_details'] = {
                'total_pages': data_handler.boxwhisker_plot.get('total_pages', 0),
                'current_page': data_handler.boxwhisker_plot.get('current_page', 1),
                'plots_count': len(data_handler.boxwhisker_plot.get('plots', [])),
                'has_page_data': 'page_data' in data_handler.boxwhisker_plot,
                'available_pages': list(data_handler.boxwhisker_plot.get('page_data', {}).keys())
            }
        
        # If composite scores data is available, add more details
        if hasattr(data_handler, 'composite_scores') and data_handler.composite_scores:
            import pandas as pd
            
            # FAIL CLEARLY if composite_scores is malformed
            if 'scores' not in data_handler.composite_scores:
                raise ValueError(f"Missing 'scores' key in composite_scores. Available keys: {list(data_handler.composite_scores.keys())}")
            
            model_columns = [col for col in data_handler.composite_scores['scores'].columns 
                            if col.startswith('model_')]
            
            debug_info['composite_details'] = {
                'model_count': len(model_columns),
                'has_formulas': 'formulas' in data_handler.composite_scores,
                'formulas_count': len(data_handler.composite_scores['formulas']) if 'formulas' in data_handler.composite_scores else 0,
                'models_per_page': 4,  # This is hard-coded in the original implementation
                'estimated_total_pages': (len(model_columns) + 4 - 1) // 4
            }
        
        return jsonify({
            'status': 'success',
            'debug_info': debug_info
        })
    except Exception as e:
        logger.error(f"Error in visualization debug: {str(e)}", exc_info=True)
        return jsonify({
            'status': 'error',
            'message': f'Error in visualization debug: {str(e)}'
        }), 500


@debug_bp.route('/debug/llm_service', methods=['GET'])
@validate_session
@handle_errors
def debug_llm_service():
    """
    Debug endpoint to check if the LLM service is functioning properly.
    This helps diagnose API connection issues.
    """
    try:
        # Get services from the container
        conversational_epidemiologist = current_app.services.conversational_epidemiologist
        
        # Check ConversationalEpidemiologist (NEW SYSTEM)
        if conversational_epidemiologist:
            try:
                # Test basic functionality
                connection_status = "Connected - ConversationalEpidemiologist active"
            except Exception as e:
                connection_status = f"Error testing ConversationalEpidemiologist: {str(e)}"
        else:
            connection_status = "ConversationalEpidemiologist not available"
        
        return jsonify({
            'status': 'success',
            'conversational_system_available': conversational_epidemiologist is not None,
            'connection_status': connection_status,
            'session_permanent': current_app.config.get('SESSION_PERMANENT', False),
            'session_lifetime': str(current_app.config.get('PERMANENT_SESSION_LIFETIME', 'Not set'))
        })
    except Exception as e:
        logger.error(f"Error checking LLM service: {str(e)}", exc_info=True)
        return jsonify({
            'status': 'error',
            'message': f'Error checking LLM service: {str(e)}'
        }), 500


# ========================================================================
# DEBUG UTILITY FUNCTIONS
# ========================================================================

def get_full_debug_info(session_id, data_handler=None):
    """
    Get comprehensive debug information for a session.
    
    Args:
        session_id: Session identifier
        data_handler: Data handler instance
        
    Returns:
        dict: Comprehensive debug information
    """
    debug_info = {
        'session_id': session_id,
        'timestamp': datetime.utcnow().isoformat(),
        'system_info': {},
        'data_info': {},
        'analysis_info': {},
        'service_info': {}
    }
    
    # System information
    try:
        import psutil
        debug_info['system_info'] = {
            'memory_usage': psutil.virtual_memory().percent,
            'cpu_usage': psutil.cpu_percent(),
            'disk_usage': psutil.disk_usage('/').percent if hasattr(psutil, 'disk_usage') else None
        }
    except ImportError:
        debug_info['system_info']['note'] = 'System monitoring not available'
    
    # Data information
    if data_handler:
        debug_info['data_info'] = {
            'has_df': hasattr(data_handler, 'df') and data_handler.df is not None,
            'has_shapefile': hasattr(data_handler, 'shapefile_data') and data_handler.shapefile_data is not None,
            'has_vulnerability_rankings': hasattr(data_handler, 'vulnerability_rankings') and data_handler.vulnerability_rankings is not None,
            'has_composite_scores': hasattr(data_handler, 'composite_scores') and data_handler.composite_scores is not None,
            'has_boxplot_data': hasattr(data_handler, 'boxwhisker_plot') and data_handler.boxwhisker_plot is not None
        }
        
        if hasattr(data_handler, 'df') and data_handler.df is not None:
            debug_info['data_info']['df_shape'] = data_handler.df.shape
            debug_info['data_info']['df_columns'] = list(data_handler.df.columns)
    
    # Analysis information
    if data_handler and hasattr(data_handler, 'vulnerability_rankings') and data_handler.vulnerability_rankings is not None:
        debug_info['analysis_info'] = {
            'rankings_shape': data_handler.vulnerability_rankings.shape,
            'rankings_columns': list(data_handler.vulnerability_rankings.columns),
            'analysis_complete': True
        }
    else:
        debug_info['analysis_info']['analysis_complete'] = False
    
    # Service information
    try:
        services = current_app.services
        debug_info['service_info'] = {
            'data_service_available': hasattr(services, 'data_service') and services.data_service is not None,
            'conversational_epidemiologist_available': hasattr(services, 'conversational_epidemiologist') and services.conversational_epidemiologist is not None,
            'analysis_service_available': hasattr(services, 'analysis_service') and services.analysis_service is not None,
            'visualization_service_available': hasattr(services, 'visualization_service') and services.visualization_service is not None,
            'report_service_available': hasattr(services, 'report_service') and services.report_service is not None
        }
    except Exception:
        debug_info['service_info']['note'] = 'Services not available'
    
    return debug_info


def validate_session_consistency(session, data_handler=None):
    """
    Validate consistency between session state and actual data.
    
    Args:
        session: Flask session object
        data_handler: Data handler instance
        
    Returns:
        dict: Validation results with inconsistencies
    """
    inconsistencies = []
    
    # Check CSV loaded flag
    csv_loaded_in_session = session.get('csv_loaded', False)
    csv_actually_loaded = data_handler and hasattr(data_handler, 'df') and data_handler.df is not None
    
    if csv_loaded_in_session != csv_actually_loaded:
        inconsistencies.append(f"CSV loaded mismatch: session={csv_loaded_in_session}, actual={csv_actually_loaded}")
    
    # Check shapefile loaded flag
    shapefile_loaded_in_session = session.get('shapefile_loaded', False)
    shapefile_actually_loaded = data_handler and hasattr(data_handler, 'shapefile_data') and data_handler.shapefile_data is not None
    
    if shapefile_loaded_in_session != shapefile_actually_loaded:
        inconsistencies.append(f"Shapefile loaded mismatch: session={shapefile_loaded_in_session}, actual={shapefile_actually_loaded}")
    
    # Check analysis complete flag
    analysis_complete_in_session = session.get('analysis_complete', False)
    analysis_actually_complete = (data_handler and 
                                hasattr(data_handler, 'vulnerability_rankings') and 
                                data_handler.vulnerability_rankings is not None)
    
    if analysis_complete_in_session != analysis_actually_complete:
        inconsistencies.append(f"Analysis complete mismatch: session={analysis_complete_in_session}, actual={analysis_actually_complete}")
    
    return {
        'is_consistent': len(inconsistencies) == 0,
        'inconsistencies': inconsistencies,
        'total_issues': len(inconsistencies)
    }


def fix_session_inconsistencies(session, data_handler=None):
    """
    Fix inconsistencies between session state and actual data.
    
    Args:
        session: Flask session object
        data_handler: Data handler instance
        
    Returns:
        dict: Fix results
    """
    fixes_applied = []
    
    if not data_handler:
        return {'fixes_applied': [], 'message': 'No data handler available'}
    
    # Fix CSV loaded flag
    csv_actually_loaded = hasattr(data_handler, 'df') and data_handler.df is not None
    if session.get('csv_loaded', False) != csv_actually_loaded:
        session['csv_loaded'] = csv_actually_loaded
        fixes_applied.append(f"Set csv_loaded to {csv_actually_loaded}")
    
    # Fix shapefile loaded flag
    shapefile_actually_loaded = hasattr(data_handler, 'shapefile_data') and data_handler.shapefile_data is not None
    if session.get('shapefile_loaded', False) != shapefile_actually_loaded:
        session['shapefile_loaded'] = shapefile_actually_loaded
        fixes_applied.append(f"Set shapefile_loaded to {shapefile_actually_loaded}")
    
    # Fix analysis complete flag
    analysis_actually_complete = (hasattr(data_handler, 'vulnerability_rankings') and 
                                data_handler.vulnerability_rankings is not None)
    if session.get('analysis_complete', False) != analysis_actually_complete:
        session['analysis_complete'] = analysis_actually_complete
        fixes_applied.append(f"Set analysis_complete to {analysis_actually_complete}")
        
        # If analysis is complete, try to set variables used
        if analysis_actually_complete and not session.get('variables_used'):
            if hasattr(data_handler, 'selected_variables') and data_handler.selected_variables:
                session['variables_used'] = data_handler.selected_variables
                fixes_applied.append(f"Set variables_used from data_handler")
    
    # Fix data_loaded flag
    data_loaded = csv_actually_loaded and shapefile_actually_loaded
    if session.get('data_loaded', False) != data_loaded:
        session['data_loaded'] = data_loaded
        fixes_applied.append(f"Set data_loaded to {data_loaded}")
    
    return {
        'fixes_applied': fixes_applied,
        'total_fixes': len(fixes_applied),
        'message': f"Applied {len(fixes_applied)} fixes" if fixes_applied else "No fixes needed"
    } 