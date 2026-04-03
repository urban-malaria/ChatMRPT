# app/web/routes/visualization_routes.py
"""
Visualization Routes module for visualization and media operations.

This module contains the visualization-related routes for the ChatMRPT web application:
- Visualization generation (get_visualization)
- Navigation for composite maps and boxplots
- File serving for visualization assets
- Generic visualization navigation
"""

import json
import os
import logging
import time
import traceback
import zipfile
from datetime import datetime

import geopandas as gpd
import pandas as pd
from typing import List, Optional, Tuple
from flask import Blueprint, session, request, current_app, jsonify, send_from_directory
from app.auth.decorators import require_auth

from app.utils.decorators import handle_errors, log_execution_time, validate_session
from app.utils.exceptions import ValidationError
from app.utils.core_utils import convert_to_json_serializable
from ...services.universal_viz_explainer import get_universal_viz_explainer
from ...analysis.itn_pipeline import generate_itn_map
from ...tools.variable_distribution import VariableDistribution

logger = logging.getLogger(__name__)

# Create the visualization routes blueprint
viz_bp = Blueprint('visualization', __name__)


@viz_bp.route('/get_visualization', methods=['POST'])
@require_auth
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
@require_auth
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
@require_auth
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
@require_auth
@validate_session
@handle_errors
@log_execution_time
def serve_viz_file(session_id, filename):
    """Serve visualization files for a session"""
    try:
        # Construct the file path (hardened)
        from pathlib import Path
        uploads_root = Path(current_app.config['UPLOAD_FOLDER']).resolve()
        session_folder = (uploads_root / session_id).resolve()
        file_path = (session_folder / filename).resolve()

        # Security check - ensure file exists and is inside the session folder
        try:
            inside_session = str(file_path).startswith(str(session_folder) + os.path.sep)
        except Exception:
            inside_session = False

        if (not file_path.exists()) or (not inside_session):
            return jsonify({
                'status': 'error',
                'message': 'Visualization file not found'
            }), 404
        
        # Optional: validate session ID matches current Flask session if it exists
        # This maintains backward compatibility while allowing data analysis V3 to work
        current_session_id = session.get('session_id')
        if current_session_id and current_session_id != session_id:
            # Only enforce session validation if Flask session exists
            # For data analysis V3, file existence is the authorization
            logger.warning(f"Session mismatch but file exists: Flask={current_session_id}, URL={session_id}")
            # Allow access if file exists (data analysis V3 pattern)
        
        # Serve file with safer path handling
        rel_path = str(file_path.relative_to(session_folder))
        response = send_from_directory(str(session_folder), rel_path)

        # Set a more restrictive CSP while allowing necessary external assets (Plotly/CDNs)
        # Adjust as needed if tiles/CDNs require additional origins
        csp = (
            "default-src 'self'; "
            "script-src 'self' 'unsafe-inline' 'unsafe-eval' blob: https://cdn.plot.ly https://*.plot.ly; "
            "worker-src 'self' blob:; "
            "style-src 'self' 'unsafe-inline' https:; "
            "img-src 'self' data: blob: https:; "
            "connect-src 'self' https:; "
            "font-src 'self' https:; "
            "frame-ancestors 'self'; "
            "object-src 'none'"
        )
        # Remove any existing CSP and apply our header
        try:
            response.headers.pop('Content-Security-Policy', None)
        except Exception:
            pass
        response.headers['Content-Security-Policy'] = csp
        return response
    
    except Exception as e:
        logger.error(f"Error serving visualization file: {str(e)}", exc_info=True)
        return jsonify({
            'status': 'error',
            'message': 'Error serving visualization file'
        }), 500


@viz_bp.route('/visualization/rerender', methods=['POST'])
@require_auth
@validate_session
@handle_errors
def rerender_visualization():
    """Re-render a visualization with updated geographic level or filters."""
    payload = request.get_json(silent=True) or {}
    viz_type = payload.get('viz_type')
    if not viz_type:
        return jsonify({'status': 'error', 'message': 'viz_type is required'}), 400

    geographic_level = (payload.get('geographic_level') or 'ward').lower()
    selected_lgas = [str(code) for code in (payload.get('selected_lgas') or []) if code is not None]
    session_id = session.get('session_id') or payload.get('session_id')
    if not session_id:
        return jsonify({'status': 'error', 'message': 'Session not available'}), 400

    if viz_type == 'variable_distribution':
        viz_params = payload.get('viz_params') or {}
        variable_name = viz_params.get('variable_name')
        if not variable_name:
            return jsonify({'status': 'error', 'message': 'variable_name missing for variable distribution'}), 400
        tool = VariableDistribution(
            variable_name=variable_name,
            geographic_level=geographic_level,
            selected_lgas=selected_lgas,
        )
        result = tool.execute(session_id=session_id)
        if not result.success:
            return jsonify({'status': 'error', 'message': result.message}), 500
        web_path = (result.data or {}).get('web_path') or result.web_path
        return jsonify({'status': 'success', 'web_path': web_path})

    if viz_type == 'itn_distribution':
        web_path, error_message = _rerender_itn_map(
            session_id=session_id,
            geographic_level=geographic_level,
            selected_lgas=selected_lgas,
        )
        if error_message:
            return jsonify({'status': 'error', 'message': error_message}), 400
        return jsonify({'status': 'success', 'web_path': web_path})

    return jsonify({'status': 'error', 'message': f'Unsupported visualization type: {viz_type}'}), 400


# Legacy compatibility for pre/post survey map link without session ID
@viz_bp.route('/serve_viz_file/vulnerability_map_composite.html')
def serve_vulnerability_map_legacy():
    """Serve static vulnerability map for legacy link without requiring auth or session.

    This supports links like /serve_viz_file/vulnerability_map_composite.html used in pre/post survey.
    """
    try:
        import os
        viz_dir = os.path.join(current_app.root_path, 'static', 'visualizations')
        filename = 'vulnerability_map_composite.html'
        return send_from_directory(viz_dir, filename, mimetype='text/html')
    except Exception as e:
        logger.error(f"Error serving legacy vulnerability map: {e}")
        return jsonify({'error': 'Visualization not available'}), 404


@viz_bp.route('/explain_readiness', methods=['GET'])
@require_auth
@validate_session
def explain_readiness():
    """Report availability of visualization explanation backends (kaleido/html2image/playwright/selenium/wkhtmltoimage)."""
    import shutil
    readiness = {
        'kaleido': False,
        'html2image': False,
        'playwright': False,
        'selenium': False,
        'wkhtmltoimage': False
    }
    # Module checks
    try:
        import kaleido  # noqa: F401
        readiness['kaleido'] = True
    except Exception:
        readiness['kaleido'] = False
    try:
        import html2image  # noqa: F401
        readiness['html2image'] = True
    except Exception:
        readiness['html2image'] = False
    try:
        import playwright  # noqa: F401
        readiness['playwright'] = True
    except Exception:
        readiness['playwright'] = False
    try:
        import selenium  # noqa: F401
        readiness['selenium'] = True
    except Exception:
        readiness['selenium'] = False
    # Binary checks
    readiness['wkhtmltoimage'] = shutil.which('wkhtmltoimage') is not None

    return jsonify({'status': 'success', 'readiness': readiness})


@viz_bp.route('/navigate_visualization', methods=['POST'])
@require_auth
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


def _rerender_itn_map(session_id: str, geographic_level: str, selected_lgas: List[str]) -> tuple[Optional[str], Optional[str]]:
    upload_dir = os.path.join(current_app.config['UPLOAD_FOLDER'], session_id)
    results_path = os.path.join(upload_dir, 'itn_distribution_results.json')
    if not os.path.exists(results_path):
        return None, 'ITN distribution results not found for this session.'

    try:
        with open(results_path, 'r') as handle:
            saved_results = json.load(handle)
    except Exception as exc:
        logger.error(f"Failed to read ITN results: {exc}")
        return None, 'Unable to load ITN distribution results.'

    prioritized_records = saved_results.get('prioritized', [])
    reprioritized_records = saved_results.get('reprioritized', [])
    prioritized_df = pd.DataFrame(prioritized_records)
    reprioritized_df = pd.DataFrame(reprioritized_records)

    if prioritized_df.empty and reprioritized_df.empty:
        return None, 'No ITN allocation data available to render a map.'

    stats = saved_results.get('stats', {})
    total_nets = saved_results.get('total_nets', stats.get('total_nets', 0))
    avg_household_size = saved_results.get('avg_household_size', 5.0)
    method = saved_results.get('method', 'composite')
    urban_threshold = saved_results.get('urban_threshold', 75.0)

    data_service = getattr(current_app.services, 'data_service', None)
    data_handler = data_service.get_handler(session_id) if data_service else None
    shp_data = getattr(data_handler, 'shapefile_data', None) if data_handler else None
    rankings = getattr(data_handler, 'rankings', None) if data_handler else None

    if shp_data is None:
        shp_data = _load_session_shapefile(upload_dir)
    if shp_data is None:
        return None, 'Unable to load shapefile data for the session.'

    if rankings is None or (hasattr(rankings, 'empty') and rankings.empty):
        rankings = _load_session_rankings(upload_dir)

    try:
        web_path = generate_itn_map(
            shp_data=shp_data,
            prioritized=prioritized_df,
            reprioritized=reprioritized_df,
            rankings=rankings,
            session_id=session_id,
            urban_threshold=urban_threshold,
            total_nets=total_nets,
            avg_household_size=avg_household_size,
            method=method,
            stats=stats,
            geographic_level=geographic_level,
            selected_lgas=selected_lgas,
        )
        return web_path, None
    except Exception as exc:
        logger.error(f"Failed to re-render ITN map: {exc}")
        return None, 'Unable to regenerate the ITN map with the requested settings.'


def _load_session_shapefile(upload_dir: str) -> Optional[gpd.GeoDataFrame]:
    shapefile_dir = os.path.join(upload_dir, 'shapefile')
    os.makedirs(shapefile_dir, exist_ok=True)
    shp_files = [f for f in os.listdir(shapefile_dir) if f.endswith('.shp')]
    if not shp_files:
        zip_path = os.path.join(upload_dir, 'raw_shapefile.zip')
        if os.path.exists(zip_path):
            try:
                with zipfile.ZipFile(zip_path, 'r') as zip_ref:
                    zip_ref.extractall(shapefile_dir)
                shp_files = [f for f in os.listdir(shapefile_dir) if f.endswith('.shp')]
            except Exception as exc:
                logger.error(f"Failed to extract shapefile zip: {exc}")
                return None
    if not shp_files:
        return None
    try:
        return gpd.read_file(os.path.join(shapefile_dir, shp_files[0]))
    except Exception as exc:
        logger.error(f"Failed to load shapefile: {exc}")
        return None


def _load_session_rankings(upload_dir: str) -> Optional[pd.DataFrame]:
    csv_path = os.path.join(upload_dir, 'unified_dataset.csv')
    if not os.path.exists(csv_path):
        return None
    try:
        return pd.read_csv(csv_path)
    except Exception as exc:
        logger.error(f"Failed to load unified dataset for rankings: {exc}")
        return None


@viz_bp.route('/explain_visualization', methods=['POST'])
@require_auth
@validate_session
@handle_errors
@log_execution_time
def explain_visualization():
    """Handle visualization explanation requests using AI vision"""
    try:
        data = request.json
        viz_url = data.get('viz_url')  # New: handle URL from explain button
        viz_path = data.get('viz_path')  # New: relative path
        visualization_path = data.get('visualization_path')  # Legacy support
        base64_data = data.get('base64_data')
        title = data.get('title', 'Visualization')
        viz_type = data.get('viz_type', 'unknown')

        # Get session ID - try multiple sources
        session_id = session.get('session_id') or data.get('session_id')

        logger.info(f"Explain visualization called with viz_path: {viz_path}, session_id: {session_id}")

        # Get the universal visualization explainer service
        from app.visualization.explainer import get_universal_viz_explainer

        # Get LLM manager from services container
        llm_manager = current_app.services.llm_manager

        # Create explainer instance
        explainer = get_universal_viz_explainer(llm_manager=llm_manager)

        # LOG ALL INCOMING DATA
        logger.info(f"=== EXPLAIN VISUALIZATION REQUEST ===")
        logger.info(f"viz_path received: {viz_path}")
        logger.info(f"visualization_path received: {visualization_path}")
        logger.info(f"viz_url received: {viz_url}")
        logger.info(f"session_id: {session_id}")

        # CRITICAL: Check if viz_path is actually being used as the primary path
        # The frontend sends viz_path, NOT visualization_path!
        if not visualization_path and viz_path:
            logger.info(f"Entering viz_path construction block")
            # Handle different types of viz_path
            import os

            # Check if it's a pickle URL from Data Analysis V3
            if viz_path.startswith('/images/plotly_figures/pickle/'):
                # Extract the pickle filename from the URL
                pickle_filename = viz_path.split('/')[-1]  # Get filename like "xxx.pickle"

                if not session_id:
                    logger.error(f"No session_id available for pickle file: {pickle_filename}")
                    return jsonify({
                        'status': 'error',
                        'message': 'Session ID required for pickle file explanations'
                    }), 400

                # Construct ABSOLUTE path to the pickle file in session uploads/visualizations subdirectory
                visualization_path = os.path.join(current_app.instance_path, 'uploads', session_id, 'visualizations', pickle_filename)
                logger.info(f"Constructed pickle path: {visualization_path}")
                logger.info(f"File exists at constructed path: {os.path.exists(visualization_path)}")
            elif viz_path.startswith('/static/visualizations/'):
                # Handle static HTML visualizations from Data Analysis V3
                # These are in the app/static/visualizations directory
                viz_filename = viz_path.split('/')[-1]  # Get filename like "data_analysis_xxx.html"
                visualization_path = os.path.join(current_app.root_path, 'static/visualizations', viz_filename)
            elif viz_path.startswith('/serve_viz_file/'):
                # Handle served session visualization paths:
                #   /serve_viz_file/<session_id>/visualizations/<file>
                #   /serve_viz_file/<session_id>/<file>  (e.g. tpr_distribution_map.html)
                try:
                    parts = viz_path.strip('/').split('/')
                    # parts[0] = 'serve_viz_file', parts[1] = session_id, rest = relative path
                    if len(parts) >= 3 and parts[0] == 'serve_viz_file':
                        sid = parts[1]
                        rel_path = '/'.join(parts[2:])
                        visualization_path = os.path.join(
                            current_app.instance_path, 'uploads', sid, rel_path
                        )
                        logger.info(f"Resolved serve_viz_file path to: {visualization_path}")
                    elif session_id:
                        filename = parts[-1]
                        visualization_path = os.path.join(
                            current_app.instance_path, 'uploads', session_id, filename
                        )
                        logger.info(f"Resolved serve_viz_file path using request session_id to: {visualization_path}")
                except Exception as e:
                    logger.warning(f"Failed to resolve serve_viz_file path: {e}")
            else:
                # For other paths, construct from viz_path
                visualization_path = os.path.join(current_app.instance_path, 'uploads', viz_path)
        
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
            logger.info(f"Entering visualization_path block with path: {visualization_path}")
            # For file-based visualizations, construct full path
            import os

            # Normalize and safely join to avoid duplicating the session ID
            if not os.path.isabs(visualization_path):
                uploads_root = current_app.config['UPLOAD_FOLDER']  # instance/uploads
                # Strip any leading slashes to ensure consistent joining
                vis_rel = visualization_path.lstrip('/')
                # If path already begins with the session_id, join from uploads_root
                if session_id and (vis_rel.startswith(f"{session_id}/") or vis_rel == session_id):
                    full_path = os.path.join(uploads_root, vis_rel)
                else:
                    # Otherwise, treat it as relative to this session's folder
                    full_path = os.path.join(uploads_root, session_id, vis_rel)
            else:
                full_path = visualization_path

            # Debug logging
            logger.info(f"=== CALLING EXPLAINER ===")
            logger.info(f"Full path for explanation: {full_path}")
            logger.info(f"File exists: {os.path.exists(full_path)}")
            logger.info(f"viz_type: {viz_type}")

            # Get AI explanation
            explanation = explainer.explain_visualization(
                viz_path=full_path,
                viz_type=viz_type,
                session_id=session_id
            )
            logger.info(f"Explanation result: {explanation[:100] if explanation else 'None'}")
        else:
            logger.error(f"No visualization_path or base64_data available!")
            logger.error(f"viz_path: {viz_path}, visualization_path: {visualization_path}, base64_data: {bool(base64_data)}")

            # FALLBACK: Try to use viz_path directly if it's a pickle URL
            if viz_path and viz_path.startswith('/images/plotly_figures/pickle/'):
                logger.warning("FALLBACK: Using viz_path directly for pickle file")
                pickle_filename = viz_path.split('/')[-1]

                if session_id:
                    import os
                    full_path = os.path.join(current_app.instance_path, 'uploads', session_id, 'visualizations', pickle_filename)
                    logger.info(f"FALLBACK path: {full_path}, exists: {os.path.exists(full_path)}")

                    explanation = explainer.explain_visualization(
                        viz_path=full_path,
                        viz_type=viz_type,
                        session_id=session_id
                    )
                else:
                    return jsonify({
                        'status': 'error',
                        'message': 'Session ID required for pickle file explanations'
                    }), 400
            else:
                return jsonify({
                    'status': 'error',
                    'message': 'Either visualization_path or base64_data must be provided'
                }), 400
        
        # If explainer returned an explicit error string, surface it as an error response
        if isinstance(explanation, str) and explanation.strip().upper().startswith('ERROR:'):
            return jsonify({
                'status': 'error',
                'message': explanation.replace('ERROR:', '').strip() or 'Explanation failed'
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
