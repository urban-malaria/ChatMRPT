"""ITN Distribution Routes."""
from flask import Blueprint, request, jsonify, session, send_from_directory, current_app
from app.analysis.itn_pipeline import calculate_itn_distribution
from app.services.data_handler import DataHandler
import logging
import os

logger = logging.getLogger(__name__)

itn_bp = Blueprint('itn', __name__, url_prefix='/api/itn')

@itn_bp.route('/update-distribution', methods=['POST'])
def update_itn_distribution():
    """Update ITN distribution with new threshold."""
    try:
        data = request.get_json()
        # Get session_id from multiple sources
        session_id = data.get('session_id') or session.get('session_id')
        
        if not session_id:
            return jsonify({'status': 'error', 'message': 'No active session'}), 400
        
        logger.info(f"ITN threshold update request for session {session_id}")
        
        # Try to retrieve stored ITN parameters
        stored_params = None
        try:
            # Try Redis first
            from app.services.redis_state import get_redis_state_manager
            redis_manager = get_redis_state_manager()
            stored_params = redis_manager.get_custom_data(session_id, 'itn_parameters')
            if stored_params:
                logger.info(f"Retrieved ITN parameters from Redis: {stored_params}")
        except Exception as e:
            logger.debug(f"Could not get params from Redis: {e}")
            
        # Fall back to file-based storage
        if not stored_params:
            try:
                import json
                params_path = f"instance/uploads/{session_id}/itn_parameters.json"
                if os.path.exists(params_path):
                    with open(params_path, 'r') as f:
                        stored_params = json.load(f)
                    logger.info(f"Retrieved ITN parameters from file: {stored_params}")
            except Exception as e:
                logger.debug(f"Could not get params from file: {e}")
        
        # Get parameters - use stored values as defaults if available
        if stored_params:
            urban_threshold = float(data.get('urban_threshold', stored_params.get('urban_threshold', 30)))
            total_nets = int(data.get('total_nets', stored_params.get('total_nets', 10000)))
            avg_household_size = float(data.get('avg_household_size', stored_params.get('avg_household_size', 5.0)))
            method = data.get('method', stored_params.get('method', 'composite'))
        else:
            # Use provided values or defaults
            urban_threshold = float(data.get('urban_threshold', 30))
            total_nets = int(data.get('total_nets', 10000))
            avg_household_size = float(data.get('avg_household_size', 5.0))
            method = data.get('method', 'composite')
        
        logger.info(f"Using parameters: threshold={urban_threshold}, nets={total_nets}, household={avg_household_size}, method={method}")
        
        # Load data handler with proper session folder
        session_folder = os.path.join('instance', 'uploads', session_id)
        data_handler = DataHandler(session_folder)
        
        # Load existing analysis data
        data_handler._attempt_data_reload()
        
        # Load session state for variables and relationships
        data_handler.load_session_state()
        
        # Ensure we have the necessary data
        if data_handler.csv_data is None:
            return jsonify({'status': 'error', 'message': 'No analysis data found'}), 400
        
        # Recalculate distribution
        result = calculate_itn_distribution(
            data_handler,
            session_id=session_id,
            total_nets=total_nets,
            avg_household_size=avg_household_size,
            urban_threshold=urban_threshold,
            method=method
        )
        
        if result['status'] == 'success':
            return jsonify({
                'status': 'success',
                'map_path': result['map_path'],
                'stats': result['stats']
            })
        else:
            return jsonify(result), 400
            
    except Exception as e:
        logger.error(f"Error updating ITN distribution: {str(e)}")
        return jsonify({'status': 'error', 'message': str(e)}), 500


# Create a separate blueprint for the embed route (no URL prefix)
itn_embed_bp = Blueprint('itn_embed', __name__)

@itn_embed_bp.route('/itn_embed/<session_id>')
def serve_itn_embed(session_id):
    """Serve ITN map HTML file for embedding."""
    try:
        # This route is no longer used - ITN maps are served via /serve_viz_file
        # Redirect to the correct endpoint
        from flask import redirect
        # Find the latest ITN distribution map in the session folder
        session_dir = os.path.join('instance', 'uploads', session_id, 'visualizations')
        
        if not os.path.exists(session_dir):
            logger.error(f"Session directory not found: {session_dir}")
            return "<h1>Session not found</h1>", 404
            
        # Find ITN distribution map files
        itn_files = [f for f in os.listdir(session_dir) if f.startswith('itn_distribution_map_') and f.endswith('.html')]
        
        if not itn_files:
            logger.error(f"No ITN map files found in: {session_dir}")
            return "<h1>ITN map not found</h1>", 404
            
        # Get the most recent file
        itn_files.sort()
        latest_file = itn_files[-1]
        
        # Redirect to the correct serve_viz_file endpoint
        return redirect(f'/serve_viz_file/{session_id}/visualizations/{latest_file}')
        
    except Exception as e:
        logger.error(f"Error serving ITN embed: {str(e)}", exc_info=True)
        return f"<h1>Error: {str(e)}</h1>", 500