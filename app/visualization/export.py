# app/visualization/export.py
import logging
import os
import numpy as np
import pandas as pd
import geopandas as gpd
from flask import current_app, session
from werkzeug.utils import secure_filename
from typing import Dict, List, Optional, Any, Union

# Set up logging
logger = logging.getLogger(__name__)


def ensure_wgs84_crs(gdf):
    """
    Ensure the GeoDataFrame is using WGS84 (EPSG:4326) CRS
    
    Args:
        gdf: GeoDataFrame to check/transform
        
    Returns:
        GeoDataFrame in WGS84 CRS
    """
    # Create a copy to avoid modifying the original
    gdf_copy = gdf.copy()
    
    # Check if the GeoDataFrame has a CRS
    if gdf_copy.crs is None:
        logger.warning("GeoDataFrame has no CRS. Assuming WGS84.")
        gdf_copy.set_crs(epsg=4326, inplace=True)
        return gdf_copy
    
    # Check if the CRS is already WGS84
    if gdf_copy.crs == "EPSG:4326" or gdf_copy.crs == 4326:
        return gdf_copy
    
    try:
        # Log the transformation
        logger.info("Transforming GeoDataFrame from {} to WGS84 (EPSG:4326)".format(gdf_copy.crs))
        
        # Reproject to WGS84
        gdf_copy = gdf_copy.to_crs(epsg=4326)
        return gdf_copy
    except Exception as e:
        logger.error("Error transforming CRS: {}".format(str(e)))
        # Return original if transformation fails
        return gdf


def prepare_geodataframe_for_json(gdf):
    """
    Prepare a GeoDataFrame for JSON serialization by converting non-serializable types
    
    Args:
        gdf: GeoDataFrame to prepare
        
    Returns:
        GeoDataFrame with serializable types
    """
    # Create a copy to avoid modifying the original
    gdf_copy = gdf.copy()
    
    # Convert any datetime/timestamp columns to strings
    for col in gdf_copy.columns:
        if pd.api.types.is_datetime64_any_dtype(gdf_copy[col]):
            gdf_copy[col] = gdf_copy[col].astype(str)
    
    return gdf_copy


def create_plotly_html(fig, filename, include_plotlyjs='cdn', config=None):
    """
    Convert plotly figure to HTML file, saving to the INSTANCE path's upload folder.

    Args:
        fig: Plotly figure object
        filename: Desired output filename (will be secured)
        include_plotlyjs: How to include plotly.js ('cdn' or True for full)
        config: Optional configuration dictionary for Plotly

    Returns:
        str: Web-accessible path using the /serve_viz_file/ route, or None on error.
    """
    if not filename:
        # Generate a random filename if none provided
        safe_filename = "plotly_{}.html".format(np.random.randint(1000000))
    else:
        # Ensure the provided filename is web-safe and has .html extension
        safe_filename = secure_filename(filename)
        if not safe_filename.endswith('.html'):
             safe_filename += '.html'

    session_id = session.get('session_id', 'default')

    # --- THIS IS THE CRUCIAL PART ---
    # Get the UPLOAD_FOLDER path (which should point to instance/uploads) from the app config
    upload_dir = current_app.config.get('UPLOAD_FOLDER')
    if not upload_dir:
        logger.error("UPLOAD_FOLDER not configured in Flask app config.")
        return None # Cannot save without upload folder config

    # Define the specific session folder path ON DISK within the configured UPLOAD_FOLDER
    session_folder_disk = os.path.join(upload_dir, session_id)
    # ================================

    # Ensure the target directory exists
    try:
        os.makedirs(session_folder_disk, exist_ok=True)
    except OSError as e:
        logger.error("Could not create session upload directory {}: {}".format(session_folder_disk, e))
        return None

    # Define the full path to the file on disk
    file_path_disk = os.path.join(session_folder_disk, safe_filename)

    # Use provided config or default to responsive options
    if config is None:
        config = {
            'responsive': True,
            'displayModeBar': True,
            'scrollZoom': True,
            'fillFrame': True  # Add fillFrame to maximize plot area
        }

    # Write HTML file to the correct disk path
    try:
        fig.write_html(file_path_disk, include_plotlyjs=include_plotlyjs, full_html=True, config=config)
        logger.info("Successfully saved visualization to disk: {}".format(file_path_disk))
    except Exception as e:
        logger.error("Failed to write Plotly HTML to {}: {}".format(file_path_disk, e))
        return None # Indicate failure

    # Return the web-accessible path using the dedicated route /serve_viz_file/
    # This URL tells the browser where to REQUEST the file from the server.
    web_path = "/serve_viz_file/{}/{}".format(session_id, safe_filename)
    logger.info("Returning web path for visualization: {}".format(web_path))
    return web_path


def create_secure_filename(filename, session_id=None):
    """
    Create a secure filename for visualizations with optional session ID
    
    Args:
        filename: Base filename
        session_id: Optional session identifier
        
    Returns:
        str: Secure filename
    """
    if not filename:
        # Generate a random filename if none provided
        filename = "viz_{}".format(np.random.randint(1000000))
    
    # Secure the filename
    safe_filename = secure_filename(filename)
    
    # Add session prefix if provided
    if session_id:
        safe_filename = "{}_{}_{}".format(session_id, 
                                         int(np.random.random() * 10000), 
                                         safe_filename)
    
    # Ensure .html extension
    if not safe_filename.endswith('.html'):
        safe_filename += '.html'
    
    return safe_filename


def get_visualization_file_path(filename, session_id=None):
    """
    Get the full file path for a visualization file
    
    Args:
        filename: Visualization filename
        session_id: Session identifier
        
    Returns:
        str: Full file path or None if upload folder not configured
    """
    upload_dir = current_app.config.get('UPLOAD_FOLDER')
    if not upload_dir:
        logger.error("UPLOAD_FOLDER not configured in Flask app config.")
        return None
    
    if session_id:
        session_folder = os.path.join(upload_dir, session_id)
        # Ensure session directory exists
        try:
            os.makedirs(session_folder, exist_ok=True)
        except OSError as e:
            logger.error("Could not create session directory {}: {}".format(session_folder, e))
            return None
        return os.path.join(session_folder, filename)
    else:
        return os.path.join(upload_dir, filename)


def get_web_accessible_path(filename, session_id=None):
    """
    Get the web-accessible path for a visualization file
    
    Args:
        filename: Visualization filename
        session_id: Session identifier
        
    Returns:
        str: Web-accessible path
    """
    if session_id:
        return "/serve_viz_file/{}/{}".format(session_id, filename)
    else:
        return "/serve_viz_file/{}".format(filename)


def cleanup_old_visualizations(session_id, max_files=20):
    """
    Clean up old visualization files to prevent disk space issues
    
    Args:
        session_id: Session identifier
        max_files: Maximum number of files to keep per session
        
    Returns:
        int: Number of files cleaned up
    """
    upload_dir = current_app.config.get('UPLOAD_FOLDER')
    if not upload_dir:
        return 0
    
    session_folder = os.path.join(upload_dir, session_id)
    if not os.path.exists(session_folder):
        return 0
    
    try:
        # Get all HTML files in the session folder
        files = []
        for filename in os.listdir(session_folder):
            if filename.endswith('.html'):
                filepath = os.path.join(session_folder, filename)
                # Get file modification time
                mtime = os.path.getmtime(filepath)
                files.append((mtime, filepath))
        
        # Sort by modification time (oldest first)
        files.sort()
        
        # Remove oldest files if we exceed max_files
        cleanup_count = 0
        while len(files) > max_files:
            _, filepath = files.pop(0)
            try:
                os.remove(filepath)
                cleanup_count += 1
                logger.info("Cleaned up old visualization file: {}".format(filepath))
            except OSError as e:
                logger.warning("Could not remove file {}: {}".format(filepath, e))
        
        return cleanup_count
        
    except Exception as e:
        logger.error("Error during visualization cleanup: {}".format(e))
        return 0


def validate_plotly_figure(fig):
    """
    Validate that a figure is a valid Plotly figure object
    
    Args:
        fig: Object to validate
        
    Returns:
        bool: True if valid Plotly figure, False otherwise
    """
    try:
        # Check if it has the basic attributes of a Plotly figure
        if hasattr(fig, 'data') and hasattr(fig, 'layout'):
            return True
        
        # Check if it's a Plotly graph object
        import plotly.graph_objects as go
        if isinstance(fig, go.Figure):
            return True
            
        return False
    except Exception:
        return False


def get_export_summary(session_id, file_count_limit=50):
    """
    Get a summary of exported visualizations for a session
    
    Args:
        session_id: Session identifier
        file_count_limit: Maximum number of files to report
        
    Returns:
        dict: Export summary with file information
    """
    upload_dir = current_app.config.get('UPLOAD_FOLDER')
    if not upload_dir:
        return {
            'status': 'error',
            'message': 'Upload folder not configured'
        }
    
    session_folder = os.path.join(upload_dir, session_id)
    if not os.path.exists(session_folder):
        return {
            'status': 'success',
            'file_count': 0,
            'files': [],
            'total_size_mb': 0
        }
    
    try:
        files = []
        total_size = 0
        
        for filename in os.listdir(session_folder):
            if filename.endswith('.html'):
                filepath = os.path.join(session_folder, filename)
                try:
                    stat = os.stat(filepath)
                    file_info = {
                        'filename': filename,
                        'size_bytes': stat.st_size,
                        'modified_time': stat.st_mtime,
                        'web_path': get_web_accessible_path(filename, session_id)
                    }
                    files.append(file_info)
                    total_size += stat.st_size
                except OSError:
                    continue
        
        # Sort by modification time (newest first)
        files.sort(key=lambda x: x['modified_time'], reverse=True)
        
        # Limit the number of files returned
        if len(files) > file_count_limit:
            files = files[:file_count_limit]
        
        return {
            'status': 'success',
            'file_count': len(files),
            'files': files,
            'total_size_mb': round(total_size / (1024 * 1024), 2)
        }
        
    except Exception as e:
        logger.error("Error getting export summary: {}".format(e))
        return {
            'status': 'error',
            'message': "Error retrieving export summary: {}".format(str(e))
        } 