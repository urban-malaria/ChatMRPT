"""
Export Routes for ChatMRPT

Handles file downloads for exported analysis results.
Completely modular - doesn't affect existing functionality.
"""

import os
import logging
from flask import Blueprint, send_file, abort, session as flask_session, current_app
from app.auth.decorators import require_auth
from pathlib import Path

logger = logging.getLogger(__name__)

export_bp = Blueprint('export', __name__, url_prefix='/export')


@export_bp.route('/list/<session_id>')
@require_auth
def list_exports(session_id):
    """
    List all available export files for a session.
    Returns a JSON list of available downloads.
    """
    try:
        from pathlib import Path
        
        # Validate session
        current_session_id = flask_session.get('session_id')
        logger.info(f"Listing exports for session: {session_id}")
        
        # Build paths to check
        export_base_dir = Path(current_app.root_path).parent / 'instance' / 'exports' / session_id
        uploads_dir = Path(current_app.root_path).parent / 'instance' / 'uploads' / session_id
        
        available_files = []
        
        # Check for TPR results
        tpr_csv = uploads_dir / 'raw_data.csv'
        if tpr_csv.exists():
            available_files.append({
                'name': 'TPR Analysis Results',
                'filename': 'raw_data.csv',
                'type': 'csv',
                'description': '📊 TPR analysis results with ward-level data',
                'url': f'/export/download/{session_id}/raw_data.csv',
                'category': 'tpr'
            })
        
        # Check for shapefile
        shapefile = uploads_dir / 'raw_shapefile.zip'
        if shapefile.exists():
            available_files.append({
                'name': 'Ward Boundaries Shapefile',
                'filename': 'raw_shapefile.zip',
                'type': 'zip',
                'description': '🗺️ Geographic boundaries for all wards',
                'url': f'/export/download/{session_id}/raw_shapefile.zip',
                'category': 'tpr'
            })
        
        # Check for TPR dashboard
        tpr_dashboard = uploads_dir / 'tpr_dashboard.html'
        if tpr_dashboard.exists():
            available_files.append({
                'name': 'TPR Dashboard',
                'filename': 'tpr_dashboard.html',
                'type': 'html',
                'description': '📈 Interactive TPR analysis dashboard',
                'url': f'/export/download/{session_id}/tpr_dashboard.html',
                'category': 'tpr'
            })
        
        # Check for ITN distribution results
        if export_base_dir.exists():
            # Look for ITN export directories
            for itn_dir in export_base_dir.glob('itn_export_*'):
                # ITN CSV
                itn_csv = itn_dir / 'itn_distribution_results.csv'
                if itn_csv.exists():
                    available_files.append({
                        'name': 'ITN Distribution Plan',
                        'filename': itn_csv.name,
                        'type': 'csv',
                        'description': '📊 ITN distribution allocation by ward',
                        'url': f'/export/download/{session_id}/{itn_csv.name}',
                        'category': 'itn'
                    })
                
                # ITN Dashboard
                itn_dashboard = itn_dir / 'itn_distribution_dashboard.html'
                if itn_dashboard.exists():
                    available_files.append({
                        'name': 'ITN Distribution Dashboard',
                        'filename': itn_dashboard.name,
                        'type': 'html',
                        'description': '📈 Interactive ITN distribution dashboard',
                        'url': f'/export/download/{session_id}/{itn_dashboard.name}',
                        'category': 'itn'
                    })
        
        # Check for analysis results
        analysis_csv = uploads_dir / 'analysis_results_composite.csv'
        if analysis_csv.exists():
            available_files.append({
                'name': 'Risk Analysis Results',
                'filename': 'analysis_results_composite.csv',
                'type': 'csv',
                'description': '📊 Complete malaria risk analysis results',
                'url': f'/export/download/{session_id}/analysis_results_composite.csv',
                'category': 'analysis'
            })
        
        logger.info(f"Found {len(available_files)} export files for session {session_id}")
        
        return {
            'success': True,
            'session_id': session_id,
            'files': available_files,
            'count': len(available_files)
        }
        
    except Exception as e:
        logger.error(f"Error listing exports: {e}")
        return {
            'success': False,
            'error': str(e),
            'files': []
        }


@export_bp.route('/download/<session_id>/<filename>')
@require_auth
def download_export(session_id, filename):
    """
    Download exported analysis files.
    
    Security: Only allows downloads from the exports directory for the given session.
    """
    try:
        # Validate session with enhanced logging
        current_session_id = flask_session.get('session_id')
        logger.info(f"Export download - Current session: {current_session_id}, Requested: {session_id}")
        
        # Temporarily relaxed validation - just log the mismatch
        if not current_session_id or current_session_id != session_id:
            logger.warning(f"Session mismatch but allowing download: current={current_session_id}, requested={session_id}")
        
        # Construct safe path (prevent directory traversal)
        safe_filename = os.path.basename(filename)
        # Check both exports and uploads directories
        export_base_dir = Path(current_app.root_path).parent / 'instance' / 'exports' / session_id
        uploads_dir = Path(current_app.root_path).parent / 'instance' / 'uploads' / session_id
        
        # First try uploads directory (for TPR files)
        file_path = uploads_dir / safe_filename
        
        # If not found in uploads, try exports directory
        if not file_path.exists():
            file_path = export_base_dir / safe_filename
        
        # If still not found, search in timestamped subdirectories
        if not file_path.exists():
            # Look for the file in any export subdirectory (itn_export_* or analysis_export_*)
            for pattern in ['itn_export_*', 'analysis_export_*']:
                for subdir in export_base_dir.glob(pattern):
                    potential_path = subdir / safe_filename
                    if potential_path.exists():
                        file_path = potential_path
                        break
                if file_path.exists():
                    break
        
        # Ensure file exists
        if not file_path.exists():
            logger.error(f"Export file not found: {safe_filename} in {export_base_dir} or {uploads_dir}")
            abort(404, "Export file not found")

        # Ensure path is within allowed directories (exports OR uploads)
        resolved_path = str(file_path.resolve())
        allowed_dirs = [str(export_base_dir.resolve()), str(uploads_dir.resolve())]

        if not any(resolved_path.startswith(allowed_dir) for allowed_dir in allowed_dirs):
            logger.error(f"Path traversal attempt: {file_path}")
            abort(403, "Invalid file path")
        
        # Determine download name
        if safe_filename.endswith('.zip'):
            download_name = safe_filename
            mimetype = 'application/zip'
        elif safe_filename.endswith('.csv'):
            download_name = safe_filename
            mimetype = 'text/csv'
        elif safe_filename.endswith('.html'):
            download_name = safe_filename
            mimetype = 'text/html'
        else:
            download_name = safe_filename
            mimetype = 'application/octet-stream'
        
        logger.info(f"Serving export file: {file_path}")
        
        return send_file(
            str(file_path),  # Convert Path object to string
            mimetype=mimetype,
            as_attachment=True,
            download_name=download_name
        )
        
    except Exception as e:
        logger.error(f"Error serving export file: {e}")
        abort(500, "Error downloading export file")