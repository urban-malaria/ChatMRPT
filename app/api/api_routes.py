"""
API Routes for React Frontend
Provides all necessary JSON endpoints for the React application
"""

import os
import uuid
import json
import traceback
from datetime import datetime
from flask import Blueprint, jsonify, request, session, current_app, send_file, g
from werkzeug.utils import secure_filename
import pandas as pd
from app.auth.decorators import require_auth

api_bp = Blueprint('api', __name__, url_prefix='/api')

# File upload configuration
ALLOWED_EXTENSIONS = {'csv', 'xlsx', 'xls', 'zip', 'shp', 'geojson'}
MAX_FILE_SIZE = 32 * 1024 * 1024  # 32MB

def allowed_file(filename):
    """Check if file extension is allowed."""
    return '.' in filename and filename.rsplit('.', 1)[1].lower() in ALLOWED_EXTENSIONS


@api_bp.route('/health', methods=['GET'])
def api_health():
    """API health check endpoint."""
    return jsonify({
        'status': 'healthy',
        'timestamp': datetime.utcnow().isoformat(),
        'service': 'ChatMRPT API',
        'version': '2.0.0'
    }), 200


@api_bp.route('/session/start', methods=['POST'])
@require_auth
def start_session():
    """Initialize a new session for the user."""
    try:
        # Create new session ID if not exists
        if 'session_id' not in session:
            session_id = str(uuid.uuid4())
            session['session_id'] = session_id
            session['base_session_id'] = session_id
            session['created_at'] = datetime.utcnow().isoformat()
        else:
            session_id = session['session_id']
            if 'base_session_id' not in session:
                session['base_session_id'] = session_id.split('__', 1)[0] if '__' in session_id else session_id

        if getattr(g, 'conversation_id', None):
            base_session_id = session.get('base_session_id', session_id)
            composite = f"{base_session_id}__{g.conversation_id}"
            session['session_id'] = composite
            session_id = composite

        # Initialize session data
        session['conversation_history'] = []
        session['data_loaded'] = False
        session['analysis_complete'] = False
        session['csv_loaded'] = False
        session['shapefile_loaded'] = False

        # Create session directory
        session_dir = os.path.join(current_app.config.get('UPLOAD_FOLDER', 'instance/uploads'), session_id)
        os.makedirs(session_dir, exist_ok=True)

        return jsonify({
            'success': True,
            'session_id': session_id,
            'message': 'Session initialized successfully',
            'data': {
                'session_id': session_id,
                'created_at': session.get('created_at'),
                'status': 'active'
            }
        }), 201

    except Exception as e:
        return jsonify({
            'success': False,
            'error': str(e),
            'message': 'Failed to initialize session'
        }), 500


@api_bp.route('/session/status', methods=['GET'])
@require_auth
def session_status():
    """Get current session status."""
    if 'session_id' not in session:
        return jsonify({
            'success': False,
            'message': 'No active session',
            'authenticated': False
        }), 401

    return jsonify({
        'success': True,
        'session_id': session.get('session_id'),
        'data_loaded': session.get('data_loaded', False),
        'analysis_complete': session.get('analysis_complete', False),
        'csv_loaded': session.get('csv_loaded', False),
        'shapefile_loaded': session.get('shapefile_loaded', False),
        'created_at': session.get('created_at'),
        'authenticated': True
    }), 200


@api_bp.route('/upload', methods=['POST', 'OPTIONS'])
@api_bp.route('/upload/csv', methods=['POST', 'OPTIONS'])
@api_bp.route('/upload/shapefile', methods=['POST', 'OPTIONS'])
@require_auth
def upload_file():
    """Handle file uploads from React frontend."""
    if request.method == 'OPTIONS':
        # Handle preflight request
        return '', 204

    try:
        # Check session
        if 'session_id' not in session:
            return jsonify({
                'success': False,
                'message': 'No active session. Please start a session first.'
            }), 401

        session_id = session['session_id']

        # Check if file is present
        if 'file' not in request.files:
            return jsonify({
                'success': False,
                'message': 'No file provided'
            }), 400

        file = request.files['file']

        if file.filename == '':
            return jsonify({
                'success': False,
                'message': 'No file selected'
            }), 400

        if not allowed_file(file.filename):
            return jsonify({
                'success': False,
                'message': f'Invalid file type. Allowed types: {", ".join(ALLOWED_EXTENSIONS)}'
            }), 400

        # Check file size
        file.seek(0, os.SEEK_END)
        file_size = file.tell()
        file.seek(0)

        if file_size > MAX_FILE_SIZE:
            return jsonify({
                'success': False,
                'message': f'File too large. Maximum size: {MAX_FILE_SIZE // (1024*1024)}MB'
            }), 413

        # Save file
        filename = secure_filename(file.filename)
        session_dir = os.path.join(current_app.config.get('UPLOAD_FOLDER', 'instance/uploads'), session_id)
        os.makedirs(session_dir, exist_ok=True)

        filepath = os.path.join(session_dir, filename)
        file.save(filepath)

        # Update session based on file type
        file_ext = filename.rsplit('.', 1)[1].lower()
        if file_ext in ['csv', 'xlsx', 'xls']:
            session['csv_loaded'] = True
            session['csv_filename'] = filename

            # Try to read and get basic info
            try:
                if file_ext == 'csv':
                    df = pd.read_csv(filepath)
                else:
                    df = pd.read_excel(filepath)

                file_info = {
                    'rows': len(df),
                    'columns': len(df.columns),
                    'column_names': df.columns.tolist()[:10],  # First 10 columns
                    'size': file_size
                }
            except:
                file_info = {'size': file_size}

        elif file_ext in ['shp', 'zip', 'geojson']:
            session['shapefile_loaded'] = True
            session['shapefile_filename'] = filename
            file_info = {'size': file_size, 'type': 'geospatial'}
        else:
            file_info = {'size': file_size}

        session['data_loaded'] = session.get('csv_loaded', False) or session.get('shapefile_loaded', False)

        return jsonify({
            'success': True,
            'message': f'File {filename} uploaded successfully',
            'filename': filename,
            'file_info': file_info,
            'session_status': {
                'csv_loaded': session.get('csv_loaded', False),
                'shapefile_loaded': session.get('shapefile_loaded', False),
                'data_loaded': session.get('data_loaded', False)
            }
        }), 200

    except Exception as e:
        return jsonify({
            'success': False,
            'error': str(e),
            'message': 'Failed to upload file'
        }), 500


@api_bp.route('/upload/status', methods=['GET'])
@require_auth
def upload_status():
    """Get status of uploaded files."""
    if 'session_id' not in session:
        return jsonify({
            'success': False,
            'message': 'No active session'
        }), 401

    session_id = session['session_id']
    session_dir = os.path.join(current_app.config.get('UPLOAD_FOLDER', 'instance/uploads'), session_id)

    files = []
    if os.path.exists(session_dir):
        for filename in os.listdir(session_dir):
            filepath = os.path.join(session_dir, filename)
            if os.path.isfile(filepath):
                files.append({
                    'name': filename,
                    'size': os.path.getsize(filepath),
                    'type': filename.split('.')[-1] if '.' in filename else 'unknown'
                })

    return jsonify({
        'success': True,
        'files': files,
        'csv_loaded': session.get('csv_loaded', False),
        'shapefile_loaded': session.get('shapefile_loaded', False)
    }), 200


@api_bp.route('/analysis/start', methods=['POST'])
@require_auth
def start_analysis():
    """Start analysis with specified parameters."""
    try:
        if 'session_id' not in session:
            return jsonify({
                'success': False,
                'message': 'No active session'
            }), 401

        if not session.get('data_loaded', False):
            return jsonify({
                'success': False,
                'message': 'Please upload data first'
            }), 400

        data = request.get_json() or {}
        analysis_type = data.get('analysis_type', 'composite')

        session['analysis_complete'] = False
        session['current_analysis_type'] = analysis_type
        session['analysis_status'] = 'started'

        return jsonify({
            'success': True,
            'message': f'{analysis_type.capitalize()} analysis started',
            'analysis_id': str(uuid.uuid4()),
            'status': 'started',
            'type': analysis_type
        }), 202

    except Exception as e:
        return jsonify({
            'success': False,
            'error': str(e),
            'message': 'Failed to start analysis'
        }), 500


@api_bp.route('/analysis/status', methods=['GET'])
@require_auth
def analysis_status():
    """Get current analysis status."""
    if 'session_id' not in session:
        return jsonify({
            'success': False,
            'message': 'No active session'
        }), 401

    return jsonify({
        'success': True,
        'session_id': session.get('session_id'),
        'analysis_complete': session.get('analysis_complete', False),
        'analysis_type': session.get('current_analysis_type', None),
        'analysis_results': session.get('analysis_results', None),
        'data_loaded': session.get('data_loaded', False)
    }), 200


@api_bp.route('/analysis/composite', methods=['POST'])
@require_auth
def run_composite_analysis():
    """Run composite analysis on uploaded data."""
    try:
        if 'session_id' not in session:
            return jsonify({
                'success': False,
                'message': 'No active session'
            }), 401

        if not session.get('data_loaded', False):
            return jsonify({
                'success': False,
                'message': 'Please upload data first'
            }), 400

        # Here you would run the actual analysis
        # For now, return a mock response
        session['analysis_complete'] = True
        session['current_analysis_type'] = 'composite'

        return jsonify({
            'success': True,
            'message': 'Composite analysis started',
            'analysis_id': str(uuid.uuid4()),
            'status': 'processing'
        }), 200

    except Exception as e:
        return jsonify({
            'success': False,
            'error': str(e),
            'message': 'Failed to start analysis'
        }), 500


@api_bp.route('/analysis/pca', methods=['POST'])
@require_auth
def run_pca_analysis():
    """Run PCA analysis on uploaded data."""
    try:
        if 'session_id' not in session:
            return jsonify({
                'success': False,
                'message': 'No active session'
            }), 401

        if not session.get('data_loaded', False):
            return jsonify({
                'success': False,
                'message': 'Please upload data first'
            }), 400

        session['analysis_complete'] = True
        session['current_analysis_type'] = 'pca'

        return jsonify({
            'success': True,
            'message': 'PCA analysis started',
            'analysis_id': str(uuid.uuid4()),
            'status': 'processing'
        }), 200

    except Exception as e:
        return jsonify({
            'success': False,
            'error': str(e),
            'message': 'Failed to start PCA analysis'
        }), 500


@api_bp.route('/analysis/vulnerability', methods=['POST'])
@require_auth
def run_vulnerability_analysis():
    """Run vulnerability analysis on uploaded data."""
    try:
        if 'session_id' not in session:
            return jsonify({
                'success': False,
                'message': 'No active session'
            }), 401

        if not session.get('data_loaded', False):
            return jsonify({
                'success': False,
                'message': 'Please upload data first'
            }), 400

        session['analysis_complete'] = True
        session['current_analysis_type'] = 'vulnerability'

        return jsonify({
            'success': True,
            'message': 'Vulnerability analysis started',
            'analysis_id': str(uuid.uuid4()),
            'status': 'processing'
        }), 200

    except Exception as e:
        return jsonify({
            'success': False,
            'error': str(e),
            'message': 'Failed to start vulnerability analysis'
        }), 500


@api_bp.route('/visualization/list', methods=['GET'])
@require_auth
def list_visualizations():
    """List available visualizations for the session."""
    if 'session_id' not in session:
        return jsonify({
            'success': False,
            'message': 'No active session'
        }), 401

    session_id = session['session_id']
    viz_dir = os.path.join(current_app.config.get('UPLOAD_FOLDER', 'instance/uploads'), session_id, 'visualizations')

    visualizations = []
    if os.path.exists(viz_dir):
        for filename in os.listdir(viz_dir):
            if filename.endswith(('.html', '.png', '.jpg', '.svg')):
                visualizations.append({
                    'filename': filename,
                    'type': filename.split('.')[-1],
                    'url': f'/api/visualization/get/{filename}'
                })

    return jsonify({
        'success': True,
        'visualizations': visualizations,
        'count': len(visualizations)
    }), 200


@api_bp.route('/visualization/get/<filename>', methods=['GET'])
@require_auth
def get_visualization(filename):
    """Retrieve a specific visualization file."""
    if 'session_id' not in session:
        return jsonify({
            'success': False,
            'message': 'No active session'
        }), 401

    session_id = session['session_id']
    viz_path = os.path.join(
        current_app.config.get('UPLOAD_FOLDER', 'instance/uploads'),
        session_id,
        'visualizations',
        secure_filename(filename)
    )

    if not os.path.exists(viz_path):
        return jsonify({
            'success': False,
            'message': 'Visualization not found'
        }), 404

    return send_file(viz_path)


@api_bp.route('/data/clear', methods=['POST'])
@require_auth
def clear_data():
    """Clear session data and files."""
    try:
        if 'session_id' in session:
            session_id = session['session_id']

            # Clear session directory
            session_dir = os.path.join(current_app.config.get('UPLOAD_FOLDER', 'instance/uploads'), session_id)
            if os.path.exists(session_dir):
                import shutil
                shutil.rmtree(session_dir)

            # Clear session data
            session.clear()

        return jsonify({
            'success': True,
            'message': 'Session data cleared successfully'
        }), 200

    except Exception as e:
        return jsonify({
            'success': False,
            'error': str(e),
            'message': 'Failed to clear session data'
        }), 500


@api_bp.errorhandler(404)
def not_found(error):
    """Handle 404 errors for API."""
    return jsonify({
        'success': False,
        'error': 'Not found',
        'message': 'The requested API endpoint does not exist'
    }), 404


@api_bp.errorhandler(500)
def internal_error(error):
    """Handle 500 errors for API."""
    return jsonify({
        'success': False,
        'error': 'Internal server error',
        'message': 'An unexpected error occurred'
    }), 500
