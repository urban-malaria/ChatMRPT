from flask import Blueprint, send_from_directory
import os

react_bp = Blueprint('react', __name__)

@react_bp.route('/react')
@react_bp.route('/react/<path:path>')
def serve_react(path='index.html'):
    """Serve the React application"""
    react_dir = os.path.join(os.path.dirname(__file__), '..', 'static', 'react')
    
    if path != "" and os.path.exists(os.path.join(react_dir, path)):
        return send_from_directory(react_dir, path)
    else:
        return send_from_directory(react_dir, 'index.html')
