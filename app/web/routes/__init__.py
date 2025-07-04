# app/web/routes/__init__.py
"""
Web routes package for organizing Flask route handlers.

This package breaks down the monolithic main.py blueprint into smaller, focused modules:
- core_routes.py: Core application routes (index, session management, app status)
- upload_routes.py: File upload handling routes  
- analysis_routes.py: Analysis processing and AI chat routes
- visualization_routes.py: Visualization and media serving routes
- reports_api_routes.py: Reports generation and API endpoints
- debug_routes.py: Debug and admin interface routes
"""

from .core_routes import core_bp
from .upload_routes import upload_bp
from .analysis_routes import analysis_bp
from .visualization_routes import viz_bp
from .reports_api_routes import reports_bp
from .debug_routes import debug_bp

__all__ = [
    'core_bp',
    'upload_bp',
    'analysis_bp',
    'viz_bp',
    'reports_bp',
    'debug_bp',
    'register_all_blueprints'
]

def register_all_blueprints(app):
    """Register all route blueprints with the Flask app."""
    # Register core routes (index, session management, etc.)
    app.register_blueprint(core_bp)
    
    # Register upload routes (file upload handling)
    app.register_blueprint(upload_bp)
    
    # Register analysis routes (analysis processing and AI chat)
    app.register_blueprint(analysis_bp)
    
    # Register visualization routes (visualization generation and navigation)
    app.register_blueprint(viz_bp)
    
    # Register reports and API routes (report generation and API endpoints)
    app.register_blueprint(reports_bp)
    
    # Register debug routes (debugging and admin interface)
    app.register_blueprint(debug_bp)


# Legacy compatibility - provide main_bp for backward compatibility
main_bp = core_bp 