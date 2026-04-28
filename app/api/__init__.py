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
- export_routes.py: Export download functionality for analysis results
"""

import logging

from .admin import admin_bp
from .core_routes import core_bp
from .upload_routes import upload_bp
from .analysis import analysis_bp  # Modular analysis routes (chat, exec, vote)
from .visualization_routes import viz_bp
from .reports_routes import reports_bp
from .debug_routes import debug_bp
from .itn_routes import itn_bp, itn_embed_bp
from .export_routes import export_bp
from .session_routes import session_bp
from .arena_routes import arena_bp
from .conversation_routes import conversations_bp

# API routes for React frontend
try:
    from .api_routes import api_bp
    API_ROUTES_AVAILABLE = True
except ImportError as e:
    api_bp = None
    API_ROUTES_AVAILABLE = False
    print(f"API routes not available: {e}")

# Survey routes for cognitive assessment
try:
    from app.survey import survey_bp
    SURVEY_AVAILABLE = True
except ImportError as e:
    survey_bp = None
    SURVEY_AVAILABLE = False
    print(f"Survey module not available: {e}")

# Pre-Post Test routes for knowledge assessment
try:
    from app.prepost import prepost_bp
    PREPOST_AVAILABLE = True
except ImportError as e:
    prepost_bp = None
    PREPOST_AVAILABLE = False
    print(f"Pre-Post Test module not available: {e}")

# TPR routes removed - replaced with new data analysis pipeline

# Data Analysis V3 - New implementation
try:
    from .data_analysis_routes import data_analysis_v3_bp
    DATA_ANALYSIS_V3_AVAILABLE = True
except ImportError as e:
    data_analysis_v3_bp = None
    DATA_ANALYSIS_V3_AVAILABLE = False
    print(f"Data Analysis V3 not available: {e}")

# Data Analysis V2 removed - will be reimplemented
data_analysis_v2_bp = None
DATA_ANALYSIS_V2_AVAILABLE = False

# Legacy data analysis (to be removed)
DATA_ANALYSIS_AVAILABLE = False
data_analysis_bp = None

__all__ = [
    'core_bp',
    'upload_bp',
    'analysis_bp',
    'viz_bp',
    'reports_bp',
    'debug_bp',
    'itn_bp',
    'itn_embed_bp',
    'export_bp',
    'session_bp',
    'arena_bp',
    'conversations_bp',
    'api_bp',
    'data_analysis_bp',
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
    
    # Register ITN routes (ITN distribution API)
    app.register_blueprint(itn_bp)
    
    # Register ITN embed routes (ITN map visualization serving)
    app.register_blueprint(itn_embed_bp)
    
    # Register export routes (export download functionality)
    app.register_blueprint(export_bp)
    
    # Register session routes (session state verification)
    app.register_blueprint(session_bp)
    
    # Register Arena routes (model comparison interface)
    app.register_blueprint(arena_bp)

    # Register Conversation History routes
    app.register_blueprint(conversations_bp)

    # Register API routes (React frontend API)
    if API_ROUTES_AVAILABLE and api_bp:
        app.register_blueprint(api_bp)

    # Register Survey routes (cognitive assessment)
    if SURVEY_AVAILABLE and survey_bp:
        app.register_blueprint(survey_bp)

    # Register Pre-Post Test routes (knowledge assessment)
    if PREPOST_AVAILABLE and prepost_bp:
        app.register_blueprint(prepost_bp)

    logger = logging.getLogger(__name__)
    logger.info("✅ Export routes registered")
    logger.info("✅ Session routes registered")
    logger.info("✅ Arena routes registered")
    logger.info("✅ Conversation history routes registered")

    if API_ROUTES_AVAILABLE:
        logger.info("✅ API routes registered")

    if SURVEY_AVAILABLE:
        logger.info("✅ Survey routes registered")

    if PREPOST_AVAILABLE:
        logger.info("✅ Pre-Post Test routes registered")
    
    # Register Data Analysis V3 routes
    if DATA_ANALYSIS_V3_AVAILABLE and data_analysis_v3_bp:
        app.register_blueprint(data_analysis_v3_bp)
        logger.info("✅ Data Analysis V3 routes registered")
    
    # Data Analysis V2 removed - will be reimplemented
    
    # Legacy data analysis routes (deprecated)
    if DATA_ANALYSIS_AVAILABLE and data_analysis_bp:
        app.register_blueprint(data_analysis_bp)
        logger.info("✅ Legacy Data Analysis routes registered")


# Legacy compatibility - provide main_bp for backward compatibility
main_bp = core_bp 