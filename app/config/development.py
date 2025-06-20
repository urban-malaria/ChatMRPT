"""
Development configuration with debug settings and relaxed security.
"""

import os
from .base import BaseConfig


class DevelopmentConfig(BaseConfig):
    """Configuration for development environment."""
    
    # Development specific settings
    DEBUG = True
    TESTING = False
    
    # Relaxed CORS for development
    CORS_ORIGINS = ['http://localhost:5000', 'http://127.0.0.1:5000', 'http://localhost:3000']
    
    # Development logging
    LOG_LEVEL = 'DEBUG'
    
    # Disable some security headers for easier development
    SECURITY_HEADERS = {
        'X-Content-Type-Options': 'nosniff',
        # X-Frame-Options removed for iframe testing
        'X-XSS-Protection': '1; mode=block'
    }
    
    # Enable Flask debug toolbar if available
    DEBUG_TB_ENABLED = True
    DEBUG_TB_INTERCEPT_REDIRECTS = False
    
    # Hot reloading settings
    USE_RELOADER = True
    
    @classmethod
    def init_app(cls, app):
        """Initialize development-specific settings."""
        super().init_app(app)
        
        # Development-specific initialization
        print(f"[DEV] ChatMRPT Development Mode")
        print(f"[DEV] Instance folder: {cls.INSTANCE_FOLDER_PATH}")
        print(f"[DEV] OpenAI API Key: {'Set' if cls.OPENAI_API_KEY else 'Missing'}")
        print(f"[DEV] Debug Mode: {app.config.get('DEBUG', False)}")
        
        # Set up development-specific middleware
        @app.before_request
        def log_request_info():
            if app.config.get('DEBUG'):
                from flask import request
                app.logger.debug(f"Request: {request.method} {request.url}")
        
        # Enhanced error pages for development
        @app.errorhandler(500)
        def internal_error(error):
            app.logger.error(f"Internal server error: {error}", exc_info=True)
            return {"error": "Internal server error", "debug": str(error)}, 500 