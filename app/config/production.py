"""
Production configuration with enhanced security and performance settings.
"""

import os
from .base import BaseConfig
from flask import request


class ProductionConfig(BaseConfig):
    """Configuration for production environment."""
    
    # Production settings
    DEBUG = False
    TESTING = False
    
    # Enhanced security
    SECRET_KEY = os.environ.get('SECRET_KEY') or None
    
    # Strict CORS settings
    CORS_ORIGINS = os.environ.get('CORS_ORIGINS', '').split(',') if os.environ.get('CORS_ORIGINS') else []
    
    # Production logging
    LOG_LEVEL = os.environ.get('LOG_LEVEL', 'WARNING')
    
    # Full security headers
    SECURITY_HEADERS = {
        'X-Content-Type-Options': 'nosniff',
        'X-Frame-Options': 'DENY',
        'X-XSS-Protection': '1; mode=block',
        'Strict-Transport-Security': 'max-age=31536000; includeSubDomains',
        'Content-Security-Policy': "default-src 'self'"
    }
    
    # Performance optimizations
    SEND_FILE_MAX_AGE_DEFAULT = 31536000  # 1 year
    JSONIFY_PRETTYPRINT_REGULAR = False
    
    # Session security
    SESSION_COOKIE_SECURE = True
    SESSION_COOKIE_HTTPONLY = True
    SESSION_COOKIE_SAMESITE = 'Lax'
    
    # Disable debug features
    DEBUG_TB_ENABLED = False
    USE_RELOADER = False
    
    @classmethod
    def init_app(cls, app):
        """Initialize production-specific settings."""
        super().init_app(app)
        
        # Validate required environment variables
        if not cls.SECRET_KEY:
            raise ValueError("SECRET_KEY environment variable is required for production")
        
        if not cls.OPENAI_API_KEY:
            app.logger.warning("OPENAI_API_KEY not set - AI features will be disabled")
        
        # Production-specific error handlers
        @app.errorhandler(404)
        def not_found_error(error):
            app.logger.warning(f"404 Not Found: {request.url}")
            return {"error": "Resource not found"}, 404

        @app.errorhandler(500)
        def internal_error(error):
            app.logger.error(f"Internal server error", exc_info=True)
            return {"error": "Internal server error"}, 500
        
        @app.errorhandler(403)
        def forbidden_error(error):
            app.logger.warning(f"403 Forbidden: {request.url}")
            return {"error": "Access forbidden"}, 403
        
        # Security middleware
        @app.before_request
        def security_checks():
            # Add any production security checks here
            pass
        
        app.logger.info("🚀 ChatMRPT Production Mode Initialized")
        app.logger.info(f"📊 Debug Mode: {app.config.get('DEBUG', False)}")
        app.logger.info(f"🔒 Security Headers: Enabled")
        app.logger.info(f"🤖 AI Features: {'Enabled' if cls.OPENAI_API_KEY else 'Disabled'}") 