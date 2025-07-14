"""Production configuration with enhanced security."""
import os
from app.config.base import BaseConfig


class ProductionConfig(BaseConfig):
    """Production configuration."""
    
    # CRITICAL: Always disable debug in production
    DEBUG = False
    TESTING = False
    
    # Security settings
    SECRET_KEY = os.environ.get('SECRET_KEY', 'dev_secret_key_please_change_this')
    
    # Secure session cookies
    SESSION_COOKIE_SECURE = True  # HTTPS only
    SESSION_COOKIE_HTTPONLY = True  # No JS access
    SESSION_COOKIE_SAMESITE = 'Lax'  # CSRF protection
    PERMANENT_SESSION_LIFETIME = 3600  # 1 hour
    
    # Database configuration
    SQLALCHEMY_DATABASE_URI = os.environ.get(
        'DATABASE_URL',
        'sqlite:///instance/interactions.db'
    )
    if SQLALCHEMY_DATABASE_URI.startswith('postgres://'):
        # Fix for SQLAlchemy compatibility
        SQLALCHEMY_DATABASE_URI = SQLALCHEMY_DATABASE_URI.replace(
            'postgres://', 'postgresql://', 1
        )
    
    # CORS settings - restrict to specific domains
    CORS_ORIGINS = os.environ.get('CORS_ORIGINS', '').split(',')
    
    # File upload settings
    MAX_CONTENT_LENGTH = int(os.environ.get('MAX_CONTENT_LENGTH', 16 * 1024 * 1024))
    
    # API Keys (from environment only)
    OPENAI_API_KEY = os.environ.get('OPENAI_API_KEY')
    
    # Admin key (from environment only)
    ADMIN_KEY = os.environ.get('ADMIN_KEY')
    
    # Logging
    LOG_LEVEL = os.environ.get('LOG_LEVEL', 'INFO')
    LOG_FILE = os.environ.get('LOG_FILE', 'instance/app.log')
    
    # Security headers additions
    SECURITY_HEADERS = {
        'Strict-Transport-Security': 'max-age=31536000; includeSubDomains',
        'X-Content-Type-Options': 'nosniff',
        'X-Frame-Options': 'DENY',
        'X-XSS-Protection': '1; mode=block',
        'Content-Security-Policy': (
            "default-src 'self'; "
            "script-src 'self' 'unsafe-inline' https://unpkg.com https://cdn.jsdelivr.net https://cdn.plot.ly; "
            "style-src 'self' 'unsafe-inline' https://unpkg.com https://cdn.jsdelivr.net https://cdnjs.cloudflare.com https://fonts.googleapis.com; "
            "img-src 'self' data: https:; "
            "font-src 'self' https://fonts.gstatic.com https://cdnjs.cloudflare.com; "
            "connect-src 'self' https://tile.openstreetmap.org"
        )
    }
    
    @classmethod
    def init_app(cls, app):
        """Initialize production-specific settings and validate environment."""
        super().init_app(app)
        
        # Validate critical environment variables for production
        if not cls.SECRET_KEY or cls.SECRET_KEY == 'dev_secret_key_please_change_this':
            raise ValueError("SECRET_KEY must be set for production!")
        
        if not cls.OPENAI_API_KEY:
            raise ValueError("OPENAI_API_KEY must be set for production!")
        
        if not cls.ADMIN_KEY:
            raise ValueError("ADMIN_KEY must be set for production!")
        
        # Production-specific logging
        import logging
        from logging.handlers import RotatingFileHandler
        
        if not app.debug:
            file_handler = RotatingFileHandler(
                cls.LOG_FILE,
                maxBytes=10240000,  # 10MB
                backupCount=10
            )
            file_handler.setFormatter(logging.Formatter(
                '%(asctime)s %(levelname)s: %(message)s '
                '[in %(pathname)s:%(lineno)d]'
            ))
            file_handler.setLevel(getattr(logging, cls.LOG_LEVEL.upper()))
            app.logger.addHandler(file_handler)
            
            app.logger.setLevel(getattr(logging, cls.LOG_LEVEL.upper()))
            app.logger.info('ChatMRPT production startup')