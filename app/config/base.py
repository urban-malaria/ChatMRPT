"""
Base configuration class with common settings.
"""

import os
from pathlib import Path
from datetime import timedelta


class BaseConfig:
    """Base configuration with common settings for all environments."""
    
    # Application Settings
    SECRET_KEY = os.environ.get('SECRET_KEY', 'dev_secret_key_please_change_this')
    
    # File Upload Settings
    MAX_CONTENT_LENGTH = 32 * 1024 * 1024  # 32 MB upload limit
    ALLOWED_EXTENSIONS_CSV = {'csv', 'xlsx', 'xls'}
    ALLOWED_EXTENSIONS_SHP = {'zip'}
    
    # Session Configuration
    SESSION_TYPE = 'filesystem'
    SESSION_PERMANENT = True  # Make sessions persist
    PERMANENT_SESSION_LIFETIME = timedelta(days=1)  # Set session timeout to 1 day
    SESSION_USE_SIGNER = True
    SESSION_COOKIE_SECURE = False  # Set to True when using HTTPS in production
    SESSION_COOKIE_HTTPONLY = True
    SESSION_COOKIE_SAMESITE = 'Lax'
    
    # Path Configuration
    PROJECT_ROOT = Path(__file__).parent.parent.parent
    INSTANCE_FOLDER_PATH = PROJECT_ROOT / 'instance'
    UPLOAD_FOLDER = INSTANCE_FOLDER_PATH / 'uploads'
    REPORTS_FOLDER = INSTANCE_FOLDER_PATH / 'reports'
    SESSION_FILE_DIR = INSTANCE_FOLDER_PATH / 'flask_session'
    LOG_FILE = INSTANCE_FOLDER_PATH / 'app.log'
    INTERACTIONS_DB_FILE = INSTANCE_FOLDER_PATH / 'interactions.db'
    
    # AI/LLM Configuration
    OPENAI_API_KEY = os.environ.get('OPENAI_API_KEY')
    OPENAI_MODEL_NAME = os.environ.get('OPENAI_MODEL_NAME', 'gpt-4o')
    
    # Logging Configuration
    LOG_LEVEL = os.environ.get('LOG_LEVEL', 'INFO')
    LOG_MAX_BYTES = 1024 * 1024 * 5  # 5MB per log file
    LOG_BACKUP_COUNT = 5
    
    # Security Headers
    SECURITY_HEADERS = {
        'X-Content-Type-Options': 'nosniff',
        'X-Frame-Options': 'DENY',
        'X-XSS-Protection': '1; mode=block',
        'Content-Security-Policy': "default-src 'self'; script-src 'self' 'unsafe-inline' 'unsafe-eval' https://cdn.jsdelivr.net https://unpkg.com https://cdn.plot.ly; style-src 'self' 'unsafe-inline' https://fonts.googleapis.com https://cdn.jsdelivr.net; font-src 'self' https://fonts.gstatic.com; img-src 'self' data: https:; connect-src 'self'",
        'Referrer-Policy': 'strict-origin-when-cross-origin',
        'Permissions-Policy': 'geolocation=(), microphone=(), camera=()'
    }
    
    # CORS Settings
    CORS_ORIGINS = ['http://localhost:5000']
    
    # Performance Settings
    SEND_FILE_MAX_AGE_DEFAULT = 31536000  # 1 year for static files
    
    # Reloader Exclusions (for development)
    RELOADER_EXCLUSIONS = [
        '*.zip', '*.csv', '*.json', '*.png', '*.html', '*.xlsx', '*.xls',
        '*.shp', '*.dbf', '*.shx', '*.prj', '*.cpg', '*.geojson', '*.pdf', 
        '*.docx', '*.md'
    ]
    
    @classmethod
    def init_app(cls, app):
        """
        Initialize application with configuration.
        
        Args:
            app: Flask application instance
        """
        # Create necessary directories
        for folder in [cls.INSTANCE_FOLDER_PATH, cls.UPLOAD_FOLDER, 
                      cls.REPORTS_FOLDER, cls.SESSION_FILE_DIR]:
            folder.mkdir(parents=True, exist_ok=True)
        
        # Set path configurations as strings for Flask
        app.config['UPLOAD_FOLDER'] = str(cls.UPLOAD_FOLDER)
        app.config['REPORTS_FOLDER'] = str(cls.REPORTS_FOLDER)
        app.config['SESSION_FILE_DIR'] = str(cls.SESSION_FILE_DIR)
        app.config['LOG_FILE'] = str(cls.LOG_FILE)
        app.config['INTERACTIONS_DB_FILE'] = str(cls.INTERACTIONS_DB_FILE)
        
        # Apply security headers
        @app.after_request
        def set_security_headers(response):
            for header, value in cls.SECURITY_HEADERS.items():
                response.headers[header] = value
            return response 