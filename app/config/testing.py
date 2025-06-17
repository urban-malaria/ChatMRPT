"""
Testing configuration for unit and integration tests.
"""

import tempfile
import os
from pathlib import Path
from .base import BaseConfig


class TestingConfig(BaseConfig):
    """Configuration for testing environment."""
    
    # Testing settings
    DEBUG = False
    TESTING = True
    
    # Use in-memory database for faster tests
    SECRET_KEY = 'testing_secret_key'
    
    # Test-specific paths (use temp directories)
    TEMP_DIR = Path(tempfile.mkdtemp())
    INSTANCE_FOLDER_PATH = TEMP_DIR / 'instance'
    UPLOAD_FOLDER = INSTANCE_FOLDER_PATH / 'uploads'
    REPORTS_FOLDER = INSTANCE_FOLDER_PATH / 'reports'
    SESSION_FILE_DIR = INSTANCE_FOLDER_PATH / 'flask_session'
    LOG_FILE = INSTANCE_FOLDER_PATH / 'test_app.log'
    INTERACTIONS_DB_FILE = INSTANCE_FOLDER_PATH / 'test_interactions.db'
    
    # Disable external API calls in tests
    OPENAI_API_KEY = 'test_api_key'
    OPENAI_MODEL_NAME = 'test_model'
    
    # Testing optimizations
    WTF_CSRF_ENABLED = False
    SESSION_PROTECTION = None
    
    # Disable security headers for testing
    SECURITY_HEADERS = {}
    
    # Fast logging for tests
    LOG_LEVEL = 'ERROR'  # Only log errors in tests
    
    # Disable CORS for tests
    CORS_ORIGINS = ['*']
    
    # Test file limits (smaller for faster tests)
    MAX_CONTENT_LENGTH = 1 * 1024 * 1024  # 1 MB for tests
    
    @classmethod
    def init_app(cls, app):
        """Initialize testing-specific settings."""
        super().init_app(app)
        
        # Create test directories
        for folder in [cls.INSTANCE_FOLDER_PATH, cls.UPLOAD_FOLDER, 
                      cls.REPORTS_FOLDER, cls.SESSION_FILE_DIR]:
            folder.mkdir(parents=True, exist_ok=True)
        
        # Disable logging during tests (except errors)
        import logging
        logging.getLogger('werkzeug').setLevel(logging.ERROR)
        logging.getLogger('flask').setLevel(logging.ERROR)
        
        app.logger.info("🧪 ChatMRPT Testing Mode Initialized")
        app.logger.info(f"📂 Test instance folder: {cls.INSTANCE_FOLDER_PATH}")
    
    @classmethod
    def cleanup(cls):
        """Clean up test files after testing."""
        import shutil
        if cls.TEMP_DIR.exists():
            shutil.rmtree(cls.TEMP_DIR, ignore_errors=True) 