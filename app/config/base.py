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
    
    # Phase 1: Conversation Memory Configuration
    CHROMA_PERSIST_DIRECTORY = os.environ.get('CHROMA_PERSIST_DIRECTORY', str(INSTANCE_FOLDER_PATH / 'memory'))
    CHROMA_COLLECTION_NAME = os.environ.get('CHROMA_COLLECTION_NAME', 'chatmrpt_conversations')
    MEMORY_EMBEDDING_MODEL = os.environ.get('MEMORY_EMBEDDING_MODEL', 'BAAI/llm-embedder')
    MEMORY_RETRIEVAL_K = int(os.environ.get('MEMORY_RETRIEVAL_K', '5'))
    MEMORY_CLEANUP_DAYS = int(os.environ.get('MEMORY_CLEANUP_DAYS', '30'))
    ENABLE_CONVERSATION_MEMORY = os.environ.get('ENABLE_CONVERSATION_MEMORY', 'true').lower() == 'true'
    
    # Phase 2: Tool Orchestration Configuration
    TOOL_CONFIDENCE_THRESHOLD = float(os.environ.get('TOOL_CONFIDENCE_THRESHOLD', '0.7'))
    TOOL_SIMILARITY_MODEL = os.environ.get('TOOL_SIMILARITY_MODEL', 'all-MiniLM-L6-v2')
    ENABLE_TOOL_CACHING = os.environ.get('ENABLE_TOOL_CACHING', 'true').lower() == 'true'
    ENABLE_INTELLIGENT_TOOL_SELECTION = os.environ.get('ENABLE_INTELLIGENT_TOOL_SELECTION', 'true').lower() == 'true'
    
    # Phase 3: ReAct Agent Configuration
    AGENT_MAX_ITERATIONS = int(os.environ.get('AGENT_MAX_ITERATIONS', '5'))
    AGENT_TEMPERATURE = float(os.environ.get('AGENT_TEMPERATURE', '0.1'))
    ENABLE_AGENT_REFLECTION = os.environ.get('ENABLE_AGENT_REFLECTION', 'true').lower() == 'true'
    MALARIA_EXPERTISE_LEVEL = os.environ.get('MALARIA_EXPERTISE_LEVEL', 'expert')
    AGENT_MAX_TOKENS = int(os.environ.get('AGENT_MAX_TOKENS', '2000'))
    ENABLE_TOOL_CHAINING = os.environ.get('ENABLE_TOOL_CHAINING', 'true').lower() == 'true'
    
    # Phase 4: LangChain Integration Configuration
    LANGCHAIN_TEMPERATURE = float(os.environ.get('LANGCHAIN_TEMPERATURE', '0.7'))
    LANGCHAIN_MAX_TOKENS = int(os.environ.get('LANGCHAIN_MAX_TOKENS', '2000'))
    ENABLE_LANGCHAIN_INTEGRATION = os.environ.get('ENABLE_LANGCHAIN_INTEGRATION', 'true').lower() == 'true'
    REDIS_URL = os.environ.get('REDIS_URL', None)
    REDIS_SESSION_TTL = int(os.environ.get('REDIS_SESSION_TTL', '3600'))
    ENABLE_REDIS_SESSIONS = os.environ.get('ENABLE_REDIS_SESSIONS', 'false').lower() == 'true'
    
    # Phase 5: Reflection Engine Configuration
    ENABLE_REFLECTION_ENGINE = os.environ.get('ENABLE_REFLECTION_ENGINE', 'true').lower() == 'true'
    ENABLE_REFLECTION_LEARNING = os.environ.get('ENABLE_REFLECTION_LEARNING', 'true').lower() == 'true'
    REFLECTION_METRICS_WINDOW = int(os.environ.get('REFLECTION_METRICS_WINDOW', '1000'))
    REFLECTION_CACHE_SIZE = int(os.environ.get('REFLECTION_CACHE_SIZE', '10000'))
    REFLECTION_CACHE_TTL_HOURS = int(os.environ.get('REFLECTION_CACHE_TTL_HOURS', '24'))
    REFLECTION_OPTIMIZATION_THRESHOLD = float(os.environ.get('REFLECTION_OPTIMIZATION_THRESHOLD', '0.1'))
    REFLECTION_INTERVAL_MINUTES = int(os.environ.get('REFLECTION_INTERVAL_MINUTES', '30'))
    ENABLE_ASYNC_REFLECTION = os.environ.get('ENABLE_ASYNC_REFLECTION', 'true').lower() == 'true'
    REFLECTION_PERSIST_DIRECTORY = os.environ.get('REFLECTION_PERSIST_DIRECTORY', str(INSTANCE_FOLDER_PATH / 'reflection'))
    ENABLE_PERFORMANCE_MONITORING = os.environ.get('ENABLE_PERFORMANCE_MONITORING', 'true').lower() == 'true'
    
    # Phase 6: Production Deployment Configuration
    DEPLOYMENT_ENVIRONMENT = os.environ.get('DEPLOYMENT_ENVIRONMENT', 'development')
    WORKERS = int(os.environ.get('WORKERS', '4'))
    MAX_REQUESTS = int(os.environ.get('MAX_REQUESTS', '1000'))
    TIMEOUT = int(os.environ.get('TIMEOUT', '30'))
    ENABLE_METRICS = os.environ.get('ENABLE_METRICS', 'true').lower() == 'true'
    ENABLE_HEALTH_CHECKS = os.environ.get('ENABLE_HEALTH_CHECKS', 'true').lower() == 'true'
    MAX_CONCURRENT_REQUESTS = int(os.environ.get('MAX_CONCURRENT_REQUESTS', '100'))
    MEMORY_LIMIT_MB = int(os.environ.get('MEMORY_LIMIT_MB', '2048'))
    METRICS_PORT = int(os.environ.get('METRICS_PORT', '8001'))
    
    # Production Security Configuration  
    ENABLE_RATE_LIMITING = os.environ.get('ENABLE_RATE_LIMITING', 'true').lower() == 'true'
    MAX_REQUESTS_PER_MINUTE = int(os.environ.get('MAX_REQUESTS_PER_MINUTE', '60'))
    ENABLE_INPUT_VALIDATION = os.environ.get('ENABLE_INPUT_VALIDATION', 'true').lower() == 'true'
    ENABLE_OUTPUT_SANITIZATION = os.environ.get('ENABLE_OUTPUT_SANITIZATION', 'true').lower() == 'true'
    SESSION_TIMEOUT_MINUTES = int(os.environ.get('SESSION_TIMEOUT_MINUTES', '60'))
    ENABLE_CORS_PROTECTION = os.environ.get('ENABLE_CORS_PROTECTION', 'true').lower() == 'true'
    ALLOWED_ORIGINS = os.environ.get('ALLOWED_ORIGINS', 'http://localhost:5000').split(',')
    ENABLE_CSRF_PROTECTION = os.environ.get('ENABLE_CSRF_PROTECTION', 'true').lower() == 'true'
    ENABLE_XSS_PROTECTION = os.environ.get('ENABLE_XSS_PROTECTION', 'true').lower() == 'true'
    
    # Production Optimization
    ENABLE_AUTO_SCALING = os.environ.get('ENABLE_AUTO_SCALING', 'false').lower() == 'true'
    OPTIMIZATION_INTERVAL_MINUTES = int(os.environ.get('OPTIMIZATION_INTERVAL_MINUTES', '5'))
    ENABLE_PRODUCTION_LOGGING = os.environ.get('ENABLE_PRODUCTION_LOGGING', 'true').lower() == 'true'
    
    # Logging Configuration
    LOG_LEVEL = os.environ.get('LOG_LEVEL', 'INFO')
    LOG_MAX_BYTES = 1024 * 1024 * 5  # 5MB per log file
    LOG_BACKUP_COUNT = 5
    
    # Security Headers
    SECURITY_HEADERS = {
        'X-Content-Type-Options': 'nosniff',
        'X-Frame-Options': 'DENY',
        'X-XSS-Protection': '1; mode=block',
        'Content-Security-Policy': "default-src 'self'; script-src 'self' 'unsafe-inline' 'unsafe-eval' https://cdn.jsdelivr.net https://unpkg.com https://cdn.plot.ly; style-src 'self' 'unsafe-inline' https://fonts.googleapis.com https://cdn.jsdelivr.net https://cdnjs.cloudflare.com; font-src 'self' https://fonts.gstatic.com; img-src 'self' data: https:; connect-src 'self'",
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
        memory_folder = Path(cls.CHROMA_PERSIST_DIRECTORY)
        reflection_folder = Path(cls.REFLECTION_PERSIST_DIRECTORY)
        for folder in [cls.INSTANCE_FOLDER_PATH, cls.UPLOAD_FOLDER, 
                      cls.REPORTS_FOLDER, cls.SESSION_FILE_DIR, memory_folder, reflection_folder]:
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