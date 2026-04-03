"""
Optimized Production Configuration - Phase 2
Enhanced with connection pooling, caching, and performance optimizations
Date: August 27, 2025
"""

import os
from datetime import timedelta
from app.config.base import BaseConfig


class ProductionOptimizedConfig(BaseConfig):
    """Optimized production configuration for high performance."""
    
    # Core settings
    DEBUG = False
    TESTING = False
    ENV = 'production'
    
    # Security
    SECRET_KEY = os.environ.get('SECRET_KEY')
    if not SECRET_KEY:
        raise ValueError("SECRET_KEY environment variable is not set!")
    
    # Enhanced Session Configuration
    SESSION_COOKIE_SECURE = False  # Will enable after SSL setup
    SESSION_COOKIE_HTTPONLY = True
    SESSION_COOKIE_SAMESITE = 'Lax'
    PERMANENT_SESSION_LIFETIME = timedelta(hours=24)
    SESSION_TYPE = 'redis'
    SESSION_PERMANENT = False
    SESSION_USE_SIGNER = True
    SESSION_KEY_PREFIX = 'chatmrpt:session:'
    
    # Redis Configuration (for sessions and caching)
    REDIS_URL = os.environ.get(
        'REDIS_URL',
        'redis://chatmrpt-redis-staging.1b3pmt.0001.use2.cache.amazonaws.com:6379/0'
    )
    
    # Database Configuration with Connection Pooling
    SQLALCHEMY_DATABASE_URI = os.environ.get(
        'DATABASE_URL',
        'sqlite:///instance/interactions.db'
    )
    
    # Optimized connection pool settings
    SQLALCHEMY_ENGINE_OPTIONS = {
        'pool_size': 20,           # Number of persistent connections
        'pool_recycle': 3600,      # Recycle connections after 1 hour
        'pool_pre_ping': True,     # Test connections before using
        'max_overflow': 40,        # Maximum overflow connections
        'pool_timeout': 30,        # Timeout for getting connection
        'echo_pool': False,        # Don't log pool checkouts
        'connect_args': {
            'check_same_thread': False  # For SQLite only
        }
    }
    
    SQLALCHEMY_TRACK_MODIFICATIONS = False
    SQLALCHEMY_RECORD_QUERIES = False  # Disable query recording in production
    
    # File Upload Settings
    MAX_CONTENT_LENGTH = 64 * 1024 * 1024  # 64MB
    UPLOAD_FOLDER = 'instance/uploads'
    DATA_UPLOAD_MAX_MEMORY_SIZE = 64 * 1024 * 1024
    
    # Performance Optimizations
    SEND_FILE_MAX_AGE_DEFAULT = 31536000  # 1 year for static files
    JSONIFY_PRETTYPRINT_REGULAR = False
    JSON_SORT_KEYS = False
    TEMPLATES_AUTO_RELOAD = False
    
    # Caching Configuration
    CACHE_TYPE = 'redis'
    CACHE_REDIS_URL = REDIS_URL
    CACHE_DEFAULT_TIMEOUT = 300  # 5 minutes default
    CACHE_KEY_PREFIX = 'chatmrpt:cache:'
    
    # Cache timeouts by type
    CACHE_TIMEOUTS = {
        'analysis_results': 3600,      # 1 hour for analysis results
        'visualization': 1800,         # 30 minutes for visualizations
        'static_data': 86400,         # 24 hours for static data
        'user_session': 300,          # 5 minutes for user session data
        'api_response': 60,           # 1 minute for API responses
    }
    
    # Response Compression
    COMPRESS_MIMETYPES = [
        'text/html', 'text/css', 'text/xml',
        'application/json', 'application/javascript'
    ]
    COMPRESS_LEVEL = 6
    COMPRESS_MIN_SIZE = 1000  # Don't compress responses smaller than 1KB
    
    # Rate Limiting
    RATELIMIT_ENABLED = True
    RATELIMIT_STORAGE_URL = REDIS_URL
    RATELIMIT_STORAGE_OPTIONS = {
        'socket_connect_timeout': 30,
        'socket_timeout': 30,
        'decode_responses': True
    }
    RATELIMIT_DEFAULT = "200 per minute"
    RATELIMIT_HEADERS_ENABLED = True
    
    # Rate limits by endpoint
    RATELIMIT_BY_ENDPOINT = {
        'upload': '10 per minute',
        'analysis': '30 per minute',
        'visualization': '60 per minute',
        'api': '100 per minute'
    }
    
    # API Configuration
    OPENAI_API_KEY = os.environ.get('OPENAI_API_KEY')
    if not OPENAI_API_KEY:
        raise ValueError("OPENAI_API_KEY environment variable is not set!")
    
    # API Performance Settings
    OPENAI_TIMEOUT = 120
    OPENAI_MAX_RETRIES = 3
    OPENAI_RETRY_DELAY = 1  # seconds
    ANALYSIS_TIMEOUT = 300
    
    # Request Queue Settings
    CELERY_BROKER_URL = REDIS_URL
    CELERY_RESULT_BACKEND = REDIS_URL
    CELERY_TASK_SERIALIZER = 'json'
    CELERY_ACCEPT_CONTENT = ['json']
    CELERY_RESULT_SERIALIZER = 'json'
    CELERY_TIMEZONE = 'UTC'
    CELERY_ENABLE_UTC = True
    
    # Monitoring and Metrics
    METRICS_ENABLED = True
    PROMETHEUS_ENABLED = os.environ.get('PROMETHEUS_ENABLED', 'false').lower() == 'true'
    STATSD_HOST = os.environ.get('STATSD_HOST', 'localhost')
    STATSD_PORT = int(os.environ.get('STATSD_PORT', 8125))
    STATSD_PREFIX = 'chatmrpt'
    
    # Logging Configuration
    LOG_LEVEL = os.environ.get('LOG_LEVEL', 'WARNING')  # Less verbose in production
    LOG_FILE = os.environ.get('LOG_FILE', 'instance/logs/app.log')
    LOG_TO_STDOUT = os.environ.get('LOG_TO_STDOUT', 'false').lower() == 'true'
    LOG_FORMAT = '%(asctime)s %(levelname)s [%(name)s] %(message)s'
    
    # CORS Configuration
    CORS_ORIGINS = os.environ.get(
        'CORS_ORIGINS',
        'https://d225ar6c86586s.cloudfront.net,http://chatmrpt-staging-alb-752380251.us-east-2.elb.amazonaws.com'
    ).split(',')
    
    # Security Headers
    SECURITY_HEADERS = {
        'X-Content-Type-Options': 'nosniff',
        'X-Frame-Options': 'SAMEORIGIN',
        'X-XSS-Protection': '1; mode=block',
        'Referrer-Policy': 'strict-origin-when-cross-origin',
        'Permissions-Policy': 'geolocation=(), microphone=(), camera=()',
    }
    
    # Feature Flags
    FEATURE_FLAGS = {
        'connection_pooling': True,
        'redis_caching': True,
        'response_compression': True,
        'rate_limiting': True,
        'async_processing': False,  # Enable when Celery is set up
        'performance_monitoring': True,
        'error_tracking': False,    # Enable when Sentry is configured
    }
    
    # Performance Thresholds (for monitoring)
    PERFORMANCE_THRESHOLDS = {
        'response_time_warning': 1.0,   # seconds
        'response_time_critical': 2.0,  # seconds
        'memory_warning': 80,           # percent
        'memory_critical': 90,          # percent
        'cpu_warning': 70,              # percent
        'cpu_critical': 85,             # percent
    }
    
    # Admin Configuration
    ADMIN_KEY = os.environ.get('ADMIN_KEY')
    if not ADMIN_KEY:
        raise ValueError("ADMIN_KEY environment variable is not set!")
    
    @classmethod
    def init_app(cls, app):
        """Initialize app with optimized production settings."""
        BaseConfig.init_app(app)
        
        # Set up optimized logging
        import logging
        from logging.handlers import RotatingFileHandler
        import os
        
        if not app.debug and not app.testing:
            # Create logs directory
            log_dir = os.path.dirname(cls.LOG_FILE)
            if not os.path.exists(log_dir):
                os.makedirs(log_dir)
            
            # Set up rotating file handler
            file_handler = RotatingFileHandler(
                cls.LOG_FILE,
                maxBytes=10485760,  # 10MB
                backupCount=10
            )
            file_handler.setFormatter(logging.Formatter(cls.LOG_FORMAT))
            file_handler.setLevel(getattr(logging, cls.LOG_LEVEL))
            
            # Remove default handler and add optimized one
            app.logger.handlers.clear()
            app.logger.addHandler(file_handler)
            
            # Also log to stdout if configured (for CloudWatch)
            if cls.LOG_TO_STDOUT:
                stream_handler = logging.StreamHandler()
                stream_handler.setFormatter(logging.Formatter(cls.LOG_FORMAT))
                stream_handler.setLevel(getattr(logging, cls.LOG_LEVEL))
                app.logger.addHandler(stream_handler)
            
            app.logger.setLevel(getattr(logging, cls.LOG_LEVEL))
            app.logger.info('ChatMRPT Optimized Production startup')
            
        # Log configuration summary
        app.logger.info(f'Workers: Configured via Gunicorn')
        app.logger.info(f'Connection Pool: Size={cls.SQLALCHEMY_ENGINE_OPTIONS["pool_size"]}, '
                       f'Overflow={cls.SQLALCHEMY_ENGINE_OPTIONS["max_overflow"]}')
        app.logger.info(f'Cache: Type={cls.CACHE_TYPE}, Redis={cls.REDIS_URL}')
        app.logger.info(f'Rate Limiting: Enabled={cls.RATELIMIT_ENABLED}')
        app.logger.info(f'Compression: Enabled (Level {cls.COMPRESS_LEVEL})')