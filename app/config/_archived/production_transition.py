"""Enhanced production configuration for staging-to-production transition."""
import os
from datetime import timedelta
from app.config.base import BaseConfig


class ProductionTransitionConfig(BaseConfig):
    """Production configuration optimized for transition from staging."""
    
    # CRITICAL: Production settings
    DEBUG = False
    TESTING = False
    ENV = 'production'
    
    # Security settings
    SECRET_KEY = os.environ.get('SECRET_KEY')
    if not SECRET_KEY:
        raise ValueError("SECRET_KEY environment variable is not set!")
    
    # Enhanced session configuration for production load
    SESSION_COOKIE_SECURE = False  # HTTP for ALB, will enable after SSL
    SESSION_COOKIE_HTTPONLY = True
    SESSION_COOKIE_SAMESITE = 'Lax'
    PERMANENT_SESSION_LIFETIME = timedelta(hours=24)  # Extended for production
    SESSION_TYPE = 'redis'
    SESSION_PERMANENT = False
    SESSION_USE_SIGNER = True
    SESSION_KEY_PREFIX = 'chatmrpt:'
    
    # Redis configuration for session management
    REDIS_URL = os.environ.get(
        'REDIS_URL',
        'redis://chatmrpt-redis-staging.1b3pmt.0001.use2.cache.amazonaws.com:6379/0'
    )
    SESSION_REDIS = None  # Will be set in app initialization
    
    # Database configuration
    SQLALCHEMY_DATABASE_URI = os.environ.get(
        'DATABASE_URL',
        'sqlite:///instance/interactions.db'
    )
    if SQLALCHEMY_DATABASE_URI.startswith('postgres://'):
        SQLALCHEMY_DATABASE_URI = SQLALCHEMY_DATABASE_URI.replace(
            'postgres://', 'postgresql://', 1
        )
    
    # Connection pool settings for production load
    SQLALCHEMY_ENGINE_OPTIONS = {
        'pool_size': 20,
        'pool_recycle': 3600,
        'pool_pre_ping': True,
        'max_overflow': 40
    }
    SQLALCHEMY_TRACK_MODIFICATIONS = False
    
    # Increased limits for production
    MAX_CONTENT_LENGTH = 64 * 1024 * 1024  # 64MB for large datasets
    REQUEST_MAX_SIZE = 64 * 1024 * 1024
    DATA_UPLOAD_MAX_MEMORY_SIZE = 64 * 1024 * 1024
    
    # Performance optimizations
    SEND_FILE_MAX_AGE_DEFAULT = 31536000  # 1 year cache for static files
    JSONIFY_PRETTYPRINT_REGULAR = False
    JSON_SORT_KEYS = False
    
    # API Configuration
    OPENAI_API_KEY = os.environ.get('OPENAI_API_KEY')
    if not OPENAI_API_KEY:
        raise ValueError("OPENAI_API_KEY environment variable is not set!")
    
    # Request timeout settings
    OPENAI_TIMEOUT = 120  # 2 minutes for complex analyses
    ANALYSIS_TIMEOUT = 300  # 5 minutes for full analysis
    
    # Worker configuration guidance
    WORKERS_PER_CORE = 2
    MAX_WORKERS = 8
    WORKER_CLASS = 'sync'
    WORKER_CONNECTIONS = 1000
    KEEPALIVE = 5
    
    # Logging configuration
    LOG_LEVEL = os.environ.get('LOG_LEVEL', 'INFO')
    LOG_FILE = os.environ.get('LOG_FILE', 'instance/app.log')
    LOG_TO_STDOUT = os.environ.get('LOG_TO_STDOUT', 'true').lower() == 'true'
    
    # Monitoring and health checks
    HEALTH_CHECK_INTERVAL = 30  # seconds
    METRICS_ENABLED = True
    
    # Rate limiting (requests per minute)
    RATELIMIT_ENABLED = True
    RATELIMIT_STORAGE_URL = REDIS_URL
    RATELIMIT_DEFAULT = "200 per minute"
    RATELIMIT_HEADERS_ENABLED = True
    
    # CORS settings for production
    CORS_ORIGINS = os.environ.get(
        'CORS_ORIGINS',
        'https://d225ar6c86586s.cloudfront.net'
    ).split(',')
    
    # Security headers for production
    SECURITY_HEADERS = {
        'X-Content-Type-Options': 'nosniff',
        'X-Frame-Options': 'SAMEORIGIN',
        'X-XSS-Protection': '1; mode=block',
        'Referrer-Policy': 'strict-origin-when-cross-origin',
        'Permissions-Policy': 'geolocation=(), microphone=(), camera=()',
        'Content-Security-Policy': (
            "default-src 'self' blob:; "
            "script-src 'self' 'unsafe-inline' 'unsafe-eval' "
            "https://unpkg.com https://cdn.jsdelivr.net https://cdn.plot.ly blob:; "
            "style-src 'self' 'unsafe-inline' "
            "https://unpkg.com https://cdn.jsdelivr.net "
            "https://cdnjs.cloudflare.com https://fonts.googleapis.com; "
            "img-src 'self' data: https: blob:; "
            "font-src 'self' https://fonts.gstatic.com https://cdnjs.cloudflare.com; "
            "frame-src 'self' http://chatmrpt-staging-alb-752380251.us-east-2.elb.amazonaws.com "
            "https://d225ar6c86586s.cloudfront.net; "
            "connect-src 'self' https://*.tile.openstreetmap.org "
            "https://tile.openstreetmap.org https://*.tiles.mapbox.com "
            "https://api.mapbox.com wss: ws:; "
            "worker-src 'self' blob:;"
        )
    }
    
    # Cache configuration
    CACHE_TYPE = 'redis'
    CACHE_REDIS_URL = REDIS_URL
    CACHE_DEFAULT_TIMEOUT = 300
    CACHE_KEY_PREFIX = 'chatmrpt-cache:'
    
    # Admin configuration
    ADMIN_KEY = os.environ.get('ADMIN_KEY')
    if not ADMIN_KEY:
        raise ValueError("ADMIN_KEY environment variable is not set!")
    
    # Feature flags for gradual rollout
    FEATURE_FLAGS = {
        'enhanced_monitoring': True,
        'redis_sessions': True,
        'rate_limiting': True,
        'advanced_caching': True,
        'performance_profiling': os.environ.get('ENABLE_PROFILING', 'false').lower() == 'true'
    }
    
    # AWS configuration
    AWS_REGION = 'us-east-2'
    AWS_ACCESS_KEY_ID = os.environ.get('AWS_ACCESS_KEY_ID')
    AWS_SECRET_ACCESS_KEY = os.environ.get('AWS_SECRET_ACCESS_KEY')
    
    # Alert thresholds
    ALERT_THRESHOLDS = {
        'memory_percent': 80,
        'disk_percent': 85,
        'response_time_seconds': 2,
        'error_rate_percent': 1,
        'worker_minimum': 4
    }
    
    @classmethod
    def init_app(cls, app):
        """Initialize app with production settings."""
        BaseConfig.init_app(app)
        
        # Set up production logging
        import logging
        from logging.handlers import RotatingFileHandler
        
        if not app.debug and not app.testing:
            # Set up file handler
            if not os.path.exists('instance'):
                os.makedirs('instance')
            
            file_handler = RotatingFileHandler(
                cls.LOG_FILE,
                maxBytes=10485760,  # 10MB
                backupCount=10
            )
            file_handler.setFormatter(logging.Formatter(
                '%(asctime)s %(levelname)s: %(message)s '
                '[in %(pathname)s:%(lineno)d]'
            ))
            file_handler.setLevel(getattr(logging, cls.LOG_LEVEL))
            app.logger.addHandler(file_handler)
            
            # Also log to stdout for CloudWatch
            if cls.LOG_TO_STDOUT:
                stream_handler = logging.StreamHandler()
                stream_handler.setLevel(getattr(logging, cls.LOG_LEVEL))
                app.logger.addHandler(stream_handler)
            
            app.logger.setLevel(getattr(logging, cls.LOG_LEVEL))
            app.logger.info('ChatMRPT Production Transition startup')