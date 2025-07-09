# -*- coding: utf-8 -*-
# app/__init__.py
import os
import logging
from logging.handlers import RotatingFileHandler
from flask import Flask, session as flask_session, request
from flask_session import Session
from flask_login import LoginManager
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# Import blueprints and other necessary components
from .core.llm_manager import LLMManager
from .services.container import init_services

# --- Configuration ---
INSTANCE_FOLDER_PATH = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), 'instance')

# Define UPLOAD_FOLDER and REPORTS_FOLDER relative to the instance path
UPLOAD_FOLDER = os.path.join(INSTANCE_FOLDER_PATH, 'uploads')
REPORTS_FOLDER = os.path.join(INSTANCE_FOLDER_PATH, 'reports')
LOG_FILE = os.path.join(INSTANCE_FOLDER_PATH, 'app.log')
INTERACTIONS_DB_FILE = os.path.join(INSTANCE_FOLDER_PATH, 'interactions.db')

# Ensure instance folder and subdirectories exist
os.makedirs(INSTANCE_FOLDER_PATH, exist_ok=True)
os.makedirs(UPLOAD_FOLDER, exist_ok=True)
os.makedirs(REPORTS_FOLDER, exist_ok=True)

# Files and extensions to exclude from Werkzeug reloader to prevent restarts on writes
RELOADER_EXCLUSIONS = [
    os.path.relpath(UPLOAD_FOLDER, os.getcwd()) + os.path.sep + '*',
    os.path.relpath(REPORTS_FOLDER, os.getcwd()) + os.path.sep + '*',
    os.path.relpath(LOG_FILE, os.getcwd()),
    os.path.relpath(INTERACTIONS_DB_FILE, os.getcwd()),
    "*.zip", "*.csv", "*.json", "*.png", "*.html", "*.xlsx", "*.xls",
    "*.shp", "*.dbf", "*.shx", "*.prj", "*.cpg", "*.geojson", "*.pdf", "*.docx", "*.md"
]

# --- Logging Setup ---
def setup_logging(app):
    """Configure logging for the application."""
    if not app.debug or os.environ.get('WERKZEUG_RUN_MAIN') == 'true':
        # Basic configuration
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        )

        # File handler for app.log in instance folder
        file_handler = RotatingFileHandler(
            LOG_FILE, 
            maxBytes=1024 * 1024 * 5, 
            backupCount=5
        )
        file_handler.setFormatter(logging.Formatter(
            '%(asctime)s %(levelname)s: %(message)s [in %(pathname)s:%(lineno)d]'
        ))
        file_handler.setLevel(logging.INFO)
        
        # Clear existing handlers on app.logger to prevent duplicates if reloaded
        app.logger.handlers.clear()
        app.logger.addHandler(file_handler)
        
        # Set level for app.logger
        app.logger.setLevel(logging.INFO)

        # Log configuration information
        app.logger.info('Flask app configured and logging set up.')
        app.logger.info('Instance path: %s', INSTANCE_FOLDER_PATH)
        app.logger.info('Upload folder: %s', UPLOAD_FOLDER)
        app.logger.info('Reports folder: %s', REPORTS_FOLDER)
        app.logger.info('Log file: %s', LOG_FILE)
        app.logger.info('RELOADER_EXCLUSIONS configured')


def create_app(config_name=None):
    """
    Application factory function using modern configuration system.
    """
    app = Flask(__name__)

    # --- Load Configuration ---
    from .config import get_config
    config_class = get_config(config_name)
    app.config.from_object(config_class)
    
    # Initialize configuration
    config_class.init_app(app)

    # --- Initialize Extensions ---
    Session(app)
    
    # Initialize Flask-Login
    login_manager = LoginManager()
    login_manager.init_app(app)
    login_manager.login_view = 'auth.login'
    login_manager.login_message = 'Please log in to access this page.'
    
    # User loader callback
    @login_manager.user_loader
    def load_user(user_id):
        from .auth.models import User
        return User.get(user_id)
    
    # --- Initialize Database ---
    # NOTE: Database initialization handled by interaction logging system
    # from .utils.database import init_db
    # try:
    #     init_db()
    #     app.logger.info("Database initialized successfully")
    # except Exception as e:
    #     app.logger.error("Error initializing database: {}".format(str(e)))
    
    # --- Initialize Modern Service Container ---
    from .services.container import init_services
    
    # Check for tool scoring configuration
    tool_scoring_disabled = os.environ.get('DISABLE_TOOL_SCORING', 'false').lower() == 'true'
    if tool_scoring_disabled:
        app.logger.info("🚫 Tool scoring disabled for faster startup")
    
    init_services(app)
    
    # --- Memory System Removed ---
    # Using simpler in-memory conversation tracking in request_interpreter.py
    # Complex unified memory system removed for simplicity and performance
    if config_name == 'production':
        app.logger.info("🧠 Using lightweight conversation tracking")
    else:
        app.logger.info("🚀 Development mode - Using in-memory conversation tracking")
    
    # --- Register Blueprints ---
    from .web import admin_bp, register_all_blueprints
    from .auth.routes import auth
    
    # Register auth blueprint
    app.register_blueprint(auth)
    
    # Register admin blueprint separately
    app.register_blueprint(admin_bp)
    
    # Register all functional route blueprints (core, upload, analysis, visualization, etc.)
    register_all_blueprints(app)
    
    # --- Initialize Additional Routes ---
    from .routes import init_routes
    init_routes(app)
    
    # Log startup information
    app.logger.info("ChatMRPT v3.0 - Modern Architecture Initialized")
    app.logger.info("Configuration: %s", config_class.__name__)
    services_status = 'Available' if hasattr(app, 'services') else 'Not Available'
    app.logger.info("Services: %s", services_status)

    return app
