# app/__init__.py
import os
import logging
from logging.handlers import RotatingFileHandler
from flask import Flask, session as flask_session, request # <<< ADDED request HERE
from flask_session import Session # For server-side sessions
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# Import blueprints and other necessary components
# Ensure these imports are correct based on your project structure
from .routes import main as main_blueprint
from .models.interaction_logger import InteractionLogger
from .ai_utils import LLMManager # Assuming LLMManager is in ai_utils
from .services.service_container import init_services  # Import the service initialization function

# --- Configuration ---
# Determine the instance path. This should be outside the app package.
# If your app is at C:\Users\bbofo\OneDrive\Desktop\MRPT\ChatMRPT_v1\app
# Then instance_path will be C:\Users\bbofo\OneDrive\Desktop\MRPT\ChatMRPT_v1\instance
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
# This list is crucial for stable development when files are generated.
RELOADER_EXCLUSIONS = [
    os.path.relpath(UPLOAD_FOLDER, os.getcwd()) + os.path.sep + '*', # Exclude everything in uploads
    os.path.relpath(REPORTS_FOLDER, os.getcwd()) + os.path.sep + '*', # Exclude everything in reports
    os.path.relpath(LOG_FILE, os.getcwd()),
    os.path.relpath(INTERACTIONS_DB_FILE, os.getcwd()),
    "*.zip", "*.csv", "*.json", "*.png", "*.html", "*.xlsx", "*.xls",
    "*.shp", "*.dbf", "*.shx", "*.prj", "*.cpg", "*.geojson", "*.pdf", "*.docx", "*.md"
]

# --- Logging Setup ---
def setup_logging(app):
    # Configure logging only when not in debug mode with Werkzeug's reloader,
    # or when it's the main Werkzeug process. This prevents duplicate log entries.
    if not app.debug or os.environ.get('WERKZEUG_RUN_MAIN') == 'true':
        # Basic configuration
        logging.basicConfig(level=logging.INFO,
                            format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')

        # File handler for app.log in instance folder
        file_handler = RotatingFileHandler(LOG_FILE, maxBytes=1024 * 1024 * 5, backupCount=5) # 5MB per file
        file_handler.setFormatter(logging.Formatter(
            '%(asctime)s %(levelname)s: %(message)s [in %(pathname)s:%(lineno)d]'
        ))
        file_handler.setLevel(logging.INFO)
        
        # Clear existing handlers on app.logger to prevent duplicates if reloaded
        app.logger.handlers.clear()
        app.logger.addHandler(file_handler)
        
        # Set level for app.logger
        app.logger.setLevel(logging.INFO)
        
        # Optionally, configure the root logger if you want other libraries to also log to this file
        # Be careful with this as it can make logs very verbose.
        # root_logger = logging.getLogger()
        # root_logger.addHandler(file_handler)
        # root_logger.setLevel(logging.INFO) # Or your desired global level

        app.logger.info('Flask app configured and logging set up.')
        app.logger.info(f'Instance path: {INSTANCE_FOLDER_PATH}')
        app.logger.info(f'Upload folder: {UPLOAD_FOLDER}')
        app.logger.info(f'Reports folder: {REPORTS_FOLDER}')
        app.logger.info(f'Log file: {LOG_FILE}')
        app.logger.info(f'Effective RELOADER_EXCLUSIONS set in app.config: {app.config.get("RELOADER_EXCLUSIONS")}')


def create_app(config_class=None):
    """
    Application factory function.
    """
    app = Flask(__name__, instance_path=INSTANCE_FOLDER_PATH, instance_relative_config=True)

    # --- Load Configuration ---
    # Default configuration (can be a class or a dictionary)
    app.config.from_mapping(
        SECRET_KEY=os.environ.get('SECRET_KEY', 'dev_secret_key_please_change_this'), # Default for development
        SESSION_TYPE='filesystem', # Server-side sessions
        SESSION_FILE_DIR=os.path.join(INSTANCE_FOLDER_PATH, 'flask_session'),
        SESSION_PERMANENT=False,
        SESSION_USE_SIGNER=True,
        UPLOAD_FOLDER=UPLOAD_FOLDER,
        REPORTS_FOLDER=REPORTS_FOLDER,
        LOG_FILE=LOG_FILE,
        INTERACTIONS_DB_FILE=INTERACTIONS_DB_FILE,  # Add this for service container
        MAX_CONTENT_LENGTH=32 * 1024 * 1024,  # 32 MB upload limit
        OPENAI_API_KEY=os.environ.get('OPENAI_API_KEY'),
        OPENAI_MODEL_NAME=os.environ.get('OPENAI_MODEL_NAME', 'gpt-4o'), # Default model
        DEBUG=os.environ.get('FLASK_DEBUG', 'True').lower() == 'true',
        RELOADER_EXCLUSIONS=list(set(RELOADER_EXCLUSIONS)) # Ensure unique items
    )

    # Load instance config if it exists (e.g., instance/config.py)
    # This allows overriding defaults or adding production secrets without modifying core code.
    # Do this before setting up logging, so logging can use instance config if needed.
    if os.path.exists(os.path.join(app.instance_path, 'config.py')):
        try:
            app.config.from_pyfile('config.py', silent=False)
            # Use app.logger only after it's configured by setup_logging
            # Initial log for this can be a print or handled later.
            print("Successfully loaded instance/config.py.")
        except Exception as e:
            print(f"Error loading instance/config.py: {e}")
    else:
        print("instance/config.py not found, using default and environment configurations.")


    # --- Initialize Logging ---
    # Call logging setup after config is loaded (especially app.debug)
    setup_logging(app)


    # --- Initialize Extensions and Services ---
    # Server-side session management
    Session(app)
    
    # Initialize Interaction Logger
    # Use the path from app.config so it's consistent with the rest of the app
    interaction_db_path = INTERACTIONS_DB_FILE
    try:
        interaction_logger = InteractionLogger(db_path=interaction_db_path)
        app.config['INTERACTION_LOGGER'] = interaction_logger
        app.logger.info(f"InteractionLogger initialized with database at {interaction_db_path}")
        app.logger.info("InteractionLogger stored in app config")
    except ImportError: # Be more specific if possible
        app.logger.error("InteractionLogger class not found or import error.")
        app.config['INTERACTION_LOGGER'] = None
    except Exception as e:
        app.logger.error(f"Failed to initialize InteractionLogger: {e}")
        app.config['INTERACTION_LOGGER'] = None


    # Initialize LLM Manager
    try:
        llm_manager = LLMManager(
            api_key=app.config['OPENAI_API_KEY'],
            model=app.config['OPENAI_MODEL_NAME'], # CHANGED model_name to model
            interaction_logger=app.config.get('INTERACTION_LOGGER') # Pass the logger
        )
        app.config['LLM_MANAGER'] = llm_manager
        app.logger.info(f"LLMManager initialized with model: {app.config['OPENAI_MODEL_NAME']}")
    except Exception as e:
        app.logger.error(f"Failed to initialize LLMManager: {e}. AI features might be unavailable.")
        app.config['LLM_MANAGER'] = None


    # Ensure upload and report directories exist (again, for safety, if instance config changed paths)
    try:
        os.makedirs(app.config['UPLOAD_FOLDER'], exist_ok=True)
        os.makedirs(app.config['REPORTS_FOLDER'], exist_ok=True)
        # Ensure flask_session directory exists
        os.makedirs(app.config['SESSION_FILE_DIR'], exist_ok=True)
        app.logger.info("Ensured upload, report, and session directories exist.")
    except OSError as e:
        app.logger.error(f"Error creating UPLOAD_FOLDER/REPORTS_FOLDER/SESSION_FILE_DIR: {e}")

    # Initialize service container
    init_services(app)
    app.logger.info("Service container initialized")

    # --- Register Blueprints ---
    app.register_blueprint(main_blueprint)
    app.logger.info("Registered main blueprint.")

    # --- Error Handlers ---
    @app.errorhandler(404)
    def not_found_error(error):
        # Log the error if desired
        app.logger.warning(f"404 Not Found: {error} for URL {request.url}") # request is now defined
        # return render_template('404.html'), 404 # Create a 404.html template
        return "<h1>404 Not Found</h1><p>The page you are looking for does not exist.</p>", 404

    @app.errorhandler(500)
    def internal_error(error):
        # Log the error
        app.logger.error(f"500 Internal Server Error: {error} for URL {request.url}", exc_info=True) # request is now defined
        # db.session.rollback() # If using SQLAlchemy
        # return render_template('500.html'), 500 # Create a 500.html template
        return "<h1>500 Internal Server Error</h1><p>An unexpected error occurred. Please try again later.</p>", 500

    app.logger.info("Error handlers registered.")
    app.logger.info("App creation complete.")

    return app