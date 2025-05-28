"""
Database utility functions to handle connections and configurations.
"""

import os
import sqlite3
import logging
from urllib.parse import urlparse

logger = logging.getLogger(__name__)

def get_db_config():
    """
    Get database configuration based on environment.
    Supports both SQLite (default) and PostgreSQL (for production on Render).
    
    Returns:
        dict: Database configuration dictionary
    """
    # Check for PostgreSQL DATABASE_URL (provided by Render)
    database_url = os.environ.get('DATABASE_URL')
    
    if database_url and database_url.startswith('postgres'):
        # Parse the DATABASE_URL
        logger.info("Using PostgreSQL database configuration")
        return {
            'type': 'postgresql',
            'url': database_url
        }
    else:
        # Default to SQLite
        logger.info("Using SQLite database configuration")
        instance_path = os.environ.get('INSTANCE_PATH', 'instance')
        os.makedirs(instance_path, exist_ok=True)
        
        return {
            'type': 'sqlite',
            'path': os.path.join(instance_path, 'chatmrpt.db')
        }

def get_db_connection():
    """
    Get a database connection based on the configuration.
    
    Returns:
        Connection object to the database
    """
    config = get_db_config()
    
    if config['type'] == 'postgresql':
        try:
            import psycopg2
            conn = psycopg2.connect(config['url'])
            logger.info("Connected to PostgreSQL database")
            return conn
        except ImportError:
            logger.error("psycopg2 not installed. Cannot connect to PostgreSQL.")
            raise
        except Exception as e:
            logger.error(f"Error connecting to PostgreSQL: {e}")
            raise
    else:
        try:
            conn = sqlite3.connect(config['path'])
            conn.row_factory = sqlite3.Row
            logger.info(f"Connected to SQLite database at {config['path']}")
            return conn
        except Exception as e:
            logger.error(f"Error connecting to SQLite: {e}")
            raise

def init_db():
    """
    Initialize the database with required tables.
    """
    conn = get_db_connection()
    cursor = conn.cursor()
    
    # Create tables if they don't exist
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS chats (
        id TEXT PRIMARY KEY,
        title TEXT,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )
    ''')
    
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS messages (
        id TEXT PRIMARY KEY,
        chat_id TEXT,
        content TEXT,
        role TEXT,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        FOREIGN KEY (chat_id) REFERENCES chats (id)
    )
    ''')
    
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS visualizations (
        id TEXT PRIMARY KEY,
        chat_id TEXT,
        data TEXT,
        type TEXT,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        FOREIGN KEY (chat_id) REFERENCES chats (id)
    )
    ''')
    
    conn.commit()
    conn.close()
    logger.info("Database initialized with required tables") 