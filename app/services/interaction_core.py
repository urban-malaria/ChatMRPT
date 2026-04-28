# app/interaction/core.py
"""
Core Interaction Logger - Database and Session Management

This module contains the core database management and session handling
functionality extracted from the monolithic InteractionLogger class.

Functions:
- DatabaseManager: Core database operations and schema management
- SessionManager: Session lifecycle management
- Basic logging functions
"""

import os
import json
import logging
import datetime
import sqlite3
import uuid
from typing import Dict, List, Any, Optional, Union

# Set up logging
logger = logging.getLogger(__name__)


class DatabaseManager:
    """Manages database initialization and connection handling"""
    
    def __init__(self, db_path=None):
        """
        Initialize with database path
        
        Args:
            db_path: Path to SQLite database (defaults to instance/interactions.db)
        """
        if db_path is None:
            try:
                # Use instance folder by default (instance/interactions.db)
                from flask import current_app
                self.db_path = os.path.join(current_app.instance_path, 'interactions.db')
            except RuntimeError:
                # Running outside Flask context (e.g., during testing)
                self.db_path = os.path.join('instance', 'interactions.db')
        else:
            self.db_path = db_path
        
        # Ensure the database exists and has the correct schema
        self.init_database()
    
    def init_database(self):
        """Initialize the database with required tables if they don't exist"""
        try:
            # Ensure parent directory exists - check if path is not empty
            if self.db_path and os.path.dirname(self.db_path):
                os.makedirs(os.path.dirname(self.db_path), exist_ok=True)
            
            # Connect to database and create tables if they don't exist
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            # Create sessions table
            cursor.execute('''
            CREATE TABLE IF NOT EXISTS sessions (
                session_id TEXT PRIMARY KEY,
                start_time TIMESTAMP,
                last_activity TIMESTAMP,
                user_language TEXT,
                browser_info TEXT,
                ip_address TEXT,
                analysis_complete BOOLEAN DEFAULT 0,
                files_uploaded BOOLEAN DEFAULT 0
            )
            ''')
            
            # Create messages table
            cursor.execute('''
            CREATE TABLE IF NOT EXISTS messages (
                message_id TEXT PRIMARY KEY,
                session_id TEXT,
                timestamp TIMESTAMP,
                sender TEXT,
                content TEXT,
                intent TEXT,
                entities TEXT,
                FOREIGN KEY (session_id) REFERENCES sessions (session_id)
            )
            ''')
            
            # Create file_uploads table
            cursor.execute('''
            CREATE TABLE IF NOT EXISTS file_uploads (
                upload_id TEXT PRIMARY KEY,
                session_id TEXT,
                timestamp TIMESTAMP,
                file_type TEXT,
                file_name TEXT,
                file_size INTEGER,
                metadata TEXT,
                FOREIGN KEY (session_id) REFERENCES sessions (session_id)
            )
            ''')
            
            # Create analysis_events table
            cursor.execute('''
            CREATE TABLE IF NOT EXISTS analysis_events (
                event_id TEXT PRIMARY KEY,
                session_id TEXT,
                timestamp TIMESTAMP,
                event_type TEXT,
                details TEXT,
                success BOOLEAN,
                FOREIGN KEY (session_id) REFERENCES sessions (session_id)
            )
            ''')
            
            # Create errors table
            cursor.execute('''
            CREATE TABLE IF NOT EXISTS errors (
                error_id TEXT PRIMARY KEY,
                session_id TEXT,
                timestamp TIMESTAMP,
                error_type TEXT,
                error_message TEXT,
                stack_trace TEXT,
                FOREIGN KEY (session_id) REFERENCES sessions (session_id)
            )
            ''')
            
            # Create analysis_steps table - tracks each step in the analysis pipeline
            cursor.execute('''
            CREATE TABLE IF NOT EXISTS analysis_steps (
                step_id TEXT PRIMARY KEY,
                session_id TEXT,
                timestamp TIMESTAMP,
                step_name TEXT,
                input_summary TEXT,
                output_summary TEXT,
                algorithm TEXT,
                parameters TEXT,
                execution_time REAL,
                error TEXT,
                FOREIGN KEY (session_id) REFERENCES sessions (session_id)
            )
            ''')
            
            # Create algorithm_decisions table - tracks decisions made during analysis
            cursor.execute('''
            CREATE TABLE IF NOT EXISTS algorithm_decisions (
                decision_id TEXT PRIMARY KEY,
                step_id TEXT,
                session_id TEXT,
                timestamp TIMESTAMP,
                decision_type TEXT,
                options TEXT,
                criteria TEXT,
                selected_option TEXT,
                confidence REAL,
                FOREIGN KEY (session_id) REFERENCES sessions (session_id),
                FOREIGN KEY (step_id) REFERENCES analysis_steps (step_id)
            )
            ''')
            
            # Create calculations table - tracks individual calculations
            cursor.execute('''
            CREATE TABLE IF NOT EXISTS calculations (
                calculation_id TEXT PRIMARY KEY,
                step_id TEXT,
                session_id TEXT,
                timestamp TIMESTAMP,
                variable TEXT,
                operation TEXT,
                input_values TEXT,
                output_value TEXT,
                context TEXT,
                FOREIGN KEY (session_id) REFERENCES sessions (session_id),
                FOREIGN KEY (step_id) REFERENCES analysis_steps (step_id)
            )
            ''')
            
            # Create anomalies table - tracks detected anomalies in data
            cursor.execute('''
            CREATE TABLE IF NOT EXISTS anomalies (
                anomaly_id TEXT PRIMARY KEY,
                session_id TEXT,
                timestamp TIMESTAMP,
                entity_name TEXT,
                anomaly_type TEXT,
                expected_value TEXT,
                actual_value TEXT,
                significance TEXT,
                context TEXT,
                FOREIGN KEY (session_id) REFERENCES sessions (session_id)
            )
            ''')
            
            # Create variable_relationships table - tracks determined variable relationships
            cursor.execute('''
            CREATE TABLE IF NOT EXISTS variable_relationships (
                relationship_id TEXT PRIMARY KEY,
                session_id TEXT,
                timestamp TIMESTAMP,
                variable_name TEXT,
                relationship_type TEXT,
                evidence TEXT,
                confidence_score REAL,
                FOREIGN KEY (session_id) REFERENCES sessions (session_id)
            )
            ''')
            
            # Create ward_rankings table - tracks detailed ward ranking information
            cursor.execute('''
            CREATE TABLE IF NOT EXISTS ward_rankings (
                ranking_id TEXT PRIMARY KEY,
                session_id TEXT,
                timestamp TIMESTAMP,
                ward_name TEXT,
                overall_rank INTEGER,
                median_score REAL,
                vulnerability_category TEXT,
                contributing_factors TEXT,
                anomaly_flags TEXT,
                FOREIGN KEY (session_id) REFERENCES sessions (session_id)
            )
            ''')
            
            # Create visualization_metadata table - tracks context about visualizations
            cursor.execute('''
            CREATE TABLE IF NOT EXISTS visualization_metadata (
                viz_id TEXT PRIMARY KEY,
                session_id TEXT,
                timestamp TIMESTAMP,
                viz_type TEXT,
                variables_used TEXT,
                data_summary TEXT,
                visual_elements TEXT,
                patterns_detected TEXT,
                FOREIGN KEY (session_id) REFERENCES sessions (session_id)
            )
            ''')
            
            # Create llm_interactions table - tracks interactions with the LLM
            cursor.execute('''
            CREATE TABLE IF NOT EXISTS llm_interactions (
                interaction_id TEXT PRIMARY KEY,
                session_id TEXT,
                timestamp TIMESTAMP,
                prompt_type TEXT,
                prompt TEXT,
                prompt_context TEXT,
                response TEXT,
                tokens_used INTEGER,
                latency REAL,
                enhanced_timing TEXT,
                FOREIGN KEY (session_id) REFERENCES sessions (session_id)
            )
            ''')
            
            # Add enhanced_timing column if it doesn't exist (for existing databases)
            try:
                cursor.execute('''
                ALTER TABLE llm_interactions ADD COLUMN enhanced_timing TEXT
                ''')
            except sqlite3.OperationalError:
                # Column already exists, which is fine
                pass
            
            # Create explanations table - tracks explanations generated for users
            cursor.execute('''
            CREATE TABLE IF NOT EXISTS explanations (
                explanation_id TEXT PRIMARY KEY,
                session_id TEXT,
                timestamp TIMESTAMP,
                entity_type TEXT,
                entity_name TEXT,
                question_type TEXT,
                question TEXT,
                explanation TEXT,
                context_used TEXT,
                llm_interaction_id TEXT,
                FOREIGN KEY (session_id) REFERENCES sessions (session_id),
                FOREIGN KEY (llm_interaction_id) REFERENCES llm_interactions (interaction_id)
            )
            ''')

            # Create routing_decisions table - tracks routing decisions for each message
            cursor.execute('''
            CREATE TABLE IF NOT EXISTS routing_decisions (
                routing_id TEXT PRIMARY KEY,
                message_id TEXT,
                session_id TEXT,
                timestamp TIMESTAMP,
                user_message TEXT,
                routing_decision TEXT,
                routing_method TEXT,
                keywords_matched TEXT,
                confidence REAL,
                ollama_reasoning TEXT,
                session_context TEXT,
                FOREIGN KEY (session_id) REFERENCES sessions (session_id),
                FOREIGN KEY (message_id) REFERENCES messages (message_id)
            )
            ''')

            conn.commit()
            conn.close()
            logger.info(f"Interaction database initialized at {self.db_path}")
            
        except Exception as e:
            logger.error(f"Error initializing interaction database: {str(e)}", exc_info=True)
    
    def get_connection(self):
        """Get a database connection"""
        return sqlite3.connect(self.db_path)


class SessionManager:
    """Manages session lifecycle and status tracking"""
    
    def __init__(self, db_manager: DatabaseManager):
        """Initialize with database manager"""
        self.db_manager = db_manager
    
    def log_session_start(self, session_id, browser_info=None, ip_address=None, language=None):
        """Log the start of a new session"""
        try:
            conn = self.db_manager.get_connection()
            cursor = conn.cursor()
            
            now = datetime.datetime.now()
            
            # Use provided language or default to 'en'
            # Don't try to access Flask session here as it might not be available
            if language is None:
                language = 'en'
            
            cursor.execute('''
            INSERT OR REPLACE INTO sessions 
            (session_id, start_time, last_activity, user_language, browser_info, ip_address)
            VALUES (?, ?, ?, ?, ?, ?)
            ''', (session_id, now, now, language, browser_info, ip_address))
            
            conn.commit()
            conn.close()
            logger.info(f"Logged session start: {session_id}")
            
            return True
        except Exception as e:
            logger.error(f"Error logging session start: {str(e)}", exc_info=True)
            return False
    
    def update_session_language(self, session_id, language):
        """Update the language preference for a session"""
        try:
            conn = self.db_manager.get_connection()
            cursor = conn.cursor()
            
            now = datetime.datetime.now()
            
            cursor.execute('''
            UPDATE sessions 
            SET user_language = ?, last_activity = ? 
            WHERE session_id = ?
            ''', (language, now, session_id))
            
            conn.commit()
            conn.close()
            logger.info(f"Updated language to {language} for session: {session_id}")
            
            return True
        except Exception as e:
            logger.error(f"Error updating session language: {str(e)}", exc_info=True)
            return False
    
    def update_session_status(self, session_id, analysis_complete=None, files_uploaded=None):
        """Update the status flags for a session"""
        try:
            conn = self.db_manager.get_connection()
            cursor = conn.cursor()
            
            now = datetime.datetime.now()
            updates = ['last_activity = ?']
            params = [now]
            
            if analysis_complete is not None:
                updates.append('analysis_complete = ?')
                params.append(1 if analysis_complete else 0)
                
            if files_uploaded is not None:
                updates.append('files_uploaded = ?')
                params.append(1 if files_uploaded else 0)
                
            params.append(session_id)
            
            query = f'''
            UPDATE sessions 
            SET {', '.join(updates)}
            WHERE session_id = ?
            '''
            
            cursor.execute(query, params)
            
            conn.commit()
            conn.close()
            logger.info(f"Updated session status for: {session_id}")
            
            return True
        except Exception as e:
            logger.error(f"Error updating session status: {str(e)}", exc_info=True)
            return False
    
    def update_last_activity(self, session_id):
        """Update the last activity timestamp for a session"""
        try:
            conn = self.db_manager.get_connection()
            cursor = conn.cursor()
            
            now = datetime.datetime.now()
            cursor.execute('''
            UPDATE sessions SET last_activity = ? WHERE session_id = ?
            ''', (now, session_id))
            
            conn.commit()
            conn.close()
            return True
        except Exception as e:
            logger.error(f"Error updating last activity: {str(e)}", exc_info=True)
            return False


# Core logging functions
def log_message(db_manager: DatabaseManager, session_id, sender, content, intent=None, entities=None):
    """
    Log a message exchange between user and assistant with enhanced metadata
    
    Args:
        db_manager: DatabaseManager instance
        session_id: Session identifier
        sender: 'user', 'assistant', or 'system'
        content: Message content
        intent: Detected intent for user messages (optional)
        entities: Extracted entities from user messages (optional)
    
    Returns:
        str: message_id or None if error
    """
    try:
        conn = db_manager.get_connection()
        cursor = conn.cursor()
        
        now = datetime.datetime.now()
        message_id = str(uuid.uuid4())
        
        # Convert entities to JSON string if provided
        entities_json = None
        if entities:
            if isinstance(entities, dict):
                entities_json = json.dumps(entities)
            else:
                entities_json = str(entities)
        
        cursor.execute('''
        INSERT INTO messages 
        (message_id, session_id, timestamp, sender, content, intent, entities)
        VALUES (?, ?, ?, ?, ?, ?, ?)
        ''', (message_id, session_id, now, sender, content, intent, entities_json))
        
        # Update last activity for the session
        cursor.execute('''
        UPDATE sessions SET last_activity = ? WHERE session_id = ?
        ''', (now, session_id))
        
        conn.commit()
        conn.close()
        logger.info(f"Logged {sender} message for session: {session_id}")
        
        return message_id
        
    except Exception as e:
        logger.error(f"Error logging message: {str(e)}", exc_info=True)
        return None


def log_error(db_manager: DatabaseManager, session_id, error_type, error_message, stack_trace=None):
    """Log an error event"""
    try:
        conn = db_manager.get_connection()
        cursor = conn.cursor()

        now = datetime.datetime.now()
        error_id = str(uuid.uuid4())

        cursor.execute('''
        INSERT INTO errors
        (error_id, session_id, timestamp, error_type, error_message, stack_trace)
        VALUES (?, ?, ?, ?, ?, ?)
        ''', (error_id, session_id, now, error_type, error_message, stack_trace))

        conn.commit()
        conn.close()
        logger.info(f"Logged error for session: {session_id}")

        return error_id

    except Exception as e:
        logger.error(f"Error logging error: {str(e)}", exc_info=True)
        return None


def log_routing_decision(
    db_manager: DatabaseManager,
    session_id: str,
    user_message: str,
    routing_decision: str,
    routing_method: str,
    keywords_matched: Optional[List[str]] = None,
    confidence: Optional[float] = None,
    ollama_reasoning: Optional[str] = None,
    session_context: Optional[Dict] = None,
    message_id: Optional[str] = None
):
    """
    Log a routing decision to the database.

    Args:
        db_manager: DatabaseManager instance
        session_id: Session identifier
        user_message: The user's message that triggered routing
        routing_decision: The decision made ("needs_tools", "can_answer", "needs_clarification")
        routing_method: How decision was made ("fast_track", "keyword_match", "ollama_decision")
        keywords_matched: List of keywords that matched (optional)
        confidence: Confidence score 0-1 from Ollama (optional)
        ollama_reasoning: Ollama's explanation (optional)
        session_context: Dictionary of session state (optional)
        message_id: ID of the message (optional, will generate if not provided)

    Returns:
        str: The routing_id of the logged decision
    """
    try:
        now = datetime.datetime.now()
        routing_id = str(uuid.uuid4())
        if message_id is None:
            message_id = str(uuid.uuid4())

        # Serialize JSON fields
        keywords_json = json.dumps(keywords_matched) if keywords_matched else None
        context_json = json.dumps(session_context) if session_context else None

        conn = db_manager.get_connection()
        cursor = conn.cursor()

        cursor.execute('''
        INSERT INTO routing_decisions (
            routing_id, message_id, session_id, timestamp, user_message,
            routing_decision, routing_method, keywords_matched, confidence,
            ollama_reasoning, session_context
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ''', (
            routing_id, message_id, session_id, now, user_message,
            routing_decision, routing_method, keywords_json, confidence,
            ollama_reasoning, context_json
        ))

        conn.commit()
        conn.close()

        logger.info(f"Logged routing decision: {routing_decision} via {routing_method}")
        return routing_id

    except Exception as e:
        logger.error(f"Error logging routing decision: {str(e)}", exc_info=True)
        return None


# Convenience class for backward compatibility
class InteractionCore:
    """
    Core interaction logging functionality
    Provides the foundational database and session management capabilities
    """
    
    def __init__(self, db_path=None):
        """Initialize core interaction logger"""
        self.db_manager = DatabaseManager(db_path)
        self.session_manager = SessionManager(self.db_manager)
    
    def log_session_start(self, session_id, browser_info=None, ip_address=None, language=None):
        """Log session start"""
        return self.session_manager.log_session_start(session_id, browser_info, ip_address, language)
    
    def update_session_language(self, session_id, language):
        """Update session language"""
        return self.session_manager.update_session_language(session_id, language)
    
    def update_session_status(self, session_id, analysis_complete=None, files_uploaded=None):
        """Update session status"""
        return self.session_manager.update_session_status(session_id, analysis_complete, files_uploaded)
    
    def log_message(self, session_id, sender, content, intent=None, entities=None):
        """Log a message"""
        return log_message(self.db_manager, session_id, sender, content, intent, entities)
    
    def log_error(self, session_id, error_type, error_message, stack_trace=None):
        """Log an error"""
        return log_error(self.db_manager, session_id, error_type, error_message, stack_trace)