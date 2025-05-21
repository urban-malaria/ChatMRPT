# app/models/interaction_logger.py
import os
import json
import logging
import datetime
from flask import current_app, session
import sqlite3
import uuid
import traceback
from typing import Dict, List, Any, Optional, Union
from app.utilities import convert_to_json_serializable
# Set up logging
logger = logging.getLogger(__name__)

class InteractionLogger:
   """
   Enhanced class to log and store user interactions, analysis steps, and explanations 
   for the MRPT AI Assistant. Supports comprehensive tracking for audit trails,
   explanation generation, and fine-tuning data collection.
   """
   
   def __init__(self, db_path=None):
       """
       Initialize with database path
       
       Args:
           db_path: Path to SQLite database (defaults to instance/interactions.db)
       """
       if db_path is None:
           # Use instance folder by default (instance/interactions.db)
           self.db_path = os.path.join(current_app.instance_path, 'interactions.db')
       else:
           self.db_path = db_path
           
       # Ensure the database exists and has the correct schema
       self._init_database()
   
   def _init_database(self):
       """Initialize the database with required tables if they don't exist"""
       try:
           # Ensure parent directory exists
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
           
           # New tables for enhanced tracking
           
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
               FOREIGN KEY (session_id) REFERENCES sessions (session_id)
           )
           ''')
           
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
           
           conn.commit()
           conn.close()
           logger.info(f"Interaction database initialized at {self.db_path}")
           
       except Exception as e:
           logger.error(f"Error initializing interaction database: {str(e)}", exc_info=True)
   
   def log_session_start(self, session_id, browser_info=None, ip_address=None):
       """Log the start of a new session"""
       try:
           conn = sqlite3.connect(self.db_path)
           cursor = conn.cursor()
           
           now = datetime.datetime.now()
           language = session.get('current_language', 'en')
           
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
           conn = sqlite3.connect(self.db_path)
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
           conn = sqlite3.connect(self.db_path)
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
   
   def log_message(self, session_id, sender, content, intent=None, entities=None):
       """
       Log a message exchange between user and assistant with enhanced metadata
       
       Args:
           session_id: Session identifier
           sender: 'user', 'assistant', or 'system'
           content: Message content
           intent: Detected intent for user messages (optional)
           entities: Extracted entities from user messages (optional)
       
       Returns:
           str: message_id or None if error
       """
       try:
           conn = sqlite3.connect(self.db_path)
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
   
   def log_file_upload(self, session_id, file_type, file_name, file_size, metadata=None):
       """
       Log a file upload event
       
       Args:
           session_id: Session identifier
           file_type: Type of file ('csv', 'shapefile', etc.)
           file_name: Name of the uploaded file
           file_size: Size of the file in bytes
           metadata: Optional dict of file metadata
           
       Returns:
           str: upload_id or None if error
       """
       try:
           conn = sqlite3.connect(self.db_path)
           cursor = conn.cursor()
           
           now = datetime.datetime.now()
           upload_id = str(uuid.uuid4())
           
           # Ensure metadata is in JSON format
           metadata_json = None
           if metadata:
               if isinstance(metadata, dict):
                   metadata_json = json.dumps(metadata)
               else:
                   metadata_json = str(metadata)
           
           cursor.execute('''
           INSERT INTO file_uploads 
           (upload_id, session_id, timestamp, file_type, file_name, file_size, metadata)
           VALUES (?, ?, ?, ?, ?, ?, ?)
           ''', (upload_id, session_id, now, file_type, file_name, file_size, metadata_json))
           
           # Update last activity and files_uploaded for the session
           cursor.execute('''
           UPDATE sessions 
           SET last_activity = ?, files_uploaded = 1 
           WHERE session_id = ?
           ''', (now, session_id))
           
           conn.commit()
           conn.close()
           logger.info(f"Logged file upload: {file_name} for session: {session_id}")
           
           return upload_id
           
       except Exception as e:
           logger.error(f"Error logging file upload: {str(e)}", exc_info=True)
           return None
   
   def log_analysis_event(self, session_id, event_type, details, success=True):
       """
       Log an analysis event (visualization, report generation, etc.)
       
       Args:
           session_id: Session identifier
           event_type: Type of event ('visualization', 'report', etc.)
           details: Dict or string with event details
           success: Whether the event was successful
           
       Returns:
           str: event_id or None if error
       """
       try:
           conn = sqlite3.connect(self.db_path)
           cursor = conn.cursor()
           
           now = datetime.datetime.now()
           event_id = str(uuid.uuid4())
           
           # Ensure details is in JSON format
           details_json = None
           if details:
               if isinstance(details, dict):
                   details_json = json.dumps(details)
               else:
                   details_json = str(details)
           
           cursor.execute('''
           INSERT INTO analysis_events 
           (event_id, session_id, timestamp, event_type, details, success)
           VALUES (?, ?, ?, ?, ?, ?)
           ''', (event_id, session_id, now, event_type, details_json, success))
           
           # Update last activity for the session
           cursor.execute('''
           UPDATE sessions SET last_activity = ? WHERE session_id = ?
           ''', (now, session_id))
           
           conn.commit()
           conn.close()
           logger.info(f"Logged {event_type} event for session: {session_id}, success: {success}")
           
           return event_id
           
       except Exception as e:
           logger.error(f"Error logging analysis event: {str(e)}", exc_info=True)
           return None
   
   def log_error(self, session_id, error_type, error_message, stack_trace=None):
       """
       Log an error that occurred during interaction
       
       Args:
           session_id: Session identifier
           error_type: Category of error
           error_message: Description of the error
           stack_trace: Optional stack trace for debugging
           
       Returns:
           str: error_id or None if error during logging
       """
       try:
           conn = sqlite3.connect(self.db_path)
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
           logger.info(f"Logged error: {error_type} for session: {session_id}")
           
           return error_id
           
       except Exception as e:
           logger.error(f"Error logging error event: {str(e)}", exc_info=True)
           return None
   
   # Enhanced analysis tracking methods
   
   def log_analysis_step(self, session_id, step_name, input_summary=None, output_summary=None, 
                        algorithm=None, parameters=None, execution_time=None, error=None):
       """
       Log an analysis pipeline step with detailed metadata
       
       Args:
           session_id: Session identifier
           step_name: Name of the analysis step
           input_summary: Dict summarizing input data
           output_summary: Dict summarizing output data
           algorithm: Algorithm or method used
           parameters: Parameters used in the algorithm
           execution_time: Time taken to execute (seconds)
           error: Error message if step failed
           
       Returns:
           str: step_id or None if error
       """
       try:
           conn = sqlite3.connect(self.db_path)
           cursor = conn.cursor()
           
           now = datetime.datetime.now()
           step_id = str(uuid.uuid4())
           
           # Convert dictionaries to JSON strings
           input_json = json.dumps(input_summary) if isinstance(input_summary, dict) else input_summary
           output_json = json.dumps(output_summary) if isinstance(output_summary, dict) else output_summary
           params_json = json.dumps(parameters) if isinstance(parameters, dict) else parameters
           
           cursor.execute('''
           INSERT INTO analysis_steps 
           (step_id, session_id, timestamp, step_name, input_summary, output_summary, 
            algorithm, parameters, execution_time, error)
           VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
           ''', (step_id, session_id, now, step_name, input_json, output_json, 
                algorithm, params_json, execution_time, error))
           
           # Update session last activity
           cursor.execute('''
           UPDATE sessions SET last_activity = ? WHERE session_id = ?
           ''', (now, session_id))
           
           conn.commit()
           conn.close()
           logger.info(f"Logged analysis step: {step_name} for session: {session_id}")
           
           return step_id
           
       except Exception as e:
           logger.error(f"Error logging analysis step: {str(e)}", exc_info=True)
           return None
   
   def log_algorithm_decision(self, session_id, step_id, decision_type, options=None, 
                            criteria=None, selected_option=None, confidence=None):
       """
       Log a decision made during analysis
       
       Args:
           session_id: Session identifier
           step_id: ID of the analysis step this decision is part of
           decision_type: Type of decision ('variable_selection', 'threshold', etc.)
           options: List or dict of options considered
           criteria: Criteria used for decision
           selected_option: The option that was selected
           confidence: Confidence score for the decision (0-1)
           
       Returns:
           str: decision_id or None if error
       """
       try:
           conn = sqlite3.connect(self.db_path)
           cursor = conn.cursor()
           
           now = datetime.datetime.now()
           decision_id = str(uuid.uuid4())
           
           # Convert to JSON strings
           options_json = json.dumps(options) if options is not None else None
           selected_json = json.dumps(selected_option) if not isinstance(selected_option, (str, int, float, bool, type(None))) else str(selected_option)
           
           cursor.execute('''
           INSERT INTO algorithm_decisions 
           (decision_id, step_id, session_id, timestamp, decision_type, 
            options, criteria, selected_option, confidence)
           VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
           ''', (decision_id, step_id, session_id, now, decision_type, 
                options_json, criteria, selected_json, confidence))
           
           conn.commit()
           conn.close()
           logger.info(f"Logged algorithm decision: {decision_type} for session: {session_id}")
           
           return decision_id
           
       except Exception as e:
           logger.error(f"Error logging algorithm decision: {str(e)}", exc_info=True)
           return None
   
   def log_calculation(self, session_id, step_id, variable, operation, 
                     input_values=None, output_value=None, context=None):
       """
       Log a calculation performed during analysis
       
       Args:
           session_id: Session identifier
           step_id: ID of the analysis step this calculation is part of
           variable: Variable being calculated
           operation: Operation performed (e.g., 'normalization', 'imputation')
           input_values: Dict or list of input values
           output_value: Result of the calculation
           context: Additional context about the calculation
           
       Returns:
           str: calculation_id or None if error
       """
       try:
           conn = sqlite3.connect(self.db_path)
           cursor = conn.cursor()
           
           now = datetime.datetime.now()
           calculation_id = str(uuid.uuid4())
           
           # Convert complex types to JSON strings
           input_json = json.dumps(input_values) if input_values is not None else None
           output_json = json.dumps(output_value) if not isinstance(output_value, (str, int, float, bool, type(None))) else str(output_value)
           context_json = json.dumps(context) if context is not None else None
           
           cursor.execute('''
           INSERT INTO calculations 
           (calculation_id, step_id, session_id, timestamp, variable, 
            operation, input_values, output_value, context)
           VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
           ''', (calculation_id, step_id, session_id, now, variable, 
                operation, input_json, output_json, context_json))
           
           conn.commit()
           conn.close()
           logger.debug(f"Logged calculation: {operation} on {variable} for session: {session_id}")
           
           return calculation_id
           
       except Exception as e:
           logger.error(f"Error logging calculation: {str(e)}", exc_info=True)
           return None
   
   def log_anomaly(self, session_id, entity_name, anomaly_type, expected_value=None, 
                 actual_value=None, significance=None, context=None):
       """
       Log an anomaly detected during analysis
       
       Args:
           session_id: Session identifier
           entity_name: Name of the entity with the anomaly (ward, variable)
           anomaly_type: Type of anomaly detected
           expected_value: What was expected
           actual_value: What was observed
           significance: Significance level ('high', 'medium', 'low')
           context: Additional context about the anomaly
           
       Returns:
           str: anomaly_id or None if error
       """
       try:
           conn = sqlite3.connect(self.db_path)
           cursor = conn.cursor()
           
           now = datetime.datetime.now()
           anomaly_id = str(uuid.uuid4())
           
           # Convert complex values to strings
           expected_str = json.dumps(expected_value) if not isinstance(expected_value, (str, int, float, bool, type(None))) else str(expected_value)
           actual_str = json.dumps(actual_value) if not isinstance(actual_value, (str, int, float, bool, type(None))) else str(actual_value)
           context_json = json.dumps(context) if context is not None else None
           
           cursor.execute('''
           INSERT INTO anomalies 
           (anomaly_id, session_id, timestamp, entity_name, anomaly_type, 
            expected_value, actual_value, significance, context)
           VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
           ''', (anomaly_id, session_id, now, entity_name, anomaly_type, 
                expected_str, actual_str, significance, context_json))
           
           conn.commit()
           conn.close()
           logger.info(f"Logged anomaly: {anomaly_type} for {entity_name} in session: {session_id}")
           
           return anomaly_id
           
       except Exception as e:
           logger.error(f"Error logging anomaly: {str(e)}", exc_info=True)
           return None
   
   def log_variable_relationship(self, session_id, variable_name, relationship_type, 
                               evidence=None, confidence_score=None):
       """
       Log a determined variable relationship
       
       Args:
           session_id: Session identifier
           variable_name: Name of the variable
           relationship_type: Type of relationship ('direct', 'inverse')
           evidence: Evidence supporting this relationship
           confidence_score: Confidence in the relationship (0-1)
           
       Returns:
           str: relationship_id or None if error
       """
       try:
           conn = sqlite3.connect(self.db_path)
           cursor = conn.cursor()
           
           now = datetime.datetime.now()
           relationship_id = str(uuid.uuid4())
           
           # Convert evidence to JSON if needed
           evidence_json = json.dumps(evidence) if not isinstance(evidence, (str, type(None))) else evidence
           
           cursor.execute('''
           INSERT INTO variable_relationships 
           (relationship_id, session_id, timestamp, variable_name, 
            relationship_type, evidence, confidence_score)
           VALUES (?, ?, ?, ?, ?, ?, ?)
           ''', (relationship_id, session_id, now, variable_name, 
                relationship_type, evidence_json, confidence_score))
           
           conn.commit()
           conn.close()
           logger.info(f"Logged variable relationship: {relationship_type} for {variable_name} in session: {session_id}")
           
           return relationship_id
           
       except Exception as e:
           logger.error(f"Error logging variable relationship: {str(e)}", exc_info=True)
           return None
   
   def log_ward_ranking(self, session_id, ward_name, overall_rank, median_score, 
                      vulnerability_category, contributing_factors=None, anomaly_flags=None):
       """
       Log detailed information about a ward's ranking
       
       Args:
           session_id: Session identifier
           ward_name: Name of the ward
           overall_rank: Rank among all wards (1, 2, 3, etc.)
           median_score: Median risk score
           vulnerability_category: Category ('High', 'Medium', 'Low')
           contributing_factors: Dict of factors influencing the ranking
           anomaly_flags: Any anomalies or special designations
           
       Returns:
           str: ranking_id or None if error
       """
       try:
           conn = sqlite3.connect(self.db_path)
           cursor = conn.cursor()
           
           now = datetime.datetime.now()
           ranking_id = str(uuid.uuid4())
           
           # Convert complex types to JSON
           factors_json = json.dumps(contributing_factors) if contributing_factors is not None else None
           flags_json = json.dumps(anomaly_flags) if anomaly_flags is not None else None
           
           cursor.execute('''
           INSERT INTO ward_rankings 
           (ranking_id, session_id, timestamp, ward_name, overall_rank, 
            median_score, vulnerability_category, contributing_factors, anomaly_flags)
           VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
           ''', (ranking_id, session_id, now, ward_name, overall_rank, 
                median_score, vulnerability_category, factors_json, flags_json))
           
           conn.commit()
           conn.close()
           logger.debug(f"Logged ward ranking: {ward_name} (Rank: {overall_rank}) in session: {session_id}")
           
           return ranking_id
           
       except Exception as e:
           logger.error(f"Error logging ward ranking: {str(e)}", exc_info=True)
           return None
   
   def log_visualization_metadata(self, session_id, viz_type, variables_used=None, 
                                data_summary=None, visual_elements=None, patterns_detected=None):
       """
       Log detailed metadata about a visualization
       
       Args:
           session_id: Session identifier
           viz_type: Type of visualization ('variable_map', 'composite_map', etc.)
           variables_used: List of variables in the visualization
           data_summary: Summary statistics of the data
           visual_elements: Description of visual encoding elements
           patterns_detected: Patterns detected in the visualization
           
       Returns:
           str: viz_id or None if error
       """
       try:
           conn = sqlite3.connect(self.db_path)
           cursor = conn.cursor()
           
           now = datetime.datetime.now()
           viz_id = str(uuid.uuid4())
           
           # Convert complex types to JSON
           vars_json = json.dumps(variables_used) if variables_used is not None else None
           summary_json = json.dumps(convert_to_json_serializable(data_summary)) if data_summary is not None else None
           elements_json = json.dumps(visual_elements) if visual_elements is not None else None
           patterns_json = json.dumps(patterns_detected) if patterns_detected is not None else None
           
           cursor.execute('''
           INSERT INTO visualization_metadata 
           (viz_id, session_id, timestamp, viz_type, variables_used, 
            data_summary, visual_elements, patterns_detected)
           VALUES (?, ?, ?, ?, ?, ?, ?, ?)
           ''', (viz_id, session_id, now, viz_type, vars_json, 
                summary_json, elements_json, patterns_json))
           
           conn.commit()
           conn.close()
           logger.info(f"Logged visualization metadata: {viz_type} in session: {session_id}")
           
           return viz_id
           
       except Exception as e:
           logger.error(f"Error logging visualization metadata: {str(e)}", exc_info=True)
           return None
   
   def log_llm_interaction(self, session_id, prompt_type, prompt, prompt_context=None, 
                         response=None, tokens_used=None, latency=None):
       """
       Log an interaction with the LLM
       
       Args:
           session_id: Session identifier
           prompt_type: Type of prompt ('explanation', 'nlu', etc.)
           prompt: The prompt text sent to the LLM
           prompt_context: Additional context sent with the prompt
           response: The response from the LLM
           tokens_used: Number of tokens used in the interaction
           latency: Time taken for the response (seconds)
           
       Returns:
           str: interaction_id or None if error
       """
       try:
           conn = sqlite3.connect(self.db_path)
           cursor = conn.cursor()
           
           now = datetime.datetime.now()
           interaction_id = str(uuid.uuid4())
           
           # Convert context to JSON if needed
           context_json = json.dumps(prompt_context) if prompt_context is not None else None
           
           cursor.execute('''
           INSERT INTO llm_interactions 
           (interaction_id, session_id, timestamp, prompt_type, prompt, 
            prompt_context, response, tokens_used, latency)
           VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
           ''', (interaction_id, session_id, now, prompt_type, prompt, 
                context_json, response, tokens_used, latency))
           
           conn.commit()
           conn.close()
           logger.info(f"Logged LLM interaction: {prompt_type} in session: {session_id}")
           
           return interaction_id
           
       except Exception as e:
           logger.error(f"Error logging LLM interaction: {str(e)}", exc_info=True)
           return None
   
   def log_explanation(self, session_id, entity_type, entity_name, question_type, 
                     question, explanation, context_used=None, llm_interaction_id=None):
       """
       Log an explanation provided to the user
       
       Args:
           session_id: Session identifier
           entity_type: Type of entity explained ('ward', 'variable', 'visualization')
           entity_name: Name of the entity explained
           question_type: Type of question ('what', 'why', 'how')
           question: The question being answered
           explanation: The explanation provided
           context_used: What context was used to generate the explanation
           llm_interaction_id: ID of the LLM interaction used
           
       Returns:
           str: explanation_id or None if error
       """
       try:
           conn = sqlite3.connect(self.db_path)
           cursor = conn.cursor()
           
           now = datetime.datetime.now()
           explanation_id = str(uuid.uuid4())
           
           # Convert context to JSON if needed
           context_json = json.dumps(context_used) if context_used is not None else None
           
           cursor.execute('''
           INSERT INTO explanations 
           (explanation_id, session_id, timestamp, entity_type, entity_name, 
            question_type, question, explanation, context_used, llm_interaction_id)
           VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
           ''', (explanation_id, session_id, now, entity_type, entity_name, 
                question_type, question, explanation, context_json, llm_interaction_id))
           
           conn.commit()
           conn.close()
           logger.info(f"Logged explanation: {entity_type}:{entity_name} in session: {session_id}")
           
           return explanation_id
           
       except Exception as e:
           logger.error(f"Error logging explanation: {str(e)}", exc_info=True)
           return None
   
   # Query and export methods
   
   def get_session_history(self, session_id):
       """
       Get complete conversation history for a session
       
       Args:
           session_id: Session identifier
           
       Returns:
           list: List of message objects in chronological order
       """
       try:
           conn = sqlite3.connect(self.db_path)
           conn.row_factory = sqlite3.Row  # Return rows as dictionaries
           cursor = conn.cursor()
           
           # Get all messages for this session
           cursor.execute('''
           SELECT * FROM messages
           WHERE session_id = ?
           ORDER BY timestamp
           ''', (session_id,))
           
           messages = []
           for row in cursor.fetchall():
               message = dict(row)
               
               # Parse entities JSON if present
               if message['entities']:
                   try:
                       message['entities'] = json.loads(message['entities'])
                   except:
                       pass  # Keep as string if not valid JSON
               
               messages.append(message)
           
           conn.close()
           return messages
           
       except Exception as e:
           logger.error(f"Error retrieving session history: {str(e)}", exc_info=True)
           return []
   
   def get_analysis_metadata(self, session_id):
       """
       Get complete analysis metadata for a session
       
       Args:
           session_id: Session identifier
           
       Returns:
           dict: Comprehensive analysis metadata
       """
       try:
           conn = sqlite3.connect(self.db_path)
           conn.row_factory = sqlite3.Row  # Return rows as dictionaries
           cursor = conn.cursor()
           
           metadata = {
               'steps': [],
               'decisions': [],
               'calculations': [],
               'anomalies': [],
               'variable_relationships': [],
               'ward_rankings': [],
               'visualization_metadata': []
           }
           
           # Get analysis steps
           cursor.execute('''
           SELECT * FROM analysis_steps
           WHERE session_id = ?
           ORDER BY timestamp
           ''', (session_id,))
           
           for row in cursor.fetchall():
               step = dict(row)
               
               # Parse JSON fields
               for field in ['input_summary', 'output_summary', 'parameters']:
                   if step[field]:
                       try:
                           step[field] = json.loads(step[field])
                       except:
                           pass
               
               metadata['steps'].append(step)
               
               # Get decisions for this step
               step_id = step['step_id']
               cursor.execute('''
               SELECT * FROM algorithm_decisions
               WHERE step_id = ?
               ORDER BY timestamp
               ''', (step_id,))
               
               for decision_row in cursor.fetchall():
                   decision = dict(decision_row)
                   
                   # Parse JSON fields
                   for field in ['options', 'selected_option']:
                       if decision[field]:
                           try:
                               decision[field] = json.loads(decision[field])
                           except:
                               pass
                   
                   metadata['decisions'].append(decision)
               
               # Get calculations for this step
               cursor.execute('''
               SELECT * FROM calculations
               WHERE step_id = ?
               ORDER BY timestamp
               ''', (step_id,))
               
               for calc_row in cursor.fetchall():
                   calc = dict(calc_row)
                   
                   # Parse JSON fields
                   for field in ['input_values', 'output_value', 'context']:
                       if calc[field]:
                           try:
                               calc[field] = json.loads(calc[field])
                           except:
                               pass
                   
                   metadata['calculations'].append(calc)
           
           # Get anomalies
           cursor.execute('''
           SELECT * FROM anomalies
           WHERE session_id = ?
           ORDER BY timestamp
           ''', (session_id,))
           
           for row in cursor.fetchall():
               anomaly = dict(row)
               
               # Parse JSON fields
               if anomaly['context']:
                   try:
                       anomaly['context'] = json.loads(anomaly['context'])
                   except:
                       pass
               
               metadata['anomalies'].append(anomaly)
           
           # Get variable relationships
           cursor.execute('''
           SELECT * FROM variable_relationships
           WHERE session_id = ?
           ORDER BY timestamp
           ''', (session_id,))
           
           for row in cursor.fetchall():
               relationship = dict(row)
               
               # Parse JSON fields
               if relationship['evidence']:
                   try:
                       relationship['evidence'] = json.loads(relationship['evidence'])
                   except:
                       pass
               
               metadata['variable_relationships'].append(relationship)
           
           # Get ward rankings
           cursor.execute('''
           SELECT * FROM ward_rankings
           WHERE session_id = ?
           ORDER BY overall_rank
           ''', (session_id,))
           
           for row in cursor.fetchall():
               ranking = dict(row)
               
               # Parse JSON fields
               for field in ['contributing_factors', 'anomaly_flags']:
                   if ranking[field]:
                       try:
                           ranking[field] = json.loads(ranking[field])
                       except:
                           pass
               
               metadata['ward_rankings'].append(ranking)
           
           # Get visualization metadata
           cursor.execute('''
           SELECT * FROM visualization_metadata
           WHERE session_id = ?
           ORDER BY timestamp
           ''', (session_id,))
           
           for row in cursor.fetchall():
               viz_meta = dict(row)
               
               # Parse JSON fields
               for field in ['variables_used', 'data_summary', 'visual_elements', 'patterns_detected']:
                   if viz_meta[field]:
                       try:
                           viz_meta[field] = json.loads(viz_meta[field])
                       except:
                           pass
               
               metadata['visualization_metadata'].append(viz_meta)
           
           conn.close()
           return metadata
           
       except Exception as e:
           logger.error(f"Error retrieving analysis metadata: {str(e)}", exc_info=True)
           return {'error': str(e)}
   
   def get_visualization_context(self, session_id, viz_id=None, viz_type=None):
       """
       Get context about visualizations for a session
       
       Args:
           session_id: Session identifier
           viz_id: Optional specific visualization ID
           viz_type: Optional visualization type to filter by
           
       Returns:
           dict or list: Visualization context data
       """
       try:
           conn = sqlite3.connect(self.db_path)
           conn.row_factory = sqlite3.Row  # Return rows as dictionaries
           cursor = conn.cursor()
           
           query = '''
           SELECT * FROM visualization_metadata
           WHERE session_id = ?
           '''
           
           params = [session_id]
           
           if viz_id:
               query += ' AND viz_id = ?'
               params.append(viz_id)
           elif viz_type:
               query += ' AND viz_type = ?'
               params.append(viz_type)
           
           query += ' ORDER BY timestamp DESC'
           
           cursor.execute(query, params)
           
           results = []
           for row in cursor.fetchall():
               viz_meta = dict(row)
               
               # Parse JSON fields
               for field in ['variables_used', 'data_summary', 'visual_elements', 'patterns_detected']:
                   if viz_meta[field]:
                       try:
                           viz_meta[field] = json.loads(viz_meta[field])
                       except:
                           pass
               
               results.append(viz_meta)
           
           conn.close()
           
           # If specific viz_id was requested, return single item instead of list
           if viz_id and results:
               return results[0]
               
           return results
           
       except Exception as e:
           logger.error(f"Error retrieving visualization context: {str(e)}", exc_info=True)
           return [] if viz_id is None else None
   
   def get_explanations(self, session_id, entity_type=None, entity_name=None):
       """
       Get explanations provided in a session
       
       Args:
           session_id: Session identifier
           entity_type: Optional type to filter by ('ward', 'variable', etc.)
           entity_name: Optional name to filter by
           
       Returns:
           list: Explanation records
       """
       try:
           conn = sqlite3.connect(self.db_path)
           conn.row_factory = sqlite3.Row  # Return rows as dictionaries
           cursor = conn.cursor()
           
           query = '''
           SELECT * FROM explanations
           WHERE session_id = ?
           '''
           
           params = [session_id]
           
           if entity_type:
               query += ' AND entity_type = ?'
               params.append(entity_type)
               
           if entity_name:
               query += ' AND entity_name = ?'
               params.append(entity_name)
           
           query += ' ORDER BY timestamp DESC'
           
           cursor.execute(query, params)
           
           explanations = []
           for row in cursor.fetchall():
               explanation = dict(row)
               
               # Parse context JSON if present
               if explanation['context_used']:
                   try:
                       explanation['context_used'] = json.loads(explanation['context_used'])
                   except:
                       pass
               
               explanations.append(explanation)
           
           conn.close()
           return explanations
           
       except Exception as e:
           logger.error(f"Error retrieving explanations: {str(e)}", exc_info=True)
           return []
   
   def get_ward_data(self, session_id, ward_name):
       """
       Get comprehensive data about a specific ward
       
       Args:
           session_id: Session identifier
           ward_name: Name of the ward
           
       Returns:
           dict: Comprehensive ward data
       """
       try:
           conn = sqlite3.connect(self.db_path)
           conn.row_factory = sqlite3.Row  # Return rows as dictionaries
           cursor = conn.cursor()
           
           ward_data = {
               'ranking': None,
               'anomalies': [],
               'explanations': []
           }
           
           # Get ranking data
           cursor.execute('''
           SELECT * FROM ward_rankings
           WHERE session_id = ? AND ward_name = ?
           ORDER BY timestamp DESC
           LIMIT 1
           ''', (session_id, ward_name))
           
           row = cursor.fetchone()
           if row:
               ranking = dict(row)
               
               # Parse JSON fields
               for field in ['contributing_factors', 'anomaly_flags']:
                   if ranking[field]:
                       try:
                           ranking[field] = json.loads(ranking[field])
                       except:
                           pass
               
               ward_data['ranking'] = ranking
           
           # Get anomalies
           cursor.execute('''
           SELECT * FROM anomalies
           WHERE session_id = ? AND entity_name = ?
           ORDER BY timestamp
           ''', (session_id, ward_name))
           
           for row in cursor.fetchall():
               anomaly = dict(row)
               
               # Parse context JSON
               if anomaly['context']:
                   try:
                       anomaly['context'] = json.loads(anomaly['context'])
                   except:
                       pass
               
               ward_data['anomalies'].append(anomaly)
           
           # Get explanations
           cursor.execute('''
           SELECT * FROM explanations
           WHERE session_id = ? AND entity_type = 'ward' AND entity_name = ?
           ORDER BY timestamp DESC
           ''', (session_id, ward_name))
           
           for row in cursor.fetchall():
               explanation = dict(row)
               
               # Parse context JSON
               if explanation['context_used']:
                   try:
                       explanation['context_used'] = json.loads(explanation['context_used'])
                   except:
                       pass
               
               ward_data['explanations'].append(explanation)
           
           conn.close()
           return ward_data
           
       except Exception as e:
           logger.error(f"Error retrieving ward data: {str(e)}", exc_info=True)
           return {'error': str(e)}
   
   def export_to_csv(self, session_id=None, start_date=None, end_date=None, output_dir=None):
       """
       Export logs to CSV files
       
       Args:
           session_id: Optional session to filter by
           start_date: Optional start date filter (YYYY-MM-DD format)
           end_date: Optional end date filter (YYYY-MM-DD format)
           output_dir: Directory to write CSV files (defaults to current directory)
           
       Returns:
           dict: Status of export operation
       """
       try:
           if output_dir is None:
               output_dir = os.getcwd()
           
           os.makedirs(output_dir, exist_ok=True)
           
           conn = sqlite3.connect(self.db_path)
           conn.row_factory = sqlite3.Row
           cursor = conn.cursor()
           
           # Build base query conditions
           conditions = []
           params = []
           
           if session_id:
               conditions.append("session_id = ?")
               params.append(session_id)
           
           if start_date:
               conditions.append("timestamp >= ?")
               # Add time component for start of day
               params.append(f"{start_date} 00:00:00")
           
           if end_date:
               conditions.append("timestamp <= ?")
               # Add time component for end of day
               params.append(f"{end_date} 23:59:59")
           
           # Create WHERE clause if we have conditions
           where_clause = ""
           if conditions:
               where_clause = "WHERE " + " AND ".join(conditions)
           
           # Tables to export
           tables = [
               "sessions", "messages", "file_uploads", "analysis_events", 
               "errors", "analysis_steps", "algorithm_decisions", "calculations",
               "anomalies", "variable_relationships", "ward_rankings", 
               "visualization_metadata", "llm_interactions", "explanations"
           ]
           
           exported_files = {}
           
           for table in tables:
               # Get table columns
               cursor.execute(f"PRAGMA table_info({table})")
               columns = [row['name'] for row in cursor.fetchall()]
               
               # Export table data
               query = f"SELECT * FROM {table} {where_clause}"
               cursor.execute(query, params)
               rows = cursor.fetchall()
               
               if rows:
                   # Create DataFrame
                   data = []
                   for row in rows:
                       row_dict = dict(row)
                       
                       # Handle JSON fields by checking if they might be JSON
                       for col, val in row_dict.items():
                           if isinstance(val, str) and val.startswith('{') and val.endswith('}'):
                               try:
                                   row_dict[col] = json.dumps(json.loads(val))  # Normalize JSON
                               except:
                                   pass  # Not valid JSON, keep as is
                                   
                       data.append(row_dict)
                   
                   # Create filename with timestamp
                   timestamp = datetime.datetime.now().strftime('%Y%m%d_%H%M%S')
                   if session_id:
                       filename = f"{table}_{session_id}_{timestamp}.csv"
                   else:
                       filename = f"{table}_{timestamp}.csv"
                   
                   file_path = os.path.join(output_dir, filename)
                   
                   # Convert to pandas DataFrame and save as CSV
                   import pandas as pd
                   df = pd.DataFrame(data, columns=columns)
                   df.to_csv(file_path, index=False)
                   
                   exported_files[table] = file_path
           
           conn.close()
           
           return {
               'status': 'success',
               'message': f'Exported {len(exported_files)} tables to CSV',
               'files': exported_files
           }
           
       except Exception as e:
           logger.error(f"Error exporting to CSV: {str(e)}", exc_info=True)
           return {
               'status': 'error',
               'message': f'Error exporting to CSV: {str(e)}'
           }
   
   def export_to_json(self, session_id=None, include_llm_data=True, compact=False, output_file=None):
       """
       Export logs to a single JSON file
       
       Args:
           session_id: Optional session to filter by
           include_llm_data: Whether to include LLM prompts and responses
           compact: Whether to generate compact JSON (without pretty printing)
           output_file: Path to write the JSON file
           
       Returns:
           dict: Status of export operation
       """
       try:
           conn = sqlite3.connect(self.db_path)
           conn.row_factory = sqlite3.Row
           cursor = conn.cursor()
           
           # Get session(s)
           if session_id:
               cursor.execute("SELECT * FROM sessions WHERE session_id = ?", (session_id,))
               sessions = [dict(row) for row in cursor.fetchall()]
               if not sessions:
                   return {
                       'status': 'error',
                       'message': f'Session {session_id} not found'
                   }
           else:
               cursor.execute("SELECT * FROM sessions ORDER BY start_time")
               sessions = [dict(row) for row in cursor.fetchall()]
           
           # Prepare export data structure
           export_data = {
               'export_timestamp': datetime.datetime.now().isoformat(),
               'sessions': []
           }
           
           # Process each session
           for session_info in sessions:
               session_id = session_info['session_id']
               session_data = {
                   'session_info': session_info,
                   'messages': [],
                   'file_uploads': [],
                   'analysis_events': [],
                   'errors': [],
                   'analysis_details': {
                       'steps': [],
                       'decisions': [],
                       'calculations': [],
                       'anomalies': [],
                       'variable_relationships': [],
                       'ward_rankings': [],
                       'visualization_metadata': []
                   }
               }
               
               if include_llm_data:
                   session_data['llm_interactions'] = []
                   session_data['explanations'] = []
               
               # Get messages
               cursor.execute('''
               SELECT * FROM messages
               WHERE session_id = ?
               ORDER BY timestamp
               ''', (session_id,))
               
               for row in cursor.fetchall():
                   message = dict(row)
                   
                   # Parse entities JSON if present
                   if message['entities']:
                       try:
                           message['entities'] = json.loads(message['entities'])
                       except:
                           pass  # Keep as string if not valid JSON
                   
                   session_data['messages'].append(message)
               
               # Get file uploads
               cursor.execute('''
               SELECT * FROM file_uploads
               WHERE session_id = ?
               ORDER BY timestamp
               ''', (session_id,))
               
               for row in cursor.fetchall():
                   upload = dict(row)
                   
                   # Parse metadata JSON if present
                   if upload['metadata']:
                       try:
                           upload['metadata'] = json.loads(upload['metadata'])
                       except:
                           pass
                   
                   session_data['file_uploads'].append(upload)
               
               # Get analysis events
               cursor.execute('''
               SELECT * FROM analysis_events
               WHERE session_id = ?
               ORDER BY timestamp
               ''', (session_id,))
               
               for row in cursor.fetchall():
                   event = dict(row)
                   
                   # Parse details JSON if present
                   if event['details']:
                       try:
                           event['details'] = json.loads(event['details'])
                       except:
                           pass
                   
                   session_data['analysis_events'].append(event)
               
               # Get errors
               cursor.execute('''
               SELECT * FROM errors
               WHERE session_id = ?
               ORDER BY timestamp
               ''', (session_id,))
               
               for row in cursor.fetchall():
                   error = dict(row)
                   session_data['errors'].append(error)
               
               # Get analysis steps
               cursor.execute('''
               SELECT * FROM analysis_steps
               WHERE session_id = ?
               ORDER BY timestamp
               ''', (session_id,))
               
               for row in cursor.fetchall():
                   step = dict(row)
                   
                   # Parse JSON fields
                   for field in ['input_summary', 'output_summary', 'parameters']:
                       if step[field]:
                           try:
                               step[field] = json.loads(step[field])
                           except:
                               pass
                   
                   session_data['analysis_details']['steps'].append(step)
                   
                   # Get linked data for this step
                   step_id = step['step_id']
                   
                   # Get decisions
                   cursor.execute('''
                   SELECT * FROM algorithm_decisions
                   WHERE step_id = ?
                   ORDER BY timestamp
                   ''', (step_id,))
                   
                   for decision_row in cursor.fetchall():
                       decision = dict(decision_row)
                       
                       # Parse JSON fields
                       for field in ['options', 'selected_option']:
                           if decision[field]:
                               try:
                                   decision[field] = json.loads(decision[field])
                               except:
                                   pass
                       
                       session_data['analysis_details']['decisions'].append(decision)
                   
                   # Get calculations
                   cursor.execute('''
                   SELECT * FROM calculations
                   WHERE step_id = ?
                   ORDER BY timestamp
                   ''', (step_id,))
                   
                   for calc_row in cursor.fetchall():
                       calc = dict(calc_row)
                       
                       # Parse JSON fields
                       for field in ['input_values', 'output_value', 'context']:
                           if calc[field]:
                               try:
                                   calc[field] = json.loads(calc[field])
                               except:
                                   pass
                       
                       session_data['analysis_details']['calculations'].append(calc)
               
               # Get anomalies
               cursor.execute('''
               SELECT * FROM anomalies
               WHERE session_id = ?
               ORDER BY timestamp
               ''', (session_id,))
               
               for row in cursor.fetchall():
                   anomaly = dict(row)
                   
                   # Parse context JSON
                   if anomaly['context']:
                       try:
                           anomaly['context'] = json.loads(anomaly['context'])
                       except:
                           pass
                   
                   session_data['analysis_details']['anomalies'].append(anomaly)
               
               # Get variable relationships
               cursor.execute('''
               SELECT * FROM variable_relationships
               WHERE session_id = ?
               ORDER BY timestamp
               ''', (session_id,))
               
               for row in cursor.fetchall():
                   relationship = dict(row)
                   
                   # Parse evidence JSON
                   if relationship['evidence']:
                       try:
                           relationship['evidence'] = json.loads(relationship['evidence'])
                       except:
                           pass
                   
                   session_data['analysis_details']['variable_relationships'].append(relationship)
               
               # Get ward rankings
               cursor.execute('''
               SELECT * FROM ward_rankings
               WHERE session_id = ?
               ORDER BY overall_rank
               ''', (session_id,))
               
               for row in cursor.fetchall():
                   ranking = dict(row)
                   
                   # Parse JSON fields
                   for field in ['contributing_factors', 'anomaly_flags']:
                       if ranking[field]:
                           try:
                               ranking[field] = json.loads(ranking[field])
                           except:
                               pass
                   
                   session_data['analysis_details']['ward_rankings'].append(ranking)
               
               # Get visualization metadata
               cursor.execute('''
               SELECT * FROM visualization_metadata
               WHERE session_id = ?
               ORDER BY timestamp
               ''', (session_id,))
               
               for row in cursor.fetchall():
                   viz_meta = dict(row)
                   
                   # Parse JSON fields
                   for field in ['variables_used', 'data_summary', 'visual_elements', 'patterns_detected']:
                       if viz_meta[field]:
                           try:
                               viz_meta[field] = json.loads(viz_meta[field])
                           except:
                               pass
                   
                   session_data['analysis_details']['visualization_metadata'].append(viz_meta)
               
               if include_llm_data:
                   # Get LLM interactions
                   cursor.execute('''
                   SELECT * FROM llm_interactions
                   WHERE session_id = ?
                   ORDER BY timestamp
                   ''', (session_id,))
                   
                   for row in cursor.fetchall():
                       llm_interaction = dict(row)
                       
                       # Parse context JSON
                       if llm_interaction['prompt_context']:
                           try:
                               llm_interaction['prompt_context'] = json.loads(llm_interaction['prompt_context'])
                           except:
                               pass
                       
                       session_data['llm_interactions'].append(llm_interaction)
                   
                   # Get explanations
                   cursor.execute('''
                   SELECT * FROM explanations
                   WHERE session_id = ?
                   ORDER BY timestamp
                   ''', (session_id,))
                   
                   for row in cursor.fetchall():
                       explanation = dict(row)
                       
                       # Parse context JSON
                       if explanation['context_used']:
                           try:
                               explanation['context_used'] = json.loads(explanation['context_used'])
                           except:
                               pass
                       
                       session_data['explanations'].append(explanation)
               
               export_data['sessions'].append(session_data)
           
           # Generate output file name if not provided
           if output_file is None:
               if session_id:
                   output_file = f"log_export_{session_id}_{datetime.datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
               else:
                   output_file = f"log_export_all_{datetime.datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
           
           # Write to file
           with open(output_file, 'w') as f:
               if compact:
                   json.dump(export_data, f)
               else:
                   json.dump(export_data, f, indent=2)
           
           conn.close()
           
           return {
               'status': 'success',
               'message': f'Successfully exported data to {output_file}',
               'file_path': output_file,
               'sessions_exported': len(export_data['sessions'])
           }
           
       except Exception as e:
           logger.error(f"Error exporting to JSON: {str(e)}", exc_info=True)
           return {
               'status': 'error',
               'message': f'Error exporting to JSON: {str(e)}'
           }
   
   def get_conversation_training_data(self, session_id=None, min_quality=None, start_date=None, end_date=None):
       """
       Export conversations in a format suitable for LLM fine-tuning
       
       Args:
           session_id: Optional session to filter by
           min_quality: Minimum quality score to include (if available)
           start_date: Optional start date filter (YYYY-MM-DD format)
           end_date: Optional end date filter (YYYY-MM-DD format)
           
       Returns:
           list: Conversation data in fine-tuning format
       """
       try:
           conn = sqlite3.connect(self.db_path)
           conn.row_factory = sqlite3.Row
           cursor = conn.cursor()
           
           # Build query conditions
           conditions = []
           params = []
           
           if session_id:
               conditions.append("s.session_id = ?")
               params.append(session_id)
           
           if start_date:
               conditions.append("m.timestamp >= ?")
               params.append(f"{start_date} 00:00:00")
           
           if end_date:
               conditions.append("m.timestamp <= ?")
               params.append(f"{end_date} 23:59:59")
           
           # Create WHERE clause
           where_clause = ""
           if conditions:
               where_clause = "WHERE " + " AND ".join(conditions)
           
           # Get all sessions matching criteria
           query = f"""
           SELECT DISTINCT s.session_id, s.start_time
           FROM sessions s
           JOIN messages m ON s.session_id = m.session_id
           {where_clause}
           ORDER BY s.start_time
           """
           
           cursor.execute(query, params)
           sessions = cursor.fetchall()
           
           training_data = []
           
           # Process each session
           for session_row in sessions:
               session_id = session_row['session_id']
               
               # Get conversation messages in order
               cursor.execute("""
               SELECT sender, content, intent, entities
               FROM messages
               WHERE session_id = ?
               ORDER BY timestamp
               """, (session_id,))
               
               messages = cursor.fetchall()
               
               # Skip sessions with just one message
               if len(messages) < 2:
                   continue
               
               # Format as a training example
               conversation = []
               
               for message in messages:
                   role = "user" if message['sender'] == 'user' else "assistant"
                   content = message['content']
                   
                   # Skip system messages or empty content
                   if message['sender'] == 'system' or not content:
                       continue
                   
                   # Add message to conversation
                   conversation.append({
                       "role": role,
                       "content": content
                   })
               
               # Skip if we don't have both user and assistant messages
               if len(conversation) < 2:
                   continue
               
               # Get context from analysis if available
               context = None
               try:
                   # Get the latest analysis metadata for context
                   cursor.execute("""
                   SELECT step_name, input_summary, output_summary, algorithm, parameters
                   FROM analysis_steps
                   WHERE session_id = ?
                   ORDER BY timestamp DESC
                   LIMIT 5
                   """, (session_id,))
                   
                   steps = []
                   for step_row in cursor.fetchall():
                       step = {
                           'name': step_row['step_name']
                       }
                       
                       # Parse JSON fields
                       for field in ['input_summary', 'output_summary', 'parameters']:
                           if step_row[field]:
                               try:
                                   step[field] = json.loads(step_row[field])
                               except:
                                   step[field] = step_row[field]
                       
                       steps.append(step)
                   
                   if steps:
                       context = {
                           'analysis_steps': steps
                       }
                       
                       # Get the latest ward rankings
                       cursor.execute("""
                       SELECT ward_name, overall_rank, vulnerability_category
                       FROM ward_rankings
                       WHERE session_id = ?
                       ORDER BY overall_rank
                       LIMIT 10
                       """, (session_id,))
                       
                       rankings = [dict(row) for row in cursor.fetchall()]
                       if rankings:
                           context['ward_rankings'] = rankings
                       
                       # Get variables used
                       cursor.execute("""
                       SELECT variable_name, relationship_type
                       FROM variable_relationships
                       WHERE session_id = ?
                       """, (session_id,))
                       
                       variables = [dict(row) for row in cursor.fetchall()]
                       if variables:
                           context['variables'] = variables
               except:
                   context = None
               
               # Add to training data
               training_example = {
                   "messages": conversation
               }
               
               # Add context if available and needed
               if context:
                   training_example["context"] = context
               
               training_data.append(training_example)
           
           conn.close()
           return training_data
           
       except Exception as e:
           logger.error(f"Error generating training data: {str(e)}", exc_info=True)
           return []
   
   def generate_explanation_context(self, session_id, entity_type, entity_name, question=None):
       """
       Generate context for LLM explanations
       
       Args:
           session_id: Session identifier
           entity_type: Type of entity to explain ('ward', 'variable', 'visualization')
           entity_name: Name of the entity
           question: Optional specific question
           
       Returns:
           dict: Context data for the LLM explanation
       """
       try:
           conn = sqlite3.connect(self.db_path)
           conn.row_factory = sqlite3.Row
           cursor = conn.cursor()
           
           context = {
               'entity_type': entity_type,
               'entity_name': entity_name,
               'question': question,
               'analysis_metadata': {}
           }
           
           # Get analysis steps
           cursor.execute('''
           SELECT step_name, input_summary, output_summary, algorithm, parameters
           FROM analysis_steps
           WHERE session_id = ?
           ORDER BY timestamp DESC
           LIMIT 5
           ''', (session_id,))
           
           steps = []
           for row in cursor.fetchall():
               step = {
                   'name': row['step_name']
               }
               
               # Parse JSON fields
               for field in ['input_summary', 'output_summary', 'parameters']:
                   if row[field]:
                       try:
                           step[field] = json.loads(row[field])
                       except:
                           step[field] = row[field]
               
               steps.append(step)
           
           context['analysis_metadata']['steps'] = steps
           
           # Add entity-specific context
           if entity_type == 'ward':
               # Get ward ranking
               cursor.execute('''
               SELECT * FROM ward_rankings
               WHERE session_id = ? AND ward_name = ?
               ORDER BY timestamp DESC
               LIMIT 1
               ''', (session_id, entity_name))
               
               row = cursor.fetchone()
               if row:
                   ranking = dict(row)
                   
                   # Parse JSON fields
                   for field in ['contributing_factors', 'anomaly_flags']:
                       if ranking[field]:
                           try:
                               ranking[field] = json.loads(ranking[field])
                           except:
                               pass
                   
                   context['ward_data'] = ranking
               
               # Get anomalies
               cursor.execute('''
               SELECT * FROM anomalies
               WHERE session_id = ? AND entity_name = ?
               ORDER BY timestamp DESC
               ''', (session_id, entity_name))
               
               anomalies = []
               for row in cursor.fetchall():
                   anomaly = dict(row)
                   
                   # Parse context JSON
                   if anomaly['context']:
                       try:
                           anomaly['context'] = json.loads(anomaly['context'])
                       except:
                           pass
                   
                   anomalies.append(anomaly)
               
               if anomalies:
                   context['ward_anomalies'] = anomalies
               
               # Get comparative ranking data
               cursor.execute('''
               SELECT * FROM ward_rankings
               WHERE session_id = ? 
               ORDER BY overall_rank
               ''', (session_id,))
               
               ward_count = 0
               ward_position = None
               ward_percentile = None
               
               for i, row in enumerate(cursor.fetchall()):
                   ward_count += 1
                   if row['ward_name'] == entity_name:
                       ward_position = i + 1
               
               if ward_count > 0 and ward_position:
                   ward_percentile = (ward_count - ward_position) / ward_count * 100
                   
                   context['comparative_data'] = {
                       'total_wards': ward_count,
                       'rank_position': ward_position,
                       'percentile': ward_percentile
                   }
               
           elif entity_type == 'variable':
               # Get variable relationship
               cursor.execute('''
               SELECT * FROM variable_relationships
               WHERE session_id = ? AND variable_name = ?
               ORDER BY timestamp DESC
               LIMIT 1
               ''', (session_id, entity_name))
               
               row = cursor.fetchone()
               if row:
                   relationship = dict(row)
                   
                   # Parse evidence JSON
                   if relationship['evidence']:
                       try:
                           relationship['evidence'] = json.loads(relationship['evidence'])
                       except:
                           pass
                   
                   context['variable_data'] = relationship
               
               # Get relevant calculations
               cursor.execute('''
               SELECT * FROM calculations
               WHERE session_id = ? AND variable = ?
               ORDER BY timestamp DESC
               LIMIT 10
               ''', (session_id, entity_name))
               
               calculations = []
               for row in cursor.fetchall():
                   calc = dict(row)
                   
                   # Parse JSON fields
                   for field in ['input_values', 'output_value', 'context']:
                       if calc[field]:
                           try:
                               calc[field] = json.loads(calc[field])
                           except:
                               pass
                   
                   calculations.append(calc)
               
               if calculations:
                   context['variable_calculations'] = calculations
               
           elif entity_type == 'visualization':
               # Get visualization metadata
               cursor.execute('''
               SELECT * FROM visualization_metadata
               WHERE session_id = ? AND viz_type = ?
               ORDER BY timestamp DESC
               LIMIT 1
               ''', (session_id, entity_name))
               
               row = cursor.fetchone()
               if row:
                   viz_meta = dict(row)
                   
                   # Parse JSON fields
                   for field in ['variables_used', 'data_summary', 'visual_elements', 'patterns_detected']:
                       if viz_meta[field]:
                           try:
                               viz_meta[field] = json.loads(viz_meta[field])
                           except:
                               pass
                   
                   context['visualization_data'] = viz_meta
           
           # Add previous explanations if any
           cursor.execute('''
           SELECT * FROM explanations
           WHERE session_id = ? AND entity_type = ? AND entity_name = ?
           ORDER BY timestamp DESC
           LIMIT 3
           ''', (session_id, entity_type, entity_name))
           
           previous_explanations = []
           for row in cursor.fetchall():
               explanation = dict(row)
               
               # Parse context JSON
               if explanation['context_used']:
                   try:
                       explanation['context_used'] = json.loads(explanation['context_used'])
                   except:
                       pass
               
               previous_explanations.append(explanation)
           
           if previous_explanations:
               context['previous_explanations'] = previous_explanations
           
           conn.close()
           return context
           
       except Exception as e:
           logger.error(f"Error generating explanation context: {str(e)}", exc_info=True)
           return {
               'error': str(e),
               'entity_type': entity_type,
               'entity_name': entity_name
           }