# app/interaction/events.py
"""
Event Tracking and Categorization Module

This module contains all event logging functionality extracted from the 
monolithic InteractionLogger class. It handles various types of events
including file uploads, analysis events, algorithm decisions, calculations,
anomalies, ward rankings, visualization metadata, LLM interactions, and explanations.

Functions:
- File upload tracking
- Analysis event logging
- Algorithm decision logging
- Calculation tracking
- Anomaly detection logging
- Variable relationship tracking
- Ward ranking logging
- Visualization metadata tracking
- LLM interaction tracking
- Explanation logging
"""

import json
import logging
import datetime
import sqlite3
import uuid
from typing import Dict, List, Any, Optional, Union

# Local imports
from app.core.utils import convert_to_json_serializable
from .core import DatabaseManager

# Set up logging
logger = logging.getLogger(__name__)


def log_file_upload(db_manager: DatabaseManager, session_id, file_type, file_name, file_size, metadata=None):
    """
    Log a file upload event
    
    Args:
        db_manager: DatabaseManager instance
        session_id: Session identifier
        file_type: Type of file ('csv', 'shapefile', etc.)
        file_name: Name of the uploaded file
        file_size: Size of the file in bytes
        metadata: Optional dict of file metadata
        
    Returns:
        str: upload_id or None if error
    """
    try:
        conn = db_manager.get_connection()
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


def log_analysis_event(db_manager: DatabaseManager, session_id, event_type, details, success=True):
    """
    Log an analysis event (visualization, report generation, etc.)
    
    Args:
        db_manager: DatabaseManager instance
        session_id: Session identifier
        event_type: Type of event ('visualization', 'report', etc.)
        details: Dict or string with event details
        success: Whether the event was successful
        
    Returns:
        str: event_id or None if error
    """
    try:
        conn = db_manager.get_connection()
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


def log_analysis_step(db_manager: DatabaseManager, session_id, step_name, input_summary=None, output_summary=None, 
                     algorithm=None, parameters=None, execution_time=None, error=None):
    """
    Log an analysis pipeline step with detailed metadata
    
    Args:
        db_manager: DatabaseManager instance
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
        conn = db_manager.get_connection()
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


def log_algorithm_decision(db_manager: DatabaseManager, session_id, step_id, decision_type, options=None, 
                          criteria=None, selected_option=None, confidence=None):
    """
    Log a decision made during analysis
    
    Args:
        db_manager: DatabaseManager instance
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
        conn = db_manager.get_connection()
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


def log_calculation(db_manager: DatabaseManager, session_id, step_id, variable, operation, 
                   input_values=None, output_value=None, context=None):
    """
    Log a calculation performed during analysis
    
    Args:
        db_manager: DatabaseManager instance
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
        conn = db_manager.get_connection()
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


def log_anomaly(db_manager: DatabaseManager, session_id, entity_name, anomaly_type, expected_value=None, 
               actual_value=None, significance=None, context=None):
    """
    Log an anomaly detected during analysis
    
    Args:
        db_manager: DatabaseManager instance
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
        conn = db_manager.get_connection()
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


def log_variable_relationship(db_manager: DatabaseManager, session_id, variable_name, relationship_type, 
                             evidence=None, confidence_score=None):
    """
    Log a determined variable relationship
    
    Args:
        db_manager: DatabaseManager instance
        session_id: Session identifier
        variable_name: Name of the variable
        relationship_type: Type of relationship ('direct', 'inverse')
        evidence: Evidence supporting this relationship
        confidence_score: Confidence in the relationship (0-1)
        
    Returns:
        str: relationship_id or None if error
    """
    try:
        conn = db_manager.get_connection()
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


def log_ward_ranking(db_manager: DatabaseManager, session_id, ward_name, overall_rank, median_score, 
                    vulnerability_category, contributing_factors=None, anomaly_flags=None):
    """
    Log detailed information about a ward's ranking
    
    Args:
        db_manager: DatabaseManager instance
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
        conn = db_manager.get_connection()
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


def log_visualization_metadata(db_manager: DatabaseManager, session_id, viz_type, variables_used=None, 
                              data_summary=None, visual_elements=None, patterns_detected=None):
    """
    Log detailed metadata about a visualization
    
    Args:
        db_manager: DatabaseManager instance
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
        conn = db_manager.get_connection()
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


def log_llm_interaction(db_manager: DatabaseManager, session_id, prompt_type, prompt, prompt_context=None, 
                       response=None, tokens_used=None, latency=None, enhanced_timing=None):
    """
    Log an interaction with the LLM
    
    Args:
        db_manager: DatabaseManager instance
        session_id: Session identifier
        prompt_type: Type of prompt ('explanation', 'nlu', etc.)
        prompt: The prompt text sent to the LLM
        prompt_context: Additional context sent with the prompt
        response: The response from the LLM
        tokens_used: Number of tokens used in the interaction
        latency: Time taken for the response (seconds)
        enhanced_timing: Detailed timing breakdown (dict)
        
    Returns:
        str: interaction_id or None if error
    """
    try:
        conn = db_manager.get_connection()
        cursor = conn.cursor()
        
        now = datetime.datetime.now()
        interaction_id = str(uuid.uuid4())
        
        # Convert context to JSON if needed
        context_json = json.dumps(prompt_context) if prompt_context is not None else None
        enhanced_timing_json = json.dumps(enhanced_timing) if enhanced_timing is not None else None
        
        cursor.execute('''
        INSERT INTO llm_interactions 
        (interaction_id, session_id, timestamp, prompt_type, prompt, 
         prompt_context, response, tokens_used, latency, enhanced_timing)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ''', (interaction_id, session_id, now, prompt_type, prompt, 
             context_json, response, tokens_used, latency, enhanced_timing_json))
        
        conn.commit()
        conn.close()
        logger.info(f"Logged LLM interaction: {prompt_type} in session: {session_id}")
        
        return interaction_id
        
    except Exception as e:
        logger.error(f"Error logging LLM interaction: {str(e)}", exc_info=True)
        return None


def log_explanation(db_manager: DatabaseManager, session_id, entity_type, entity_name, question_type, 
                   question, explanation, context_used=None, llm_interaction_id=None):
    """
    Log an explanation provided to the user
    
    Args:
        db_manager: DatabaseManager instance
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
        conn = db_manager.get_connection()
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


# Convenience class for event management
class EventLogger:
    """
    Event logging functionality
    Provides specialized event tracking capabilities
    """
    
    def __init__(self, db_manager: DatabaseManager):
        """Initialize with database manager"""
        self.db_manager = db_manager
    
    def log_file_upload(self, session_id, file_type, file_name, file_size, metadata=None):
        """Log file upload"""
        return log_file_upload(self.db_manager, session_id, file_type, file_name, file_size, metadata)
    
    def log_analysis_event(self, session_id, event_type, details, success=True):
        """Log analysis event"""
        return log_analysis_event(self.db_manager, session_id, event_type, details, success)
    
    def log_analysis_step(self, session_id, step_name, input_summary=None, output_summary=None, 
                         algorithm=None, parameters=None, execution_time=None, error=None):
        """Log analysis step"""
        return log_analysis_step(self.db_manager, session_id, step_name, input_summary, output_summary,
                                algorithm, parameters, execution_time, error)
    
    def log_algorithm_decision(self, session_id, step_id, decision_type, options=None, 
                              criteria=None, selected_option=None, confidence=None):
        """Log algorithm decision"""
        return log_algorithm_decision(self.db_manager, session_id, step_id, decision_type, options,
                                     criteria, selected_option, confidence)
    
    def log_calculation(self, session_id, step_id, variable, operation, 
                       input_values=None, output_value=None, context=None):
        """Log calculation"""
        return log_calculation(self.db_manager, session_id, step_id, variable, operation,
                              input_values, output_value, context)
    
    def log_anomaly(self, session_id, entity_name, anomaly_type, expected_value=None, 
                   actual_value=None, significance=None, context=None):
        """Log anomaly"""
        return log_anomaly(self.db_manager, session_id, entity_name, anomaly_type, expected_value,
                          actual_value, significance, context)
    
    def log_variable_relationship(self, session_id, variable_name, relationship_type, 
                                 evidence=None, confidence_score=None):
        """Log variable relationship"""
        return log_variable_relationship(self.db_manager, session_id, variable_name, relationship_type,
                                        evidence, confidence_score)
    
    def log_ward_ranking(self, session_id, ward_name, overall_rank, median_score, 
                        vulnerability_category, contributing_factors=None, anomaly_flags=None):
        """Log ward ranking"""
        return log_ward_ranking(self.db_manager, session_id, ward_name, overall_rank, median_score,
                               vulnerability_category, contributing_factors, anomaly_flags)
    
    def log_visualization_metadata(self, session_id, viz_type, variables_used=None, 
                                  data_summary=None, visual_elements=None, patterns_detected=None):
        """Log visualization metadata"""
        return log_visualization_metadata(self.db_manager, session_id, viz_type, variables_used,
                                         data_summary, visual_elements, patterns_detected)
    
    def log_llm_interaction(self, session_id, prompt_type, prompt, prompt_context=None, 
                           response=None, tokens_used=None, latency=None, enhanced_timing=None):
        """Log LLM interaction"""
        return log_llm_interaction(self.db_manager, session_id, prompt_type, prompt, prompt_context,
                                  response, tokens_used, latency, enhanced_timing)
    
    def log_explanation(self, session_id, entity_type, entity_name, question_type, 
                       question, explanation, context_used=None, llm_interaction_id=None):
        """Log explanation"""
        return log_explanation(self.db_manager, session_id, entity_type, entity_name, question_type,
                              question, explanation, context_used, llm_interaction_id) 