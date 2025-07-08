# app/interaction/storage.py
"""
Data Persistence and Retrieval Module

This module contains all data storage and retrieval functionality extracted 
from the monolithic InteractionLogger class. It handles querying, exporting,
and retrieving comprehensive data from the interaction database.

Functions:
- Session history retrieval
- Analysis metadata aggregation
- Visualization context retrieval
- Ward data comprehensive retrieval
- Data export in multiple formats
- Query optimization utilities
"""

import os
import json
import logging
import datetime
import sqlite3
from typing import Dict, List, Any, Optional, Union

# Local imports
from .core import DatabaseManager

# Set up logging
logger = logging.getLogger(__name__)


def get_session_history(db_manager: DatabaseManager, session_id):
    """
    Get complete conversation history for a session
    
    Args:
        db_manager: DatabaseManager instance
        session_id: Session identifier
        
    Returns:
        list: List of message objects in chronological order
    """
    try:
        conn = db_manager.get_connection()
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


def get_analysis_metadata(db_manager: DatabaseManager, session_id):
    """
    Get complete analysis metadata for a session
    
    Args:
        db_manager: DatabaseManager instance
        session_id: Session identifier
        
    Returns:
        dict: Comprehensive analysis metadata
    """
    try:
        conn = db_manager.get_connection()
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


def get_visualization_context(db_manager: DatabaseManager, session_id, viz_id=None, viz_type=None):
    """
    Get context about visualizations for a session
    
    Args:
        db_manager: DatabaseManager instance
        session_id: Session identifier
        viz_id: Optional specific visualization ID
        viz_type: Optional visualization type to filter by
        
    Returns:
        dict or list: Visualization context data
    """
    try:
        conn = db_manager.get_connection()
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


def get_explanations(db_manager: DatabaseManager, session_id, entity_type=None, entity_name=None):
    """
    Get explanations provided in a session
    
    Args:
        db_manager: DatabaseManager instance
        session_id: Session identifier
        entity_type: Optional type to filter by ('ward', 'variable', etc.)
        entity_name: Optional name to filter by
        
    Returns:
        list: Explanation records
    """
    try:
        conn = db_manager.get_connection()
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


def get_ward_data(db_manager: DatabaseManager, session_id, ward_name):
    """
    Get comprehensive data about a specific ward
    
    Args:
        db_manager: DatabaseManager instance
        session_id: Session identifier
        ward_name: Name of the ward
        
    Returns:
        dict: Comprehensive ward data
    """
    try:
        conn = db_manager.get_connection()
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


def export_to_csv(db_manager: DatabaseManager, session_id=None, start_date=None, end_date=None, output_dir=None):
    """
    Export logs to CSV files
    
    Args:
        db_manager: DatabaseManager instance
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
        
        conn = db_manager.get_connection()
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
            # Validate table name against whitelist (already done above)
            if table not in tables:
                continue
                
            # Get table columns using parameterized identifier
            # SQLite doesn't support parameterized table names, but we've validated against whitelist
            cursor.execute(f"PRAGMA table_info({table})")
            columns = [row['name'] for row in cursor.fetchall()]
            
            # Export table data - table name is from our whitelist
            if where_clause:
                query = f"SELECT * FROM {table} {where_clause}"
                cursor.execute(query, params)
            else:
                cursor.execute(f"SELECT * FROM {table}")
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


def export_to_json(db_manager: DatabaseManager, session_id=None, include_llm_data=True, compact=False, output_file=None):
    """
    Export logs to a single JSON file
    
    Args:
        db_manager: DatabaseManager instance
        session_id: Optional session to filter by
        include_llm_data: Whether to include LLM prompts and responses
        compact: Whether to generate compact JSON (without pretty printing)
        output_file: Path to write the JSON file
        
    Returns:
        dict: Status of export operation
    """
    try:
        conn = db_manager.get_connection()
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
            current_session_id = session_info['session_id']
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
            
            # Only include LLM data if requested
            if include_llm_data:
                session_data['llm_interactions'] = []
                session_data['explanations'] = []
            
            # Get all data for this session
            tables_queries = [
                ('messages', 'messages'),
                ('file_uploads', 'file_uploads'),
                ('analysis_events', 'analysis_events'),
                ('errors', 'errors'),
                ('analysis_steps', 'analysis_details.steps'),
                ('algorithm_decisions', 'analysis_details.decisions'),
                ('calculations', 'analysis_details.calculations'),
                ('anomalies', 'analysis_details.anomalies'),
                ('variable_relationships', 'analysis_details.variable_relationships'),
                ('ward_rankings', 'analysis_details.ward_rankings'),
                ('visualization_metadata', 'analysis_details.visualization_metadata')
            ]
            
            if include_llm_data:
                tables_queries.extend([
                    ('llm_interactions', 'llm_interactions'),
                    ('explanations', 'explanations')
                ])
            
            for table_name, data_path in tables_queries:
                # Table names come from our hardcoded whitelist above, so they're safe
                cursor.execute(f"SELECT * FROM {table_name} WHERE session_id = ? ORDER BY timestamp", (current_session_id,))
                
                rows = []
                for row in cursor.fetchall():
                    row_dict = dict(row)
                    
                    # Parse JSON fields
                    for key, value in row_dict.items():
                        if isinstance(value, str) and value.startswith(('{', '[')):
                            try:
                                row_dict[key] = json.loads(value)
                            except:
                                pass  # Keep as string if not valid JSON
                    
                    rows.append(row_dict)
                
                # Set data at appropriate path
                if '.' in data_path:
                    main_key, sub_key = data_path.split('.', 1)
                    session_data[main_key][sub_key] = rows
                else:
                    session_data[data_path] = rows
            
            export_data['sessions'].append(session_data)
        
        conn.close()
        
        # Generate output file path if not provided
        if output_file is None:
            timestamp = datetime.datetime.now().strftime('%Y%m%d_%H%M%S')
            if session_id:
                output_file = f"interaction_export_{session_id}_{timestamp}.json"
            else:
                output_file = f"interaction_export_{timestamp}.json"
        
        # Write JSON file
        with open(output_file, 'w', encoding='utf-8') as f:
            if compact:
                json.dump(export_data, f, separators=(',', ':'), default=str)
            else:
                json.dump(export_data, f, indent=2, default=str)
        
        return {
            'status': 'success',
            'message': f'Exported data to {output_file}',
            'file': output_file,
            'sessions_exported': len(export_data['sessions'])
        }
        
    except Exception as e:
        logger.error(f"Error exporting to JSON: {str(e)}", exc_info=True)
        return {
            'status': 'error',
            'message': f'Error exporting to JSON: {str(e)}'
        }


# Convenience class for storage management
class StorageManager:
    """
    Storage and retrieval functionality
    Provides comprehensive data access capabilities
    """
    
    def __init__(self, db_manager: DatabaseManager):
        """Initialize with database manager"""
        self.db_manager = db_manager
    
    def get_session_history(self, session_id):
        """Get session history"""
        return get_session_history(self.db_manager, session_id)
    
    def get_analysis_metadata(self, session_id):
        """Get analysis metadata"""
        return get_analysis_metadata(self.db_manager, session_id)
    
    def get_visualization_context(self, session_id, viz_id=None, viz_type=None):
        """Get visualization context"""
        return get_visualization_context(self.db_manager, session_id, viz_id, viz_type)
    
    def get_explanations(self, session_id, entity_type=None, entity_name=None):
        """Get explanations"""
        return get_explanations(self.db_manager, session_id, entity_type, entity_name)
    
    def get_ward_data(self, session_id, ward_name):
        """Get ward data"""
        return get_ward_data(self.db_manager, session_id, ward_name)
    
    def export_to_csv(self, session_id=None, start_date=None, end_date=None, output_dir=None):
        """Export to CSV"""
        return export_to_csv(self.db_manager, session_id, start_date, end_date, output_dir)
    
    def export_to_json(self, session_id=None, include_llm_data=True, compact=False, output_file=None):
        """Export to JSON"""
        return export_to_json(self.db_manager, session_id, include_llm_data, compact, output_file) 