"""
Session helper for handling DataFrame serialization with Redis
"""
import pandas as pd
import json
from flask import session

class SessionHelper:
    """Helper class for Redis-compatible session management"""
    
    @staticmethod
    def store_dataframe(key, df):
        """Store DataFrame in session as JSON"""
        if df is not None:
            session[key] = df.to_json(orient='split', date_format='iso')
        else:
            session[key] = None
    
    @staticmethod
    def load_dataframe(key):
        """Load DataFrame from session JSON"""
        if key in session and session[key]:
            return pd.read_json(session[key], orient='split')
        return None
    
    @staticmethod
    def store_data(key, data):
        """Store any JSON-serializable data"""
        session[key] = json.dumps(data) if data is not None else None
    
    @staticmethod
    def load_data(key, default=None):
        """Load JSON data from session"""
        if key in session and session[key]:
            return json.loads(session[key])
        return default
    
    @staticmethod
    def clear_analysis_data():
        """Clear analysis-related session data"""
        keys_to_clear = [
            'nmep_data', 'raw_data', 'analysis_results',
            'viz_data', 'current_analysis', 'tpr_data'
        ]
        for key in keys_to_clear:
            session.pop(key, None)
