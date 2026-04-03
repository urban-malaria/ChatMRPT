"""
Direct Tool Registration for ChatMRPT

Following py-sidebot's pattern of simple, direct tool registration.
Tools are just Python functions that the LLM can call directly.

No complex routing, no parameter extraction layers - just functions.
"""

import logging
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
import duckdb
import io
import base64
import traceback
from typing import Dict, Any, Optional
from pathlib import Path

from .unified_data_state import get_data_state

logger = logging.getLogger(__name__)


async def execute_sql_query(query: str, session_id: str) -> Dict[str, Any]:
    """
    Execute SQL query on the current dataset.
    
    The table is always named 'df'. Use DuckDB SQL syntax.
    
    Args:
        query: SQL query string (e.g., "SELECT * FROM df ORDER BY composite_score DESC LIMIT 10")
        session_id: Session identifier
        
    Returns:
        Structured result data for LLM interpretation
    """
    try:
        # Get current data
        data_state = get_data_state(session_id)
        df = data_state.current_data
        
        if df is None:
            return {
                "status": "error",
                "message": "No data loaded. Please upload your CSV data and shapefile first."
            }
        
        # Create DuckDB connection and register DataFrame
        conn = duckdb.connect(':memory:')
        conn.register('df', df)
        
        logger.info(f"Executing SQL query: {query}")
        
        # Execute query
        result_df = conn.execute(query).fetchdf()
        
        # Return structured data for LLM interpretation
        if result_df.empty:
            return {
                "status": "success",
                "row_count": 0,
                "columns": [],
                "data": [],
                "message": "No results found"
            }
        
        # Convert DataFrame to structured format
        result_data = {
            "status": "success",
            "row_count": len(result_df),
            "columns": result_df.columns.tolist(),
            "data": result_df.to_dict('records'),
            "query": query,
            "data_types": {col: str(dtype) for col, dtype in result_df.dtypes.items()}
        }
        
        # Add context about the query type
        query_lower = query.lower()
        if 'order by' in query_lower and 'composite_score' in query_lower:
            result_data["query_type"] = "ranking_by_composite"
        elif 'order by' in query_lower and 'pca_score' in query_lower:
            result_data["query_type"] = "ranking_by_pca"
        elif 'where' in query_lower and 'wardname' in query_lower:
            result_data["query_type"] = "specific_ward"
        elif 'group by' in query_lower:
            result_data["query_type"] = "aggregation"
        else:
            result_data["query_type"] = "general"
        
        return result_data
            
    except Exception as e:
        logger.error(f"SQL query error: {e}")
        
        return {
            "status": "error",
            "error": str(e),
            "query": query,
            "error_type": "syntax" if "syntax" in str(e).lower() else "execution"
        }


async def execute_python_code(code: str, session_id: str) -> Dict[str, Any]:
    """
    Execute Python code for data analysis.
    
    Available variables:
    - df: The current dataframe
    - pd: pandas
    - np: numpy
    - plt: matplotlib.pyplot
    - sns: seaborn
    
    Args:
        code: Python code to execute
        session_id: Session identifier
        
    Returns:
        Structured execution results for LLM interpretation
    """
    try:
        # Get current data
        data_state = get_data_state(session_id)
        df = data_state.current_data
        
        if df is None:
            return {
                "status": "error",
                "message": "No data loaded. Please upload your CSV data and shapefile first."
            }
        
        # Set up execution environment
        globals_dict = {
            'df': df,
            'pd': pd,
            'np': np,
            'plt': plt,
            'sns': sns,
            'print': print
        }
        
        # Capture output
        output_buffer = io.StringIO()
        
        # Redirect stdout
        import sys
        old_stdout = sys.stdout
        sys.stdout = output_buffer
        
        try:
            # Execute code
            exec(code, globals_dict)
            
            # Get output
            output = output_buffer.getvalue()
            
            # Check for plots
            plot_data = None
            if plt.get_fignums():
                # Save plot to base64
                fig_buffer = io.BytesIO()
                plt.savefig(fig_buffer, format='png', dpi=100, bbox_inches='tight')
                fig_buffer.seek(0)
                plot_data = base64.b64encode(fig_buffer.read()).decode()
                plt.close('all')
            
            # Parse output to extract structured data if possible
            parsed_results = None
            if output:
                # Try to detect common patterns in output
                lines = output.strip().split('\n')
                if 'shape:' in output.lower() or 'rows' in output.lower():
                    # Dataset shape info
                    parsed_results = {"type": "shape_info", "raw_output": output}
                elif any(term in output.lower() for term in ['mean', 'std', 'min', 'max', 'count']):
                    # Statistical summary
                    parsed_results = {"type": "statistics", "raw_output": output}
                elif 'correlation' in output.lower():
                    # Correlation analysis
                    parsed_results = {"type": "correlation", "raw_output": output}
            
            return {
                "status": "success",
                "output": output,
                "parsed_results": parsed_results,
                "has_plot": plot_data is not None,
                "plot_data": plot_data,
                "code": code
            }
            
        finally:
            # Restore stdout
            sys.stdout = old_stdout
            
    except Exception as e:
        logger.error(f"Python execution error: {e}")
        return {
            "status": "error",
            "error": str(e),
            "traceback": traceback.format_exc(),
            "code": code
        }


async def create_vulnerability_map(session_id: str, method: str = 'composite') -> Dict[str, Any]:
    """
    Create vulnerability map visualization.
    
    Args:
        session_id: Session identifier
        method: Analysis method ('composite' or 'pca')
        
    Returns:
        Visualization result with file path and status
    """
    try:
        # Get visualization service
        from flask import current_app
        viz_service = current_app.services.get('visualization_service')
        
        if not viz_service:
            return {
                'response': "Visualization service not available",
                'status': 'error'
            }
        
        # Call the visualization method
        result = viz_service.create_vulnerability_map(session_id, method=method)
        
        # Format response
        if result.get('status') == 'success' and result.get('web_path'):
            return {
                'response': f"Created {method} vulnerability map",
                'visualizations': [{
                    'type': 'vulnerability_map',
                    'path': result['web_path'],
                    'title': f"Vulnerability Map ({method.title()} Method)"
                }],
                'status': 'success'
            }
        else:
            return {
                'response': result.get('message', 'Failed to create map'),
                'status': 'error'
            }
            
    except Exception as e:
        logger.error(f"Error creating vulnerability map: {e}")
        return {
            'response': f"Error creating map: {str(e)}",
            'status': 'error'
        }


async def check_data_status(session_id: str) -> str:
    """
    Check current data status for the session.
    
    Args:
        session_id: Session identifier
        
    Returns:
        Status description
    """
    data_state = get_data_state(session_id)
    info = data_state.get_data_info()
    
    stage = info['stage']
    
    if stage == 'no_data':
        return "No data uploaded yet. Please upload your CSV data and shapefile to begin."
    
    elif stage == 'pre_analysis':
        return f"""Data loaded successfully!
- Shape: {info['shape'][0]} rows × {info['shape'][1]} columns
- Ready for analysis

Would you like me to run the malaria risk analysis?"""
    
    elif stage == 'post_analysis':
        analysis_cols = info.get('analysis_columns', [])
        return f"""Analysis complete! 
- Shape: {info['shape'][0]} wards analyzed
- Available results: {', '.join(analysis_cols)}

You can now:
- Query rankings (e.g., "show top 10 highest risk wards")
- Create visualizations (e.g., "create vulnerability map")
- Compare methods (e.g., "compare PCA vs composite scores")"""
    
    return "Unknown data status"


# Tool registry following py-sidebot pattern
TOOLS = {
    'execute_sql_query': execute_sql_query,
    'execute_python_code': execute_python_code,
    'create_vulnerability_map': create_vulnerability_map,
    'check_data_status': check_data_status
}


def register_tools_with_session(chat_session):
    """
    Register all tools with a chat session.
    
    Following py-sidebot's pattern of direct tool registration.
    """
    for name, func in TOOLS.items():
        chat_session.register_tool(func)
    
    logger.info(f"Registered {len(TOOLS)} tools with chat session")