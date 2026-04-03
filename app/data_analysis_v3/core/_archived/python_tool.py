"""
Python Analysis Tool
Based on AgenticDataAnalysis complete_python_task tool
"""

import os
import logging
import pandas as pd
from typing import Tuple, Dict, Any, Annotated
from langchain_core.tools import tool
from langgraph.prebuilt import InjectedState

from ..core.executor import SecureExecutor
from ..core.metadata_cache import MetadataCache
from ..core.lazy_loader import LazyDataLoader
from ..core.encoding_handler import EncodingHandler

logger = logging.getLogger(__name__)


def _handle_execution_error(error_msg: str, current_data: Dict[str, Any]) -> str:
    """
    Convert technical errors to user-friendly messages with column suggestions.

    Keeps error handling simple - just show what's available and suggest fixes.
    The AI handles complex interpretation via system prompt.
    """
    import re
    from difflib import get_close_matches

    # Get available columns for suggestions
    df = current_data.get('df')
    available_columns = list(df.columns) if df is not None and hasattr(df, 'columns') else []

    # Try to extract column name from error if it's a KeyError/column not found
    col_match = re.search(r"['\"]([^'\"]+)['\"]", error_msg)
    missing_col = col_match.group(1) if col_match else None

    # Suggest closest matching column if we found a column name
    suggestion = None
    if missing_col and available_columns:
        matches = get_close_matches(missing_col, available_columns, n=1, cutoff=0.6)
        suggestion = matches[0] if matches else None

    # Build simple, helpful error message
    if suggestion:
        return f"""I encountered an error: Column '{missing_col}' not found.

Did you mean **'{suggestion}'**?

Available columns: {', '.join(available_columns[:15])}{'...' if len(available_columns) > 15 else ''}"""
    elif available_columns:
        return f"""I encountered an error: {error_msg}

Available columns: {', '.join(available_columns[:15])}{'...' if len(available_columns) > 15 else ''}

Please check the column names and try again."""
    else:
        return f"""I encountered an error: {error_msg}

Please try rephrasing your question or check the data."""


@tool
def analyze_data(
    graph_state: Annotated[dict, InjectedState],
    thought: str,
    python_code: str
) -> Tuple[str, Dict[str, Any]]:
    """Execute Python code for data analysis

    Args:
        thought: Internal thought about what analysis to perform and why
        python_code: Python code to execute. Use print() to show outputs. Data is available as 'df'.
    """
    # Get session ID from graph state
    session_id = graph_state.get('session_id', 'default')
    
    executor = SecureExecutor(session_id)
    
    # Load data like the original - from input_data in graph_state
    current_data = {}
    
    # Load datasets from input_data (like original AgenticDataAnalysis)
    if graph_state and "input_data" in graph_state:
        for dataset in graph_state["input_data"]:
            var_name = dataset.get("variable_name")
            
            # CRITICAL FIX: First check if data is already loaded in the state
            if "data" in dataset and dataset["data"] is not None:
                # Use the pre-loaded DataFrame from state
                current_data[var_name] = dataset["data"]
                
                # Also make available as 'df' for convenience
                if 'df' not in current_data:
                    current_data['df'] = dataset["data"]
                
                logger.info(f"Using pre-loaded {var_name} with shape {dataset['data'].shape}")
            
            # Fallback: Try to load from file path if data not in state
            elif var_name and dataset.get("data_path"):
                data_path = dataset.get("data_path")
                if os.path.exists(data_path):
                    try:
                        if data_path.endswith('.csv'):
                            current_data[var_name] = EncodingHandler.read_csv_with_encoding(data_path)
                        elif data_path.endswith(('.xlsx', '.xls')):
                            current_data[var_name] = EncodingHandler.read_excel_with_encoding(data_path)
                        
                        # Also make available as 'df' for convenience
                        if 'df' not in current_data:
                            current_data['df'] = current_data[var_name]
                            
                        logger.info(f"Loaded {var_name} from {data_path}")
                    except Exception as e:
                        logger.error(f"Error loading {var_name}: {e}")
    
    # Add any existing variables from state (like original)
    if graph_state and 'current_variables' in graph_state:
        current_data.update(graph_state['current_variables'])
    
    # Log the code being executed
    logger.info(f"Executing Python code for session {session_id}:")
    logger.info(f"Code length: {len(python_code)} chars")  # Don't truncate
    logger.info(f"Available data variables: {list(current_data.keys())}")
    if 'df' in current_data:
        logger.info(f"DataFrame shape: {current_data['df'].shape}")
        logger.info(f"DataFrame has {len(current_data['df'].columns)} columns")  # Don't truncate
    
    # Execute the code
    output, state_updates = executor.execute(python_code, current_data)

    # Log execution results
    logger.info(f"Execution completed for session {session_id}")
    logger.info(f"Output length: {len(output)} chars")
    # Don't truncate output in logs - it affects what LLM sees

    # Check for errors and convert to user-friendly messages
    if state_updates.get('errors'):
        error_msg = state_updates['errors'][0] if state_updates['errors'] else "Unknown error"
        logger.error(f"Execution errors: {error_msg}")

        # Convert technical error to user-friendly message
        friendly_error = _handle_execution_error(error_msg, current_data)

        # Return the friendly error message
        return friendly_error, state_updates
    
    if state_updates.get('output_plots'):
        logger.info(f"Generated {len(state_updates['output_plots'])} visualizations")
    
    # CRITICAL: Update the graph state with visualizations and variables
    # This is how the agent will know about generated plots
    if graph_state is not None:
        # Update current variables in the state
        if 'current_variables' in state_updates:
            graph_state['current_variables'] = state_updates['current_variables']
        
        # Add new plots to the state's output_plots list
        if 'output_plots' in state_updates and state_updates['output_plots']:
            if 'output_plots' not in graph_state:
                graph_state['output_plots'] = []
            graph_state['output_plots'].extend(state_updates['output_plots'])
            logger.info(f"Added {len(state_updates['output_plots'])} plots to graph state")
    
    # Format output using new ResponseFormatter
    from ..core.formatters import ResponseFormatter
    # Apply normalization to fix bullet points and spacing
    formatted_output = ResponseFormatter.normalize_spacing(output)
    
    # Validate output to prevent hallucinations and impossible values
    from ..core.data_validator import DataValidator
    
    # Build DYNAMIC context for validation - works with ANY dataset
    validation_context = {}
    if 'df' in current_data and hasattr(current_data['df'], 'columns'):
        df = current_data['df']
        
        # DYNAMIC: Find ALL string/object columns that might contain entity names
        # This works for ANY dataset - facilities, companies, products, locations, etc.
        for col in df.columns:
            if df[col].dtype == 'object':  # String columns
                unique_values = df[col].dropna().unique()
                # If column has reasonable number of unique values, it might be entity names
                if 2 <= len(unique_values) <= 1000:  # Reasonable range for entities
                    # Store all unique values for validation
                    if 'entity_names' not in validation_context:
                        validation_context['entity_names'] = []
                    validation_context['entity_names'].extend(unique_values.tolist())
                    logger.debug(f"Added {len(unique_values)} unique values from column '{col}' for validation")
        
        # DYNAMIC: Store numeric column ranges for validation
        validation_context['numeric_ranges'] = {}
        for col in df.columns:
            if df[col].dtype in ['int64', 'float64']:
                validation_context['numeric_ranges'][col] = {
                    'min': df[col].min(),
                    'max': df[col].max()
                }
    
    # Validate the output
    is_valid, issues = DataValidator.validate_output(formatted_output, validation_context)
    
    if not is_valid:
        logger.warning(f"Output validation failed: {issues}")
        # Sanitize the output to remove hallucinations
        formatted_output = DataValidator.sanitize_output(formatted_output, validation_context)

        # Don't provide fallback - return the formatted output even with issues
        # The health officials need to see the actual data, not error messages
        # if len(issues) > 2:
        #     logger.error(f"Too many validation issues ({len(issues)}), providing fallback response")
        #     return "I encountered issues generating accurate results. Let me try a different approach. Please ensure I'm working with the correct data columns."

    # Return tuple matching AgenticDataAnalysis pattern (output, state_updates)
    return formatted_output, state_updates


def get_data_summary(session_id: str) -> str:
    """
    Get a summary of available data for the session.
    Uses cached metadata for instant response, avoiding loading large files.
    
    Args:
        session_id: User session ID
        
    Returns:
        String description of available data
    """
    # First, try to get summary from cache (instant!)
    cached_summary = MetadataCache.get_summary_from_cache(session_id)
    if cached_summary:
        logger.info(f"Using cached metadata for session {session_id}")
        return cached_summary
    
    # Fallback: No cache exists, generate metadata on the fly (but with sampling)
    logger.info(f"No metadata cache found for session {session_id}, generating...")
    
    data_dir = f"instance/uploads/{session_id}"
    if not os.path.exists(data_dir):
        return "No data files uploaded yet."
    
    files = os.listdir(data_dir)
    # Filter for data files, excluding metadata cache
    data_files = [f for f in files if f.endswith(('.csv', '.xlsx', '.xls', '.json')) 
                  and f != 'metadata_cache.json']
    
    if not data_files:
        return "No data files uploaded yet."
    
    # Generate metadata for all files and cache it
    cache = {'files': {}}
    summary_parts = ["Available data:"]
    
    for filename in data_files:
        filepath = os.path.join(data_dir, filename)
        try:
            # Extract metadata (with sampling for large files)
            metadata = MetadataCache.extract_file_metadata(filepath, filename)
            cache['files'][filename] = metadata
            
            # Format summary
            if metadata.get('type') in ['csv', 'excel']:
                rows = metadata.get('rows', 'Unknown')
                cols = metadata.get('columns', 'Unknown')
                
                if isinstance(rows, int):
                    if metadata.get('rows_estimated'):
                        rows_str = f"~{rows:,}"
                    else:
                        rows_str = f"{rows:,}"
                else:
                    rows_str = str(rows)
                
                var_name = filename.split('.')[0].replace(' ', '_').replace('-', '_')
                size_mb = metadata.get('file_size_mb', 0)
                size_info = f" ({size_mb:.1f}MB)" if size_mb > 10 else ""
                
                summary_parts.append(f"- {var_name}: {rows_str} rows, {cols} columns{size_info}")
                
                if metadata.get('is_sampled'):
                    summary_parts.append(f"  (Metadata from sampling)")
        except Exception as e:
            logger.error(f"Error processing {filename}: {e}")
            summary_parts.append(f"- {filename}: Error reading file")
    
    # Save the cache for next time
    from datetime import datetime
    cache['last_updated'] = datetime.now().isoformat()
    MetadataCache.save_cache(session_id, cache)
    
    return "\n".join(summary_parts)